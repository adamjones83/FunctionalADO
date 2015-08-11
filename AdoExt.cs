using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Configuration;
using System.Reflection;
using System.Data;
using System.Data.Common;
using System.Globalization;

namespace FunctionalADO
{
    public static class AdoExt
    {
        public static void UseConnection(string cnxName, Action<SqlConnection> action)
        {
            var tmp = ConfigurationManager.ConnectionStrings[cnxName];
            if (tmp == null || string.IsNullOrEmpty(tmp.ConnectionString))
                throw new Exception("Unable to connect to the database.");

			using (var conn = new SqlConnection(tmp.ConnectionString))
			{
				conn.Open();
				action(conn);
				conn.Close();
			}
        }
        public static void UseCommand(string cnxName, Action<SqlCommand> action)
        {
			UseConnection(cnxName, conn => action(conn.CreateCommand()));
        }
        public static T UseCommand<T>(string cnxName, Func<SqlCommand, T> func)
        {
			T result = default(T);
			UseConnection(cnxName, conn =>
			{
				var cmd = conn.CreateCommand();
				result = func(cmd);
			});
			return result;
        }

        public static void AddParameters(this SqlCommand cmd, params SqlParameter[] parameters)
        {
            cmd.Parameters.AddRange(parameters);
        }
        
        public static SqlDataReader ExecuteReader(this SqlCommand cmd, string query)
        {
            cmd.CommandText = query;
            cmd.CommandType = System.Data.CommandType.Text;
            return cmd.ExecuteReader();
        }
        public static SafeReader AsSafeReader(this SqlDataReader reader)
        {
            return new SafeReader(reader);
        }
        public static List<T> Select<T>(this SqlDataReader reader, Func<SqlDataReader,T> selector)
        {
            List<T> result = new List<T>();
            while(reader.Read())
                result.Add(selector(reader));
            reader.Close();
            return result;
        }

        public static void BatchInsert<T>(this IEnumerable<T> items, string cnxName, string tableName)
        {

        }
        public static IEnumerable<T[]> GetBatches<T>(this IEnumerable<T> items, int batchSize = 1000)
        {
            T[] batch = new T[batchSize];
            int current = 0;
            var etor = items.GetEnumerator();
            bool done = false;
            while(!done)
            {
                for (current = 0; current < batchSize && !(done = !etor.MoveNext()); current++)
                    batch[current] = etor.Current;
                if (current == batchSize)
                {
                    yield return batch;
                    batch = new T[batchSize];
                }
                else
                {
                    if (current == 0) yield break;
                    T[] tmp = new T[current];
                    Array.Copy(batch, tmp, current);
                    yield return tmp;
                }
            }       
        }
    }

    /// <summary>
    /// A wrapper class for SqlDataReader
    /// * the familiar 'Get' methods can be used directly against field names
    /// * if a referenced field isn't present, a meaningful exception is thrown
    /// * nullable 'Get' methods simplify the reading of nullable fields
    /// * reasonable performance for most normal sized query results
    /// </summary>
    public class SafeReader
    {
        SqlDataReader _reader;
        public SafeReader(SqlDataReader reader)
        {
            _reader = reader;
        }

        public List<T> Select<T>(Func<SafeReaderRow, T> selector)
        {
            List<T> result = new List<T>();
            try
            {
                if (!_reader.Read())
                    return result;
                var columns = Enumerable.Range(0, _reader.FieldCount).ToDictionary(_reader.GetName, i => i, StringComparer.CurrentCultureIgnoreCase);
                do
                    result.Add(selector(new SafeReaderRow(_reader, columns)));
                while (_reader.Read());
                _reader.Close();
            }
            finally { _reader.Dispose(); }
            
            return result;
        }
        
        public class SafeReaderRow
        {
            private SqlDataReader _reader;
            private Dictionary<string, int> _columns;
            public SafeReaderRow(SqlDataReader reader, Dictionary<string,int> columns) 
            { 
                _reader = reader;
                _columns = columns;
            }

            private int GetColumn(string colName)
            {
                int col;
                if (!_columns.TryGetValue(colName, out col))
                    throw new Exception(string.Format("The '{0}' field wasn't present on the data reader", colName));
                return col;
            }

            public string GetString(string colName)
            {
                int col = GetColumn(colName);
                return _reader.IsDBNull(col) ? null : _reader.GetString(GetColumn(colName));
            }
            
            public bool GetBoolean(string colName)
            {
                return _reader.GetBoolean(GetColumn(colName));
            }
            public DateTime GetDateTime(string colName)
            {
                return _reader.GetDateTime(GetColumn(colName));
            }
            public short GetInt16(string colName)
            {
                return _reader.GetInt16(GetColumn(colName));
            }
            public int GetInt32(string colName)
            {
                return _reader.GetInt32(GetColumn(colName));
            }
            public long GetInt64(string colName)
            {
                return _reader.GetInt64(GetColumn(colName));
            }
            public Guid GetGuid(string colName)
            {
                return _reader.GetGuid(GetColumn(colName));
            }

            public bool? GetNullableBoolean(string colName)
            {
                int col = GetColumn(colName);
                return _reader.IsDBNull(col) ? null : (bool?)_reader.GetBoolean(col);
            }
            public DateTime? GetNullableDateTime(string colName)
            {
                int col = GetColumn(colName);
                return _reader.IsDBNull(col) ? null : (DateTime?)_reader.GetDateTime(col);
            }
            public short? GetNullableInt16(string colName)
            {
                int col = GetColumn(colName);
                return _reader.IsDBNull(col) ? null : (short?)_reader.GetInt16(col);
            }
            public int? GetNullableInt32(string colName)
            {
                int col = GetColumn(colName);
                return _reader.IsDBNull(col) ? null : (int?)_reader.GetInt32(col);
            }
            public long? GetNullableInt64(string colName)
            {
                int col = GetColumn(colName);
                return _reader.IsDBNull(col) ? null : (long?)_reader.GetInt64(col);
            }
        }
    }

}
