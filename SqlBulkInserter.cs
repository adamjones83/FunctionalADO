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

	public static class VirtualReaderExt
	{
		// Creates a VirtualReader wrapper around any IEnumerable<T> (even anonymous types)
		public static VirtualReader<T> ToDataReader<T>(this IEnumerable<T> items)
		{
			return new VirtualReader<T>(items);
		}
	}
	/// <summary>
	/// Wraps an IEnumerable to create an IDataReader with fields from the public properties of the enumerable type
	/// </summary>
	public class VirtualReader<T> : IDataReader
	{
		#region Private Member Variables
		IEnumerator<T> _etor; // Once initialized by calling GetEnumerator on _items, this allows you to read.  Can't add/edit fields when this is initialized
		string[] fieldNames;
		Func<T, object>[] fieldAccessors;
		Type[] fieldTypes; // cache the types of the fields so they don't have to be determined by reflection from the accessor methods
		Dictionary<string, int> colNumsByName;
		Dictionary<Func<T, bool>, List<T>> constraints = new Dictionary<Func<T, bool>, List<T>>();
		HashSet<Type> supported = new HashSet<Type>(GetSupportedTypes());
		#endregion

		#region Constructors
		public VirtualReader(IEnumerable<T> items)
		{
			var pInfo = typeof(T).GetProperties()
				.Where(p => supported.Contains(p.PropertyType))
				.ToArray();
			fieldNames = pInfo.Select(p => p.Name).ToArray();
			fieldAccessors = pInfo.Select(p => GetAccessor(p)).ToArray();
			fieldTypes = pInfo.Select(p => p.PropertyType).ToArray();
			colNumsByName = Enumerable.Range(0, fieldNames.Length)
				.ToDictionary(i => fieldNames[i]);

			_etor = items.GetEnumerator();
		}
		public VirtualReader(IEnumerable<T> items, IEnumerable<string> fields)
		{
			var fieldSet = new HashSet<string>(fields);
			var pInfoByName = typeof(T).GetProperties()
				.Where(p => supported.Contains(p.PropertyType) && fieldSet.Contains(p.Name))
				.ToDictionary(p => p.Name, p => p);

			// Check for fields that map to properties that don't exist
			var missing = fieldSet.Where(s => !pInfoByName.ContainsKey(s));
			if (missing.Any()) throw new Exception(string.Format("The following fields are mapped to property(s) that do not exist on type '{0}': {1}", typeof(T).Name, string.Join(", ", missing)));
			// Check for fields that map to properties of an unsupported type
			var unsupported = fieldSet.Where(s => !supported.Contains(pInfoByName[s].PropertyType));
			if (unsupported.Any()) throw new Exception(string.Format("The following field accessors can not be automatically generated because the property(s) are of an unsupported type: {0}", string.Join(", ", unsupported)));

			fieldNames = pInfoByName.Select(p => p.Key).ToArray();
			fieldAccessors = pInfoByName.Select(p => GetAccessor(p.Value)).ToArray();
			fieldTypes = pInfoByName.Select(p => p.Value.PropertyType).ToArray();
			colNumsByName = Enumerable.Range(0, fieldNames.Length)
				.ToDictionary(i => fieldNames[i], i => i);

			_etor = items.GetEnumerator();
		}
		#endregion

		#region Private Methods - These are in support of creating the virtual reader
		private static Type[] GetSupportedTypes()
		{
			var valueTypes = new Type[] { 
				typeof(sbyte), typeof(byte), typeof(char), typeof(short), 
				typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(ulong), 
				typeof(float), typeof(double), typeof(decimal),
				typeof(Guid), typeof(DateTime), typeof(bool) };
			var nullable = valueTypes.Select(t => typeof(Nullable<>).MakeGenericType(t)).ToArray();
			return valueTypes
				.Union(nullable)
				.Union(new Type[] { typeof(string) }).ToArray();
		}
		private Func<T, object> GetAccessor(PropertyInfo pi)
		{
			Func<T, object> accessor;
			if (pi.GetGetMethod().ReturnType.IsValueType)
			{
				// Have to dynamically call the generic version of this method appropriate to the 
				// value type returned by the property (so that it gets boxed properly)
				MethodInfo mi = this.GetType()
					.GetMethods(BindingFlags.NonPublic | BindingFlags.Instance)
						.Where(m => m.Name == "GetBoxingAccessor" && (m.IsGenericMethod || m.IsGenericMethodDefinition))
						.First();
				MethodInfo delegateCreator = mi.MakeGenericMethod(pi.PropertyType);
				accessor = (Func<T, object>)delegateCreator.Invoke(this, new object[] { pi });
			}
			else
			{
				// If it is already an object type you can just return the get accessor
				accessor = (Func<T, object>)Delegate.CreateDelegate(typeof(Func<T, object>), pi.GetGetMethod());
			}
			return accessor;
		}
		private Func<T, object> GetBoxingAccessor<Tout>(PropertyInfo pi)
		{
			Func<T, Tout> tmp = (Func<T, Tout>)Delegate.CreateDelegate(typeof(Func<T, Tout>), pi.GetGetMethod());
			return (T t) => (object)tmp(t);
		}
		/// <summary>
		/// Casts the provided accessor to a generic object accessor (boxing if necessary)
		/// </summary>
		/// <typeparam name="Tout">The current return type of the accessor function</typeparam>
		/// <param name="accessor">The accessor function to cast</param>
		/// <returns>A generic object accessor</returns>
		private Func<T, object> CastAccessor<Tout>(Func<T, Tout> accessor)
		{
			if (typeof(Tout).IsValueType) return (T t) => (object)accessor(t);

			// Use reflection to cast since as a generic we can't guarantee to the compiler that Tout is a reference type (it is since value types are handled above, but what does the compiler know? eh?)
			MethodInfo mi = accessor.Method;
			if (mi.IsStatic) return (Func<T, object>)Delegate.CreateDelegate(typeof(Func<T, object>), mi); // if a static method, create a static method
			return (Func<T, object>)Delegate.CreateDelegate(typeof(Func<T, object>), accessor.Target, mi); // if an instance method, create an instance method
		}
		#endregion

		#region Public Methods
		public Dictionary<string, Func<T, object>> GetFieldAccessors()
		{
			return Enumerable.Range(0, fieldNames.Length).ToDictionary(i => fieldNames[i], i => fieldAccessors[i]);
		}
		// Allows you to add another virtual field, or to override an existing field with a new accessor method
		public void SetVirtualField<Tout>(string fieldName, Func<T, Tout> accessor)
		{
			if (fieldName == null) throw new ArgumentNullException("Field name can not be null");
			if (accessor == null) throw new ArgumentNullException("Accessor function can not be null");

			var fieldsByName = Enumerable.Range(0, fieldNames.Length)
				.ToDictionary(i => fieldNames[i], i => new { Accessor = fieldAccessors[i], Type = fieldTypes[i] });
			fieldsByName[fieldName] = new { Accessor = CastAccessor(accessor), Type = typeof(Tout) };

			var fieldList = fieldsByName.Keys;
			fieldNames = fieldList.ToArray();
			fieldAccessors = fieldList.Select(f => fieldsByName[f].Accessor).ToArray();
			fieldTypes = fieldList.Select(f => fieldsByName[f].Type).ToArray();
			colNumsByName = Enumerable.Range(0, fieldNames.Length)
				.ToDictionary(i => fieldNames[i]);
		}
		public Func<T, object> GetFieldAccessor(int index)
		{
			if (index > fieldAccessors.Length) throw new IndexOutOfRangeException("The field index was greater than the number of fields on the virtual reader.");
			return fieldAccessors[index];
		}
		public Func<T, object> GetFieldAccessor(string fieldName)
		{
			if (!colNumsByName.ContainsKey(fieldName)) throw new KeyNotFoundException("The field name provided does not match a field on the current virtual reader.");
			return fieldAccessors[colNumsByName[fieldName]];
		}
		#endregion

		#region IDataReader Error Messages
		Func<string, string, string> nullMessage = (method, field) => string.Format("Attempted to call '{0}' on the '{1}' field which has a null value.", method, field);
		Func<string, string, string, string> wrongTypeMessage = (method, field, type) => string.Format("Can't call '{0}' on field '{1}' which is of type '{2}'", method, field, type);
		#endregion

		#region IDataReader Implementation
		public void Close()
		{
			_etor = null;
		}
		public int Depth
		{
			get { return 0; }
		}
		public bool IsClosed
		{
			get { return _etor == null; }
		}
		public bool NextResult()
		{
			return false;
		}
		public bool Read()
		{
			/*
			 * 1. Check all constraints on the current item
			 *		* If all pass, return true
			 *		* If any fail, add to excluded items and check the next item
			 * 2. If there are no more items, return false
			 */
			while (_etor.MoveNext())
			{
				bool tmp = true;
				foreach (var item in constraints)
				{
					if (!item.Key(_etor.Current))
					{
						tmp = false;
						if (item.Value != null) item.Value.Add(_etor.Current);
					}
				}
				if (tmp) return true;
			}
			return false;
		}
		public int RecordsAffected
		{
			get { return 0; }
		}
		public void Dispose()
		{
			_etor = null;
		}
		public int FieldCount
		{
			get { return fieldNames.Length; }
		}
		public bool GetBoolean(int i)
		{
			if (fieldTypes[i] == typeof(bool))
				return (bool)fieldAccessors[i](_etor.Current);
			if (fieldTypes[i] == typeof(bool?))
			{
				bool? val = (bool?)fieldAccessors[i](_etor.Current);
				if (val == null) throw new NullReferenceException(nullMessage("GetBoolean", fieldNames[i]));
				return val.Value;
			}
			throw new ArgumentException(wrongTypeMessage("GetBoolean", fieldNames[i], fieldTypes[i].Name));
		}
		public byte GetByte(int i)
		{
			if (fieldTypes[i] == typeof(byte))
				return (byte)fieldAccessors[i](_etor.Current);
			if (fieldTypes[i] == typeof(byte?))
			{
				byte? val = (byte?)fieldAccessors[i](_etor.Current);
				if (val == null) throw new NullReferenceException(nullMessage("GetByte", fieldNames[i]));
				return val.Value;
			}
			throw new ArgumentException(wrongTypeMessage("GetByte", fieldNames[i], fieldTypes[i].Name));
		}
		public char GetChar(int i)
		{
			if (fieldTypes[i] == typeof(char))
				return (char)fieldAccessors[i](_etor.Current);
			if (fieldTypes[i] == typeof(char?))
			{
				char? val = (char?)fieldAccessors[i](_etor.Current);
				if (val == null) throw new NullReferenceException(nullMessage("GetChar", fieldNames[i]));
				return val.Value;
			}
			throw new ArgumentException(wrongTypeMessage("GetChar", fieldNames[i], fieldTypes[i].Name));
		}
		public DateTime GetDateTime(int i)
		{
			if (fieldTypes[i] == typeof(DateTime))
				return (DateTime)fieldAccessors[i](_etor.Current);
			if (fieldTypes[i] == typeof(DateTime?))
			{
				DateTime? val = (DateTime?)fieldAccessors[i](_etor.Current);
				if (val == null) throw new NullReferenceException(nullMessage("GetDateTime", fieldNames[i]));
				return val.Value;
			}
			throw new ArgumentException(wrongTypeMessage("GetDateTime", fieldNames[i], fieldTypes[i].Name));
		}
		public decimal GetDecimal(int i)
		{
			if (fieldTypes[i] == typeof(Decimal))
				return (Decimal)fieldAccessors[i](_etor.Current);
			if (fieldTypes[i] == typeof(Decimal?))
			{
				Decimal? val = (Decimal?)fieldAccessors[i](_etor.Current);
				if (val == null) throw new NullReferenceException(nullMessage("GetDecimal", fieldNames[i]));
				return val.Value;
			}
			throw new ArgumentException(wrongTypeMessage("GetDecimal", fieldNames[i], fieldTypes[i].Name));
		}
		public double GetDouble(int i)
		{
			if (fieldTypes[i] == typeof(double))
				return (double)fieldAccessors[i](_etor.Current);
			if (fieldTypes[i] == typeof(double?))
			{
				double? val = (double?)fieldAccessors[i](_etor.Current);
				if (val == null) throw new NullReferenceException(nullMessage("GetDouble", fieldNames[i]));
				return val.Value;
			}
			throw new ArgumentException(wrongTypeMessage("GetDouble", fieldNames[i], fieldTypes[i].Name));
		}
		public float GetFloat(int i)
		{
			if (fieldTypes[i] == typeof(float))
				return (float)fieldAccessors[i](_etor.Current);
			if (fieldTypes[i] == typeof(float?))
			{
				float? val = (float?)fieldAccessors[i](_etor.Current);
				if (val == null) throw new NullReferenceException(nullMessage("GetFloat", fieldNames[i]));
				return val.Value;
			}
			throw new ArgumentException(wrongTypeMessage("GetFloat", fieldNames[i], fieldTypes[i].Name));
		}
		public Guid GetGuid(int i)
		{
			if (fieldTypes[i] == typeof(Guid))
				return (Guid)fieldAccessors[i](_etor.Current);
			if (fieldTypes[i] == typeof(Guid?))
			{
				Guid? val = (Guid?)fieldAccessors[i](_etor.Current);
				if (val == null) throw new NullReferenceException(nullMessage("GetGuid", fieldNames[i]));
				return val.Value;
			}
			throw new ArgumentException(wrongTypeMessage("GetGuid", fieldNames[i], fieldTypes[i].Name));
		}
		public short GetInt16(int i)
		{
			if (fieldTypes[i] == typeof(short))
				return (short)fieldAccessors[i](_etor.Current);
			if (fieldTypes[i] == typeof(short?))
			{
				short? val = (short?)fieldAccessors[i](_etor.Current);
				if (val == null) throw new NullReferenceException(nullMessage("GetInt16", fieldNames[i]));
				return val.Value;
			}
			throw new ArgumentException(wrongTypeMessage("GetInt16", fieldNames[i], fieldTypes[i].Name));
		}
		public int GetInt32(int i)
		{
			if (fieldTypes[i] == typeof(int))
				return (int)fieldAccessors[i](_etor.Current);
			if (fieldTypes[i] == typeof(int?))
			{
				int? val = (int?)fieldAccessors[i](_etor.Current);
				if (val == null) throw new NullReferenceException(nullMessage("GetInt32", fieldNames[i]));
				return val.Value;
			}
			throw new ArgumentException(wrongTypeMessage("GetInt32", fieldNames[i], fieldTypes[i].Name));
		}
		public long GetInt64(int i)
		{
			if (fieldTypes[i] == typeof(long))
				return (long)fieldAccessors[i](_etor.Current);
			if (fieldTypes[i] == typeof(long?))
			{
				long? val = (long?)fieldAccessors[i](_etor.Current);
				if (val == null) throw new NullReferenceException(nullMessage("GetInt64", fieldNames[i]));
				return val.Value;
			}
			throw new ArgumentException(wrongTypeMessage("GetInt64", fieldNames[i], fieldTypes[i].Name));
		}
		public string GetName(int i)
		{
			if (i >= fieldNames.Length)
				throw new IndexOutOfRangeException(string.Format("Attempted to call GetName for ordinal '{0}' but there are only {1} fields.", i, fieldNames.Length));
			return fieldNames[i];
		}
		public int GetOrdinal(string name)
		{
			if (name == null) throw new ArgumentNullException("Can't call GetOrdinal with a null field name.");
			if (!colNumsByName.ContainsKey(name)) throw new IndexOutOfRangeException(string.Format("Attempted to call GetOrdinal for non-existant field '{0}'.", name));
			return colNumsByName[name];
		}
		public string GetString(int i)
		{
			if (fieldTypes[i] == typeof(string))
			{
				var val = (string)fieldAccessors[i](_etor.Current);
				if (val == null) throw new NullReferenceException(nullMessage("GetString", fieldNames[i]));
				return val;
			}
			throw new ArgumentException(wrongTypeMessage("GetString", fieldNames[i], fieldTypes[i].Name));
		}
		public object GetValue(int i)
		{
			return fieldAccessors[i](_etor.Current);
		}
		public object this[string name]
		{
			get { return fieldAccessors[colNumsByName[name]](_etor.Current); }
		}
		public object this[int i]
		{
			get { return fieldAccessors[i](_etor.Current); }
		}
		public string GetDataTypeName(int i)
		{
			return fieldTypes[i].Name;
		}
		public Type GetFieldType(int i)
		{
			return fieldTypes[i];
		}
		public bool IsDBNull(int i)
		{
			if (fieldTypes[i].IsValueType) return false; // if a value type, return false right away
			return fieldAccessors[i](_etor.Current) == null; // otherwise check for null
		}
		public DataTable GetSchemaTable()
		{
			// THIS IS KINDA JANKITY - just sayin'... might not even be necessary, but if there is something that uses this I wouldn't rely on it too much, but I did make some of the data in the schema table good :P

			// modified this code from the fast CSV Reader from code project
			// Copyright (c) 2005 Sébastien Lorion - used here in compliance with original license
			DataTable schema = new DataTable("SchemaTable");
			schema.Locale = CultureInfo.InvariantCulture;
			schema.MinimumCapacity = fieldAccessors.Length;

			schema.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool)).ReadOnly = true;
			schema.Columns.Add(SchemaTableColumn.BaseColumnName, typeof(string)).ReadOnly = true;
			schema.Columns.Add(SchemaTableColumn.BaseSchemaName, typeof(string)).ReadOnly = true;
			schema.Columns.Add(SchemaTableColumn.BaseTableName, typeof(string)).ReadOnly = true;
			schema.Columns.Add(SchemaTableColumn.ColumnName, typeof(string)).ReadOnly = true;
			schema.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int)).ReadOnly = true;
			schema.Columns.Add(SchemaTableColumn.ColumnSize, typeof(int)).ReadOnly = true;
			schema.Columns.Add(SchemaTableColumn.DataType, typeof(object)).ReadOnly = true;
			schema.Columns.Add(SchemaTableColumn.IsAliased, typeof(bool)).ReadOnly = true;
			schema.Columns.Add(SchemaTableColumn.IsExpression, typeof(bool)).ReadOnly = true;
			schema.Columns.Add(SchemaTableColumn.IsKey, typeof(bool)).ReadOnly = true;
			schema.Columns.Add(SchemaTableColumn.IsLong, typeof(bool)).ReadOnly = true;
			schema.Columns.Add(SchemaTableColumn.IsUnique, typeof(bool)).ReadOnly = true;
			schema.Columns.Add(SchemaTableColumn.NumericPrecision, typeof(short)).ReadOnly = true;
			schema.Columns.Add(SchemaTableColumn.NumericScale, typeof(short)).ReadOnly = true;
			schema.Columns.Add(SchemaTableColumn.ProviderType, typeof(int)).ReadOnly = true;

			schema.Columns.Add(SchemaTableOptionalColumn.BaseCatalogName, typeof(string)).ReadOnly = true;
			schema.Columns.Add(SchemaTableOptionalColumn.BaseServerName, typeof(string)).ReadOnly = true;
			schema.Columns.Add(SchemaTableOptionalColumn.IsAutoIncrement, typeof(bool)).ReadOnly = true;
			schema.Columns.Add(SchemaTableOptionalColumn.IsHidden, typeof(bool)).ReadOnly = true;
			schema.Columns.Add(SchemaTableOptionalColumn.IsReadOnly, typeof(bool)).ReadOnly = true;
			schema.Columns.Add(SchemaTableOptionalColumn.IsRowVersion, typeof(bool)).ReadOnly = true;

			// null marks columns that will change for each row
			object[] schemaRow = new object[] { 
					true,					// 00- AllowDBNull
					null,					// 01- BaseColumnName
					string.Empty,			// 02- BaseSchemaName
					string.Empty,			// 03- BaseTableName
					null,					// 04- ColumnName
					null,					// 05- ColumnOrdinal
					int.MaxValue,			// 06- ColumnSize
					null,			        // 07- DataType
					false,					// 08- IsAliased
					false,					// 09- IsExpression
					false,					// 10- IsKey
					false,					// 11- IsLong
					false,					// 12- IsUnique
					DBNull.Value,			// 13- NumericPrecision
					DBNull.Value,			// 14- NumericScale
					(int) DbType.String,	    // 15- ProviderType
					string.Empty,			// 16- BaseCatalogName
					string.Empty,			// 17- BaseServerName
					false,					// 18- IsAutoIncrement
					false,					// 19- IsHidden
					true,					// 20- IsReadOnly
					false					// 21- IsRowVersion
				};

			Type fieldType;
			for (int i = 0; i < fieldAccessors.Length; i++)
			{
				fieldType = fieldTypes[i];
				schemaRow[0] = fieldType.IsClass; // Set AllowDBNull to true if the type of the field is a class (ie it can be null)
				schemaRow[1] = fieldNames[i]; // Base column name
				schemaRow[4] = fieldNames[i]; // Column name
				schemaRow[5] = i; // Column ordinal
				schemaRow[7] = fieldType;
				schema.Rows.Add(schemaRow);
			}

			return schema;
		}
		#endregion

		#region IDataReader - Not Implemented
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
		{
			throw new NotImplementedException();
		}
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
		{
			throw new NotImplementedException();
		}
		public IDataReader GetData(int i)
		{
			throw new NotImplementedException();
		}
		public int GetValues(object[] values)
		{
			throw new NotImplementedException();
		}
		#endregion
	}

	public static class SqlBulkInserter
	{
		public static void BulkInsert<T>(IEnumerable<T> items, string cnxName, string tableName, Action<long> progress)
		{
			var reader = items.ToDataReader();
			int batchSize = 1000;
			AdoExt.UseConnection(cnxName, conn =>
			{
				var bulk = new SqlBulkCopy(conn);
				bulk.BatchSize = batchSize;
				bulk.DestinationTableName = tableName;
				bulk.NotifyAfter = batchSize * 10;
				bulk.SqlRowsCopied += (s, e) => progress(e.RowsCopied);
				bulk.WriteToServer(reader);
			});
		}
	}
}
