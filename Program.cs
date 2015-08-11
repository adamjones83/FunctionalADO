using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FunctionalADO
{
    public class Program
    {
        static void Main(string[] args)
        {
			TestQuery();

			TestQueryWithParams("ca");
			
			var start = DateTime.Now;
			TestBulkInsert(1000000);
			Console.WriteLine("Inserted 5M rows in {0}", (DateTime.Now - start).ToString("hh\\:mm\\:ss\\.ff"));

            Console.WriteLine();
            Console.WriteLine("press any key to continue...");
            while (Console.KeyAvailable)
                Console.ReadKey();
            Console.ReadKey();
        }

        public static void TestQuery()
        {
            var query = @"select ct.FirstName, ct.LastName, ad.AddressLine1, ad.AddressLine2, 
	ad.City, st.StateProvinceCode, st.CountryRegionCode, ad.PostalCode zip
	from Person.Contact ct
	join HumanResources.Employee em on em.ContactID = ct.ContactID
	join HumanResources.EmployeeAddress ea on ea.EmployeeID = em.EmployeeID
	join Person.Address ad on ad.AddressID = ea.AddressID
	join Person.StateProvince st on ad.StateProvinceID = st.StateProvinceID
	where st.CountryRegionCode = 'US'";

            var contacts = AdoExt.UseCommand("adventureworks", cmd => cmd.ExecuteReader(query)
                .AsSafeReader()
                .Select(r => new ContactInfo
                {
                    FirstName = r.GetString("firstname"),
                    LastName = r.GetString("lastname"),
                    Address1 = r.GetString("addressline1"),
                    Address2 = r.GetString("addressline2"),
                    City = r.GetString("city"),
                    State = r.GetString("stateprovincecode"),
                    Zip = r.GetString("zip")
                }));

            contacts.ForEach(Console.WriteLine);
            Console.WriteLine();
            Console.WriteLine("{0} contacts.", contacts.Count);
        }

		public static void TestQueryWithParams(string stateCode)
		{
			var query = @"select ct.FirstName, ct.LastName, ad.AddressLine1, ad.AddressLine2, 
	ad.City, st.StateProvinceCode, st.CountryRegionCode, ad.PostalCode zip
	from Person.Contact ct
	join HumanResources.Employee em on em.ContactID = ct.ContactID
	join HumanResources.EmployeeAddress ea on ea.EmployeeID = em.EmployeeID
	join Person.Address ad on ad.AddressID = ea.AddressID
	join Person.StateProvince st on ad.StateProvinceID = st.StateProvinceID
	where st.CountryRegionCode = 'US'
	and st.StateProvinceCode = @stateCode";

			var contacts = AdoExt.UseCommand("adventureworks", cmd =>
			{
				cmd.AddParameters(new SqlParameter("stateCode", stateCode));
				return cmd.ExecuteReader(query)
					.AsSafeReader()
					.Select(r => new ContactInfo
					{
						FirstName = r.GetString("firstname"),
						LastName = r.GetString("lastname"),
						Address1 = r.GetString("addressline1"),
						Address2 = r.GetString("addressline2"),
						City = r.GetString("city"),
						State = r.GetString("stateprovincecode"),
						Zip = r.GetString("zip")
					});
			});

			contacts.ForEach(Console.WriteLine);
			Console.WriteLine();
			Console.WriteLine("{0} contacts.", contacts.Count);
		}

		private static List<ContactInfo> GetSampleContactInfo()
		{
			var query = @"select ct.FirstName, ct.LastName, ad.AddressLine1, ad.AddressLine2, 
	ad.City, st.StateProvinceCode, st.CountryRegionCode, ad.PostalCode zip
	from Person.Contact ct
	join HumanResources.Employee em on em.ContactID = ct.ContactID
	join HumanResources.EmployeeAddress ea on ea.EmployeeID = em.EmployeeID
	join Person.Address ad on ad.AddressID = ea.AddressID
	join Person.StateProvince st on ad.StateProvinceID = st.StateProvinceID
	where st.CountryRegionCode = 'US'";

			return AdoExt.UseCommand("adventureworks", cmd => cmd.ExecuteReader(query)
				.AsSafeReader()
				.Select(r => new ContactInfo
				{
					FirstName = r.GetString("firstname"),
					LastName = r.GetString("lastname"),
					Address1 = r.GetString("addressline1"),
					Address2 = r.GetString("addressline2"),
					City = r.GetString("city"),
					State = r.GetString("stateprovincecode"),
					Zip = r.GetString("zip")
				}));
		}
        public static void TestBulkInsert(int testCount)
        {
			// (re)create the table
			string recreateTableCommand = @"IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Contact]') AND type in (N'U'))
DROP TABLE [dbo].[Contact]
CREATE TABLE [dbo].[Contact](
	[Id] [uniqueidentifier] NOT NULL,
	[FirstName] [nchar](50) NOT NULL,
	[LastName] [nchar](50) NOT NULL,
	[Address1] [nchar](50) NOT NULL,
	[Address2] [nchar](50) NULL,
	[City] [nchar](50) NOT NULL,
	[State] [nchar](50) NOT NULL,
	[Zip] [nchar](50) NOT NULL
) ON [PRIMARY]
";
			AdoExt.UseCommand("adventureworks", cmd =>
			{
				cmd.CommandText = recreateTableCommand;
				cmd.ExecuteNonQuery();
			});

			// load a sample set of "contact info" objects from the adventure works database
			var contacts = GetSampleContactInfo();
			
			// from the sample data create the desired amount of fake rows, each with a unique 'Id'
            var toInsert = Enumerable.Range(0, int.MaxValue)
                .SelectMany(i => contacts)
                .Take(testCount)
                .Select(a => new
                {
                    Id = Guid.NewGuid(),
                    FirstName = a.FirstName,
                    LastName = a.LastName,
                    Address1 = a.Address1,
                    Address2 = a.Address2,
                    City = a.City,
                    State = a.State,
                    Zip = a.Zip
                });

			// bulk inserts the collection of the anonymous type into the 'adventureworks' database, in the 'Contact' table that was created, and writes progress to the console
			SqlBulkInserter.BulkInsert(toInsert, "adventureworks", "Contact", a => { });
        }
    }
}
