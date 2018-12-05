using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using AdamOneilSoftware;
using SampleApp.Models;
using Postulate.Lite.SqlServer.IntKey;
using Dapper;

namespace SampleApp
{
	class Program
	{
		static void Main(string[] args)
		{
			var vm = new SampleViewMaterializer(GetConnectionString(), "dbo.SalesPivot", "rpt.SalesPivot", "dbo.FnSalesChanges");

			using (var cn = GetConnection())
			{
				//GenerateSampleItems(cn);
				//GenerateSampleSales(cn, 5);
				vm.Execute(cn);
			}

		}

		private static void GenerateSampleItems(SqlConnection cn)
		{
			var tdg = new TestDataGenerator();
			tdg.GenerateUpTo<Item>(cn, 50, (db) =>
			{
				return db.QuerySingle<int>("SELECT COUNT(1) FROM [dbo].[Item]");
			}, (item) =>
			{
				item.Name = tdg.Random(Source.WidgetName) + tdg.RandomInRange(1, 1000).ToString();
				item.Cost = Convert.ToDecimal(tdg.RandomInRange(3, 120));
			}, (items) =>
			{
				foreach (var item in items) cn.PlainInsert(item);
			});
		}

		private static void GenerateSampleSales(SqlConnection cn, int count)
		{
			var regions = cn.Query<int>("SELECT [Id] FROM [dbo].[Region]").ToArray();
			var items = cn.Query<int>("SELECT [Id] FROM [dbo].[Item]").ToArray();
			var dates = Enumerable.Range(1, 1500).Select(i => new DateTime(2011, 1, 1).AddDays(i)).ToArray();			

			// I set this to a million, but I had patience for only about 600K to generate
			var tdg = new TestDataGenerator();
			tdg.Generate<Sales>(count, (s) =>
			{
				s.ItemId = tdg.Random(regions);
				s.Date = tdg.Random(dates);
				s.RegionId = tdg.Random(regions);
				s.Quantity = tdg.RandomInRange(1, 100).Value;
			}, (records) =>
			{
				foreach (var record in records) cn.PlainInsert(record);
			});
		}

		private static string GetConnectionString()
		{
			return ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;
		}

		private static SqlConnection GetConnection()
		{			
			return new SqlConnection(GetConnectionString());
		}
	}
}
