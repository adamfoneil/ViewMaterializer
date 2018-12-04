using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace ViewMaterializer
{
	public abstract class ViewMaterializer
	{
		public ViewMaterializer(string sourceView, string changesFunction, string targetTable)
		{
			SourceView = sourceView;
			ChangesFunction = changesFunction;
			TargetTable = targetTable;
		}

		/// <summary>
		/// View containing all the data we want to materialize
		/// </summary>
		public string SourceView { get; }

		/// <summary>
		/// Table function that accepts a @version argument and returns only the primary key columns of TargetTable
		/// </summary>
		public string ChangesFunction { get; }

		/// <summary>
		/// Physical table that will hold the results of the view
		/// </summary>
		public string TargetTable { get; }

		/// <summary>
		/// Synchronizes the SourceView to the TargetTable based on what's changed since the last sync
		/// </summary>
		public void Execute(SqlConnection connection)
		{
			long latestSyncVersion = GetLatestSyncVersion(connection);

			DataTable changes = connection.QueryTable(
				$"SELECT * FROM {ChangesFunction}(@version)",
				cmd => cmd.Parameters.AddWithValue("version", latestSyncVersion));

			string[] keyColumns = GetPrimaryKeyColumns(connection, TargetTable);

			using (SqlCommand cmd = BuildViewSliceCommand(connection, keyColumns, out string query))
			{				
				foreach (DataRow keyValues in changes.Rows)
				{
					Stopwatch sw = Stopwatch.StartNew();
					DataRow viewRow = GetViewSlice(cmd, keyValues);
					OnGetViewSlice(connection, sw.Elapsed, query, keyValues);

					MergeRowIntoTarget(connection, keyValues, viewRow);
				}				
			}

			SetLatestSyncVersion(connection, GetCurrentVersion(connection));
		}

		/// <summary>
		/// Override this to log the time elapsed for a view slice query
		/// </summary>		
		protected virtual void OnGetViewSlice(SqlConnection connection, TimeSpan elapsed, string query, DataRow keyValues)
		{
			// do nothing by default
		}

		/// <summary>
		/// Inserts or updates a slice of the SourceView into the TargetTable
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="viewRow"></param>
		private void MergeRowIntoTarget(SqlConnection connection, DataRow keyValues, DataRow viewRow)
		{
			if (TargetRowExists(connection, TargetTable, keyValues, viewRow))
			{
				UpdateTarget(connection, TargetTable, keyValues, viewRow);
			}
			else
			{
				InsertTarget(connection, TargetTable, viewRow);
			}
		}

		private void InsertTarget(SqlConnection connection, string targetTable, DataRow viewRow)
		{
			throw new NotImplementedException();
		}

		private void UpdateTarget(SqlConnection connection, string targetTable, DataRow keyValues, DataRow viewRow)
		{
			throw new NotImplementedException();
		}

		private bool TargetRowExists(SqlConnection connection, string targetTable, DataRow keyValues, DataRow viewRow)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Builds a query that filters the SourceView based on a set of primary key columns
		/// </summary>
		private SqlCommand BuildViewSliceCommand(SqlConnection connection, string[] keyColumns, out string query)
		{
			query = $"SELECT * FROM {SourceView} WHERE {string.Join(" AND ", keyColumns.Select(col => $"[{col}]=@{col}"))}";
			return new SqlCommand(query, connection);
		}

		/// <summary>
		/// Executes the SourceView for a given key combination. This should be a lot faster than running
		/// the entire view with no criteria. This is where the value of ViewMaterializer is supposed to show
		/// </summary>
		private DataRow GetViewSlice(SqlCommand command, DataRow keyValues)
		{
			foreach (DataColumn col in keyValues.Table.Columns)
			{
				command.Parameters[col.ColumnName].Value = keyValues[col.ColumnName];
			}

			return AdoUtil.QueryCommandRow(command);
		}

		private static string[] GetPrimaryKeyColumns(SqlConnection connection, string targetTable)
		{
			var columns = connection.QueryTable(
				$@"SELECT 
					[col].[name]
				FROM 
					[sys].[indexes] [ndx] 
					INNER JOIN [sys].[index_columns] [xcol] ON [ndx].[object_id]=[xcol].[object_id] AND [xcol].[index_id]=[ndx].[index_id]
					INNER JOIN [sys].[columns] [col] ON [xcol].[object_id]=[col].[object_id] AND [xcol].[column_id]=[col].[column_id]
				WHERE 
					[ndx].[object_id]=OBJECT_ID(@targetTable) AND
					[ndx].[is_primary_key]=1", (cmd) =>
				   {
					   cmd.Parameters.AddWithValue("targetTable", targetTable);
				   });
			return columns.AsEnumerable().Select(row => row.Field<string>("name")).ToArray();
		}

		/// <summary>
		/// Saves the latest sync version so next time we we know which version to start with
		/// </summary>
		protected abstract void SetLatestSyncVersion(SqlConnection connection, long currentVersion);

		/// <summary>
		/// Where do we start the sync from? This will be result of last call to <see cref="SetLatestSyncVersion(SqlConnection, long)"/>
		/// </summary>
		protected abstract long GetLatestSyncVersion(SqlConnection connection);

		/// <summary>
		/// Return value of this will be used the next time we get changes
		/// </summary>
		private long GetCurrentVersion(SqlConnection connection)
		{
			try
			{
				return connection.QueryValue<long>("SELECT CHANGE_TRACKING_CURRENT_VERSION()");
			}
			catch
			{
				return 0;
			}
		}		
	}
}