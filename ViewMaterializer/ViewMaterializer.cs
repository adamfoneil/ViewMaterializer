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

			ValidateKeyColumns(changes, keyColumns);

			string whereClause = string.Join(" AND ", keyColumns.Select(col => $"[{col}]=@{col}"));

			using (SqlCommand cmd = BuildViewSliceCommand(connection, whereClause, out string query))
			{				
				foreach (DataRow keyValues in changes.Rows)
				{
					Stopwatch sw = Stopwatch.StartNew();
					DataRow viewRow = GetViewSlice(cmd, keyValues);
					OnGetViewSlice(connection, sw.Elapsed, query, keyValues);

					MergeRowIntoTarget(connection, whereClause, keyValues, viewRow);
				}				
			}

			SetLatestSyncVersion(connection, GetCurrentVersion(connection));
		}

		/// <summary>
		/// Need to make sure the columns in the changes table match the keyColumns
		/// </summary>
		private void ValidateKeyColumns(DataTable changes, string[] keyColumns)
		{
			var changeColumnsOrdered = changes.Columns.OfType<DataColumn>().Select(col => col.ColumnName.ToLower()).OrderBy(s => s).ToArray();
			var keyColumnsOrdered = keyColumns.Select(s => s.ToLower()).OrderBy(s => s).ToArray();
			if (!changeColumnsOrdered.SequenceEqual(keyColumnsOrdered))
			{
				string changeColumnStr = string.Join(", ", changeColumnsOrdered);
				string keyColumnStr = string.Join(", ", keyColumnsOrdered);
				throw new InvalidOperationException($"Change table columns and target table primary key columns must match.\r\nChange columns found: {changeColumnStr}\r\nKey columns found: {keyColumnStr}");
			}
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
		private void MergeRowIntoTarget(SqlConnection connection, string whereClause, DataRow keyValues, DataRow viewRow)
		{
			if (TargetRowExists(connection, TargetTable, whereClause, keyValues, viewRow))
			{
				UpdateTarget(connection, TargetTable, whereClause, keyValues, viewRow);
			}
			else
			{
				InsertTarget(connection, TargetTable, viewRow);
			}
		}

		private bool TargetRowExists(SqlConnection connection, string targetTable, string whereClause, DataRow keyValues, DataRow viewRow)
		{
			return connection.QueryRowExists($"SELECT 1 FROM {targetTable} WHERE {whereClause}", (cmd) =>
			{
				foreach (DataColumn col in keyValues.Table.Columns)
				{
					cmd.Parameters.AddWithValue(col.ColumnName, keyValues[col.ColumnName]);
				}
			});
		}

		private static void InsertTarget(SqlConnection connection, string targetTable, DataRow viewRow)
		{
			var columns = viewRow.Table.Columns.OfType<DataColumn>().Select(col => col.ColumnName).ToArray();
			string columnList = string.Join(", ", columns.Select(s => $"[{s}]"));
			string valueList = string.Join(", ", columns.Select(s => $"@{s}"));
			connection.Execute(
				$@"INSERT INTO {targetTable} ({columnList}) VALUES ({valueList})", CommandType.Text, (cmd) =>
				{
					foreach (DataColumn col in viewRow.Table.Columns)
					{
						cmd.Parameters.AddWithValue(col.ColumnName, viewRow[col.ColumnName]);
					}
				});
		}

		private static void UpdateTarget(SqlConnection connection, string targetTable, string whereClause, DataRow keyValues, DataRow viewRow)
		{
			var keyColumns = keyValues.Table.Columns.OfType<DataColumn>().Select(col => col.ColumnName).ToArray();
			var setColumns = viewRow.Table.Columns.OfType<DataColumn>().Select(col => col.ColumnName).Except(keyColumns).ToArray();

			string setColumnList = string.Join(", ", setColumns.Select(col => $"[{col}]=@{col}"));

			connection.Execute($"UPDATE {targetTable} SET {setColumns} WHERE {whereClause}", CommandType.Text, (cmd) =>
			{
				foreach (string keyCol in keyColumns) cmd.Parameters.AddWithValue(keyCol, keyValues[keyCol]);
				foreach (string setCol in setColumns) cmd.Parameters.AddWithValue(setCol, viewRow[setCol]);
			});
		}

		/// <summary>
		/// Builds a query that filters the SourceView based on a set of primary key columns
		/// </summary>
		private SqlCommand BuildViewSliceCommand(SqlConnection connection, string whereClause, out string query)
		{
			query = $"SELECT * FROM {SourceView} WHERE {whereClause}";
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
				command.Parameters.AddWithValue(col.ColumnName, keyValues[col.ColumnName]);				
			}

			return AdoUtil.QueryRow(command);
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