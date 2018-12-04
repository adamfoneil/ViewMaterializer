using System;
using System.Data;
using System.Data.SqlClient;

namespace ViewMaterializer
{
	public static class AdoUtil
	{
		public static DataTable QueryCommand(SqlCommand command)
		{
			using (var adapter = new SqlDataAdapter(command))
			{
				DataTable result = new DataTable();
				adapter.Fill(result);
				return result;
			}
		}

		public static DataRow QueryCommandRow(SqlCommand command)
		{
			using (var adapter = new SqlDataAdapter(command))
			{
				DataTable result = new DataTable();
				adapter.Fill(result);
				return result.Rows[0];
			}
		}

		public static DataTable QueryTable(this SqlConnection connection, string selectQuery, Action<SqlCommand> setParameters = null)
		{
			using (var cmd = new SqlCommand(selectQuery, connection))
			{
				setParameters?.Invoke(cmd);
				return QueryCommand(cmd);
			}
		}

		public static DataRow QueryCommandRow(this SqlConnection connection, string selectQuery, Action<SqlCommand> setParameters = null)
		{
			using (var cmd = new SqlCommand(selectQuery, connection))
			{
				setParameters?.Invoke(cmd);
				return QueryCommandRow(cmd);
			}
		}

		public static T QueryValue<T>(this SqlConnection connection, string query, Action<SqlCommand> setParameters = null)
		{
			var table = QueryTable(connection, query, setParameters);
			return table.Rows[0].Field<T>(table.Columns[0]);
		}

		public static void Execute(this SqlConnection connection, string commandText, CommandType commandType = CommandType.Text, Action<SqlCommand> setParameters = null)
		{
			using (var cmd = new SqlCommand(commandText, connection))
			{
				cmd.CommandType = commandType;
				setParameters?.Invoke(cmd);
				cmd.ExecuteNonQuery();
			}
		}
	}
}