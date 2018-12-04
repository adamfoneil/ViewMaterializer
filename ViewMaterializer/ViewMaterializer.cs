using System.Data;
using System.Data.SqlClient;

namespace ViewMaterializer
{
	public abstract class ViewMaterializer
	{
		public ViewMaterializer(string sourceView, string targetTable)
		{
			SourceView = sourceView;
			TargetTable = targetTable;
		}

		public string SourceView { get; }
		public string TargetTable { get; }

		public void Execute(SqlConnection connection)
		{
			long currentVersion = GetCurrentVersion(connection);
			long latestSyncVersion = GetLatestSyncVersion(connection);

			// do everything

			SetLatestSyncVersion(connection, currentVersion);
		}

		/// <summary>
		/// Saves the latest sync version so next time we we know which version to start with
		/// </summary>
		protected abstract void SetLatestSyncVersion(SqlConnection connection, long currentVersion);

		/// <summary>
		/// Where do we start are sync from?
		/// </summary>
		protected abstract long GetLatestSyncVersion(SqlConnection connection);

		/// <summary>
		/// Return value of this will be used the next time we get changes
		/// </summary>
		private long GetCurrentVersion(SqlConnection connection)
		{
			return connection.QueryValue<long>("SELECT CHANGE_TRACKING_CURRENT_VERSION()");
		}
	}
}