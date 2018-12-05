using Newtonsoft.Json;
using SessionData.SqlServer;
using System.Data.SqlClient;

namespace SampleApp
{
	public class SampleViewMaterializer : ViewMaterializer.ViewMaterializer
	{
		private AppDictionary _session = null;
		private const string _key = "LastSyncVersion";

		public SampleViewMaterializer(string connectionString, string sourceView, string targetTable, string changeFunction) : base(sourceView, targetTable, changeFunction)
		{
			_session = new AppDictionary(connectionString);
			_session.Deserializers.Add(typeof(long), (s) => JsonConvert.DeserializeObject<long>(s));
		}

		protected override long GetLatestSyncVersion(SqlConnection connection)
		{
			if (!_session.ContainsKey(_key)) return 0;
			return (long)_session[_key];
		}

		protected override void SetLatestSyncVersion(SqlConnection connection, long currentVersion)
		{
			_session[_key] = currentVersion;
		}
	}
}