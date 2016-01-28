using System;
using System.Reflection;
using System.Text;
using Elasticsearch.Net;
using Nest;

namespace Foundatio.Elasticsearch.Extensions {
    public static class ElasticExtensions {
        private static readonly Lazy<PropertyInfo> _connectionSettingsProperty = new Lazy<PropertyInfo>(() => typeof(HttpConnection).GetProperty("ConnectionSettings", BindingFlags.NonPublic | BindingFlags.Instance));
        
        /* // TODO: This will be added back in the elastic beta.
        public static void EnableTrace(this IElasticClient client) {
            var conn = client.ConnectionSettings.Connection as HttpConnection;
            if (conn == null)
                return;

            var settings = _connectionSettingsProperty.Value.GetValue(conn) as ConnectionSettings;
            settings?.EnableTrace();
        }
        
        public static void DisableTrace(this IElasticClient client) {
            var conn = client.ConnectionSettings.Connection as HttpConnection;
            if (conn == null)
                return;

            var settings = _connectionSettingsProperty.Value.GetValue(conn) as ConnectionSettings;
            settings?.EnableTrace(false);
        }
        */

        public static string GetErrorMessage(this IResponse response) {
            var sb = new StringBuilder();

            if (response.OriginalException != null)
                sb.AppendLine($"Original: ({response.ApiCall.HttpStatusCode} - {response.OriginalException.GetType().Name}) {response.OriginalException.Message}");

            if (response.ServerError != null)
                sb.AppendLine($"Server: ({response.ServerError.Status} - {response.ServerError.Error.RootCause})");
            
            if (sb.Length == 0)
                sb.AppendLine("Unknown error.");

            return sb.ToString();
        }
    }
}
