using System;
using System.Linq;
using System.Reflection;
using System.Text;
using Elasticsearch.Net;
using Elasticsearch.Net.Connection;
using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Elasticsearch.Extensions {
    public static class ElasticExtensions {
        private static readonly Lazy<PropertyInfo> _connectionSettingsProperty = new Lazy<PropertyInfo>(() => typeof(HttpConnection).GetProperty("ConnectionSettings", BindingFlags.NonPublic | BindingFlags.Instance));
        
        public static void EnableTrace(this IElasticClient client) {
            var conn = client.Connection as HttpConnection;
            if (conn == null)
                return;

            var settings = _connectionSettingsProperty.Value.GetValue(conn) as ConnectionSettings;
            settings?.EnableTrace();
        }
        
        public static void DisableTrace(this IElasticClient client) {
            var conn = client.Connection as HttpConnection;
            if (conn == null)
                return;

            var settings = _connectionSettingsProperty.Value.GetValue(conn) as ConnectionSettings;
            settings?.EnableTrace(false);
        }

        public static string GetErrorMessage(this IResponse response) {
            var sb = new StringBuilder();

            if (response.ConnectionStatus?.OriginalException != null)
                sb.AppendLine($"Original: ({response.ConnectionStatus.HttpStatusCode} - {response.ConnectionStatus.OriginalException.GetType().Name}) {response.ConnectionStatus.OriginalException.Message}");

            if (response.ServerError != null)
                sb.AppendLine($"Server: ({response.ServerError.Status} - {response.ServerError.ExceptionType}) {response.ServerError.Error}");
            
            if (sb.Length == 0)
                sb.AppendLine("Unknown error.");

            return sb.ToString();
        }

        public static string GetRequest(this IResponseWithRequestInformation response) {
            string json;
            if (response.RequestInformation.RequestUrl.EndsWith("_bulk")) {
                string[] bulkCommands = Encoding.UTF8.GetString(response.RequestInformation.Request).Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);
                json = String.Join("\r\n", bulkCommands.Select(c => JObject.Parse(c).ToString(Formatting.Indented)));
            } else {
                json = JObject.Parse(Encoding.UTF8.GetString(response.RequestInformation.Request)).ToString(Formatting.Indented);
            }
            
            return $"{response.RequestInformation.RequestMethod.ToUpper()} {response.RequestInformation.RequestUrl}\r\n{json}\r\n";
        }
    }
}
