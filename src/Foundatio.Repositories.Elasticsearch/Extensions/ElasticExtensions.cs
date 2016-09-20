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
        
        public static string GetErrorMessage(this IResponse response) {
            var sb = new StringBuilder();

            if (response.ConnectionStatus?.OriginalException != null)
                sb.AppendLine($"Original: ({response.ConnectionStatus.HttpStatusCode} - {response.ConnectionStatus.OriginalException.GetType().Name}) {response.ConnectionStatus.OriginalException.Message}");

            if (response.ServerError != null)
                sb.AppendLine($"Server: ({response.ServerError.Status} - {response.ServerError.ExceptionType}) {response.ServerError.Error}");

            var bulkResponse = response as IBulkResponse;
            if (bulkResponse != null)
                sb.AppendLine($"Bulk: {String.Join("\r\n", bulkResponse.ItemsWithErrors.Select(i => i.Error))}");

            if (sb.Length == 0)
                sb.AppendLine("Unknown error.");

            return sb.ToString();
        }

        public static string GetErrorMessage(this IElasticsearchResponse response) {
            var sb = new StringBuilder();

            if (response.OriginalException != null)
                sb.AppendLine($"Original: ({response.HttpStatusCode} - {response.OriginalException.GetType().Name}) {response.OriginalException.Message}");

            var bulkResponse = response as IBulkResponse;
            if (bulkResponse != null)
                sb.AppendLine($"Bulk: {String.Join("\r\n", bulkResponse.ItemsWithErrors.Select(i => i.Error))}");

            if (sb.Length == 0)
                sb.AppendLine("Unknown error.");

            return sb.ToString();
        }

        public static string GetRequest(this IElasticsearchResponse response) {
            if (response == null)
                return String.Empty;

            string body = String.Empty;
            if (response.RequestUrl.EndsWith("_bulk") && response.Request != null && response.Request.Length > 0) {
                string[] bulkCommands = Encoding.UTF8.GetString(response.Request).Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);
                body = String.Join("\r\n", bulkCommands.Select(c => JObject.Parse(c).ToString(Formatting.Indented)));
            } else if (response.Request != null && response.Request.Length > 0) {
                body = Encoding.UTF8.GetString(response.Request);
                try {
                    if (body.StartsWith("{") || body.StartsWith("["))
                        body = JObject.Parse(body).ToString(Formatting.Indented);
                } catch { }
            }

            return $"{response.RequestMethod.ToUpper()} {response.RequestUrl}\r\n{body}\r\n";
        }
        
        public static string GetRequest(this IResponseWithRequestInformation response) {
            if (response?.RequestInformation == null)
                return String.Empty;

            string body = String.Empty;
            if (response.RequestInformation.RequestUrl.EndsWith("_bulk") && response.RequestInformation?.Request != null && response.RequestInformation.Request.Length > 0) {
                string[] bulkCommands = Encoding.UTF8.GetString(response.RequestInformation.Request).Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);
                body = String.Join("\r\n", bulkCommands.Select(c => JObject.Parse(c).ToString(Formatting.Indented)));
            } else if (response.RequestInformation?.Request != null && response.RequestInformation.Request.Length > 0) {
                body = Encoding.UTF8.GetString(response.RequestInformation.Request);
                try {
                    if (body.StartsWith("{") || body.StartsWith("["))
                        body = JObject.Parse(body).ToString(Formatting.Indented);
                } catch { }
            }
            
            return $"{response.RequestInformation.RequestMethod.ToUpper()} {response.RequestInformation.RequestUrl}\r\n{body}\r\n";
        }
    }
}
