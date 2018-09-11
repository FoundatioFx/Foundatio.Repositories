using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Elasticsearch.Net;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Microsoft.Extensions.Logging;
using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Foundatio.Repositories.Elasticsearch.Extensions {
    public static class LoggerExtensions {
        public static void LogTraceRequest(this ILogger logger, IResponse response, bool normalize = false) {
            LogTraceRequest(logger, response?.ApiCall, normalize);
        }

        public static void LogTraceRequest(this ILogger logger, IApiCallDetails response, bool normalize = false) {
            if (response == null || !logger.IsEnabled(LogLevel.Trace))
                return;

            if (response.RequestBodyInBytes != null) {
                string body = Encoding.UTF8.GetString(response.RequestBodyInBytes);
                if (normalize)
                    body = JsonUtility.NormalizeJsonString(body);

                logger.LogTrace("[{HttpMethod}] {HttpStatusCode} {HttpPathAndQuery}\r\n{HttpBody}", response.HttpMethod, response.HttpStatusCode, response.Uri.PathAndQuery, body);
            } else {
                logger.LogTrace("[{HttpMethod}] {HttpStatusCode} {HttpPathAndQuery}", response.HttpMethod, response.HttpStatusCode, response.Uri.PathAndQuery);
            }
        }

        public static void LogErrorRequest(this ILogger logger, IResponse response, string message, params object[] args) {
            LogErrorRequest(logger, null, response?.ApiCall, message, args);
        }

        public static void LogErrorRequest(this ILogger logger, IApiCallDetails response, string message, params object[] args) {
            LogErrorRequest(logger, null, response, message, args);
        }

        public static void LogErrorRequest(this ILogger logger, Exception ex, IResponse response, string message, params object[] args) {
            LogErrorRequest(logger, ex, response?.ApiCall, message, args);
        }

        public static void LogErrorRequest(this ILogger logger, Exception ex, IApiCallDetails response, string message, params object[] args) {
            if (response == null || !logger.IsEnabled(LogLevel.Error))
                return;

            var sb =  new StringBuilder(message ?? String.Empty);
            var messageArguments = new List<object>(args);

            sb.AppendLine(" [{HttpMethod}] {HttpStatusCode} {HttpPathAndQuery}");
            messageArguments.Add(response.HttpMethod);
            messageArguments.Add(response.HttpStatusCode);
            messageArguments.Add(response.Uri.PathAndQuery);

            if (response.RequestBodyInBytes != null) {
                string body = Encoding.UTF8.GetString(response.RequestBodyInBytes);
                sb.Append("Body:\r\n{HttpBody}");
                messageArguments.Add(body);
            }

            if (response.OriginalException != null) {
                sb.AppendLine("Original: [OriginalExceptionType] {OriginalExceptionMessage}");
                messageArguments.Add(response.OriginalException.GetType().Name);
                messageArguments.Add(response.OriginalException.Message);
            }

            if (response.ServerError != null) {
                sb.AppendLine("Server: [ServerStatusCode] {ServerErrors}");
                messageArguments.Add(response.ServerError.Status);
                messageArguments.Add(response.ServerError.Error);
            }

            if (response is IBulkResponse bulkResponse) {
                sb.AppendLine("Bulk: {BulkErrors}");
                messageArguments.Add(String.Join("\r\n", bulkResponse.ItemsWithErrors.Select(i => i.Error)));
            }

            AggregateException aggEx = null;
            if (ex != null && response.OriginalException != null)
                aggEx = new AggregateException(ex, response.OriginalException);

            logger.LogError(aggEx ?? response.OriginalException, sb.ToString(), messageArguments.ToArray());
        }
    }

    internal class JsonUtility {
        public static string NormalizeJsonString(string json) {
            var parsedObject = JObject.Parse(json);
            var normalizedObject = SortPropertiesAlphabetically(parsedObject);
            return JsonConvert.SerializeObject(normalizedObject, Formatting.Indented);
        }

        private static JObject SortPropertiesAlphabetically(JObject original) {
            var result = new JObject();

            foreach (var property in original.Properties().ToList().OrderBy(p => p.Name)) {
                if (property.Value is JObject value) {
                    value = SortPropertiesAlphabetically(value);
                    result.Add(property.Name, value);
                }
                else if (property.Value is JArray array) {
                    array = SortArrayAlphabetically(array);
                    result.Add(property.Name, array);
                }
                else {
                    result.Add(property.Name, property.Value);
                }
            }

            return result;
        }

        private static JArray SortArrayAlphabetically(JArray original) {
            var result = new JArray();

            foreach (var item in original) {
                if (item is JObject value)
                    result.Add(SortPropertiesAlphabetically(value));
                else if (item is JArray array)
                    result.Add(SortArrayAlphabetically(array));
                else
                    result.Add(item);
            }

            return result;
        }
    }
}