using System;
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
        [Obsolete("Use LogRequest instead")]
        public static void LogTraceRequest(this ILogger logger, IElasticsearchResponse elasticResponse, LogLevel logLevel = LogLevel.Trace) {
            LogRequest(logger, elasticResponse, logLevel);
        }

        public static void LogRequest(this ILogger logger, IElasticsearchResponse elasticResponse, LogLevel logLevel = LogLevel.Trace) {
            if (elasticResponse == null || !logger.IsEnabled(logLevel))
                return;

            var apiCall = elasticResponse?.ApiCall;
            if (apiCall?.RequestBodyInBytes != null) {
                string body = Encoding.UTF8.GetString(apiCall?.RequestBodyInBytes);
                body = JsonUtility.NormalizeJsonString(body);

                logger.Log(logLevel, "[{HttpStatusCode}] {HttpMethod} {HttpPathAndQuery}\r\n{HttpBody}", apiCall.HttpStatusCode, apiCall.HttpMethod, apiCall.Uri.PathAndQuery, body);
            } else {
                logger.Log(logLevel, "[{HttpStatusCode}] {HttpMethod} {HttpPathAndQuery}", apiCall.HttpStatusCode, apiCall.HttpMethod, apiCall.Uri.PathAndQuery);
            }
        }

        public static void LogErrorRequest(this ILogger logger, IElasticsearchResponse elasticResponse, string message, params object[] args) {
            LogErrorRequest(logger, null, elasticResponse, message, args);
        }

        public static void LogErrorRequest(this ILogger logger, Exception ex, IElasticsearchResponse elasticResponse, string message, params object[] args) {
            if (elasticResponse == null || !logger.IsEnabled(LogLevel.Error))
                return;

            var response = elasticResponse as IResponse;

            AggregateException aggEx = null;
            if (ex != null && response?.OriginalException != null)
                aggEx = new AggregateException(ex, response.OriginalException);

            logger.LogError(aggEx ?? response?.OriginalException, elasticResponse.GetErrorMessage(message), args);
        }
    }

    internal class JsonUtility {
        public static string NormalizeJsonString(string json) {
            if (String.IsNullOrEmpty(json))
                return json;

            JObject parsedObject;
            JObject normalizedObject;
            
            if (json.Contains("\n")) {
                var sb = new StringBuilder();
                
                foreach (string line in json.Split(new[] { '\n' }, StringSplitOptions.RemoveEmptyEntries)) {
                    try {
                        parsedObject = JObject.Parse(line);
                        normalizedObject = SortPropertiesAlphabetically(parsedObject);
                        sb.AppendLine(JsonConvert.SerializeObject(normalizedObject, Formatting.Indented));
                    } catch {
                        // just return the original json
                        sb.AppendLine(line);
                    }
                }

                return sb.ToString();
            }
            
            parsedObject = JObject.Parse(json);
            normalizedObject = SortPropertiesAlphabetically(parsedObject);
            return JsonConvert.SerializeObject(normalizedObject, Formatting.Indented);
        }

        private static JObject SortPropertiesAlphabetically(JObject original) {
            var result = new JObject();

            foreach (var property in original.Properties().ToList().OrderBy(p => p.Name)) {
                if (property.Value is JObject value) {
                    value = SortPropertiesAlphabetically(value);
                    result.Add(property.Name, value);
                } else if (property.Value is JArray array) {
                    array = SortArrayAlphabetically(array);
                    result.Add(property.Name, array);
                } else {
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