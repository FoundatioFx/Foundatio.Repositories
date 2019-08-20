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
        public static void LogTraceRequest(this ILogger logger, IElasticsearchResponse elasticResponse) {
            if (elasticResponse == null || !logger.IsEnabled(LogLevel.Trace))
                return;

            var apiCall = elasticResponse?.ApiCall;
            if (apiCall?.RequestBodyInBytes != null) {
                string body = Encoding.UTF8.GetString(apiCall?.RequestBodyInBytes);
                body = JsonUtility.NormalizeJsonString(body);

                logger.LogTrace("[{HttpStatusCode}] {HttpMethod} {HttpPathAndQuery}\r\n{HttpBody}", apiCall.HttpStatusCode, apiCall.HttpMethod, apiCall.Uri.PathAndQuery, body);
            } else {
                logger.LogTrace("[{HttpStatusCode}] {HttpMethod} {HttpPathAndQuery}", apiCall.HttpStatusCode, apiCall.HttpMethod, apiCall.Uri.PathAndQuery);
            }
        }

        public static void LogErrorRequest(this ILogger logger, IElasticsearchResponse elasticResponse, string message, params object[] args) {
            LogErrorRequest(logger, null, elasticResponse, message, args);
        }

        public static void LogErrorRequest(this ILogger logger, Exception ex, IElasticsearchResponse elasticResponse, string message, params object[] args) {
            if (elasticResponse == null || !logger.IsEnabled(LogLevel.Error))
                return;

            var sb =  new StringBuilder();
            var messageArguments = new List<object>(args);

            if (!String.IsNullOrEmpty(message))
                sb.AppendLine(message);

            var response = elasticResponse as IResponse;
            if (response?.OriginalException != null) {
                sb.AppendLine("Original: [OriginalExceptionType] {OriginalExceptionMessage}");
                messageArguments.Add(response.OriginalException.GetType().Name);
                messageArguments.Add(response.OriginalException.Message);
            }

            if (response?.ServerError?.Error != null) {
                sb.AppendLine("Server Error (Index={ErrorIndex}): {ErrorReason}");
                messageArguments.Add(response.ServerError.Error?.Index);
                messageArguments.Add(response.ServerError.Error.Reason);
            }

            if (elasticResponse is BulkResponse bulkResponse) {
                sb.AppendLine("Bulk: {BulkErrors}");
                messageArguments.Add(String.Join("\r\n", bulkResponse.ItemsWithErrors.Select(i => i.Error)));
            }

            if (elasticResponse.ApiCall != null) {
                sb.AppendLine("[{HttpStatusCode}] {HttpMethod} {HttpPathAndQuery}");
                messageArguments.Add(elasticResponse.ApiCall.HttpStatusCode);
                messageArguments.Add(elasticResponse.ApiCall.HttpMethod);
                messageArguments.Add(elasticResponse.ApiCall.Uri?.PathAndQuery);
            }

            if (elasticResponse.ApiCall?.RequestBodyInBytes != null) {
                string body = Encoding.UTF8.GetString(elasticResponse.ApiCall?.RequestBodyInBytes);
                body = JsonUtility.NormalizeJsonString(body);
                sb.AppendLine("{HttpBody}");
                messageArguments.Add(body);
            }

            AggregateException aggEx = null;
            if (ex != null && response?.OriginalException != null)
                aggEx = new AggregateException(ex, response.OriginalException);

            logger.LogError(aggEx ?? response?.OriginalException, sb.ToString(), messageArguments.ToArray());
        }
    }

    internal class JsonUtility {
        public static string NormalizeJsonString(string json) {
            try {
                var parsedObject = JObject.Parse(json);
                var normalizedObject = SortPropertiesAlphabetically(parsedObject);
                return JsonConvert.SerializeObject(normalizedObject, Formatting.Indented);
            } catch (JsonReaderException) {
                var sb = new StringBuilder();
                
                foreach (string line in json.Split(new[] { '\n' }, StringSplitOptions.RemoveEmptyEntries)) {
                    try {
                        var parsedObject = JObject.Parse(line);
                        var normalizedObject = SortPropertiesAlphabetically(parsedObject);
                        sb.AppendLine(JsonConvert.SerializeObject(normalizedObject, Formatting.Indented));
                    } catch {
                        // just return the original json
                        sb.AppendLine(line);
                    }
                }

                return sb.ToString();
            }
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