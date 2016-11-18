using System;
using System.Linq;
using System.Text;
using Elasticsearch.Net;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Extensions {
    public static class ElasticExtensions {
        public static string GetErrorMessage(this IApiCallDetails response) {
            var sb = new StringBuilder();

            if (response.OriginalException != null)
                sb.AppendLine($"Original: ({response.HttpStatusCode} - {response.OriginalException.GetType().Name}) {response.OriginalException.Message}");

            if (response.ServerError != null)
                sb.AppendLine($"Server: ({response.ServerError.Status}) {response.ServerError.Error}");

            var bulkResponse = response as IBulkResponse;
            if (bulkResponse != null)
                sb.AppendLine($"Bulk: {String.Join("\r\n", bulkResponse.ItemsWithErrors.Select(i => i.Error))}");

            if (sb.Length == 0)
                sb.AppendLine("Unknown error.");

            return sb.ToString();
        }

        // TODO: Handle IFailureReason/BulkIndexByScrollFailure and other bulk response types.
        public static string GetErrorMessage(this IResponse response) {
            var sb = new StringBuilder();

            if (response.OriginalException != null)
                sb.AppendLine($"Original: ({response.ApiCall.HttpStatusCode} - {response.OriginalException.GetType().Name}) {response.OriginalException.Message}");

            if (response.ServerError != null)
                sb.AppendLine($"Server: ({response.ServerError.Status}) {response.ServerError.Error}");

            var bulkResponse = response as IBulkResponse;
            if (bulkResponse != null)
                sb.AppendLine($"Bulk: {String.Join("\r\n", bulkResponse.ItemsWithErrors.Select(i => i.Error))}");

            if (sb.Length == 0)
                sb.AppendLine("Unknown error.");

            return sb.ToString();
        }

        public static string GetRequest(this IApiCallDetails response) {
            if (response == null)
                return String.Empty;

            return response.RequestBodyInBytes != null ?
                $"{response.HttpMethod} {response.Uri.PathAndQuery}\r\n{Encoding.UTF8.GetString(response.RequestBodyInBytes)}\r\n"
                : $"{response.HttpMethod} {response.Uri.PathAndQuery}\r\n";
        }

        public static string GetRequest(this IResponse response) {
            return GetRequest(response?.ApiCall);
        }
    }
}