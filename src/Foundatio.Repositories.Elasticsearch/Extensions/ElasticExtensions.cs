using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Elasticsearch.Net;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Extensions {
    public static class ElasticExtensions {
        private const string ALIAS_KEY = "@@alias";

        public static TDescriptor Alias<TDescriptor>(this TDescriptor descriptor, string alias) where TDescriptor : IDescriptor {
            if (descriptor == null)
                throw new ArgumentNullException(nameof(descriptor));

            if (alias == null)
                throw new ArgumentNullException(nameof(alias));

            var property = descriptor as IProperty;
            if (property == null)
                throw new ArgumentException($"{nameof(descriptor)} must implement {nameof(IProperty)} to use aliases", nameof(descriptor));

            if (property.LocalMetadata == null)
                property.LocalMetadata = new Dictionary<string, object>();

            property.LocalMetadata.Add(ALIAS_KEY, alias);
            return descriptor;
        }

        public static string GetAliasFromDescriptor<TDescriptor>(this TDescriptor descriptor) where TDescriptor : IDescriptor {
            if (descriptor == null)
                throw new ArgumentNullException(nameof(descriptor));

            var property = descriptor as IProperty;
            return property?.GetAlias();
        }

        public static string GetAlias(this IProperty property) {
            if (property == null)
                throw new ArgumentNullException(nameof(property));

            if (property.LocalMetadata == null)
                return null;

            object alias;
            if (property.LocalMetadata.TryGetValue(ALIAS_KEY, out alias))
                return (string)alias;

            return null;
        }

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