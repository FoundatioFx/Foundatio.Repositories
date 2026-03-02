using System;
using System.Text;
using System.Text.Json;
using Elastic.Transport.Products.Elasticsearch;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

internal static class IBodyWithApiCallDetailsExtensions
{
    private static readonly JsonSerializerOptions _options = new() { PropertyNameCaseInsensitive = true, };

    public static T DeserializeRaw<T>(this ElasticsearchResponse call) where T : class, new()
    {
        if (call?.ApiCallDetails?.ResponseBodyInBytes == null)
            return default;

        string rawResponse = Encoding.UTF8.GetString(call.ApiCallDetails.ResponseBodyInBytes);
        return JsonSerializer.Deserialize<T>(rawResponse, _options);
    }

    public static Exception OriginalException(this ElasticsearchResponse response)
    {
        return response?.ApiCallDetails?.OriginalException;
    }
}
