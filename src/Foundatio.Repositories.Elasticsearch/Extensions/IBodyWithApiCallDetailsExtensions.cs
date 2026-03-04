using System;
using Elastic.Transport.Products.Elasticsearch;
using Foundatio.Serializer;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

internal static class IBodyWithApiCallDetailsExtensions
{
    public static T DeserializeRaw<T>(this ElasticsearchResponse call, ITextSerializer serializer) where T : class, new()
    {
        ArgumentNullException.ThrowIfNull(serializer);

        if (call?.ApiCallDetails?.ResponseBodyInBytes == null)
            return default;

        return serializer.Deserialize<T>(call.ApiCallDetails.ResponseBodyInBytes);
    }

    public static Exception OriginalException(this ElasticsearchResponse response)
    {
        return response?.ApiCallDetails?.OriginalException;
    }
}
