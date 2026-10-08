using System;
using Elastic.Transport.Products.Elasticsearch;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

internal static class IBodyWithApiCallDetailsExtensions
{
    public static Exception? OriginalException(this ElasticsearchResponse response)
    {
        return response?.ApiCallDetails?.OriginalException;
    }
}
