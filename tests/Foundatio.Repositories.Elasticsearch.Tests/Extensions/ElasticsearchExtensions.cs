using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public static class ElasticsearchExtensions
{
    public static async Task AssertSingleIndexAlias(this ElasticsearchClient client, string indexName, string aliasName)
    {
        var aliasResponse = await client.Indices.GetAliasAsync((Indices)aliasName, a => a.IgnoreUnavailable());
        Assert.True(aliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        Assert.NotNull(aliasResponse.Aliases);
        var indices = aliasResponse.Aliases;
#else
        Assert.NotNull(aliasResponse.Values);
        var indices = aliasResponse.Values;
#endif
        Assert.Contains(indexName, indices.Keys);
        Assert.Single(indices);
        var aliasedIndex = indices[indexName];
        Assert.NotNull(aliasedIndex);
        Assert.Contains(aliasName, aliasedIndex.Aliases.Keys);
        Assert.Single(aliasedIndex.Aliases);
    }

    public static async Task<int> GetAliasIndexCount(this ElasticsearchClient client, string aliasName)
    {
        var response = await client.Indices.GetAliasAsync((Indices)aliasName, a => a.IgnoreUnavailable());

        if (!response.IsValidResponse)
        {
            if (response.ApiCallDetails is { HttpStatusCode: 404 })
                return 0;

            throw new InvalidOperationException($"Failed to get alias '{aliasName}': {response.ElasticsearchServerError?.Error?.Reason ?? "Unknown error"}");
        }

#if ELASTICSEARCH9
        return (response.Aliases ?? throw new InvalidOperationException("Aliases response was null")).Count;
#else
        return (response.Values ?? throw new InvalidOperationException("Values response was null")).Count;
#endif
    }

    public static async Task<IReadOnlyCollection<string>> GetIndicesPointingToAliasAsync(this ElasticsearchClient client, string aliasName)
    {
        var response = await client.Indices.GetAliasAsync((Indices)aliasName, a => a.IgnoreUnavailable());

        if (!response.IsValidResponse)
        {
            if (response.ApiCallDetails is { HttpStatusCode: 404 })
                return [];

            throw new InvalidOperationException($"Failed to get alias '{aliasName}': {response.ElasticsearchServerError?.Error?.Reason ?? "Unknown error"}");
        }

#if ELASTICSEARCH9
        return (response.Aliases ?? throw new InvalidOperationException("Aliases response was null")).Keys.ToList();
#else
        return (response.Values ?? throw new InvalidOperationException("Values response was null")).Keys.ToList();
#endif
    }

    public static IReadOnlyCollection<string> GetIndicesPointingToAlias(this ElasticsearchClient client, string aliasName)
    {
        var response = client.Indices.GetAlias((Indices)aliasName, a => a.IgnoreUnavailable());

        if (!response.IsValidResponse)
        {
            if (response.ApiCallDetails is { HttpStatusCode: 404 })
                return [];

            throw new InvalidOperationException($"Failed to get alias '{aliasName}': {response.ElasticsearchServerError?.Error?.Reason ?? "Unknown error"}");
        }

#if ELASTICSEARCH9
        return (response.Aliases ?? throw new InvalidOperationException("Aliases response was null")).Keys.ToList();
#else
        return (response.Values ?? throw new InvalidOperationException("Values response was null")).Keys.ToList();
#endif
    }
}
