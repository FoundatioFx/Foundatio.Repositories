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
        Assert.Contains(indexName, aliasResponse.Aliases.Keys);
        Assert.Single(aliasResponse.Aliases);
        var aliasedIndex = aliasResponse.Aliases[indexName];
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

        return response.Aliases.Count;
    }

    public static async Task<IReadOnlyCollection<string>> GetIndicesPointingToAliasAsync(this ElasticsearchClient client, string aliasName)
    {
        var response = await client.Indices.GetAliasAsync((Indices)aliasName, a => a.IgnoreUnavailable());

        if (!response.IsValidResponse)
            return [];

        return response.Aliases.Keys.ToList();
    }

    public static IReadOnlyCollection<string> GetIndicesPointingToAlias(this ElasticsearchClient client, string aliasName)
    {
        var response = client.Indices.GetAlias((Indices)aliasName, a => a.IgnoreUnavailable());

        if (!response.IsValidResponse)
            return [];

        return response.Aliases.Keys.ToList();
    }
}
