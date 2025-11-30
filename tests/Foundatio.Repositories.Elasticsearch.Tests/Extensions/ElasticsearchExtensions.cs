using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public static class ElasticsearchExtensions
{
    public static async Task AssertSingleIndexAlias(this ElasticsearchClient client, string indexName, string aliasName)
    {
        var aliasResponse = await client.Indices.GetAliasAsync(aliasName, a => a.IgnoreUnavailable());
        Assert.True(aliasResponse.IsValidResponse);
        Assert.Contains(indexName, aliasResponse.Indices);
        Assert.Single(aliasResponse.Indices);
        var aliasedIndex = aliasResponse.Indices[indexName];
        Assert.NotNull(aliasedIndex);
        Assert.Contains(aliasName, aliasedIndex.Aliases);
        Assert.Single(aliasedIndex.Aliases);
    }

    public static async Task<int> GetAliasIndexCount(this ElasticsearchClient client, string aliasName)
    {
        var response = await client.Indices.GetAliasAsync(aliasName, a => a.IgnoreUnavailable());
        // TODO: Fix this properly once https://github.com/elastic/elasticsearch-net/issues/3828 is fixed in beta2
        if (!response.IsValidResponse)
            return 0;

        if (!response.IsValidResponse && response.ElasticsearchServerError?.Status == 404)
            return 0;

        Assert.True(response.IsValidResponse);
        return response.Indices.Count;
    }
}
