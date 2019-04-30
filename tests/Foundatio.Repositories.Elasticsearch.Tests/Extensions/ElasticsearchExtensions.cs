using System.Threading.Tasks;
using Nest;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public static class ElasticsearchExtensions {
        public static async Task AssertSingleIndexAlias(this IElasticClient client, string indexName, string aliasName) {
            var aliasResponse = await client.GetAliasAsync(a => a.Name(aliasName).IgnoreUnavailable());
            Assert.True(aliasResponse.IsValid);
            Assert.Contains(indexName, aliasResponse.Indices);
            Assert.Single(aliasResponse.Indices);
            var aliasedIndex = aliasResponse.Indices[indexName];
            Assert.NotNull(aliasedIndex);
            Assert.Contains(aliasName, aliasedIndex.Aliases);
            Assert.Single(aliasedIndex.Aliases);
        }

        public static async Task<int> GetAliasIndexCount(this IElasticClient client, string aliasName) {
            var aliasResponse = await client.GetAliasAsync(a => a.Name(aliasName).IgnoreUnavailable());
            if (aliasResponse.ServerError.Status == 404)
                return 0;
            
            Assert.True(aliasResponse.IsValid);
            return aliasResponse.Indices.Count;
        }
    }
}