using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed partial class IndexCompatibilityUpgradeTests
{
    private const string CrossMajorIndexName = "compatibility-major-chain";

    [Fact(Timeout = 120_000)]
    public async Task UpgradeIndexCompatibilityAsync_AcrossSequentialMajors_PreservesDataAndCanonicalNames()
    {
        string? phaseValue = Environment.GetEnvironmentVariable("FOUNDATIO_COMPATIBILITY_CHAIN_MAJOR");
        if (!Int32.TryParse(phaseValue, out int serverMajor))
            Assert.Skip("Set FOUNDATIO_COMPATIBILITY_CHAIN_MAJOR and invoke explicit tests only during the persistent Elasticsearch 7 to 8 to 9 release validation.");

        string? address = Environment.GetEnvironmentVariable("ELASTICSEARCH_URL");
        if (String.IsNullOrWhiteSpace(address))
            Assert.Skip("Set ELASTICSEARCH_URL to the isolated persistent-data validation cluster; never use the default application cluster for this test.");

        // Arrange
        using var configuration = new ChainElasticConfiguration(new Uri(address));
        using var index = new VersionedIndex(configuration, CrossMajorIndexName, 1);
        configuration.AddIndex(index);
        var client = configuration.Client;
        await client.WaitForReadyAsync(TestContext.Current.CancellationToken);
        var before = Assert.Single(await index.GetIndexCompatibilityAsync(TestContext.Current.CancellationToken));
        Assert.Equal(serverMajor - 1, before.CreatedMajor);
        Assert.Equal(serverMajor, before.ServerMajor);
        Assert.Equal(IndexCompatibilityState.RequiresReindex, before.State);
        string targetIndex = CompatibilityIndexName.Create(before.Name, serverMajor, CrossMajorIndexName);
        await AssertDocumentsAsync(client);

        // Act
        await configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken);

        // Assert
        var countResponse = await client.CountAsync<object>(d => d.Indices(CrossMajorIndexName), TestContext.Current.CancellationToken);
        Assert.True(countResponse.IsValidResponse, countResponse.GetErrorMessage());
        Assert.Equal(2, countResponse.Count);
        await AssertDocumentsAsync(client);

        var canonicalResponse = await client.Indices.GetAsync((Indices)$"{CrossMajorIndexName}-v1", cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(canonicalResponse.IsValidResponse, canonicalResponse.GetErrorMessage());
        Assert.Equal(targetIndex, canonicalResponse.Indices.Keys.Single().ToString());

        var settingsResponse = await client.Indices.GetSettingsAsync((Indices)targetIndex,
            d => d.IncludeDefaults(false), TestContext.Current.CancellationToken);
        Assert.True(settingsResponse.IsValidResponse, settingsResponse.GetErrorMessage());
        var settings = settingsResponse.Settings.Values.Single().Settings?.Index;
        var version = settings?.Version;
        Assert.Equal(serverMajor, Foundatio.Repositories.Elasticsearch.Configuration.Index.ParseCreatedMajor(version?.Created, version?.CreatedString));
        Assert.Null(settings?.RefreshInterval);
        Assert.Null(settings?.DefaultPipeline);
        Assert.Null(settings?.FinalPipeline);

        var after = Assert.Single(await index.GetIndexCompatibilityAsync(TestContext.Current.CancellationToken));
        Assert.Equal(IndexCompatibilityState.Current, after.State);
        var aliases = canonicalResponse.Indices.Values.Single().Aliases;
        Assert.NotNull(aliases);
        Assert.DoesNotContain($"reindexed-v{serverMajor - 1}-{CrossMajorIndexName}-v1", aliases.Keys.Select(k => k.ToString()));
        Assert.DoesNotContain(ElasticIndexCompatibilityUpgrader.OwnershipAlias, aliases.Keys.Select(k => k.ToString()));
    }

    private static async Task AssertDocumentsAsync(ElasticsearchClient client)
    {
        for (int id = 1; id <= 2; id++)
        {
            string message = id is 1 ? "first" : "second";
            string expected = $$"""{"message":"{{message}}","sequence":{{id}},"nested":{"enabled":true},"tags":["a","b"]}""";
            var document = await client.GetAsync<JsonElement>(id.ToString(), d => d.Index(CrossMajorIndexName).Routing($"tenant-{id}"), TestContext.Current.CancellationToken);
            Assert.True(document.IsValidResponse, document.GetErrorMessage());
            Assert.True(document.Found);
            Assert.Equal(id.ToString(), document.Id);
            Assert.Equal($"tenant-{id}", document.Routing);
            Assert.True(JsonNode.DeepEquals(JsonNode.Parse(expected), JsonNode.Parse(document.Source.GetRawText())));
            var count = await client.CountAsync<JsonElement>(d => d.Indices(CrossMajorIndexName).Query(q => q.Term(t => t.Field("message").Value(message))), TestContext.Current.CancellationToken);
            Assert.True(count.IsValidResponse, count.GetErrorMessage());
            Assert.Equal(1, count.Count);
        }
    }

    private sealed class ChainElasticConfiguration(Uri address) : ElasticConfiguration
    {
        protected override NodePool CreateConnectionPool() => new SingleNodePool(address);
    }
}
