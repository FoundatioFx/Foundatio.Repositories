using System;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class IndexCompatibilityCrossMajorTests
{
    private const string IndexName = "compatibility-major-chain";

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
        using var index = new VersionedIndex(configuration, IndexName, 1);
        configuration.AddIndex(index);
        var client = configuration.Client;
        await client.WaitForReadyAsync(TestContext.Current.CancellationToken);
        var before = Assert.Single(await index.GetIndexCompatibilityAsync(TestContext.Current.CancellationToken));
        Assert.Equal(serverMajor - 1, before.CreatedMajor);
        Assert.Equal(serverMajor, before.ServerMajor);
        Assert.Equal(IndexCompatibilityState.RequiresReindex, before.State);
        string targetIndex = CompatibilityIndexName.Create(before.Name, serverMajor, IndexName);

        // Act
        await configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken);

        // Assert
        var countResponse = await client.CountAsync<object>(d => d.Indices(IndexName), TestContext.Current.CancellationToken);
        Assert.True(countResponse.IsValidResponse, countResponse.GetErrorMessage());
        Assert.Equal(2, countResponse.Count);

        var canonicalResponse = await client.Indices.GetAsync((Indices)$"{IndexName}-v1", cancellationToken: TestContext.Current.CancellationToken);
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
        Assert.DoesNotContain($"reindexed-v{serverMajor - 1}-{IndexName}-v1", aliases.Keys.Select(k => k.ToString()));
        Assert.DoesNotContain(ElasticIndexCompatibilityUpgrader.OwnershipAlias, aliases.Keys.Select(k => k.ToString()));
    }

    private sealed class ChainElasticConfiguration(Uri address) : ElasticConfiguration
    {
        protected override NodePool CreateConnectionPool() => new SingleNodePool(address);
    }
}
