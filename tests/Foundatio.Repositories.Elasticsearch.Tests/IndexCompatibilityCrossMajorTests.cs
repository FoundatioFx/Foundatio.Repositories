using System;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class IndexCompatibilityCrossMajorTests : ElasticRepositoryTestBase
{
    private const string IndexName = "compatibility-major-chain";

    public IndexCompatibilityCrossMajorTests(ITestOutputHelper output) : base(output) { }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_AcrossSequentialMajors_PreservesDataAndCanonicalNames()
    {
        string? phaseValue = Environment.GetEnvironmentVariable("FOUNDATIO_COMPATIBILITY_CHAIN_MAJOR");
        if (!Int32.TryParse(phaseValue, out int serverMajor))
            return;

        // Arrange
        using var index = new VersionedIndex(_configuration, IndexName, 1);
        var before = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        Assert.Equal(serverMajor - 1, before.CreatedMajor);
        Assert.Equal(serverMajor, before.ServerMajor);
        Assert.Equal(IndexCompatibilityState.RequiresReindex, before.State);
        string targetIndex = CompatibilityIndexName.Create(before.Name, serverMajor, IndexName);

        // Act
        await _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken);

        // Assert
        var countResponse = await _client.CountAsync<object>(d => d.Indices(IndexName), TestCancellationToken);
        Assert.True(countResponse.IsValidResponse, countResponse.GetErrorMessage());
        Assert.Equal(2, countResponse.Count);

        var canonicalResponse = await _client.Indices.GetAsync((Indices)$"{IndexName}-v1", cancellationToken: TestCancellationToken);
        Assert.True(canonicalResponse.IsValidResponse, canonicalResponse.GetErrorMessage());
        Assert.Equal(targetIndex, canonicalResponse.Indices.Keys.Single().ToString());

        var settingsResponse = await _client.Indices.GetSettingsAsync((Indices)targetIndex, cancellationToken: TestCancellationToken);
        Assert.True(settingsResponse.IsValidResponse, settingsResponse.GetErrorMessage());
        var version = settingsResponse.Settings.Values.Single().Settings?.Index?.Version;
        Assert.Equal(serverMajor, Foundatio.Repositories.Elasticsearch.Configuration.Index.ParseCreatedMajor(version?.Created, version?.CreatedString));

        var after = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        Assert.Equal(IndexCompatibilityState.Current, after.State);
        var aliases = canonicalResponse.Indices.Values.Single().Aliases;
        Assert.NotNull(aliases);
        Assert.DoesNotContain($"reindexed-v{serverMajor - 1}-{IndexName}-v1", aliases.Keys.Select(k => k.ToString()));
    }
}
