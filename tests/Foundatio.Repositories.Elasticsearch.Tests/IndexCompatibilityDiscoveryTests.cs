using System;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class IndexCompatibilityDiscoveryTests : ElasticRepositoryTestBase
{
    public IndexCompatibilityDiscoveryTests(ITestOutputHelper output) : base(output) { }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync(false);
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_ForVersionedIndex_ExcludesSuffixLookalikes()
    {
        string name = $"compat-discovery-versioned-{Guid.NewGuid():N}";
        using var index = new VersionedIndex<object>(_configuration, name, 1);
        string lookalikeIndex = $"{index.VersionedName}-other";
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{index.VersionedName},{lookalikeIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        await index.ConfigureAsync();
        var createResponse = await _client.Indices.CreateAsync(lookalikeIndex, cancellationToken: TestCancellationToken);
        Assert.True(createResponse.IsValidResponse, createResponse.GetErrorMessage());

        var compatibility = await index.GetIndexCompatibilityAsync(TestCancellationToken);

        var info = Assert.Single(compatibility);
        Assert.Equal(index.VersionedName, info.Name);
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_ForVersionedIndex_DiscoversReplacementThroughCanonicalAlias()
    {
        string name = $"compat-discovery-alias-{Guid.NewGuid():N}";
        using var index = new VersionedIndex<object>(_configuration, name, 1);
        string targetIndex = $"reindexed-v8-{index.VersionedName}";
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{index.VersionedName},{targetIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        var createResponse = await _client.Indices.CreateAsync(targetIndex, d => d.Aliases(a => a
            .Add(index.VersionedName, new Alias())), TestCancellationToken);
        Assert.True(createResponse.IsValidResponse, createResponse.GetErrorMessage());

        var compatibility = await index.GetIndexCompatibilityAsync(TestCancellationToken);

        var info = Assert.Single(compatibility);
        Assert.Equal(targetIndex, info.Name);
    }
}
