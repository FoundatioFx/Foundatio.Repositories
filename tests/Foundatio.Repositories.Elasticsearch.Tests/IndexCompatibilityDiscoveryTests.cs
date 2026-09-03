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
    public async Task DeleteAsync_WithVersionWildcard_PreservesClosedIndexes()
    {
        string name = $"compat-delete-closed-{Guid.NewGuid():N}";
        using var index = new DailyIndex<object>(_configuration, name, 1);
        string openName = $"{name}-v1-2024.01.01";
        string closedName = $"{name}-v1-2024.01.02";
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{openName},{closedName}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        var open = await _client.Indices.CreateAsync(openName, cancellationToken: TestCancellationToken);
        var closed = await _client.Indices.CreateAsync(closedName, cancellationToken: TestCancellationToken);
        Assert.True(open.IsValidResponse, open.GetErrorMessage());
        Assert.True(closed.IsValidResponse, closed.GetErrorMessage());
        var close = await _client.Indices.CloseAsync(closedName, TestCancellationToken);
        Assert.True(close.IsValidResponse, close.GetErrorMessage());

        await index.DeleteAsync();

        var closedExists = await _client.Indices.ExistsAsync(closedName, cancellationToken: TestCancellationToken);
        var openExists = await _client.Indices.ExistsAsync(openName, cancellationToken: TestCancellationToken);
        Assert.True(closedExists.Exists, closedExists.GetErrorMessage());
        Assert.False(openExists.Exists, openExists.DebugInformation);
    }

    [Fact]
    public async Task GetCurrentVersionAsync_ExcludesClosedPartitionsButIncludesHiddenOpenPartitions()
    {
        string name = $"compat-discovery-closed-{Guid.NewGuid():N}";
        using var index = new DailyIndex<object>(_configuration, name, 3);
        string closedIndex = $"{name}-v1-2024.01.01";
        string hiddenIndex = $"{name}-v2-2024.01.02";
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{closedIndex},{hiddenIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        var closed = await _client.Indices.CreateAsync(closedIndex, d => d
            .Settings(s => s.NumberOfReplicas(0))
            .Aliases(a => a.Add($"{name}-2024.01.01", new Alias())), TestCancellationToken);
        Assert.True(closed.IsValidResponse, closed.GetErrorMessage());
        var close = await _client.Indices.CloseAsync(closedIndex, TestCancellationToken);
        Assert.True(close.IsValidResponse, close.GetErrorMessage());
        var hidden = await _client.Indices.CreateAsync(hiddenIndex, d => d
            .Settings(s => s.Hidden(true).NumberOfReplicas(0))
            .Aliases(a => a.Add($"{name}-2024.01.02", new Alias { IsHidden = true })), TestCancellationToken);
        Assert.True(hidden.IsValidResponse, hidden.GetErrorMessage());

        Assert.Equal(2, await index.GetCurrentVersionAsync());
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
