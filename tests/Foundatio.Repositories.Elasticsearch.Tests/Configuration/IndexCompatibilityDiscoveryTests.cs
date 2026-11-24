using System;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Foundatio.Lock;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Exceptions;
using Foundatio.Utility;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class IndexCompatibilityDiscoveryTests : ElasticRepositoryTestBase
{
    public IndexCompatibilityDiscoveryTests(ITestOutputHelper output) : base(output) { }

    [Theory]
    [InlineData("reindexed-v9-{name}", false, true)]
    [InlineData("reindexed-v9-{name}", true, false)]
    [InlineData("unrelated-{name}", false, false)]
    [InlineData("{name}-v1", false, true)]
    [InlineData("{name}-v1-extra", false, false)]
    [InlineData("{name}-v1", false, false, true)]
    public async Task DeleteAsyncThroughAnAliasOnlyDeletesAnOwnedIndex(string physicalNamePattern, bool registerPhysicalIndex, bool shouldDelete, bool routed = false)
    {
        // Arrange
        string name = $"compat-delete-{Guid.NewGuid():N}";
        string physical = physicalNamePattern.Replace("{name}", name, StringComparison.Ordinal);
        using var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log);
        using var index = new Index<object>(configuration, name);
        using var sibling = new Index<object>(configuration, physical);
        configuration.AddIndex(index);
        if (registerPhysicalIndex)
            configuration.AddIndex(sibling);

        await using AsyncDisposableAction cleanup = new(async () =>
            await _client.Indices.DeleteAsync(physical, d => d.IgnoreUnavailable(), TestCancellationToken));
        var alias = routed ? new Alias { Routing = "tenant-a" } : new Alias();
        var create = await _client.Indices.CreateAsync(physical, d => d.Aliases(a => a.Add(name, alias)), TestCancellationToken);
        Assert.True(create.IsValidResponse, create.GetErrorMessage());

        // Act
        var error = await Record.ExceptionAsync(() => index.DeleteAsync());

        // Assert
        if (registerPhysicalIndex)
            Assert.IsType<RepositoryException>(error);
        else
            Assert.Null(error);
        var exists = await _client.Indices.ExistsAsync(physical, cancellationToken: TestCancellationToken);
        Assert.Equal(!shouldDelete, exists.Exists);
    }

    [Fact]
    public async Task RetentionRemovesExpiredNativeAndCanonicalIndexesAndPreservesLookalikes()
    {
        // Arrange
        string prefix = $"retention-{Guid.NewGuid():N}";
        string native = $"{prefix}-2020.01.01";
        string canonical = $"{prefix}-2020.01.02";
        string replacement = $"reindexed-v9-{canonical}";
        string foreign = $"reindexed-v9-{prefix}-2020.01.03";
        await using AsyncDisposableAction cleanup = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{native},{replacement},{foreign}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        Assert.True((await _client.Indices.CreateAsync(native, cancellationToken: TestCancellationToken)).IsValidResponse);
        Assert.True((await _client.Indices.CreateAsync(replacement, d => d.Aliases(a => a.Add(canonical, new Alias())), TestCancellationToken)).IsValidResponse);
        Assert.True((await _client.Indices.CreateAsync(foreign, cancellationToken: TestCancellationToken)).IsValidResponse);
        var job = new RetentionJob(_client, _configuration.LockProvider, prefix);

        // Act
        await job.RunAsync(TestCancellationToken);

        // Assert
        Assert.False((await _client.Indices.ExistsAsync(native, cancellationToken: TestCancellationToken)).Exists);
        Assert.False((await _client.Indices.ExistsAsync(replacement, cancellationToken: TestCancellationToken)).Exists);
        Assert.True((await _client.Indices.ExistsAsync(foreign, cancellationToken: TestCancellationToken)).Exists);
    }

    private sealed class RetentionJob : CleanupIndexesJob
    {
        public RetentionJob(ElasticsearchClient client, ILockProvider locks, string prefix)
            : base(client, locks, TimeProvider.System, NullLoggerFactory.Instance)
        {
            AddIndex(prefix, TimeSpan.FromDays(1));
        }
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync(false);
    }

    [Fact]
    public async Task DeleteAsync_WithVersionWildcard_PreservesClosedIndexes()
    {
        // Arrange
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

        // Act
        await index.DeleteAsync();

        // Assert: a version wildcard delete must skip closed partitions it cannot safely inspect
        var closedExists = await _client.Indices.ExistsAsync(closedName, cancellationToken: TestCancellationToken);
        var openExists = await _client.Indices.ExistsAsync(openName, cancellationToken: TestCancellationToken);
        Assert.True(closedExists.Exists, closedExists.GetErrorMessage());
        Assert.False(openExists.Exists, openExists.DebugInformation);
    }

    [Fact]
    public async Task GetCurrentVersionAsync_ExcludesClosedPartitionsButIncludesHiddenOpenPartitions()
    {
        // Arrange
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

        // Act & Assert
        Assert.Equal(2, await index.GetCurrentVersionAsync());
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_ForVersionedIndex_ExcludesSuffixLookalikes()
    {
        // Arrange
        string name = $"compat-discovery-versioned-{Guid.NewGuid():N}";
        using var index = new VersionedIndex<object>(_configuration, name, 1);
        string lookalikeIndex = $"{index.VersionedName}-other";
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{index.VersionedName},{lookalikeIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        await index.ConfigureAsync();
        var createResponse = await _client.Indices.CreateAsync(lookalikeIndex, cancellationToken: TestCancellationToken);
        Assert.True(createResponse.IsValidResponse, createResponse.GetErrorMessage());

        // Act
        var compatibility = await index.GetIndexCompatibilityAsync(TestCancellationToken);

        // Assert
        var info = Assert.Single(compatibility);
        Assert.Equal(index.VersionedName, info.Name);
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_ForVersionedIndex_DiscoversReplacementThroughCanonicalAlias()
    {
        // Arrange
        string name = $"compat-discovery-alias-{Guid.NewGuid():N}";
        using var index = new VersionedIndex<object>(_configuration, name, 1);
        string targetIndex = $"reindexed-v8-{index.VersionedName}";
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{index.VersionedName},{targetIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        var createResponse = await _client.Indices.CreateAsync(targetIndex, d => d.Aliases(a => a
            .Add(index.VersionedName, new Alias())), TestCancellationToken);
        Assert.True(createResponse.IsValidResponse, createResponse.GetErrorMessage());

        // Act
        var compatibility = await index.GetIndexCompatibilityAsync(TestCancellationToken);

        // Assert
        var info = Assert.Single(compatibility);
        Assert.Equal(targetIndex, info.Name);
    }
}
