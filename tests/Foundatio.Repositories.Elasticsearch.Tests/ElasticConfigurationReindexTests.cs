using System;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Messaging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Resilience;
using Foundatio.Serializer;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ElasticConfigurationReindexTests
{
    [Fact]
    public void Constructor_RetainsOriginalSevenParameterSignature()
    {
        var constructor = typeof(ElasticConfiguration).GetConstructor([
            typeof(IQueue<WorkItemData>), typeof(ICacheClient), typeof(IMessageBus),
            typeof(ITextSerializer), typeof(TimeProvider), typeof(IResiliencePolicyProvider), typeof(ILoggerFactory)
        ]);

        Assert.NotNull(constructor);
        using var configuration = (ElasticConfiguration)constructor.Invoke([null, null, null, null, null, null, null]);
        Assert.NotNull(configuration.LockProvider);
    }

    [Fact]
    public async Task ReindexAsync_WhenCancelled_InvalidatesMarkerAndStopsRemainingIndexes()
    {
        using var configuration = new ElasticConfiguration();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var first = new ObservedIndex(configuration, "first", token =>
        {
            cancellation.Cancel();
            token.ThrowIfCancellationRequested();
        });
        var second = new ObservedIndex(configuration, "second");
        configuration.AddIndex(first);
        configuration.AddIndex(second);

        await configuration.ConfigureIndexesAsync(beginReindexingOutdated: false);
        await configuration.ConfigureIndexesAsync(beginReindexingOutdated: false);
        Assert.Equal(1, first.ConfigureCalls);
        Assert.Equal(1, second.ConfigureCalls);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => configuration.ReindexAsync(cancellationToken: cancellation.Token));

        Assert.Equal(1, first.ReindexCalls);
        Assert.Equal(0, second.ReindexCalls);
        await configuration.ConfigureIndexesAsync(beginReindexingOutdated: false);
        Assert.Equal(2, first.ConfigureCalls);
        Assert.Equal(2, second.ConfigureCalls);
    }

    [Fact]
    public async Task ReindexAsync_WhenIncomplete_InvalidatesMarkerWithoutRetrying()
    {
        using var configuration = new ElasticConfiguration();
        var failure = new ReindexIncompleteException("first-v1", "first-v2", "simulated incomplete copy");
        var index = new ObservedIndex(configuration, "first", _ => throw failure);
        configuration.AddIndex(index);
        await configuration.ConfigureIndexesAsync(beginReindexingOutdated: false);

        var aggregate = await Assert.ThrowsAsync<AggregateException>(() => configuration.ReindexAsync(cancellationToken: TestContext.Current.CancellationToken));

        Assert.Same(failure, Assert.Single(aggregate.InnerExceptions));
        Assert.Equal(1, index.ReindexCalls);
        await configuration.ConfigureIndexesAsync(beginReindexingOutdated: false);
        Assert.Equal(2, index.ConfigureCalls);
    }

    private sealed class ObservedIndex(ElasticConfiguration configuration, string name, Action<CancellationToken>? reindex = null)
        : VersionedIndex(configuration, name, 2)
    {
        public int ConfigureCalls { get; private set; }
        public int ReindexCalls { get; private set; }

        public override Task<int> GetCurrentVersionAsync() => Task.FromResult(1);

        public override Task ConfigureAsync()
        {
            ConfigureCalls++;
            return Task.CompletedTask;
        }

        public override Task MaintainAsync(bool includeOptionalTasks = true) => Task.CompletedTask;

        public override Task ReindexAsync(Func<int, string?, Task>? progressCallbackAsync = null, CancellationToken cancellationToken = default)
        {
            ReindexCalls++;
            reindex?.Invoke(cancellationToken);
            return Task.CompletedTask;
        }
    }
}
