using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Messaging;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Utility;
using Foundatio.Xunit;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;
using IAsyncLifetime = Xunit.IAsyncLifetime;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public abstract class ElasticRepositoryTestBase : TestWithLoggingBase, IAsyncLifetime
{
    protected readonly MyAppElasticConfiguration _configuration;
    protected readonly InMemoryCacheClient _cache;
    protected readonly ElasticsearchClient _client;
    protected readonly IQueue<WorkItemData> _workItemQueue;
    protected readonly InMemoryMessageBus _messageBus;

    public ElasticRepositoryTestBase(ITestOutputHelper output) : base(output)
    {
        Log.SetLogLevel<ScheduledTimer>(LogLevel.Warning);

        _cache = new InMemoryCacheClient(new InMemoryCacheClientOptions { LoggerFactory = Log });
        _messageBus = new InMemoryMessageBus(new InMemoryMessageBusOptions { LoggerFactory = Log });
        _workItemQueue = new InMemoryQueue<WorkItemData>(new InMemoryQueueOptions<WorkItemData> { LoggerFactory = Log });
        _configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log);
        _client = _configuration.Client;
    }

    private static bool _elasticsearchReady;
    public virtual async Task InitializeAsync()
    {
        if (!_elasticsearchReady)
            await _client.WaitForReadyAsync(new CancellationTokenSource(TimeSpan.FromMinutes(1)).Token);

        _elasticsearchReady = true;
    }

    protected virtual async Task RemoveDataAsync(bool configureIndexes = true)
    {
        var minimumLevel = Log.DefaultLogLevel;
        Log.DefaultLogLevel = LogLevel.Warning;

        var sw = Stopwatch.StartNew();
        _logger.LogInformation("Starting remove data");

        await _workItemQueue.DeleteQueueAsync();
        await _configuration.DeleteIndexesAsync();
        await DeleteWildcardIndicesAsync("employee*");
        if (configureIndexes)
            await _configuration.ConfigureIndexesAsync(null, false);

        await _cache.RemoveAllAsync();
        _cache.ResetStats();
        await _client.Indices.RefreshAsync(Indices.All);
        _messageBus.ResetMessagesSent();
        sw.Stop();
        _logger.LogInformation("Done removing data {Duration:g}", sw.Elapsed);

        Log.DefaultLogLevel = minimumLevel;
    }

    protected async Task DeleteWildcardIndicesAsync(string pattern)
    {
        // Resolve wildcards to actual index names to avoid issues with action.destructive_requires_name=true
        var resolveResponse = await _client.Indices.ResolveIndexAsync(pattern);
        if (resolveResponse.IsValidResponse && resolveResponse.Indices != null && resolveResponse.Indices.Count > 0)
        {
            var indexNames = resolveResponse.Indices.Select(i => i.Name).ToArray();
            await _client.Indices.DeleteAsync((Indices)indexNames, i => i.IgnoreUnavailable());
        }
    }

    public virtual Task DisposeAsync() => Task.CompletedTask;
}
