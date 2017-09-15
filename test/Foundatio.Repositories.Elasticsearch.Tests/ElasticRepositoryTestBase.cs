using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Logging.Xunit;
using Foundatio.Messaging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Utility;
using Nest;
using Xunit.Abstractions;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public abstract class ElasticRepositoryTestBase : TestWithLoggingBase {
        protected readonly MyAppElasticConfiguration _configuration;
        protected readonly InMemoryCacheClient _cache;
        protected readonly IElasticClient _client;
        protected readonly IQueue<WorkItemData> _workItemQueue;
        protected readonly InMemoryMessageBus _messageBus;

        public ElasticRepositoryTestBase(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = LogLevel.Trace;
            Log.SetLogLevel<ScheduledTimer>(LogLevel.Warning);

            _cache = new InMemoryCacheClient(new InMemoryCacheClientOptions { LoggerFactory = Log });
            _messageBus = new InMemoryMessageBus(new InMemoryMessageBusOptions { LoggerFactory = Log });
            _workItemQueue = new InMemoryQueue<WorkItemData>(new InMemoryQueueOptions<WorkItemData> { LoggerFactory = Log });
            _configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log);
            _client = _configuration.Client;
        }

        protected virtual async Task RemoveDataAsync(bool configureIndexes = true) {
            var minimumLevel = Log.MinimumLevel;
            Log.MinimumLevel = LogLevel.Warning;

            await _workItemQueue.DeleteQueueAsync();
            await _configuration.DeleteIndexesAsync();
            if (configureIndexes)
                await _configuration.ConfigureIndexesAsync(null, false);

            await _cache.RemoveAllAsync();
            await _client.RefreshAsync(Indices.All);
            _messageBus.ResetMessagesSent();

            Log.MinimumLevel = minimumLevel;
        }
    }
}