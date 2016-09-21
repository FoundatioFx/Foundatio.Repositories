using System;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Logging;
using Foundatio.Logging.Xunit;
using Foundatio.Messaging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Utility;
using Nest;
using Xunit.Abstractions;
using LogLevel = Foundatio.Logging.LogLevel;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public abstract class ElasticRepositoryTestBase : TestWithLoggingBase {
        protected readonly MyAppElasticConfiguration _configuration;
        protected readonly InMemoryCacheClient _cache;
        protected readonly IElasticClient _client;
        protected readonly IQueue<WorkItemData> _workItemQueue;
        protected readonly InMemoryMessageBus _messgeBus;

        public ElasticRepositoryTestBase(ITestOutputHelper output) : base(output) {
            SystemClock.Reset();
            Log.MinimumLevel = LogLevel.Trace;
            Log.SetLogLevel<ScheduledTimer>(LogLevel.Warning);

            _cache = new InMemoryCacheClient(Log);
            _messgeBus = new InMemoryMessageBus(Log);
            _workItemQueue = new InMemoryQueue<WorkItemData>(loggerFactory: Log);
            _configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messgeBus, Log);
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
            await _client.RefreshAsync();

            Log.MinimumLevel = minimumLevel;
        }
    }
}