using System;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Logging.Xunit;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Nest;
using Xunit.Abstractions;
using LogLevel = Foundatio.Logging.LogLevel;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public abstract class ElasticRepositoryTestBase : TestWithLoggingBase {
        protected readonly InMemoryCacheClient _cache;
        protected readonly ElasticConfiguration _configuration;
        protected readonly IElasticClient _client;

        public ElasticRepositoryTestBase(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = LogLevel.Trace;

            _cache = new InMemoryCacheClient(Log);
            _configuration = GetElasticConfiguration();
            _client = _configuration.Client;
        }

        protected abstract ElasticConfiguration GetElasticConfiguration();

        protected virtual async Task RemoveDataAsync() {
            var minimumLevel = Log.MinimumLevel;
            Log.MinimumLevel = LogLevel.Error;

            await _cache.RemoveAllAsync();

            _configuration.DeleteIndexes();
            _configuration.ConfigureIndexes();
            await _configuration.Client.RefreshAsync();

            Log.MinimumLevel = minimumLevel;
        }
    }
}