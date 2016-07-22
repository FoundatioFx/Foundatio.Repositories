using System;
using System.Threading.Tasks;
using Foundatio.Logging;
using Foundatio.Logging.Xunit;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Nest;
using Xunit.Abstractions;
using LogLevel = Foundatio.Logging.LogLevel;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public abstract class ElasticRepositoryTestBase : TestWithLoggingBase {
        protected readonly ElasticConfiguration _configuration;
        protected readonly IElasticClient _client;

        public ElasticRepositoryTestBase(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = LogLevel.Trace;

            _configuration = GetElasticConfiguration(Log);
            _client = _configuration.Client;
        }

        protected abstract ElasticConfiguration GetElasticConfiguration(ILoggerFactory log);

        protected virtual async Task RemoveDataAsync() {
            var minimumLevel = Log.MinimumLevel;
            Log.MinimumLevel = LogLevel.Error;

            _configuration.DeleteIndexes();
            _configuration.ConfigureIndexes();
            await _configuration.Client.RefreshAsync();

            Log.MinimumLevel = minimumLevel;
        }
    }
}