using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Messaging;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class DeleteByQueryRetryTests
{
    [Theory]
    [InlineData(1, 0, 1000, 1)]
    [InlineData(1, 3, 1000, 2)]
    [InlineData(0, 3, 997, 1)]
    public async Task RemoveAllAsync_CountsRetriesAndNotifiesOnce(int retryCount, long conflicts, long expectedDeleted, int expectedRequests)
    {
        var messages = new List<string>();
        using var loggerFactory = LoggerFactory.Create(builder => builder.AddProvider(new CapturingLoggerProvider(messages)));
        var requests = new List<string>();
        string Body(long deleted, long versionConflicts) => JsonSerializer.Serialize(new
        {
            took = 1, timed_out = false, total = 1000, deleted, batches = 1, version_conflicts = versionConflicts,
            noops = 0, retries = new { bulk = 0, search = 0 }, throttled_millis = 0, requests_per_second = -1, throttled_until_millis = 0, failures = Array.Empty<object>()
        });
        var settings = new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://in-memory.invalid:9200")),
                new SequenceRequestInvoker((200, Body(1000 - conflicts, conflicts)), (200, Body(conflicts, 0))))
            .MaximumRetries(0).OnRequestCompleted(details => requests.Add(details.Uri!.AbsolutePath));
        using var configuration = new StubConfiguration(new ElasticsearchClient(settings), loggerFactory);
        using var repository = new UncachedRepository(new StubIndex(configuration));

        long deleted = await repository.RemoveAllAsync(q => q, o => o.ImmediateConsistency().Retry(retryCount));

        Assert.Equal(expectedDeleted, deleted);
        Assert.Equal(expectedRequests, requests.Count);
        Assert.All(requests, path => Assert.EndsWith("/_delete_by_query", path));
        Assert.Equal(2, ((InMemoryMessageBus)configuration.MessageBus).MessagesSent);
        Assert.Equal(retryCount is 0 && conflicts > 0, messages.Any(m => m.Contains("unresolved version conflicts", StringComparison.Ordinal)));
    }

    private sealed class StubIndex(ElasticConfiguration configuration) : Configuration.Index<Identity>(configuration, "identity")
    {
        protected override ElasticMappingResolver CreateMappingResolver() => ElasticMappingResolver.NullInstance;
    }

    private sealed class StubConfiguration(ElasticsearchClient client, ILoggerFactory loggerFactory) : ElasticConfiguration(loggerFactory: loggerFactory)
    {
        protected override ElasticsearchClient CreateElasticClient() => client;
    }

    private sealed class UncachedRepository : ElasticRepositoryBase<Identity>
    {
        public UncachedRepository(IIndex index) : base(index) => DisableCache();
    }
}
