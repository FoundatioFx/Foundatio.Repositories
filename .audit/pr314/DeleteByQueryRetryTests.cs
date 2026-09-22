using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport;
using Foundatio.Caching;
using Foundatio.Messaging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Xunit;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class DeleteByQueryRetryTests : TestWithLoggingBase
{
    public DeleteByQueryRetryTests(ITestOutputHelper output) : base(output) { }

    [Fact]
    public async Task AccumulatesDeletedCountAndNotifiesOnceAcrossRetriesAsync()
    {
        using var invoker = new ScriptedRequestInvoker([(7, 3), (2, 1), (1, 0)]);
        using var configuration = new TestConfiguration(invoker, Log);
        using var repository = new TestRepository(configuration);

        long deleted = await repository.RemoveAllAsync(o => o.ImmediateConsistency().Retry(2));

        Assert.Equal(10, deleted);
        Assert.Equal(3, invoker.DeleteRequests.Count);
        Assert.Empty(invoker.RemainingResponses);
        Assert.Equal(2, Assert.IsType<InMemoryMessageBus>(configuration.MessageBus).MessagesSent);
        Assert.All(invoker.DeleteRequests, uri =>
        {
            Assert.Equal("/identity/_delete_by_query", uri.AbsolutePath);
            Assert.Contains("conflicts=proceed", uri.Query);
            Assert.Contains("refresh=true", uri.Query);
        });
        Assert.DoesNotContain(Log.LogEntries, entry => entry.Message.Contains("unresolved version conflicts", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    public async Task ReturnsCumulativePartialCountAtExactRetryBudgetAsync(int retries)
    {
        using var invoker = new ScriptedRequestInvoker(Enumerable.Repeat((2L, 1L), retries + 1));
        using var configuration = new TestConfiguration(invoker, Log);
        using var repository = new TestRepository(configuration);

        long deleted = await repository.RemoveAllAsync(o => o.Retry(retries));

        Assert.Equal(2L * (retries + 1), deleted);
        Assert.Equal(retries + 1, invoker.DeleteRequests.Count);
        Assert.Empty(invoker.RemainingResponses);
        Assert.Equal(2, Assert.IsType<InMemoryMessageBus>(configuration.MessageBus).MessagesSent);
        Assert.Contains(Log.LogEntries, entry => entry.LogLevel == LogLevel.Warning
            && entry.Message.Contains($"after {retries + 1} attempts (1 unresolved version conflicts)", StringComparison.Ordinal));
    }

    [Fact]
    public async Task DefaultBudgetAllowsTenRetriesAsync()
    {
        using var invoker = new ScriptedRequestInvoker(Enumerable.Repeat((1L, 1L), 11));
        using var configuration = new TestConfiguration(invoker, Log);
        using var repository = new TestRepository(configuration);

        Assert.Equal(11, await repository.RemoveAllAsync());
        Assert.Equal(11, invoker.DeleteRequests.Count);
        Assert.Empty(invoker.RemainingResponses);
    }

    [Fact]
    public async Task StopsAfterFirstResponseWithoutConflictsAsync()
    {
        using var invoker = new ScriptedRequestInvoker([(5, 0)]);
        using var configuration = new TestConfiguration(invoker, Log);
        using var repository = new TestRepository(configuration);

        Assert.Equal(5, await repository.RemoveAllAsync());
        Assert.Single(invoker.DeleteRequests);
        Assert.DoesNotContain(Log.LogEntries, entry => entry.Message.Contains("unresolved version conflicts", StringComparison.Ordinal));
    }

    [Fact]
    public async Task DoesNotNotifyWhenNoDocumentsWereDeletedAsync()
    {
        using var invoker = new ScriptedRequestInvoker([(0, 1), (0, 0)]);
        using var configuration = new TestConfiguration(invoker, Log);
        using var repository = new TestRepository(configuration);

        Assert.Equal(0, await repository.RemoveAllAsync());
        Assert.Equal(2, invoker.DeleteRequests.Count);
        Assert.Equal(0, Assert.IsType<InMemoryMessageBus>(configuration.MessageBus).MessagesSent);
    }

    [Fact]
    public async Task RespectsDisabledNotificationsAcrossRetriesAsync()
    {
        using var invoker = new ScriptedRequestInvoker([(1, 1), (1, 0)]);
        using var configuration = new TestConfiguration(invoker, Log);
        using var repository = new TestRepository(configuration);

        Assert.Equal(2, await repository.RemoveAllAsync(o => o.Notifications(false)));
        Assert.Equal(2, invoker.DeleteRequests.Count);
        Assert.Equal(0, Assert.IsType<InMemoryMessageBus>(configuration.MessageBus).MessagesSent);
    }

    [Fact]
    public async Task PropagatesRequestFailureWithoutConflictRetriesAsync()
    {
        using var invoker = new ScriptedRequestInvoker([]) { FailRequest = true };
        using var configuration = new TestConfiguration(invoker, Log);
        using var repository = new TestRepository(configuration);

        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.RemoveAllAsync());

        Assert.Contains("scripted failure", exception.Message);
        Assert.Single(invoker.DeleteRequests);
        Assert.Equal(0, Assert.IsType<InMemoryMessageBus>(configuration.MessageBus).MessagesSent);
    }

    private sealed class TestRepository : ElasticRepositoryBase<Identity>
    {
        public TestRepository(TestConfiguration configuration) : base(configuration.Identities)
        {
            DisableCache();
        }
    }

    private sealed class TestConfiguration : ElasticConfiguration
    {
        private readonly ScriptedRequestInvoker _invoker;

        public TestConfiguration(ScriptedRequestInvoker invoker, ILoggerFactory loggerFactory)
            : base(cacheClient: new NullCacheClient(), loggerFactory: loggerFactory)
        {
            _invoker = invoker;
            AddIndex(Identities = new IdentityIndex(this));
        }

        public IdentityIndex Identities { get; }

        protected override ElasticsearchClient CreateElasticClient()
        {
            var settings = new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), _invoker)
                .DisableDirectStreaming()
                .DisablePing()
                .MaximumRetries(0);
            foreach (var index in Indexes)
                index.ConfigureSettings(settings);

            return new ElasticsearchClient(settings);
        }
    }

    // Exercise the real client serializer and repository retry loop without a concurrent writer or a server.
    private sealed class ScriptedRequestInvoker : InMemoryRequestInvoker, IRequestInvoker
    {
        public ScriptedRequestInvoker(IEnumerable<(long Deleted, long Conflicts)> responses)
            : base(null, headers: new Dictionary<string, IEnumerable<string>> { ["X-Elastic-Product"] = ["Elasticsearch"] })
        {
            RemainingResponses = new Queue<(long Deleted, long Conflicts)>(responses);
        }

        public Queue<(long Deleted, long Conflicts)> RemainingResponses { get; }
        public List<Uri> DeleteRequests { get; } = [];
        public bool FailRequest { get; init; }

        TResponse IRequestInvoker.Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
        {
            throw new InvalidOperationException($"Unexpected synchronous request: {endpoint.Uri}");
        }

        Task<TResponse> IRequestInvoker.RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (typeof(TResponse) == typeof(GetMappingResponse))
            {
                byte[] mapping = Encoding.UTF8.GetBytes("""
                    {"identity":{"mappings":{"properties":{"id":{"type":"keyword"}}}}}
                    """);
                return BuildResponseAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken, mapping);
            }

            if (typeof(TResponse) != typeof(DeleteByQueryResponse))
                throw new InvalidOperationException($"Unexpected response type: {typeof(TResponse).Name}, request: {endpoint.Uri}");

            DeleteRequests.Add(endpoint.Uri);
            if (FailRequest)
            {
                byte[] error = Encoding.UTF8.GetBytes("""
                    {"error":{"type":"internal_server_error","reason":"scripted failure"},"status":500}
                    """);
                return BuildResponseAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken, error, 500);
            }

            if (!RemainingResponses.TryDequeue(out var response))
                throw new InvalidOperationException("The repository exceeded the scripted delete-by-query request budget.");

            byte[] body = JsonSerializer.SerializeToUtf8Bytes(new
            {
                took = 1,
                timed_out = false,
                total = response.Deleted + response.Conflicts,
                deleted = response.Deleted,
                batches = 1,
                version_conflicts = response.Conflicts,
                noops = 0,
                retries = new { bulk = 0, search = 0 },
                throttled_millis = 0,
                requests_per_second = -1,
                throttled_until_millis = 0,
                failures = Array.Empty<object>()
            });
            return BuildResponseAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken, body);
        }
    }
}
