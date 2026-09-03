using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Transport;
using Foundatio.Caching;
using Foundatio.Lock;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Exceptions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    private sealed class CustomDateIndex : DailyIndex<object>
    {
        public CustomDateIndex(IElasticConfiguration configuration) : base(configuration, "logs", 1) { }
        protected override DateTime GetIndexDate(string name) => new(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        protected override int GetIndexVersion(string name) => 1;
        public async Task<int> CountDiscoveredIndexesAsync() => (await GetIndexesAsync()).Count;
        public TypeMapping? GetLatestMapping() => GetLatestIndexMapping();
    }

    [Fact]
    public async Task GetIndexesAsync_WithCustomNaming_UsesVirtualDateAndVersionParsers()
    {
        var invoker = new SequenceRequestInvoker(new StubResponse(200, """
            {"logs-v1-2024.01.01-archive":{"aliases":{"logs-2024.01.01":{},"logs":{}}}}
            """));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var index = new CustomDateIndex(configuration);
        configuration.AddIndex(index);

        Assert.Equal(1, await index.CountDiscoveredIndexesAsync());
        Assert.Equal(1, configuration.RequestCount);
    }

    [Fact]
    public void GetLatestIndexMapping_WithCustomNaming_UsesVirtualDateAndVersionParsers()
    {
        var invoker = new SequenceRequestInvoker(
            new StubResponse(200, """{"logs-v1-2024.01.01-archive":{"aliases":{"logs-2024.01.01":{},"logs":{}}}}"""),
            new StubResponse(200, """{"logs-v1-2024.01.01-archive":{"mappings":{"properties":{"message":{"type":"keyword"}}}}}"""));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var index = new CustomDateIndex(configuration);
        configuration.AddIndex(index);

        var mapping = index.GetLatestMapping();

        Assert.NotNull(mapping);
        Assert.IsType<KeywordProperty>(mapping.Properties!["message"]);
        Assert.Equal(2, configuration.RequestCount);
    }

    [Theory]
    [InlineData("{}", 0)]
    [InlineData("{\"logs-v1-2024.01.01\":{}}", 1)]
    public async Task GetIndexesAsync_WithGeneratedName_StillRequiresCanonicalAlias(string aliases, int expectedCount)
    {
        var invoker = new SequenceRequestInvoker(new StubResponse(200, """
            {"reindexed-v9-logs-v1-2024.01.01":{"aliases":ALIASES}}
            """.Replace("ALIASES", aliases, StringComparison.Ordinal)));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var index = new CustomDateIndex(configuration);
        configuration.AddIndex(index);

        Assert.Equal(expectedCount, await index.CountDiscoveredIndexesAsync());
        Assert.Equal(1, configuration.RequestCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RecoverUnderLockAsync_WithOnlyMarkedSource_LeavesAmbiguousStateUntouched(bool blocked)
    {
        var invoker = new SequenceRequestInvoker(
            new StubResponse(200, """{"version":{"number":"9.0.0"}}"""),
            new StubResponse(200, """
                {"employees":{"aliases":{".foundatio-compatibility-upgrade":{"is_hidden":true}},"settings":{"index":{"blocks":{"write":BLOCKED}}}}}
                """.Replace("BLOCKED", blocked ? "true" : "false", StringComparison.Ordinal)),
            new StubResponse(200, """{"nodes":{}}"""));
        var methods = new List<string>();
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker)
            .OnRequestCompleted(call => methods.Add(call.HttpMethod.ToString())));
        var recovery = new ElasticIndexCompatibilityRecovery(client, null, NullLogger.Instance);
        using var configuration = new ElasticConfiguration();
        using var index = new Index<object>(configuration, "employees");

        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            recovery.RecoverUnderLockAsync(index, "employees", TestContext.Current.CancellationToken));

        Assert.Contains("ManualIntervention", exception.Message);
        Assert.Equal(["GET", "GET", "GET"], methods);
    }

    [Fact]
    public async Task UpgradeAsync_WhenCreateOutcomeIsUnknownAndTargetAbsent_PreservesWriteFence()
    {
        const string info = """{"version":{"number":"9.0.0"}}""";
        const string markedSource = """{"employees":{"aliases":{".foundatio-compatibility-upgrade":{"is_hidden":true}},"settings":{"index":{"blocks":{"write":"true"}}}}}""";
        var invoker = new SequenceRequestInvoker(
            new StubResponse(404, """{"error":{"type":"index_not_found_exception"},"status":404}"""),
            new StubResponse(200, """{"employees":{"aliases":{},"mappings":{"_source":{"enabled":true}},"settings":{}}}"""),
            new StubResponse(200, """{"employees":{"settings":{}}}"""),
            new StubResponse(200, """{"acknowledged":true}"""),
            new StubResponse(200, """{"acknowledged":true,"shards_acknowledged":true,"indices":[{"name":"employees","blocked":true}]}"""),
            new StubResponse(200, """{"_shards":{"total":1,"successful":1,"failed":0}}"""),
            new StubResponse(500, "", new TimeoutException("The _create_from response was not received.")),
            new StubResponse(200, info), new StubResponse(200, markedSource), new StubResponse(200, """{"nodes":{}}"""),
            new StubResponse(200, info), new StubResponse(200, markedSource), new StubResponse(200, """{"nodes":{}}"""),
            new StubResponse(200, """{"acknowledged":true}"""),
            new StubResponse(200, """{"acknowledged":true}"""),
            new StubResponse(200, info), new StubResponse(200, """{"employees":{"aliases":{},"settings":{"index":{}}}}"""), new StubResponse(200, """{"nodes":{}}"""));
        var requests = new List<string>();
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker)
            .OnRequestCompleted(call => requests.Add($"{call.HttpMethod} {call.Uri?.AbsolutePath}")));
        using var configuration = new ElasticConfiguration();
        using var index = new Index<object>(configuration, "employees");
        using var cache = new InMemoryCacheClient();
        var locks = new ThrottlingLockProvider(cache);
        await using var reindexLock = await locks.AcquireAsync("compatibility-upgrade", cancellationToken: TestContext.Current.CancellationToken);
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, TimeProvider.System);
        var compatibility = new IndexCompatibilityInfo { Name = "employees", CreatedMajor = 8, ServerMajor = 9, ServerVersion = "9.0.0" };

        var exception = await Assert.ThrowsAsync<RepositoryException>(() => upgrader.UpgradeAsync(index, compatibility, reindexLock, (_, _) => Task.CompletedTask, CancellationToken.None));

        Assert.Contains("ManualIntervention", exception.Message);
        Assert.Contains("_create_from response was not received", exception.ToString());
        Assert.DoesNotContain("PUT /employees/_settings", requests);
        Assert.Equal(10, requests.Count);
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("{\"X-Opaque-Id\":\"operator-retry\"}")]
    public async Task InspectAsync_WithUnidentifiedActiveTask_RequiresManualIntervention(string headers)
    {
        var invoker = new SequenceRequestInvoker(
            new StubResponse(200, """{"version":{"number":"9.0.0"}}"""),
            new StubResponse(200, """
                {
                  "employees":{"aliases":{".foundatio-compatibility-upgrade":{"is_hidden":true}},"settings":{"index":{"blocks":{"write":"true"}}}},
                  "reindexed-v9-employees":{"aliases":{".foundatio-compatibility-upgrade":{"is_hidden":true}},"settings":{"index":{}}}
                }
                """),
            new StubResponse(200, """
                {"nodes":{"node-1":{"name":"node","transport_address":"127.0.0.1:9300","host":"host","ip":"127.0.0.1","roles":[],"attributes":{},"tasks":{
                  "node-1:1":{"node":"node-1","id":1,"type":"transport","action":"indices:data/write/reindex","status":{},"description":"reindex from [employees] to [reindexed-v9-employees]","start_time_in_millis":1,"running_time_in_nanos":1,"cancellable":true,"cancelled":false,"headers":HEADERS}
                }}}}
                """.Replace("HEADERS", headers, StringComparison.Ordinal)));
        var methods = new List<string>();
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker)
            .OnRequestCompleted(call => methods.Add(call.HttpMethod.ToString())));
        var recovery = new ElasticIndexCompatibilityRecovery(client, null, NullLogger.Instance);
        using var configuration = new ElasticConfiguration();
        using var index = new Index<object>(configuration, "employees");

        var status = await recovery.InspectAsync(index, "employees", TestContext.Current.CancellationToken);

        Assert.Equal(["GET", "GET", "GET"], methods);
        Assert.Equal(IndexCompatibilityRecoveryAction.ManualIntervention, status.Action);
        Assert.Null(status.ActiveReindexTaskCount);
    }
}
