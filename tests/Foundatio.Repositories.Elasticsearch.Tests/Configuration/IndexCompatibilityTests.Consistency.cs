using System;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Lock;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Exceptions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    [Fact]
    public async Task Consistency_UnknownSubmissionIsNotRetriedAcrossNodes()
    {
        var invoker = new SequenceRequestInvoker(
            new StubResponse(503, "{}", Request: "POST /_reindex"),
            new StubResponse(200, "{\"task\":\"node:1\"}", Request: "POST /_reindex"));
        var nodes = new StaticNodePool(new[] { new Uri("http://node-a:9200"), new Uri("http://node-b:9200") });
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(nodes, invoker).DisablePing().MaximumRetries(5));
        var runner = new ElasticReindexTaskRunner(client, TimeProvider.System);
        int terminated = 0;
        await Assert.ThrowsAsync<ElasticReindexTaskUncertainException>(() => runner.RunCompatibilityReindexAsync(
            "source", "target", null, null, (_, _) => Task.CompletedTask, () => terminated++, TestContext.Current.CancellationToken));
        Assert.Single(invoker.Requests);
        Assert.Equal(0, terminated);
        Assert.Equal(1, invoker.RemainingResponses);
    }

    [Theory]
    [InlineData("yellow", false, 1, 0, 0)]
    [InlineData("green", true, 0, 0, 0)]
    [InlineData("green", false, 1, 0, 0)]
    [InlineData("green", false, 0, 1, 0)]
    [InlineData("green", false, 0, 0, 1)]
    public async Task Consistency_HealthDoesNotApproveUnreadyReplicas(string status, bool timedOut, int unassigned, int initializing, int relocating)
    {
        string body = System.Text.Json.JsonSerializer.Serialize(new
        {
            status,
            timed_out = timedOut,
            unassigned_shards = unassigned,
            initializing_shards = initializing,
            relocating_shards = relocating,
            active_primary_shards = 1,
            active_shards = 1
        });
        var invoker = new SequenceRequestInvoker(new StubResponse(200, body, Request: "GET /_cluster/health/target"));
        var time = new FakeTimeProvider();
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker)
            .OnRequestCompleted(_ => time.Advance(TimeSpan.FromMinutes(2))));
        await Assert.ThrowsAsync<RepositoryException>(() => CompatibilityTargetHealth.WaitAsync(
            client, "target", time, TimeSpan.FromMinutes(1), () => Task.CompletedTask, TestContext.Current.CancellationToken));
        Assert.Single(invoker.Requests);
    }

    [Fact]
    public async Task Consistency_HealthPollsUntilDestinationIsGreenAndRenewsLease()
    {
        const string yellow = "{\"status\":\"yellow\",\"timed_out\":true,\"unassigned_shards\":1}";
        const string green = "{\"status\":\"green\",\"timed_out\":false,\"active_primary_shards\":1,\"active_shards\":2}";
        var invoker = new SequenceRequestInvoker(new StubResponse(408, yellow), new StubResponse(200, green));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker));
        int renewals = 0;
        await CompatibilityTargetHealth.WaitAsync(client, "target", TimeProvider.System, TimeSpan.FromMinutes(2),
            () =>
            {
                renewals++;
                return Task.CompletedTask;
            }, TestContext.Current.CancellationToken);
        Assert.Equal(2, renewals);
        Assert.Equal(0, invoker.RemainingResponses);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task Consistency_InvalidHealthTimeoutFailsBeforeRequests(int seconds)
    {
        var invoker = new SequenceRequestInvoker([]);
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var index = new Index<object>(configuration, "employees") { CompatibilityUpgradeHealthTimeout = TimeSpan.FromSeconds(seconds) };
        var upgrader = new ElasticIndexCompatibilityUpgrader(configuration.Client, TimeProvider.System);
        var compatibility = new IndexCompatibilityInfo { Name = "employees", CreatedMajor = 8, ServerMajor = 9, ServerVersion = "9.0.0" };
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => upgrader.ValidateAsync(index, compatibility, TestContext.Current.CancellationToken));
        Assert.Empty(invoker.Requests);
    }

    [Theory]
    [InlineData(5)]
    [InlineData(10)]
    public async Task Consistency_LeaseLossAfterCallbackDoesNotAuthorizeCleanup(int progressToLoseLease)
    {
        var responses = CreateSafetySetupResponses();
        var invoker = new SequenceRequestInvoker([.. responses]);
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker));
        using var configuration = new ElasticConfiguration();
        using var index = new Index<object>(configuration, "employees");
        var compatibility = new IndexCompatibilityInfo { Name = "employees", CreatedMajor = 8, ServerMajor = 9, ServerVersion = "9.0.0" };
        var lease = new ConsistencyLease();
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, TimeProvider.System);
        var exception = await Assert.ThrowsAsync<RepositoryException>(() => upgrader.UpgradeAsync(index, compatibility, lease,
            (progress, _) =>
            {
                if (progress == progressToLoseLease)
                    lease.Lost = true;
                return Task.CompletedTask;
            }, TestContext.Current.CancellationToken));
        Assert.Contains("lease ownership", exception.Message);
        Assert.DoesNotContain(invoker.Requests, r => r.StartsWith("DELETE", StringComparison.Ordinal) || r == "PUT /employees/_settings" || r == "POST /_reindex");
        Assert.Equal(progressToLoseLease is 5 ? 6 : 10, invoker.Requests.Count);
    }

    private sealed class ConsistencyLease : ILock
    {
        public bool Lost { get; set; }
        public string LockId => "lease";
        public string Resource => "migration";
        public DateTime AcquiredTimeUtc => DateTime.UtcNow;
        public TimeSpan TimeWaitedForLock => TimeSpan.Zero;
        public int RenewalCount { get; private set; }
        public Task RenewAsync(TimeSpan? timeUntilExpires = null)
        {
            if (Lost)
                throw new LockException("Lease was acquired by another owner.");
            RenewalCount++;
            return Task.CompletedTask;
        }
        public Task ReleaseAsync() => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
