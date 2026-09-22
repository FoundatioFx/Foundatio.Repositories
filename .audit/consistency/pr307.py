from pathlib import Path
import sys
root = Path(sys.argv[1])

def edit(path, old, new):
    p = root / path
    s = p.read_text(encoding='utf-8-sig')
    assert s.count(old) == 1, (path, old[:120], s.count(old))
    p.write_text(s.replace(old, new))

def write(path, text):
    p = root / path
    p.parent.mkdir(parents=True, exist_ok=True)
    assert not p.exists(), path
    p.write_text(text)

write('tests/Foundatio.Repositories.Elasticsearch.Tests/Configuration/IndexCompatibilityTests.Consistency.cs', '''using System;
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
            new StubResponse(200, "{\\"task\\":\\"node:1\\"}", Request: "POST /_reindex"));
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
            status, timed_out = timedOut, unassigned_shards = unassigned,
            initializing_shards = initializing, relocating_shards = relocating,
            active_primary_shards = 1, active_shards = 1
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
        const string yellow = "{\\"status\\":\\"yellow\\",\\"timed_out\\":true,\\"unassigned_shards\\":1}";
        const string green = "{\\"status\\":\\"green\\",\\"timed_out\\":false,\\"active_primary_shards\\":1,\\"active_shards\\":2}";
        var invoker = new SequenceRequestInvoker(new StubResponse(200, yellow), new StubResponse(200, green));
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
''')
write('src/Foundatio.Repositories.Elasticsearch/Configuration/CompatibilityTargetHealth.cs', '''using System;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

internal static class CompatibilityTargetHealth
{
    public static async Task WaitAsync(ElasticsearchClient client, string targetIndex, TimeProvider timeProvider,
        TimeSpan timeout, Func<Task> renewLeaseAsync, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
        long started = timeProvider.GetTimestamp();
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var remaining = timeout - timeProvider.GetElapsedTime(started);
            if (remaining <= TimeSpan.Zero)
                throw new RepositoryException($"Compatibility destination '{targetIndex}' did not allocate all configured primary and replica shards within {timeout}. No cutover was authorized.");

            await renewLeaseAsync().AnyContext();
            var pollTimeout = remaining < TimeSpan.FromSeconds(30) ? remaining : TimeSpan.FromSeconds(30);
            var response = await client.Cluster.HealthAsync(d => d
                .Indices(targetIndex)
                .WaitForStatus(HealthStatus.Green)
                .WaitForNoInitializingShards()
                .WaitForNoRelocatingShards()
                .Timeout(pollTimeout)
                .RequestConfiguration(r => r.MaxRetries(0).RequestTimeout(pollTimeout + TimeSpan.FromSeconds(5))), cancellationToken).AnyContext();
            if (!response.IsValidResponse)
                throw new RepositoryException($"Unable to establish replica readiness for compatibility destination '{targetIndex}'. {response.DebugInformation}");

            if (!response.TimedOut && response.Status is HealthStatus.Green
                && response.UnassignedShards is 0 && response.InitializingShards is 0 && response.RelocatingShards is 0)
                return;
        }
    }
}
''')
edit('src/Foundatio.Repositories.Elasticsearch/Configuration/Index.cs', '    public float? ReindexRequestsPerSecond { get; set; }', '''    public float? ReindexRequestsPerSecond { get; set; }

    /// <summary>
    /// Maximum time to wait for the compatibility destination's configured replicas before destructive cutover.
    /// Defaults to 30 minutes. Must be positive. Short health polls renew the migration lease while waiting.
    /// A destination with zero configured replicas can be green; this preserves, rather than increases, the source policy.
    /// </summary>
    public TimeSpan CompatibilityUpgradeHealthTimeout { get; set; } = TimeSpan.FromMinutes(30);''')
up = 'src/Foundatio.Repositories.Elasticsearch/Configuration/ElasticIndexCompatibilityUpgrader.cs'
edit(up, '    private readonly ElasticsearchClient _client;', '    private readonly ElasticsearchClient _client;\n    private readonly TimeProvider _timeProvider;')
edit(up, '        _client = client;', '        _client = client;\n        _timeProvider = timeProvider;')
edit(up, '        EnsureCreateFromSupported(compatibility.ServerVersion);', '''        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(index.CompatibilityUpgradeHealthTimeout, TimeSpan.Zero);
        EnsureCreateFromSupported(compatibility.ServerVersion);''')
edit(up, '        async Task ReportProgressAsync(int progress, string? message)', '''        bool leaseOwnershipUncertain = false;
        async Task RenewLeaseAsync()
        {
            try
            {
                await reindexLock.RenewAsync().WaitAsync(TimeSpan.FromSeconds(30)).AnyContext();
            }
            catch
            {
                leaseOwnershipUncertain = true;
                throw;
            }
        }

        async Task ReportProgressAsync(int progress, string? message)''')
edit(up, '            await reindexLock.RenewAsync().AnyContext();\n            _logger.LogInformation', '            await RenewLeaseAsync().AnyContext();\n            _logger.LogInformation')
edit(up, '            }\n        }\n\n        bool workflowAttempted', '            }\n\n            await RenewLeaseAsync().AnyContext();\n        }\n\n        bool workflowAttempted')
for mutation in ['await AddWorkflowMarkerAsync(sourceIndex, false, cancellationToken)', 'await CreateTargetAsync(sourceIndex, targetIndex, cancellationToken)', 'var reindexResult = await _reindexTaskRunner.RunCompatibilityReindexAsync(', 'await RemoveWriteBlockAsync(targetIndex, cancellationToken)', 'await RemoveWorkflowMarkerAsync(targetIndex, cancellationToken)']:
    edit(up, '            ' + mutation, '            await RenewLeaseAsync().AnyContext();\n            ' + mutation)
edit(up, '            await WaitForTargetHealthAsync(targetIndex, cancellationToken).AnyContext();', '''            await CompatibilityTargetHealth.WaitAsync(_client, targetIndex, _timeProvider,
                index.CompatibilityUpgradeHealthTimeout, RenewLeaseAsync, cancellationToken).AnyContext();''')
edit(up, '            var cutoverResponse = await _client.Indices.UpdateAliasesAsync(a => a.Actions(aliasActions), cancellationToken).AnyContext();', '''            await RenewLeaseAsync().AnyContext();
            var cutoverResponse = await _client.Indices.UpdateAliasesAsync(a => a.Actions(aliasActions)
                .RequestConfiguration(r => r.MaxRetries(0)), cancellationToken).AnyContext();''')
edit(up, '            if (!workflowAttempted)\n                throw;', '''            if (!workflowAttempted)
                throw;

            if (leaseOwnershipUncertain)
                throw new RepositoryException($"Compatibility upgrade for '{sourceIndex}' lost confirmed lease ownership. No automatic reset, unblocking, or recovery was attempted; inspect the retained artifacts.", upgradeException);''')
edit(up, '                    await _recovery.ResetCurrentAttemptAsync', '                    await RenewLeaseAsync().AnyContext();\n                    await _recovery.ResetCurrentAttemptAsync')
edit(up, '                await _recovery.RecoverUnderLockAsync', '                await RenewLeaseAsync().AnyContext();\n                await _recovery.RecoverUnderLockAsync')
p = root / up
s = p.read_text()
start = s.index('    private async Task WaitForTargetHealthAsync(')
end = s.index('    private static IReadOnlyDictionary<string, Alias> CreateAliasActions', start)
p.write_text(s[:start] + s[end:])
edit('src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexTaskRunner.cs', 'request => request.OpaqueId(opaqueId)', 'request => request.OpaqueId(opaqueId).MaxRetries(0)')
for name in ['docs/guide/index-management.md', '.agents/skills/foundatio-repositories/references/index-lifecycle.md']:
    with (root / name).open('a') as f:
        f.write('''\n\n### Compatibility durability and uncertain ownership\n\nCompatibility cutover now waits for the specific destination to become green after its original replica settings are restored. Yellow is not sufficient: unassigned replicas cannot replace the redundancy of a source about to be deleted. `Index.CompatibilityUpgradeHealthTimeout` defaults to 30 minutes and must be positive. Bounded health polls renew the lease while waiting. Green with zero configured replicas still provides no redundancy; the migration preserves the configured policy rather than creating one.\n\nCompatibility asynchronous copy submission and cutover disable client transport retries. Proxies must not retry these non-idempotent operations either. `X-Opaque-Id` correlates task lineage, not an idempotency key. Unknown submissions and task 404s retain uncertainty. A failed lease renewal forbids automatic reset, recovery and unblocking; the source and target remain for inspection. The Foundatio dependency must also surface a failed compare-and-renew: a heartbeat is not a storage-enforced fencing token, and cannot revoke requests already dispatched.\n''')
