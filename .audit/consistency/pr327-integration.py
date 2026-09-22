from pathlib import Path
import sys
root=Path(sys.argv[1])
p=root/'tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexConsistencyIntegrationTests.cs'
p.write_text(r'''using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Utility;
using Xunit;
using HttpMethod = Elastic.Transport.HttpMethod;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ReindexConsistencyIntegrationTests : ElasticRepositoryTestBase
{
    public ReindexConsistencyIntegrationTests(ITestOutputHelper output) : base(output) { }

    [Fact]
    public async Task RoutedDuplicates_DeleteOnlyTheMissingRoutingAndKeepRetiredSourceFenced()
    {
        var item = await CreateFixtureAsync(true);
        await using var cleanup = new AsyncDisposableAction(() => CleanupAsync(item));
        var (first, second) = await GetDistinctRoutesAsync(item.OldIndex);
        await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/shared?routing={first}&refresh=true", new { value = "first" });
        await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/shared?routing={second}&refresh=true", new { value = "second" });
        bool deleted = false;
        var reindexer = new ElasticReindexer(_client, _configuration.Serializer);
        await reindexer.ReindexAsync(item, async (progress, _) =>
        {
            if (progress is 91 && !deleted)
            {
                deleted = true;
                await RawAsync(HttpMethod.DELETE, $"/{item.OldIndex}/_doc/shared?routing={first}&refresh=true");
            }
        }, TestCancellationToken);
        Assert.True(deleted);
        Assert.Equal(404, (await RawAsync(HttpMethod.GET, $"/{item.NewIndex}/_doc/shared?routing={first}", allowFailure: true)).ApiCallDetails.HttpStatusCode);
        using var survivor = JsonDocument.Parse((await RawAsync(HttpMethod.GET, $"/{item.NewIndex}/_doc/shared?routing={second}")).Body!);
        Assert.Equal("second", survivor.RootElement.GetProperty("_source").GetProperty("value").GetString());
        Assert.Equal(403, (await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/stale?routing={first}", new { value = "stale" }, true)).ApiCallDetails.HttpStatusCode);
        await RawAsync(HttpMethod.PUT, $"/{item.Alias}/_doc/new?routing={second}", new { value = "new" });
    }

    [Fact]
    public async Task DeletePlusInsert_DoesNotUseEqualCountsAsIdentityProof()
    {
        var item = await CreateFixtureAsync(false);
        await using var cleanup = new AsyncDisposableAction(() => CleanupAsync(item));
        await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/a?refresh=true", new { value = "a" });
        await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/b?refresh=true", new { value = "b" });
        bool changed = false;
        var reindexer = new ElasticReindexer(_client, _configuration.Serializer);
        await reindexer.ReindexAsync(item, async (progress, _) =>
        {
            if (progress is 91 && !changed)
            {
                changed = true;
                await RawAsync(HttpMethod.DELETE, $"/{item.OldIndex}/_doc/b?refresh=true");
                await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/c?refresh=true", new { value = "c" });
            }
        }, TestCancellationToken);
        Assert.True(changed);
        Assert.Equal(404, (await RawAsync(HttpMethod.GET, $"/{item.NewIndex}/_doc/b", allowFailure: true)).ApiCallDetails.HttpStatusCode);
        await RawAsync(HttpMethod.GET, $"/{item.NewIndex}/_doc/a");
        await RawAsync(HttpMethod.GET, $"/{item.NewIndex}/_doc/c");
    }

    [Fact]
    public async Task PrimaryCheckpointVector_DetectsAQuietShardUpdate()
    {
        var item = await CreateFixtureAsync(true);
        await using var cleanup = new AsyncDisposableAction(() => CleanupAsync(item));
        var (hot, quiet) = await GetDistinctRoutesAsync(item.OldIndex);
        for (int i = 0; i < 30; i++)
            await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/hot?routing={hot}", new { value = i });
        await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/quiet?routing={quiet}", new { value = 1 });
        var before = await ReindexSourceCheckpoint.ReadAsync(_client, item.OldIndex, TestCancellationToken);
        await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/quiet?routing={quiet}", new { value = 2 });
        var after = await ReindexSourceCheckpoint.ReadAsync(_client, item.OldIndex, TestCancellationToken);
        Assert.Equal(before.Primaries.Values.Max(), after.Primaries.Values.Max());
        Assert.False(before.Matches(after));
    }

    private async Task<ReindexWorkItem> CreateFixtureAsync(bool routed)
    {
        string alias = $"consistency-{Guid.NewGuid():N}";
        var item = new ReindexWorkItem { OldIndex = $"{alias}-v1", NewIndex = $"{alias}-v2", Alias = alias, QuiesceSource = true, DeleteOld = false };
        await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}", new
        {
            settings = new { number_of_shards = 2, number_of_replicas = 0 },
            mappings = new { _routing = new { required = routed } },
            aliases = new Dictionary<string, object> { [alias] = new { } }
        });
        await RawAsync(HttpMethod.PUT, $"/{item.NewIndex}", new
        {
            settings = new { number_of_shards = 2, number_of_replicas = 0 },
            mappings = new { _routing = new { required = routed } }
        });
        return item;
    }

    private async Task<(string First, string Second)> GetDistinctRoutesAsync(string index)
    {
        string? first = null;
        int? firstShard = null;
        for (int i = 0; i < 100; i++)
        {
            string route = $"tenant-{i}";
            using var response = JsonDocument.Parse((await RawAsync(HttpMethod.GET, $"/{index}/_search_shards?routing={route}")).Body!);
            int shard = response.RootElement.GetProperty("shards")[0][0].GetProperty("shard").GetInt32();
            if (firstShard is null)
            {
                first = route;
                firstShard = shard;
            }
            else if (shard != firstShard)
                return (first!, route);
        }
        throw new InvalidOperationException("Could not route the test documents to different shards.");
    }

    private async Task CleanupAsync(ReindexWorkItem item)
    {
        await RawAsync(HttpMethod.DELETE, $"/{item.OldIndex},{item.NewIndex}?ignore_unavailable=true");
    }

    private async Task<StringResponse> RawAsync(HttpMethod method, string path, object? body = null, bool allowFailure = false)
    {
        var response = await _client.Transport.RequestAsync<StringResponse>(method, path,
            body is null ? null : PostData.String(JsonSerializer.Serialize(body)), TestCancellationToken);
        if (!allowFailure)
            Assert.True(response.ApiCallDetails.HasSuccessfulStatusCode, response.Body);
        return response;
    }
}
''')
p=root/'tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexConsistencyEvidenceTests.cs'
s=p.read_text();position=s.rfind('\n}')
s=s[:position]+r'''
    [Fact]
    public async Task ChangedIdentityInspection_ExaminesBeyondTheFirstThousandResults()
    {
        var start = DateTime.UtcNow;
        var validIds = System.Linq.Enumerable.Range(0, 1000).Select(_ => new
        {
            _index = "source", _id = Foundatio.Repositories.Utility.ObjectId.GenerateNewId(start.AddMinutes(1)).ToString()
        }).ToArray();
        string PageFor(object hits) => JsonSerializer.Serialize(new
        {
            _scroll_id = "scroll", timed_out = false, _shards = new { total = 1, successful = 1, failed = 0 }, hits = new { hits }
        });
        int requests = 0;
        var invoker = new SequenceRequestInvoker(
            (200, """{"_shards":{"total":2,"successful":2,"failed":0}}"""),
            (200, PageFor(validIds)),
            (200, PageFor(new[] { new { _index = "source", _id = Foundatio.Repositories.Utility.ObjectId.GenerateNewId(start.AddDays(-1)).ToString() } })),
            (200, """{"succeeded":true,"num_freed":1}"""));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker).OnRequestCompleted(_ => requests++));
        var reindexer = new ElasticReindexer(client, new Foundatio.Serializer.SystemTextJsonSerializer());
        var flags = System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance;
        var planType = typeof(ElasticReindexer).GetNestedType("CatchUpPlan", System.Reflection.BindingFlags.NonPublic)!;
        var checkpoint = new ReindexSourceCheckpoint("generation", new Dictionary<int, long> { [0] = 0 });
        var plan = Activator.CreateInstance(planType, flags | System.Reflection.BindingFlags.Public, null,
            new object?[] { true, false, checkpoint, false, true }, null)!;
        var inspect = typeof(ElasticReindexer).GetMethod("EnsureNoUncatchableChangesAsync", flags)!;
        var task = (Task)inspect.Invoke(reindexer, new object[]
        {
            new ReindexWorkItem { OldIndex = "source", NewIndex = "target", Alias = "logical" }, plan, start, TestContext.Current.CancellationToken
        })!;
        await Assert.ThrowsAsync<ReindexIncompleteException>(() => task);
        Assert.Equal(4, requests);
    }
''' +s[position:]
s=s.replace('using System.Collections.Generic;','using System.Collections.Generic;\nusing System.Linq;')
p.write_text(s)
for name in ['docs/guide/index-management.md', '.agents/skills/foundatio-repositories/references/index-lifecycle.md']:
    p=root/name
    with p.open('a') as f:
        f.write('''\n\n## Consistency hardening: required migration contract\n\nQuiesced migration rejects nonempty transformation scripts before any Elasticsearch request. Arbitrary scripts can change identities, routing, or membership, so count equality is not a transformation-aware verification protocol. Rebuild a fresh destination while writers are stopped or use a separate transformation-aware snapshot/change-replay migration.\n\nUnscriped quiesced reconciliation examines every destination identity with routing preserved, validates every multi-get and bulk item, requires complete scroll/shard/timeout evidence, and rejects both count deficits and surpluses. Failure is never treated as absence. Source checkpoints include the physical UUID and every expected primary shard; a single maximum across shards and a fixed changed-ID sample are not safety proofs. Changed-ID inspection pages with bounded memory rather than stopping at 1,000 results.\n\nAfter a source is successfully fenced for reconciliation, it remains fenced on success or failure until it is deleted or an operator establishes safe recovery. This intentionally replaces unconditional unblocking in `finally`: a retained retired source must not accept writes from stale clients. A failed/lost alias response does not prove the swap failed. A queued `QuiesceSource` flag also cannot prove that a particular promoted generation was verified; without durable completion evidence, redelivery refuses to acknowledge or recopy.\n\nCopy submission and alias cutover use a single effective transport attempt. The supported Elastic transport ignores local retry limits alone, so these requests select and pin one node through the pool. External proxies must also avoid blind retries. This does not provide idempotency or a durable task-attempt journal after an unknown submission.\n\nThe default non-quiesced path remains best effort under live writes, including hard deletes and changes after inspection. Per-shard checks detect specific failures but are not a write barrier. A blocked full rescan is proportional to the entire dataset, not a brief delta-only outage. Low-downtime migration requires durable change capture, ordered/idempotent replay including retained tombstones, an exact final boundary, replica readiness and controlled writer retirement.\n\nShared release gate: consume the lost-lease repair in FoundatioFx/Foundatio#573, implement generation/attempt-bound durable recovery and stale-controller exclusion, and validate the combined release tree with Repositories #307. Neither a heartbeat nor an audit passing on one branch supplies storage-enforced fencing.\n''')
