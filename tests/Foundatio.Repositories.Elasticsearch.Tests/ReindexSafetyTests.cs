using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;
using Foundatio.Repositories.Exceptions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ReindexSafetyTests
{
    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task DefaultCleanup_RequiresCompleteRefreshAndCountEvidence(int failedStep)
    {
        const string refresh = """{"_shards":{"total":1,"successful":1,"failed":0}}""";
        const string count = """{"count":5,"_shards":{"total":1,"successful":1,"failed":0}}""";
        var responses = new[] { (200, refresh), (200, refresh), (200, count), (200, count) };
        if (failedStep >= 0)
            responses[failedStep] = (200, """{"count":5,"_shards":{"total":1,"successful":0,"failed":1}}""");
        int requests = 0;
        var settings = new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://in-memory.invalid:9200")),
                new SequenceRequestInvoker(responses))
            .MaximumRetries(0).OnRequestCompleted(_ => requests++);
        var reindexer = new ElasticReindexer(new ElasticsearchClient(settings), new Foundatio.Serializer.SystemTextJsonSerializer());
        var method = typeof(ElasticReindexer).GetMethod("VerifyDocumentCountsAsync", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        Func<int, string?, Task> progress = (_, _) => Task.CompletedTask;

        bool mayDeleteSource = await (Task<bool>)method.Invoke(reindexer, [WorkItem(), progress, TestContext.Current.CancellationToken])!;

        Assert.Equal(failedStep < 0, mayDeleteSource);
        Assert.Equal(failedStep < 0 ? 4 : failedStep + 1, requests);
    }

    [Fact]
    public async Task QueuedLock_WhenContendedWithoutCallerCancellation_ReturnsNoLock()
    {
        using var configuration = new Foundatio.Repositories.Elasticsearch.Configuration.ElasticConfiguration(lockProvider: new DenyingLockProvider());
        var handler = new ReindexWorkItemHandler(configuration);

        Assert.Null(await handler.GetWorkItemLockAsync(WorkItem(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public Task CompletionRead_WhenUnavailable_DoesNotAuthorizeReplay()
    {
        var settings = new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://in-memory.invalid:9200")),
            new SequenceRequestInvoker(
                (200, """{"destination":{"settings":{"index":{"uuid":"generation"}}}}"""),
                (503, """{"error":{"type":"unavailable","reason":"unavailable"},"status":503}""")))
            .MaximumRetries(0);
        var reindexer = new ElasticReindexer(new ElasticsearchClient(settings), new Foundatio.Serializer.SystemTextJsonSerializer());

        return Assert.ThrowsAsync<RepositoryException>(() => reindexer.HasCompletionEvidenceAsync(
            new ReindexWorkItem { Alias = "alias", OldIndex = "source", NewIndex = "destination" }, TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(403, "{}")]
    [InlineData(503, "{}")]
    [InlineData(200, "{\"_shards\":{\"total\":1,\"successful\":0,\"failed\":1}}")]
    [InlineData(200, "{\"_shards\":{\"total\":1,\"successful\":0,\"failed\":0}}")]
    public async Task Refresh_WhenIncomplete_StopsBeforeCopy(int status, string body)
    {
        int requests = 0;
        var settings = new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://in-memory.invalid:9200")),
                new SequenceRequestInvoker((status, body)))
            .MaximumRetries(0).OnRequestCompleted(_ => requests++);
        var reindexer = new ElasticReindexer(new ElasticsearchClient(settings), new Foundatio.Serializer.SystemTextJsonSerializer());
        var method = typeof(ElasticReindexer).GetMethod("RefreshForCopyAsync", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;

        await Assert.ThrowsAsync<ReindexIncompleteException>(() => (Task)method.Invoke(reindexer, [WorkItem(), TestContext.Current.CancellationToken])!);
        Assert.Equal(1, requests);
    }

    [Fact]
    public async Task ReindexDispatch_UsesForcedNodeSoTransientResponseIsNotRetried()
    {
        var pool = new StaticNodePool([
            new Uri("http://node-1.invalid:9200"),
            new Uri("http://node-2.invalid:9200"),
            new Uri("http://node-3.invalid:9200")
        ]);
        var invoker = new SequenceRequestInvoker((502, "{}"));
        var settings = new ElasticsearchClientSettings(pool, invoker).MaximumRetries(2);
        var client = new ElasticsearchClient(settings);
        Uri node = ElasticReindexer.SelectSingleDispatchNode(client.Transport.Configuration);
        var request = new RequestConfiguration { ForceNode = node };

        var response = await client.Transport.RequestAsync<StringResponse>(
            new EndpointPath(Elastic.Transport.HttpMethod.POST, "/_reindex"),
            PostData.String("{}"),
            configureActivity: null,
            request,
            TestContext.Current.CancellationToken);

        Assert.Equal(502, response.ApiCallDetails.HttpStatusCode);
        var bound = new BoundConfiguration(client.Transport.Configuration, request);
        Assert.Equal(0, bound.MaxRetries);
        Assert.Equal(node, bound.ForceNode);
    }

    [Fact]
    public async Task PriorTask_WhenTaskLookupReturns404_KeepsReplayFenced()
    {
        var entry = new ReindexSafetyState.Entry("task", "source", "destination", "alias", String.Empty, TaskId: "node:1");
        var settings = new ElasticsearchClientSettings(
                new SingleNodePool(new Uri("http://in-memory.invalid:9200")),
                new SequenceRequestInvoker(
                    (200, JsonSerializer.Serialize(new { _source = entry })),
                    (404, """{"error":{"type":"resource_not_found_exception","reason":"task result unavailable"},"status":404}""")))
            .MaximumRetries(0);

        var exception = await Assert.ThrowsAsync<ReindexCompletionUnknownException>(() => ReindexTaskLease.AcquireAsync(
            new ElasticsearchClient(settings),
            new ReindexWorkItem { Alias = "alias", OldIndex = "source", NewIndex = "destination" },
            NullLogger.Instance,
            TestContext.Current.CancellationToken));

        Assert.Contains("could not be confirmed stopped", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PriorTask_WhenCancellationIsAcknowledgedButStillRunning_KeepsReplayFenced()
    {
        var entry = new ReindexSafetyState.Entry("task", "source", "destination", "alias", String.Empty, TaskId: "node:1");
        string active = """{"completed":false,"task":{"node":"node","id":1,"type":"transport","action":"indices:data/write/reindex","start_time_in_millis":1,"running_time_in_nanos":1,"cancellable":true,"cancelled":false,"headers":{}}}""";
        var requests = new List<string>();
        var settings = new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://in-memory.invalid:9200")),
                new SequenceRequestInvoker((200, JsonSerializer.Serialize(new { _source = entry })), (200, active), (200, "{\"nodes\":{}}"), (200, active)))
            .MaximumRetries(0).OnRequestCompleted(details => requests.Add(details.HttpMethod.ToString()));

        await Assert.ThrowsAsync<ReindexCompletionUnknownException>(() => ReindexTaskLease.AcquireAsync(
            new ElasticsearchClient(settings), new ReindexWorkItem { Alias = "alias", OldIndex = "source", NewIndex = "destination" },
            NullLogger.Instance, TestContext.Current.CancellationToken));
        Assert.Equal(["GET", "GET", "POST", "GET"], requests);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task VersionRecheck_WhenItFailsOrCancels_ReleasesAcquiredLock(bool cancel)
    {
        using var configuration = new ElasticConfiguration();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var index = new FailedVersionRecheck(configuration, () =>
        {
            if (cancel)
            {
                cancellation.Cancel();
                cancellation.Token.ThrowIfCancellationRequested();
            }
            throw new InvalidOperationException("version read unavailable");
        });

        Assert.NotNull(await Record.ExceptionAsync(() => index.AcquireAsync(cancellation.Token)));
        Assert.False(await configuration.LockProvider.IsLockedAsync(ElasticReindexer.GetLockName("recheck")));
        Assert.Equal(2, index.Reads);
    }

    private sealed class FailedVersionRecheck(ElasticConfiguration configuration, Action fail) : VersionedIndex(configuration, "recheck", 2)
    {
        public int Reads { get; private set; }
        public override Task<int> GetCurrentVersionAsync()
        {
            if (++Reads > 1)
                fail();
            return Task.FromResult(1);
        }
        public Task AcquireAsync(CancellationToken token) => TryAcquireReindexLeaseAsync(token);
    }

    private const string Settings = """{"source":{"settings":{"index":{"uuid":"generation-1","number_of_shards":"2"}}}}""";
    private const string Stats = """
        {"_shards":{"total":2,"successful":2,"failed":0},"indices":{"source":{"uuid":"generation-1","shards":{
          "0":[{"routing":{"primary":true},"seq_no":{"max_seq_no":100,"local_checkpoint":100}}],
          "1":[{"routing":{"primary":true},"seq_no":{"max_seq_no":50,"local_checkpoint":50}}]
        }}}}
        """;

    private static ReindexSourceCheckpoint Checkpoint(string stats = Stats, string settings = Settings)
    {
        using var settingsDocument = JsonDocument.Parse(settings);
        using var statsDocument = JsonDocument.Parse(stats);
        return ReindexSourceCheckpoint.Parse("source", settingsDocument.RootElement, statsDocument.RootElement);
    }

    [Fact]
    public void Checkpoint_DetectsLowerShardChangesHiddenByGlobalMaximum()
    {
        Assert.True(Checkpoint().Matches(Checkpoint()));
        Assert.False(Checkpoint().Matches(Checkpoint(Stats.Replace(":50", ":51", StringComparison.Ordinal))));
    }

    [Theory]
    [InlineData("missing")]
    [InlineData("duplicate")]
    [InlineData("failed")]
    [InlineData("uuid")]
    [InlineData("checkpoint")]
    public void Checkpoint_RejectsIncompleteOrWrongGenerationEvidence(string defect)
    {
        var root = JsonNode.Parse(Stats)!;
        var source = root["indices"]!["source"]!;
        switch (defect)
        {
            case "missing": source["shards"]!.AsObject().Remove("1"); break;
            case "duplicate": source["shards"]!["1"]!.AsArray().Add(source["shards"]!["1"]![0]!.DeepClone()); break;
            case "failed": root["_shards"]!["failed"] = 1; break;
            case "uuid": source["uuid"] = "another-generation"; break;
            case "checkpoint": source["shards"]!["1"]![0]!["seq_no"]!.AsObject().Remove("local_checkpoint"); break;
        }
        Assert.Throws<RepositoryException>(() => Checkpoint(root.ToJsonString()));
    }

    [Theory]
    [InlineData("{\"docs\":[]}")]
    [InlineData("{\"docs\":[{\"_index\":\"source\",\"_id\":\"a\",\"error\":{\"type\":\"unavailable_shards_exception\"}}]}")]
    [InlineData("{\"docs\":[{\"_index\":\"source\",\"_id\":\"a\"}]}")]
    [InlineData("{\"docs\":[{\"_index\":\"other\",\"_id\":\"a\",\"found\":false}]}")]
    [InlineData("{\"docs\":[{\"_index\":\"source\",\"_id\":\"other\",\"found\":false}]}")]
    public void MultiGet_DoesNotTreatErrorsOrMissingItemsAsDeletes(string body)
    {
        using var document = JsonDocument.Parse(body);
        Assert.Throws<RepositoryException>(() => ReindexDocumentBatch.ReadMissing("source", [new("a", "tenant")], document.RootElement));
    }

    [Fact]
    public void MultiGet_PreservesRoutingAndOnlyDeletesProvenMissingIdentities()
    {
        using var document = JsonDocument.Parse("""
            {"docs":[{"_index":"source","_id":"a","_routing":"tenant-a","found":true},
              {"_index":"source","_id":"a","found":false}]}
            """);
        var missing = ReindexDocumentBatch.ReadMissing("source", [new("a", "tenant-a"), new("a", "tenant-b")], document.RootElement);
        Assert.Equal(new ReindexDocumentBatch.Key("a", "tenant-b"), Assert.Single(missing));
    }

    [Fact]
    public void SearchIdentity_ReadsStoredRouting()
    {
        using var document = JsonDocument.Parse("""{"_id":"a","fields":{"_routing":["tenant-a"]}}""");
        Assert.Equal(new ReindexDocumentBatch.Key("a", "tenant-a"), ReindexDocumentBatch.ReadKey(document.RootElement));
    }

    [Theory]
    [InlineData("{\"errors\":true,\"items\":[{\"delete\":{\"_index\":\"dest\",\"_id\":\"a\",\"status\":429,\"error\":{}}}]}")]
    [InlineData("{\"errors\":false,\"items\":[]}")]
    [InlineData("{\"errors\":false,\"items\":[{\"delete\":{\"_index\":\"dest\",\"_id\":\"a\",\"status\":503,\"result\":\"deleted\"}}]}")]
    [InlineData("{\"errors\":false,\"items\":[{\"delete\":{\"_index\":\"other\",\"_id\":\"a\",\"status\":200,\"result\":\"deleted\"}}]}")]
    public void BulkDelete_RejectsPartialOrInvalidSuccess(string body)
    {
        using var document = JsonDocument.Parse(body);
        Assert.Throws<RepositoryException>(() => ReindexDocumentBatch.RequireSuccessfulDeletes("dest", [new("a", null)], document.RootElement));
    }

    [Theory]
    [InlineData(200, "deleted")]
    [InlineData(404, "not_found")]
    public void BulkDelete_AcceptsConfirmedDeletionOrAlreadyMissing(int status, string result)
    {
        string body = JsonSerializer.Serialize(new { errors = false, items = new[] { new { delete = new { _index = "dest", _id = "a", status, result } } } });
        using var document = JsonDocument.Parse(body);
        ReindexDocumentBatch.RequireSuccessfulDeletes("dest", [new("a", null)], document.RootElement);
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("{\"timed_out\":true,\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0}}")]
    [InlineData("{\"timed_out\":false,\"_shards\":{\"total\":2,\"successful\":1,\"failed\":1}}")]
    [InlineData("{\"timed_out\":false,\"_shards\":{\"total\":2,\"successful\":1,\"failed\":0}}")]
    public void Search_RejectsIncompleteEvidence(string body)
    {
        using var document = JsonDocument.Parse(body);
        Assert.Throws<RepositoryException>(() => ReindexResponse.RequireCompleteSearch(document.RootElement));
    }

    [Fact]
    public void CompletionIdentity_IsBoundedAndUnambiguous()
    {
        var work = WorkItem() with
        {
            Alias = new string('a', 250),
            OldIndex = new string('b', 250),
            NewIndex = new string('c', 250)
        };
        Assert.True(Encoding.UTF8.GetByteCount(ElasticReindexer.GetCompletionId(work)) < 512);
        Assert.NotEqual(ElasticReindexer.GetCompletionId(new() { Alias = "a|b", OldIndex = "c", NewIndex = "d" }),
            ElasticReindexer.GetCompletionId(new() { Alias = "a", OldIndex = "b|c", NewIndex = "d" }));
    }

    [Theory]
    [InlineData("range")]
    [InlineData("timestamp")]
    [InlineData("script")]
    [InlineData("quiesce")]
    public void CompletionFingerprint_BindsEveryCopySemantic(string option)
    {
        var work = WorkItem();
        string original = ElasticReindexer.GetTransformationFingerprint(work);
        work = option switch
        {
            "range" => work with { StartUtc = new DateTime(2026, 9, 22, 0, 0, 0, DateTimeKind.Utc) },
            "timestamp" => work with { TimestampField = "updatedUtc" },
            "script" => work with { Script = "ctx.op = 'noop';" },
            "quiesce" => work with { QuiesceSource = true },
            _ => throw new ArgumentException("Unknown test option", nameof(option))
        };
        Assert.NotEqual(original, ElasticReindexer.GetTransformationFingerprint(work));
        string copy = ElasticReindexer.GetTransformationFingerprint(work);
        work = work with { ReindexBatchSize = 10, ReindexRequestsPerSecond = 2, DeleteOld = true };
        Assert.Equal(copy, ElasticReindexer.GetTransformationFingerprint(work));
    }

    [Fact]
    public async Task AliasRead_CancellationDuringResponseRemainsCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        var settings = new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://in-memory.invalid:9200")),
            new SequenceRequestInvoker((503, "{}"))).MaximumRetries(0).OnRequestCompleted(_ => cancellation.Cancel());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ReindexWorkItemHandler.IsAlreadyPromotedAsync(new ElasticsearchClient(settings), WorkItem(), cancellation.Token));
    }

    [Fact]
    public void OriginalReindexSignaturesRemainAvailable()
    {
        var callback = typeof(Func<int, string, Task>);
        foreach (var type in new[] { typeof(IIndex), typeof(Configuration.Index), typeof(VersionedIndex), typeof(DailyIndex) })
            Assert.NotNull(type.GetMethod("ReindexAsync", [callback]));
        foreach (var type in new[] { typeof(IElasticConfiguration), typeof(ElasticConfiguration) })
            Assert.NotNull(type.GetMethod("ReindexAsync", [typeof(IEnumerable<IIndex>), callback]));
        Assert.NotNull(typeof(ElasticReindexer).GetMethod("ReindexAsync", [typeof(ReindexWorkItem), callback]));
    }

    [Fact]
    public async Task CancellationAwareCall_DispatchesLegacyOverrideAndIsolatesConcurrentTokens()
    {
        using var configuration = new ElasticConfiguration();
        var tokens = new List<CancellationToken>();
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var first = new CancellationTokenSource();
        using var second = new CancellationTokenSource();
        using var index = new LegacyOverride(configuration, async token =>
        {
            if (token == first.Token)
            {
                firstEntered.SetResult();
                await secondEntered.Task.WaitAsync(TestContext.Current.CancellationToken);
            }
            else
                secondEntered.SetResult();
            lock (tokens)
                tokens.Add(token);
        });
        var task1 = index.ReindexAsync(cancellationToken: first.Token);
        await firstEntered.Task.WaitAsync(TestContext.Current.CancellationToken);
        var task2 = index.ReindexAsync(cancellationToken: second.Token);
        await Task.WhenAll(task1, task2);
        Assert.Contains(first.Token, tokens);
        Assert.Contains(second.Token, tokens);
        Assert.Equal(2, tokens.Count);
    }

    private sealed class LegacyOverride(ElasticConfiguration configuration, Func<CancellationToken, Task> action)
        : VersionedIndex(configuration, "legacy", 2)
    {
        public override Task ReindexAsync(Func<int, string?, Task>? progressCallbackAsync) => action(ReindexCancellationToken);
    }

    private static ReindexWorkItem WorkItem() => new() { Alias = "alias", OldIndex = "source", NewIndex = "dest" };
}
