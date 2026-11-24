using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Utility;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ReindexRecoveryTests(ITestOutputHelper output) : ElasticRepositoryTestBase(output)
{
    [Theory]
    [InlineData("noop")]
    [InlineData("delete")]
    public Task QuiescedScript_RebuildsFinalMembershipAfterHardDeletesAndConditionalDrops(string operation)
        => WithIndexesAsync(async work =>
        {
            foreach (string id in new[] { "drop", "remove", "keep" })
                await PutDocumentAsync(work.OldIndex, id);
            work = work with { Script = $"if (ctx._source.isDeleted) {{ ctx.op = '{operation}'; }}" };
            bool changed = false;
            await Reindexer().ReindexAsync(work, async (progress, _) =>
            {
                if (progress is 91 && !changed)
                {
                    changed = true;
                    await PutDocumentAsync(work.OldIndex, "drop", true);
                    await SendAsync(HttpMethod.DELETE, $"/{work.OldIndex}/_doc/remove?refresh=true");
                }
            }, TestCancellationToken);

            Assert.True(changed);
            Assert.Equal(["keep"], await IdsAsync(work.NewIndex));
            Assert.True(await Reindexer().HasCompletionEvidenceAsync(work, TestCancellationToken));
        });

    [Fact]
    public Task QuiescedCopy_CarriesSoftDeleteWithoutTimestampAdvance_AndBlocksOnlyAfterFirstPass()
        => WithIndexesAsync(async work =>
        {
            await PutDocumentAsync(work.OldIndex, "existing");
            bool firstPassWritable = false;
            bool secondPassBlocked = false;
            await Reindexer().ReindexAsync(work, async (progress, message) =>
            {
                if (progress is 91 && !firstPassWritable)
                {
                    // All writes retain the old updatedUtc value: timestamp-based catch-up cannot see this.
                    await PutDocumentAsync(work.OldIndex, "existing", true);
                    await PutDocumentAsync(work.OldIndex, "late");
                    firstPassWritable = true;
                }
                if (message?.StartsWith("Blocked writes to", StringComparison.Ordinal) is true)
                {
                    var response = await _client.Transport.RequestAsync<StringResponse>(HttpMethod.PUT,
                        $"/{work.OldIndex}/_doc/rejected", PostData.String("{}"), TestCancellationToken);
                    Assert.Equal(403, response.ApiCallDetails.HttpStatusCode);
                    secondPassBlocked = true;
                }
            }, TestCancellationToken);

            Assert.True(firstPassWritable);
            Assert.True(secondPassBlocked);
            Assert.Equal(["existing", "late"], await IdsAsync(work.NewIndex));
            var existing = await SendAsync(HttpMethod.GET, $"/{work.NewIndex}/_doc/existing");
            Assert.True(existing.GetProperty("_source").GetProperty("isDeleted").GetBoolean());
            await PutDocumentAsync(work.Alias, "after-cutover");
            Assert.Equal(["after-cutover", "existing", "late"], await IdsAsync(work.NewIndex));
        });

    [Fact]
    public Task QuiescedDeleteReconciliation_PreservesRoutedDocuments()
        => WithIndexesAsync(async work =>
        {
            for (int i = 0; i < 16; i++)
                await PutDocumentAsync(work.OldIndex, $"keep-{i:D2}", routing: $"tenant-{i}");
            await PutDocumentAsync(work.OldIndex, "remove", routing: "tenant-delete");
            bool changed = false;
            await Reindexer().ReindexAsync(work, async (progress, _) =>
            {
                if (progress is 91 && !changed)
                {
                    changed = true;
                    await SendAsync(HttpMethod.DELETE, $"/{work.OldIndex}/_doc/remove?routing=tenant-delete&refresh=true");
                }
            }, TestCancellationToken);

            Assert.Equal(Enumerable.Range(0, 16).Select(i => $"keep-{i:D2}"), await IdsAsync(work.NewIndex));
            for (int i = 0; i < 16; i++)
            {
                var result = await SendAsync(HttpMethod.GET, $"/{work.NewIndex}/_doc/keep-{i:D2}?routing=tenant-{i}");
                Assert.True(result.GetProperty("found").GetBoolean());
                Assert.Equal($"tenant-{i}", result.GetProperty("_routing").GetString());
            }
        });

    [Fact]
    public Task DefaultCopy_CancellationAfterPromotionFinishesRequiredCatchUp()
        => WithIndexesAsync(async work =>
        {
            work = work with { QuiesceSource = false, TimestampField = "updatedUtc" };
            await PutDocumentAsync(work.OldIndex, "existing");
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
            bool added = false;
            await Reindexer().ReindexAsync(work, async (progress, _) =>
            {
                if (progress is 91 && !added)
                {
                    added = true;
                    await SendAsync(HttpMethod.PUT, $"/{work.OldIndex}/_doc/late?refresh=true",
                        new { id = "late", isDeleted = false, updatedUtc = DateTime.UtcNow });
                }
                if (progress is 92)
                    cancellation.Cancel();
            }, cancellation.Token);

            Assert.True(cancellation.IsCancellationRequested);
            Assert.Equal(["existing", "late"], await IdsAsync(work.NewIndex));
            Assert.True(await Reindexer().HasCompletionEvidenceAsync(work, TestCancellationToken));
        });

    [Fact]
    public Task ObjectIdGuard_ExaminesChangesBeyondTheFirstPage()
        => WithIndexesAsync(async work =>
        {
            work = work with { QuiesceSource = false };
            string oldId = ObjectId.GenerateNewId(DateTime.UtcNow.AddYears(-1)).ToString();
            await PutDocumentAsync(work.OldIndex, oldId);
            bool changed = false;
            int pages = 0;
            await Assert.ThrowsAsync<ReindexIncompleteException>(() => Reindexer().ReindexAsync(work, async (progress, message) =>
            {
                if (message?.StartsWith("Checking changes on source shard", StringComparison.Ordinal) is true)
                    pages++;
                if (progress is 91 && !changed)
                {
                    changed = true;
                    var bulk = new StringBuilder();
                    for (int i = 0; i < 1001; i++)
                    {
                        string id = ObjectId.GenerateNewId().ToString();
                        bulk.Append(JsonSerializer.Serialize(new { index = new { _index = work.OldIndex, _id = id } })).Append('\n');
                        bulk.Append(JsonSerializer.Serialize(new { id, isDeleted = false })).Append('\n');
                    }
                    var response = await _client.Transport.RequestAsync<StringResponse>(HttpMethod.POST, "/_bulk",
                        PostData.String(bulk.ToString()), TestCancellationToken);
                    using var body = ReindexResponse.Parse(response, "Seeding changed IDs", TestCancellationToken);
                    Assert.False(body.RootElement.GetProperty("errors").GetBoolean());
                    // The updated older ID is appended after the 1,001 reachable creations in _doc order.
                    await PutDocumentAsync(work.OldIndex, oldId, true);
                }
            }, TestCancellationToken));
            Assert.True(pages >= 2);
            var alias = await SendAsync(HttpMethod.GET, $"/{work.OldIndex}/_alias/{work.Alias}");
            Assert.True(alias.TryGetProperty(work.OldIndex, out _));
        }, shards: 1);

    [Fact]
    public Task ConfirmedOrphanBlock_IsRecoveredByTheSameMigration()
        => WithIndexesAsync(async work =>
        {
            // Deliberately leave the first handle undisposed, representing a process that terminated.
            await IndexWriteBlock.ApplyAsync(_client, work, _logger, TestCancellationToken);
            string id = ReindexSafetyState.GetId("block", work.OldIndex);
            Assert.NotNull(await ReindexSafetyState.ReadAsync(_client, id, TestCancellationToken));

            await IndexWriteBlock.RecoverAsync(_client, work, _logger, TestCancellationToken);

            Assert.Null(await ReindexSafetyState.ReadAsync(_client, id, TestCancellationToken));
            await PutDocumentAsync(work.OldIndex, "recovered");
        });

    [Theory]
    [InlineData("applying", false)]
    [InlineData("applied", true)]
    public Task AmbiguousOrWrongGenerationOwnership_DoesNotClearTheBlock(string phase, bool wrongGeneration)
        => WithIndexesAsync(async work =>
        {
            string uuid = await ReindexSafetyState.ReadIndexUuidAsync(_client, work.OldIndex, TestCancellationToken);
            var entry = new ReindexSafetyState.Entry("block", work.OldIndex, work.NewIndex, work.Alias,
                wrongGeneration ? "another-generation" : uuid, Phase: phase);
            await ReindexSafetyState.WriteAsync(_client, ReindexSafetyState.GetId("block", work.OldIndex), entry, true, TestCancellationToken);
            await SendAsync(HttpMethod.PUT, $"/{work.OldIndex}/_block/write");

            await Assert.ThrowsAsync<ReindexCompletionUnknownException>(() => IndexWriteBlock.RecoverAsync(_client, work, _logger, TestCancellationToken));

            var response = await _client.Transport.RequestAsync<StringResponse>(HttpMethod.PUT,
                $"/{work.OldIndex}/_doc/still-blocked", PostData.String("{}"), TestCancellationToken);
            Assert.Equal(403, response.ApiCallDetails.HttpStatusCode);
        });

    [Fact]
    public Task StaleWorker_CannotUpdateOrRemoveANewerOwnershipRecord()
        => WithIndexesAsync(async work =>
        {
            string id = ReindexSafetyState.GetId("task", work.NewIndex);
            var old = new ReindexSafetyState.Entry("task", work.OldIndex, work.NewIndex, work.Alias, String.Empty);
            await ReindexSafetyState.WriteAsync(_client, id, old, true, TestCancellationToken);
            await ReindexSafetyState.DeleteAsync(_client, id, old.OwnerToken, TestCancellationToken);
            var current = old with { OwnerToken = "new-owner" };
            await ReindexSafetyState.WriteAsync(_client, id, current, true, TestCancellationToken);

            await Assert.ThrowsAsync<RepositoryException>(() => ReindexSafetyState.WriteAsync(_client, id, old, false, TestCancellationToken));
            await Assert.ThrowsAsync<RepositoryException>(() => ReindexSafetyState.DeleteAsync(_client, id, old.OwnerToken, TestCancellationToken));

            Assert.Equal(current, await ReindexSafetyState.ReadAsync(_client, id, TestCancellationToken));
        });

    [Fact]
    public Task UnknownDispatch_RemainsFencedWithoutStartingAnotherTask()
        => WithIndexesAsync(async work =>
        {
            string id = ReindexSafetyState.GetId("task", work.NewIndex);
            var intent = new ReindexSafetyState.Entry("task", work.OldIndex, work.NewIndex, work.Alias, String.Empty);
            await ReindexSafetyState.WriteAsync(_client, id, intent, true, TestCancellationToken);

            await Assert.ThrowsAsync<ReindexCompletionUnknownException>(() => ReindexTaskLease.AcquireAsync(_client, work, _logger, TestCancellationToken));

            Assert.Equal(intent, await ReindexSafetyState.ReadAsync(_client, id, TestCancellationToken));
            Assert.Empty(await IdsAsync(work.NewIndex));
        });

    [Fact]
    public Task CancelledCopy_ConfirmsServerTaskStoppedBeforeClearingItsFence()
        => WithIndexesAsync(async work =>
        {
            foreach (string id in new[] { "a", "b", "c" })
                await PutDocumentAsync(work.OldIndex, id);
            work = work with { ReindexBatchSize = 1, ReindexRequestsPerSecond = 1 };
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
            string? taskId = null;
            string fence = ReindexSafetyState.GetId("task", work.NewIndex);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Reindexer().ReindexAsync(work, async (progress, _) =>
            {
                if (progress is > 0 and < 90 && taskId is null)
                {
                    taskId = (await ReindexSafetyState.ReadAsync(_client, fence, TestCancellationToken))?.TaskId;
                    Assert.NotNull(taskId);
                    cancellation.Cancel();
                }
            }, cancellation.Token));

            Assert.NotNull(taskId);
            var status = await _client.Tasks.GetAsync(taskId, TestCancellationToken);
            Assert.True(status.ApiCallDetails.HttpStatusCode is 404 || status.IsValidResponse && status.Completed);
            Assert.Null(await ReindexSafetyState.ReadAsync(_client, fence, TestCancellationToken));
        });

    [Fact]
    public Task UnrecordedRetiredDestination_CannotMoveANewerAliasBackwards()
        => WithIndexesAsync(async work =>
        {
            // The alias advanced past this old source. Its absence on the retired destination is not
            // permission to run an old migration again when completion evidence is missing.
            string newer = work.Alias + "-v3";
            try
            {
                await SendAsync(HttpMethod.PUT, "/" + newer);
                await SendAsync(HttpMethod.POST, "/_aliases", new { actions = new object[]
                {
                    new { remove = new { index = work.OldIndex, alias = work.Alias } },
                    new { add = new { index = newer, alias = work.Alias } }
                } });
                await Assert.ThrowsAsync<ReindexCompletionUnknownException>(() => Reindexer().ReindexAsync(work, cancellationToken: TestCancellationToken));
                var aliases = await SendAsync(HttpMethod.GET, $"/_alias/{work.Alias}");
                Assert.True(aliases.TryGetProperty(newer, out _));
                Assert.False(aliases.TryGetProperty(work.NewIndex, out _));
            }
            finally
            {
                await SendAsync(HttpMethod.DELETE, "/" + newer + "?ignore_unavailable=true");
            }
        });

    private ElasticReindexer Reindexer() => new(_client, _serializer, Log.CreateLogger<ElasticReindexer>());

    private async Task WithIndexesAsync(Func<ReindexWorkItem, Task> action, int shards = 2)
    {
        string prefix = "audit-reindex-" + Guid.NewGuid().ToString("N");
        var work = new ReindexWorkItem { Alias = prefix, OldIndex = prefix + "-v1", NewIndex = prefix + "-v2", QuiesceSource = true };
        try
        {
            foreach (string index in new[] { work.OldIndex, work.NewIndex })
                await SendAsync(HttpMethod.PUT, "/" + index, new
                {
                    settings = new { number_of_shards = shards, number_of_replicas = 0 },
                    mappings = new { properties = new { id = new { type = "keyword" }, isDeleted = new { type = "boolean" }, updatedUtc = new { type = "date" } } },
                    aliases = index == work.OldIndex ? new Dictionary<string, object> { [work.Alias] = new { } } : new Dictionary<string, object>()
                });
            await action(work);
        }
        finally
        {
            await SendAsync(HttpMethod.DELETE, $"/{work.OldIndex},{work.NewIndex}?ignore_unavailable=true");
            foreach (string id in new[] { ReindexSafetyState.GetId("block", work.OldIndex), ReindexSafetyState.GetId("task", work.NewIndex) })
            {
                var entry = await ReindexSafetyState.ReadAsync(_client, id, TestCancellationToken);
                if (entry is not null)
                    await ReindexSafetyState.DeleteAsync(_client, id, entry.OwnerToken, TestCancellationToken);
            }
        }
    }

    private Task PutDocumentAsync(string index, string id, bool deleted = false, string? routing = null)
        => SendAsync(HttpMethod.PUT, $"/{index}/_doc/{id}?refresh=true" + (routing is null ? "" : "&routing=" + routing),
            new { id, isDeleted = deleted, updatedUtc = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc) });

    private async Task<string[]> IdsAsync(string index)
    {
        var result = await SendAsync(HttpMethod.POST, $"/{index}/_search", new { size = 2000, _source = false });
        ReindexResponse.RequireCompleteSearch(result);
        return result.GetProperty("hits").GetProperty("hits").EnumerateArray().Select(h => h.GetProperty("_id").GetString()!).Order(StringComparer.Ordinal).ToArray();
    }

    private async Task<JsonElement> SendAsync(HttpMethod method, string path, object? body = null)
    {
        var response = await _client.Transport.RequestAsync<StringResponse>(method, path,
            body is null ? PostData.Empty : PostData.String(JsonSerializer.Serialize(body)), TestCancellationToken);
        Assert.True(response.ApiCallDetails.HasSuccessfulStatusCode, response.Body);
        using var document = JsonDocument.Parse(response.Body!);
        return document.RootElement.Clone();
    }
}
