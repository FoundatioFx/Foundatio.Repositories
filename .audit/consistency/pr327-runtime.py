from pathlib import Path
import sys,re
root=Path(sys.argv[1]); p=root/'src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexer.cs'
s=p.read_text(encoding='utf-8-sig')
def replace(old,new):
 global s
 assert s.count(old)==1,(old[:100],s.count(old))
 s=s.replace(old,new)
def method(name,new):
 global s
 match=re.search(r'^    (?:private|internal|public) [^\n]+ '+re.escape(name)+r'\([^\n]*\)\n    \{.*?^    \}',s,re.M|re.S)
 assert match,name
 start=match.start()
 doc=re.search(r'(?:^    ///[^\n]*\n)+$',s[:start],re.M)
 if doc:start=doc.start()
 s=s[:start]+new.rstrip()+s[match.end():]
replace('        if (progressCallbackAsync == null)', '''        if (workItem.QuiesceSource && !String.IsNullOrWhiteSpace(workItem.Script))
            throw new NotSupportedException("Quiesced migration does not support arbitrary transformation scripts. Use a fresh destination copied while writers are stopped, or a transformation-aware migration protocol; counts cannot validate transformed identities or deletes.");

        if (progressCallbackAsync == null)''')
method('ReconcileThenPromoteAsync','''    /// <summary>Reconciles before promotion and retains the retired source fence on every uncertain exit.</summary>
    private async Task<bool> ReconcileThenPromoteAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, CatchUpPlan catchUpPlan, CancellationToken cancellationToken)
    {
        await using var writeBlock = await IndexWriteBlock.ApplyAsync(_client, workItem.OldIndex, _logger, cancellationToken).AnyContext();
        writeBlock.Retain();
        await progressCallbackAsync(92, $"Blocked writes to {workItem.OldIndex} to reconcile the copy").AnyContext();
        var before = await ReindexSourceCheckpoint.ReadAsync(_client, workItem.OldIndex, cancellationToken).AnyContext();
        await ConvergeCatchUpAsync(workItem, progressCallbackAsync, catchUpPlan, cancellationToken).AnyContext();
        await ReconcileDeletesAsync(workItem, progressCallbackAsync, cancellationToken).AnyContext();
        await EnsureDocumentCountsMatchAsync(workItem, progressCallbackAsync, cancellationToken).AnyContext();
        var after = await ReindexSourceCheckpoint.ReadAsync(_client, workItem.OldIndex, cancellationToken).AnyContext();
        if (!before.Matches(after))
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex, "the source generation or a primary-shard checkpoint changed behind the write block; retain both indexes for inspection");
        await SwitchAliasesAsync(workItem, progressCallbackAsync, 99, cancellationToken).AnyContext();
        return true;
    }''')
method('PlanCatchUpAsync','''    private async Task<CatchUpPlan> PlanCatchUpAsync(ReindexWorkItem workItem, CancellationToken cancellationToken)
    {
        if (workItem.OldIndex == workItem.NewIndex)
            return new CatchUpPlan(CanCatchUp: !String.IsNullOrEmpty(workItem.TimestampField), InPlace: true);
        if (workItem.QuiesceSource || !String.IsNullOrEmpty(workItem.TimestampField))
            return new CatchUpPlan(CanCatchUp: true);
        await RefreshForCopyAsync(workItem, cancellationToken).AnyContext();
        var checkpoint = await ReindexSourceCheckpoint.ReadAsync(_client, workItem.OldIndex, cancellationToken).AnyContext();
        var sample = await GetSampleDocumentIdAsync(workItem.OldIndex, cancellationToken).AnyContext();
        if (sample.Status is SampleIdStatus.Failed)
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex, "the source could not be sampled completely; refusing to infer a safe catch-up plan");
        bool creationTimeOnly = sample.Status is SampleIdStatus.Found && ObjectId.TryParse(sample.Id!, out _);
        return new CatchUpPlan(CanCatchUp: creationTimeOnly, SourceIsEmpty: sample.Status is SampleIdStatus.Empty,
            SourceCheckpoint: checkpoint, CatchUpIsCreationTimeOnly: creationTimeOnly);
    }''')
method('EnsureCatchUpPossibleAsync','''    private async Task EnsureCatchUpPossibleAsync(ReindexWorkItem workItem, CatchUpPlan plan, DateTime startTime, CancellationToken cancellationToken)
    {
        if (plan.InPlace || (plan.CanCatchUp && !plan.CatchUpIsCreationTimeOnly))
            return;
        if (plan.SourceCheckpoint is null)
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex, "complete source checkpoints were not recorded");
        if (plan.CatchUpIsCreationTimeOnly)
        {
            await EnsureNoUncatchableChangesAsync(workItem, plan, startTime, cancellationToken).AnyContext();
            return;
        }
        var current = await ReindexSourceCheckpoint.ReadAsync(_client, workItem.OldIndex, cancellationToken).AnyContext();
        if (!plan.SourceCheckpoint.Matches(current))
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                "the source generation or a primary-shard sequence number changed during the copy and the model cannot catch up; use QuiesceSource or stop writers");
    }''')
method('EnsureNoUncatchableChangesAsync','''    private async Task EnsureNoUncatchableChangesAsync(ReindexWorkItem workItem, CatchUpPlan plan, DateTime startTime, CancellationToken cancellationToken)
    {
        if (plan.SourceCheckpoint is null)
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex, "source checkpoints were not recorded; use QuiesceSource");
        await RefreshForCopyAsync(workItem, cancellationToken).AnyContext();
        string watermark = ObjectId.GenerateNewId(startTime).ToString();
        foreach (var shard in plan.SourceCheckpoint.Primaries)
        {
            string? scrollId = null;
            try
            {
                while (true)
                {
                    string path = scrollId is null
                        ? $"/{Uri.EscapeDataString(workItem.OldIndex)}/_search?scroll={SCROLL_KEEP_ALIVE}&allow_partial_search_results=false&preference=_shards%3A{shard.Key}"
                        : "/_search/scroll";
                    string body = scrollId is null
                        ? JsonSerializer.Serialize(new { size = DELETE_RECONCILE_BATCH_SIZE, _source = false, stored_fields = new[] { "_routing" }, query = new { range = new { _seq_no = new { gt = shard.Value } } } })
                        : JsonSerializer.Serialize(new { scroll = SCROLL_KEEP_ALIVE, scroll_id = scrollId });
                    var response = await _client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.POST, path, PostData.String(body), cancellationToken).AnyContext();
                    if (response.ApiCallDetails?.HasSuccessfulStatusCode is not true || String.IsNullOrEmpty(response.Body))
                        throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex, "changed-document inspection failed; use QuiesceSource");
                    var page = ReindexEvidence.ReadPage(response.Body, workItem.OldIndex);
                    scrollId = page.ScrollId;
                    if (page.Ids.Count is 0)
                        break;
                    if (page.Ids.Any(identity => !ObjectId.TryParse(identity.Id, out _) || String.Compare(identity.Id, watermark, StringComparison.Ordinal) < 0))
                        throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex, "a pre-existing or non-ObjectId document changed outside the creation-time catch-up range; use QuiesceSource");
                }
            }
            finally
            {
                await ClearScrollAsync(scrollId, cancellationToken).AnyContext();
            }
        }
        var current = await ReindexSourceCheckpoint.ReadAsync(_client, workItem.OldIndex, cancellationToken).AnyContext();
        if (!String.Equals(current.Uuid, plan.SourceCheckpoint.Uuid, StringComparison.Ordinal))
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex, "the source physical generation changed during inspection; use QuiesceSource");
    }''')
method('GetIdsChangedSinceAsync','')
s=re.sub(r'    /// <summary>\n    /// How many changed ids.*?    private const int CHANGED_ID_SAMPLE_SIZE = 1000;\n','',s,flags=re.S)
replace('        long? StartingMaxSequenceNumber = null,\n        bool SequenceNumberReadable = false,','        ReindexSourceCheckpoint? SourceCheckpoint = null,')
method('EnsureSourceHasSettledAsync','''    private async Task EnsureSourceHasSettledAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, CancellationToken cancellationToken)
    {
        ReindexSourceCheckpoint? previous = null;
        for (int check = 0; check < MAX_SETTLE_CHECKS; check++)
        {
            await RefreshForCopyAsync(workItem, cancellationToken).AnyContext();
            var current = await ReindexSourceCheckpoint.ReadAsync(_client, workItem.OldIndex, cancellationToken).AnyContext();
            if (previous is not null && previous.Matches(current))
                return;
            previous = current;
            await progressCallbackAsync(92, $"Verifying all primary-shard checkpoints for {workItem.OldIndex}").AnyContext();
        }
        throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex, "the source did not stabilize behind its write block; retain both indexes for inspection");
    }''')
method('TryGetMaxSequenceNumberAsync','')
start=s.index('    /// <summary>\n    /// Removes documents from the destination')
end=s.index('        _logger.LogInformation(',start)
s=s[:start]+'''    /// <summary>Reconciles every destination identity against the blocked source, with routing preserved.</summary>
    private async Task ReconcileDeletesAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, CancellationToken cancellationToken)
    {
        await RefreshForCopyAsync(workItem, cancellationToken).AnyContext();
        long sourceCount = await GetDocumentCountAsync(workItem.OldIndex, cancellationToken).AnyContext();
        long destinationCount = await GetDocumentCountAsync(workItem.NewIndex, cancellationToken).AnyContext();

'''+s[end:]
s=s.replace('so documents were deleted during the copy. Reconciling.', 'reconciling every routed identity against the blocked source.')
method('GetNextDestinationIdBatchAsync','''    private async Task<(List<ReindexDocumentIdentity> Ids, string ScrollId)> GetNextDestinationIdBatchAsync(string index, string? scrollId, CancellationToken cancellationToken)
    {
        string path = scrollId is null
            ? $"/{Uri.EscapeDataString(index)}/_search?scroll={SCROLL_KEEP_ALIVE}&allow_partial_search_results=false"
            : "/_search/scroll";
        string body = scrollId is null
            ? JsonSerializer.Serialize(new { size = DELETE_RECONCILE_BATCH_SIZE, _source = false, stored_fields = new[] { "_routing" } })
            : JsonSerializer.Serialize(new { scroll = SCROLL_KEEP_ALIVE, scroll_id = scrollId });
        var response = await _client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.POST, path, PostData.String(body), cancellationToken).AnyContext();
        if (response.ApiCallDetails?.HasSuccessfulStatusCode is not true || String.IsNullOrEmpty(response.Body))
            throw new RepositoryException($"Unable to read a complete reconciliation page from '{index}'.");
        return ReindexEvidence.ReadPage(response.Body, index);
    }''')
method('GetIdsMissingFromSourceAsync','''    private async Task<List<ReindexDocumentIdentity>> GetIdsMissingFromSourceAsync(string index, List<ReindexDocumentIdentity> ids, CancellationToken cancellationToken)
    {
        var docs = ids.Select(identity =>
        {
            var item = new Dictionary<string, string> { ["_id"] = identity.Id };
            if (identity.Routing is not null)
                item["routing"] = identity.Routing;
            return item;
        });
        string body = JsonSerializer.Serialize(new { docs });
        var response = await _client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.POST,
            $"/{Uri.EscapeDataString(index)}/_mget?_source=false", PostData.String(body), cancellationToken).AnyContext();
        if (response.ApiCallDetails?.HasSuccessfulStatusCode is not true || String.IsNullOrEmpty(response.Body))
            throw new RepositoryException($"Unable to establish document presence in '{index}'.");
        return ReindexEvidence.ReadMissing(response.Body, index, ids);
    }''')
method('DeleteDocumentsAsync','''    private async Task DeleteDocumentsAsync(ReindexWorkItem workItem, List<ReindexDocumentIdentity> ids, CancellationToken cancellationToken)
    {
        var request = new StringBuilder();
        foreach (var identity in ids)
        {
            var metadata = new Dictionary<string, string> { ["_index"] = workItem.NewIndex, ["_id"] = identity.Id };
            if (identity.Routing is not null)
                metadata["routing"] = identity.Routing;
            request.Append(JsonSerializer.Serialize(new Dictionary<string, object> { ["delete"] = metadata })).Append('\n');
        }
        var response = await _client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.POST,
            "/_bulk", PostData.String(request.ToString()), cancellationToken).AnyContext();
        if (response.ApiCallDetails?.HasSuccessfulStatusCode is not true || String.IsNullOrEmpty(response.Body))
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex, "reconciliation bulk deletion did not complete");
        ReindexEvidence.RequireDeletes(response.Body, workItem.NewIndex, ids);
    }''')
method('EnsureDocumentCountsMatchAsync','''    private async Task EnsureDocumentCountsMatchAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, CancellationToken cancellationToken)
    {
        await RefreshForCopyAsync(workItem, cancellationToken).AnyContext();
        long source = await GetDocumentCountAsync(workItem.OldIndex, cancellationToken).AnyContext();
        long target = await GetDocumentCountAsync(workItem.NewIndex, cancellationToken).AnyContext();
        await progressCallbackAsync(97, $"Old Docs: {source} New Docs: {target}").AnyContext();
        if (source != target)
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex, $"exact reconciliation counts differ ({source} source / {target} target); no cutover was authorized");
        await progressCallbackAsync(98, "Verified exact counts after routed-identity reconciliation").AnyContext();
    }''')
start=s.index('    private async Task<long> GetDocumentCountAsync(');end=s.index('\n    }',start)
part=s[start:end].replace('if (!response.IsValidResponse)', 'if (!response.IsValidResponse || response.Shards is null || response.Shards.Failed > 0 || response.Shards.Successful != response.Shards.Total)')
s=s[:start]+part+s[end:]
start=s.index('        var result = await _resiliencePolicy.ExecuteAsync(async ct =>',s.index('private async Task<ReindexResult> InternalReindexAsync'))
end=s.index('        if (result.Task is null)',start)
part=s[start:end]
part=part.replace('        var result = await _resiliencePolicy.ExecuteAsync(async ct =>\n        {\n            var response = await _client.ReindexAsync(d =>', '        var result = await _client.ReindexAsync(d =>')
part=part.replace('                d.WaitForCompletion(false);','                d.WaitForCompletion(false);\n                d.RequestConfiguration(r => SingleAttemptRequest.Configure(_client, r));')
part=part.replace('            }, ct).AnyContext();\n            _logger.LogRequest(response);\n\n            return response;\n        }, cancellationToken).AnyContext();', '            }, cancellationToken).AnyContext();\n        _logger.LogRequest(result);')
s=s[:start]+part+s[end:]
replace('if (result.Task is null)', 'if (!result.IsValidResponse || result.Task is null)')
s=s.replace('Elasticsearch did not return a reindex task, so no documents were copied', 'Elasticsearch did not confirm a reindex task ID; submission may have committed and must not be blindly repeated')
replace('var bulkResponse = await _client.Indices.UpdateAliasesAsync(x => x.Actions(aliasActions), cancellationToken).AnyContext();', 'var bulkResponse = await _client.Indices.UpdateAliasesAsync(x => x.Actions(aliasActions).RequestConfiguration(r => SingleAttemptRequest.Configure(_client, r)), cancellationToken).AnyContext();')
replace('        if (!bulkResponse.IsValidResponse)', '        if (!bulkResponse.IsValidResponse || !bulkResponse.Acknowledged)')
s=s.replace('so traffic is still being served by {workItem.OldIndex} ({bulkResponse.ElasticsearchServerError})', 'its outcome is unconfirmed; retain both indexes and inspect alias topology before retrying ({bulkResponse.ElasticsearchServerError})')
s=s.replace('The alias update failed, so traffic is still being served by the old index.', 'The alias update was not confirmed. A lost response does not establish which index serves traffic.')
method('RecordVerifiedCompletionAsync','''    /// <summary>Refuses to synthesize durable completion evidence from a work-item flag.</summary>
    internal Task RecordVerifiedCompletionAsync(ReindexWorkItem workItem, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(workItem);
        throw new ReindexCompletionUnknownException(workItem.Alias, workItem.OldIndex, workItem.NewIndex,
            "a promoted alias and QuiesceSource flag do not prove which attempt verified this generation; inspect durable completion evidence");
    }''')
p.write_text(s)
p=root/'src/Foundatio.Repositories.Elasticsearch/Repositories/IndexWriteBlock.cs'
s=p.read_text();old='    private bool _releaseAttempted;';assert s.count(old)==1;s=s.replace(old,old+'\n    private bool _retained;')
old='    private bool TryBeginRelease()';assert s.count(old)==1;s=s.replace(old,'''    /// <summary>Keeps the fence on a retired or uncertain source until deliberate operator recovery.</summary>
    public void Retain() => _retained = true;

'''+old)
s=s.replace('        if (_releaseAttempted)\n            return false;', '        if (_releaseAttempted || _retained)\n            return false;')
p.write_text(s)
p=root/'src/Foundatio.Repositories.Elasticsearch/Jobs/ReindexWorkItemHandler.cs';s=p.read_text()
start=s.index('            case RedeliveryDisposition.PromotedAfterVerification:');end=s.index('            case RedeliveryDisposition.SafeToStart:',start);s=s[:start]+s[end:]
start=s.index('        /// <remarks>\n        /// Only reachable on the default ordering');end=s.index('\n    }',start);s=s[:start]+'        PromotedButUnconfirmed'+s[end:]
old='        return workItem.QuiesceSource\n            ? RedeliveryDisposition.PromotedAfterVerification\n            : RedeliveryDisposition.PromotedButUnconfirmed;';assert old in s;s=s.replace(old,'        return RedeliveryDisposition.PromotedButUnconfirmed;')
s=s.replace('The\n    /// quiesced recovery path below retains its separate completion-record recovery behavior.', 'A work-item flag is not evidence that the promoted generation was actually verified.')
p.write_text(s)
p=root/'src/Foundatio.Repositories.Elasticsearch/Extensions/SingleAttemptRequest.cs'
p.write_text('''using System;
using System.Linq;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

internal static class SingleAttemptRequest
{
    public static RequestConfigurationDescriptor Configure(ElasticsearchClient client, RequestConfigurationDescriptor request)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(request);
        // Request-local MaxRetries is ignored by the supported transport. Pinning a normal pool-selected
        // node also sets the effective retry count to zero without changing shared client configuration.
        return request.ForceNode(client.ElasticsearchClientSettings.NodePool.CreateView().First().Uri).MaxRetries(0);
    }
}
''')
