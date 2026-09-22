using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Elastic.Clients.Elasticsearch.Tasks;
using Elastic.Transport;
using Elastic.Transport.Products.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Utility;
using Foundatio.Resilience;
using Foundatio.Serializer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Elasticsearch;

public class ElasticReindexer
{
    private readonly ElasticsearchClient _client;
    private readonly ITextSerializer _serializer;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;
    private const string ID_FIELD = "id";
    private const int MAX_STATUS_FAILS = 10;

    /// <summary>
    /// Returns the distributed lock resource name for serializing reindex operations on the given alias.
    /// </summary>
    public static string GetLockName(string alias)
    {
        ArgumentException.ThrowIfNullOrEmpty(alias);

        return String.Concat("reindex:", alias);
    }

    /// <summary>
    /// Returns the name of the index that captures documents which failed to copy, so callers can find and
    /// replay them. See <see cref="ReindexFailure"/> for the shape of the captured records.
    /// </summary>
    public static string GetFailureIndexName(string index)
    {
        ArgumentException.ThrowIfNullOrEmpty(index);

        return String.Concat(index, "-error");
    }

    /// <summary>
    /// Returns the name of the index holding <see cref="ReindexCompletion"/> records.
    /// </summary>
    /// <remarks>
    /// A single shared index rather than one per alias: it holds one small document per physical migration, so
    /// keeping it in one place keeps both the shard count and the lookup constant. It is deliberately not
    /// derived from the migrated index's name, so it is never swept up by the <c>{name}-v*</c> patterns used to
    /// enumerate and delete index versions, and it outlives cleanup of the source it describes.
    /// </remarks>
    public static string GetCompletionIndexName() => "foundatio-reindex-completions";

    /// <summary>Returns a bounded, unambiguous key for the physical migration.</summary>
    public static string GetCompletionId(ReindexWorkItem workItem)
    {
        ArgumentNullException.ThrowIfNull(workItem);
        string identity = JsonSerializer.Serialize(new[] { workItem.Alias, workItem.OldIndex, workItem.NewIndex });
        return "v2-" + Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(identity))).ToLowerInvariant();
    }

    /// <summary>Fingerprints result-affecting copy options; throughput and cleanup options are excluded.</summary>
    internal static string GetTransformationFingerprint(ReindexWorkItem workItem)
    {
        ArgumentNullException.ThrowIfNull(workItem);
        string contract = JsonSerializer.Serialize(new
        {
            Version = 2,
            Script = workItem.Script ?? String.Empty,
            TimestampField = workItem.TimestampField ?? String.Empty,
            StartUtc = workItem.StartUtc?.ToString("O", System.Globalization.CultureInfo.InvariantCulture),
            workItem.QuiesceSource
        });
        return Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(contract))).ToLowerInvariant();
    }

    public ElasticReindexer(ElasticsearchClient client, ITextSerializer serializer, ILogger? logger = null) : this(client, serializer, TimeProvider.System, logger)
    {
    }

    public ElasticReindexer(ElasticsearchClient client, ITextSerializer serializer, TimeProvider timeProvider, ILogger? logger = null) : this(client, serializer, timeProvider ?? TimeProvider.System, new ResiliencePolicyProvider(), logger ?? NullLogger.Instance)
    {
    }

    public ElasticReindexer(ElasticsearchClient client, ITextSerializer serializer, TimeProvider timeProvider, IResiliencePolicyProvider resiliencePolicyProvider, ILogger? logger = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger.Instance;
    }

    /// <summary>
    /// Copies documents from the old index to the new one and moves the aliases across.
    /// </summary>
    /// <param name="workItem">Describes the source, destination, alias, and copy options.</param>
    /// <param name="progressCallbackAsync">Invoked with a percentage and a status message.</param>
    public Task ReindexAsync(ReindexWorkItem workItem, Func<int, string?, Task>? progressCallbackAsync)
        => ReindexAsync(workItem, progressCallbackAsync, CancellationToken.None);

    /// <summary>Copies and promotes an index with cancellation before its irreversible cutover boundary.</summary>
    public async Task ReindexAsync(ReindexWorkItem workItem, Func<int, string?, Task>? progressCallbackAsync = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(workItem);

        if (String.IsNullOrEmpty(workItem.OldIndex))
            throw new ArgumentNullException(nameof(workItem.OldIndex));

        if (String.IsNullOrEmpty(workItem.NewIndex))
            throw new ArgumentNullException(nameof(workItem.NewIndex));

        // Validated alongside the indexes because the alias is what the cutover moves. Without it there is
        // nothing to switch, so a blank alias would copy every document and then silently skip the promotion,
        // leaving traffic on the old index while reporting a complete reindex.
        if (String.IsNullOrEmpty(workItem.Alias))
            throw new ArgumentNullException(nameof(workItem.Alias));

        if (workItem.ReindexBatchSize is <= 0)
            throw new ArgumentOutOfRangeException(nameof(workItem.ReindexBatchSize), workItem.ReindexBatchSize, "Must be greater than zero when specified.");

        // Checked explicitly (rather than a `float.NaN` constant pattern) so the intent - and the fact
        // that infinities are rejected alongside NaN - is obvious without knowing pattern-matching's
        // NaN semantics. `<= 0` alone wouldn't catch +Infinity, since +Infinity > 0.
        if (workItem.ReindexRequestsPerSecond is float requestsPerSecond && (requestsPerSecond <= 0 || float.IsNaN(requestsPerSecond) || float.IsInfinity(requestsPerSecond)))
            throw new ArgumentOutOfRangeException(nameof(workItem.ReindexRequestsPerSecond), workItem.ReindexRequestsPerSecond, "Must be a positive, finite number when specified.");

        if (progressCallbackAsync == null)
        {
            progressCallbackAsync = (progress, message) =>
            {
                _logger.LogInformation("Reindex Progress {Progress:F1}%: {Message}", progress, message);
                return Task.CompletedTask;
            };
        }

        using var _ = _logger.BeginScope(new Dictionary<string, object>
        {
            [nameof(workItem.OldIndex)] = workItem.OldIndex,
            [nameof(workItem.NewIndex)] = workItem.NewIndex,
            [nameof(workItem.Alias)] = workItem.Alias
        });

        _logger.LogInformation("Received reindex work item for {OldIndex} -> {NewIndex}", workItem.OldIndex, workItem.NewIndex);
        var startTime = _timeProvider.GetUtcNow().UtcDateTime.AddSeconds(-1);
        await progressCallbackAsync(0, "Starting reindex...").AnyContext();

        cancellationToken.ThrowIfCancellationRequested();
        if (workItem.QuiesceSource && workItem.StartUtc.HasValue)
            throw new ArgumentException("Quiesced promotion requires a full source copy; StartUtc is not supported.", nameof(workItem));
        if (workItem.OldIndex != workItem.NewIndex)
        {
            if (await HasCompletionEvidenceAsync(workItem, cancellationToken).AnyContext())
            {
                await progressCallbackAsync(100, "Already reindexed").AnyContext();
                return;
            }
            await EnsureSourceOwnsAliasAsync(workItem, cancellationToken).AnyContext();
            await EnsureDestinationNotServingTrafficAsync(workItem, cancellationToken).AnyContext();
            await IndexWriteBlock.RecoverAsync(_client, workItem, _logger, cancellationToken).AnyContext();
        }

        // Determined before the copy so an unsupported configuration is refused cheaply rather than after an
        // expensive copy. This is a per-shard stats read, not a scan.
        var catchUpPlan = await PlanCatchUpAsync(workItem, cancellationToken).AnyContext();

        var firstPassResult = await InternalReindexAsync(workItem, progressCallbackAsync, 0, 90, workItem.StartUtc, cancellationToken).AnyContext();
        EnsureCopyCompleted(workItem, firstPassResult);

        await progressCallbackAsync(91, $"Total: {firstPassResult.Total:N0} Completed: {firstPassResult.Completed:N0}").AnyContext();

        bool countsVerified;

        if (workItem.QuiesceSource && workItem.OldIndex != workItem.NewIndex)
            countsVerified = await ReconcileThenPromoteAsync(workItem, progressCallbackAsync, catchUpPlan, cancellationToken).AnyContext();
        else
            countsVerified = await PromoteThenReconcileAsync(workItem, progressCallbackAsync, catchUpPlan, startTime, cancellationToken).AnyContext();

        // Recorded here because every step that can fail the migration has now succeeded, and deliberately
        // before the queue item is acknowledged and before the source is deleted. Writing it any earlier would
        // make it evidence of a migration still in progress; writing it after cleanup would leave a finished
        // migration indistinguishable from one that failed after the cutover.
        if (workItem.OldIndex != workItem.NewIndex)
        {
            using var completionTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await RecordCompletionAsync(workItem, completionTimeout.Token).AnyContext();
        }

        // Cleanup is deliberately after the completion record and is not allowed to undo it: deleting the
        // source is an optimization, and a failure there must not send a finished migration back to copying.
        if (countsVerified && workItem.DeleteOld && workItem.OldIndex != workItem.NewIndex)
            await DeleteOldIndexAsync(workItem, progressCallbackAsync, cancellationToken).AnyContext();

        await progressCallbackAsync(100, "Reindex complete").AnyContext();
    }

    /// <summary>
    /// The historical ordering: promote the alias, then reconcile. Retained as the default because the
    /// alternative blocks writes to the source.
    /// </summary>
    /// <remarks>
    /// Everything reconciled here happens after traffic has already moved, so live writes and the catch-up pass
    /// can interfere. See <see cref="ReindexWorkItem.QuiesceSource"/> for what that costs and
    /// <see cref="VerifyDocumentCountsAsync"/> for why verification cannot fail the migration in this ordering.
    /// </remarks>
    private async Task<bool> PromoteThenReconcileAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, CatchUpPlan catchUpPlan, DateTime startTime, CancellationToken cancellationToken)
    {
        // Enforced BEFORE any alias changes. When the catch-up pass cannot run, the only way to keep the
        // promise the cutover implies is to not make it: if the source changed during the copy, those changes
        // cannot be found again, so promoting the destination would silently serve an incomplete index. This
        // deliberately runs pre-Switch - turning the old post-cutover warning into a post-cutover exception
        // would report the failure without preventing it, since the alias has already moved.
        await EnsureCatchUpPossibleAsync(workItem, catchUpPlan, startTime, progressCallbackAsync, cancellationToken).AnyContext();

        if (workItem.OldIndex != workItem.NewIndex)
        {
            cancellationToken.ThrowIfCancellationRequested();
            // Once promotion begins, its outcome may be ambiguous. Finish required reconciliation independently
            // of host shutdown; the no-progress watchdog still bounds stalled server tasks.
            await SwitchAliasesAsync(workItem, progressCallbackAsync, 92, CancellationToken.None).AnyContext();
            cancellationToken = CancellationToken.None;
        }

        if (catchUpPlan.CanCatchUp)
            await RunCatchUpPassAsync(workItem, progressCallbackAsync, startTime, 93, 96, cancellationToken).AnyContext();

        // Verify the destination isn't short of the source on every reindex, not only when the old index
        // happens to be scheduled for deletion - the shortfall gate used to live inside the delete branch, so
        // a DeleteOld = false reindex was never checked at all.
        return await VerifyDocumentCountsAsync(workItem, progressCallbackAsync, cancellationToken).AnyContext();
    }

    /// <summary>
    /// After the live first pass, block and drain source writes, perform a full final copy and deletion
    /// reconciliation, then verify unchanged per-primary checkpoints and exact output counts before promotion.
    /// </summary>
    /// <remarks>Cleanup preserves operator blocks. Unconfirmed release or process termination uses durable ownership recovery.</remarks>
    private async Task<bool> ReconcileThenPromoteAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, CatchUpPlan catchUpPlan, CancellationToken cancellationToken)
    {
        var writeBlock = await IndexWriteBlock.ApplyAsync(_client, workItem, _logger, cancellationToken).AnyContext();
        try
        {
            await progressCallbackAsync(92, $"Blocked writes to {workItem.OldIndex} to reconcile the copy").AnyContext();

            var checkpoint = await TryGetSourceCheckpointAsync(workItem.OldIndex, cancellationToken).AnyContext();
            if (checkpoint is null)
                throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex, "blocked source checkpoints could not be read");
            var finalPass = await ConvergeCatchUpAsync(workItem, progressCallbackAsync, catchUpPlan, cancellationToken).AnyContext();

            await ReconcileDeletesAsync(workItem, progressCallbackAsync, cancellationToken).AnyContext();

            await EnsureDocumentCountsMatchAsync(workItem, finalPass, progressCallbackAsync, cancellationToken).AnyContext();
            var finalCheckpoint = await TryGetSourceCheckpointAsync(workItem.OldIndex, cancellationToken).AnyContext();
            if (finalCheckpoint is null || !checkpoint.Matches(finalCheckpoint))
                throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex, "source changed while the write block should have been held");
            cancellationToken.ThrowIfCancellationRequested();

            await SwitchAliasesAsync(workItem, progressCallbackAsync, 99, cancellationToken).AnyContext();

            // Released explicitly rather than left to the finally: on the success path an index that stays
            // read-only is a failed migration, so it must surface. The finally is the backstop for every other
            // path, where it only logs so it cannot replace the exception that got us there.
            await writeBlock.ReleaseAsync().AnyContext();

            return true;
        }
        finally
        {
            await writeBlock.DisposeAsync().AnyContext();
        }
    }

    /// <summary>Plans non-blocking catch-up and captures every primary checkpoint when changes may be unreachable.</summary>
    /// <remarks>
    /// Quiesced copies always rescan the whole source. An empty non-quiesced source still needs a checkpoint
    /// guard; one sampled ObjectId only selects a strategy and does not prove that all changed IDs are reachable.
    /// </remarks>
    private async Task<CatchUpPlan> PlanCatchUpAsync(ReindexWorkItem workItem, CancellationToken cancellationToken)
    {
        // Elasticsearch rejects a reindex whose source and destination are the same index, so this never
        // reaches the guard - the copy fails first. Exempting it keeps the guard from reading a sequence
        // number it cannot use and from ever masking that validation error with a misleading refusal.
        if (workItem.OldIndex == workItem.NewIndex)
            return new CatchUpPlan(CanCatchUp: !String.IsNullOrEmpty(workItem.TimestampField), InPlace: true);

        if (workItem.QuiesceSource || !String.IsNullOrEmpty(workItem.TimestampField))
            return new CatchUpPlan(CanCatchUp: true);

        // The sample below is a search, so it only sees refreshed writes. Without this, a source loaded with
        // refresh disabled looks empty, which would disable both the catch-up pass and the change guard while
        // the copy - which refreshes first - copied a full index.
        await RefreshForCopyAsync(workItem, cancellationToken).AnyContext();

        var sampleResult = await GetSampleDocumentIdAsync(workItem.OldIndex, cancellationToken).AnyContext();

        switch (sampleResult.Status)
        {
            case SampleIdStatus.Empty:
                _logger.LogInformation("Reindex {OldIndex} -> {NewIndex}: Source index is empty, skipping second pass.", workItem.OldIndex, workItem.NewIndex);
                var emptyCheckpoint = await TryGetSourceCheckpointAsync(workItem.OldIndex, cancellationToken).AnyContext();
                return new CatchUpPlan(CanCatchUp: false, SourceIsEmpty: true, StartingCheckpoint: emptyCheckpoint, SequenceNumberReadable: emptyCheckpoint is not null);

            case SampleIdStatus.Found when ObjectId.TryParse(sampleResult.Id!, out _):
                // A baseline is recorded here too, even though this branch *can* catch up. The ObjectId catch-up
                // ranges on the id, which encodes only *creation* time, so it finds documents created during the
                // copy but is blind to an in-place update of an older document, and to a delete. The baseline is
                // what lets the guard tell those apart afterwards.
                ReindexSourceCheckpoint? objectIdMaxSeqNo = await TryGetSourceCheckpointAsync(workItem.OldIndex, cancellationToken).AnyContext();
                _logger.LogInformation("Reindex {OldIndex} -> {NewIndex}: Using ObjectId-based second pass (no TimestampField).", workItem.OldIndex, workItem.NewIndex);
                return new CatchUpPlan(CanCatchUp: true, StartingCheckpoint: objectIdMaxSeqNo, SequenceNumberReadable: objectIdMaxSeqNo is not null, CatchUpIsCreationTimeOnly: true);

            case SampleIdStatus.Found:
                // The one case that genuinely cannot catch up. Record the source's sequence number so the
                // enforcement below can tell a static source (safe) from one being written to (not safe).
                ReindexSourceCheckpoint? maxSeqNo = await TryGetSourceCheckpointAsync(workItem.OldIndex, cancellationToken).AnyContext();
                _logger.LogInformation(
                    "Reindex {OldIndex} -> {NewIndex}: No TimestampField and IDs are not ObjectIds (sample: {SampleId}), so documents written during the copy cannot be caught up. The copy will only be promoted if the source does not change while it runs.",
                    workItem.OldIndex, workItem.NewIndex, sampleResult.Id);
                return new CatchUpPlan(CanCatchUp: false, StartingCheckpoint: maxSeqNo, SequenceNumberReadable: maxSeqNo is not null);

            default:
                throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                    "source ID classification failed; refusing to assume that timestamp-free catch-up is safe");
        }
    }

    /// <summary>
    /// Refuses to promote a destination when the source changed during a copy that has no way to catch up.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Called before the aliases are switched, so refusing leaves the alias on the source and the source intact.
    /// The condition is that this migration cannot perform its required catch-up <em>and</em> the source
    /// actually changed — not merely that the documents use custom ids. A static source needs no catch-up, so
    /// that case is unaffected.
    /// </para>
    /// <para>
    /// Change is detected by the complete per-primary checkpoint vector, which advances on inserts, updates, and
    /// deletes alike. Document counts cannot do this: an update leaves the count unchanged, and an insert can
    /// offset a delete.
    /// </para>
    /// <para>
    /// Fails closed when the sequence number could not be read, because "no evidence of change" is not evidence
    /// of no change — the mistake this whole class of bug is made of.
    /// </para>
    /// </remarks>
    /// <exception cref="ReindexIncompleteException">
    /// The source changed during the copy and those changes cannot be found again.
    /// </exception>
    private async Task EnsureCatchUpPossibleAsync(ReindexWorkItem workItem, CatchUpPlan plan, DateTime startTime, Func<int, string?, Task> progressCallbackAsync, CancellationToken cancellationToken)
    {
        if (plan.InPlace)
            return;

        if (plan.CatchUpIsCreationTimeOnly)
        {
            await EnsureNoUncatchableChangesAsync(workItem, plan, startTime, progressCallbackAsync, cancellationToken).AnyContext();
            return;
        }

        if (plan.CanCatchUp)
            return;

        if (!plan.SequenceNumberReadable)
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                $"{workItem.OldIndex} has no timestamp field and its document ids are not ObjectIds, so documents written during the copy cannot be caught up - and whether any were written could not be determined because the source's sequence numbers could not be read. Refusing to promote {workItem.NewIndex}. Add IHaveDates to the model, use ObjectId-format ids, or stop writes to {workItem.OldIndex} for the duration of the migration.");

        ReindexSourceCheckpoint? currentMaxSeqNo = await TryGetSourceCheckpointAsync(workItem.OldIndex, cancellationToken).AnyContext();
        if (currentMaxSeqNo is null)
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                $"{workItem.OldIndex} has no timestamp field and its document ids are not ObjectIds, so documents written during the copy cannot be caught up - and whether any were written could not be confirmed because the source's sequence numbers could not be re-read. Refusing to promote {workItem.NewIndex}.");

        if (!currentMaxSeqNo.Matches(plan.StartingCheckpoint!))
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                $"{workItem.OldIndex} was written to during the copy (primary shard checkpoints changed), and because it has no timestamp field and its document ids are not ObjectIds those changes cannot be caught up. Refusing to promote {workItem.NewIndex}; {workItem.OldIndex} is unchanged and still serving the alias. Add IHaveDates to the model, use ObjectId-format ids, or stop writes for the duration of the migration.");

        _logger.LogInformation("Reindex {OldIndex} -> {NewIndex}: Source primary shard checkpoints did not change during the copy; no catch-up is required.",
            workItem.OldIndex, workItem.NewIndex);
    }

    /// <summary>Examines every changed document on each primary's own sequence-number range.</summary>
    private async Task EnsureNoUncatchableChangesAsync(ReindexWorkItem workItem, CatchUpPlan plan, DateTime startTime, Func<int, string?, Task> progressCallbackAsync, CancellationToken cancellationToken)
    {
        if (plan.StartingCheckpoint is null)
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex, "source checkpoints are unknown; ObjectId catch-up cannot establish safety");
        await RefreshForCopyAsync(workItem, cancellationToken).AnyContext();
        var current = await TryGetSourceCheckpointAsync(workItem.OldIndex, cancellationToken).AnyContext();
        if (current is null || current.IndexUuid != plan.StartingCheckpoint.IndexUuid || current.Shards.Count != plan.StartingCheckpoint.Shards.Count)
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex, "source identity or shard observations changed during copy");
        if (current.Matches(plan.StartingCheckpoint))
            return;

        string watermark = ObjectId.GenerateNewId(startTime).ToString();
        foreach (var (shardId, baseline) in plan.StartingCheckpoint.Shards)
        {
            string? scrollId = null;
            try
            {
                while (true)
                {
                    StringResponse response;
                    if (scrollId is null)
                    {
                        string body = JsonSerializer.Serialize(new
                        {
                            size = CHANGED_ID_BATCH_SIZE,
                            _source = new[] { ID_FIELD },
                            sort = new[] { "_doc" },
                            query = new { range = new { _seq_no = new { gt = baseline.Local } } }
                        });
                        response = await _client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.POST,
                            $"/{Uri.EscapeDataString(workItem.OldIndex)}/_search?scroll={SCROLL_KEEP_ALIVE}&preference=_shards:{shardId}&allow_partial_search_results=false",
                            PostData.String(body), cancellationToken).AnyContext();
                    }
                    else
                    {
                        response = await _client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.POST, "/_search/scroll",
                            PostData.String(JsonSerializer.Serialize(new { scroll = SCROLL_KEEP_ALIVE, scroll_id = scrollId })), cancellationToken).AnyContext();
                    }
                    using var document = ReindexResponse.Parse(response, "Reading changed source documents", cancellationToken);
                    var root = document.RootElement;
                    ReindexResponse.RequireCompleteSearch(root);
                    scrollId = ReindexResponse.Text(root, "_scroll_id");
                    var hits = ReindexResponse.Required(ReindexResponse.Required(root, "hits", JsonValueKind.Object), "hits", JsonValueKind.Array);
                    if (hits.GetArrayLength() is 0)
                        break;
                    if (String.IsNullOrEmpty(scrollId))
                        throw new RepositoryException("Changed-document search did not provide a scroll ID.");
                    await progressCallbackAsync(91, $"Checking changes on source shard {shardId}").AnyContext();
                    foreach (var hit in hits.EnumerateArray())
                    {
                        string id = ReindexResponse.Text(hit, "_id");
                        var source = ReindexResponse.Required(hit, "_source", JsonValueKind.Object);
                        if (!ObjectId.TryParse(id, out _) || String.CompareOrdinal(id, watermark) < 0
                            || !String.Equals(ReindexResponse.Text(source, ID_FIELD), id, StringComparison.Ordinal))
                            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                                $"document {id} changed outside the ObjectId catch-up range; use QuiesceSource or a timestamp field");
                    }
                }
            }
            finally
            {
                await ClearScrollAsync(scrollId, CancellationToken.None).AnyContext();
            }
        }
    }

    /// <summary>
    /// Page size for a complete changed-document scan; this is not a correctness cutoff.
    /// </summary>
    private const int CHANGED_ID_BATCH_SIZE = 1000;

    /// <summary>
    /// Whether a reindex can catch up writes that land during the copy, plus the evidence needed to tell
    /// whether any did.
    /// </summary>
    /// <remarks>
    /// <c>CatchUpIsCreationTimeOnly</c> means the catch-up pass can only find documents by <em>creation</em> time,
    /// so it cannot express an in-place update of a pre-existing document or a delete. It is set for the ObjectId
    /// branch, where the range is on the id itself.
    /// </remarks>
    private sealed record CatchUpPlan(
        bool CanCatchUp,
        bool SourceIsEmpty = false,
        ReindexSourceCheckpoint? StartingCheckpoint = null,
        bool SequenceNumberReadable = false,
        bool InPlace = false,
        bool CatchUpIsCreationTimeOnly = false);

    /// <summary>
    /// Copies documents written to the source while the first pass was running. Throws if this pass does not
    /// complete, so a reindex that skipped the catch-up is never reported as successful.
    /// </summary>
    private async Task<ReindexResult> RunCatchUpPassAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, DateTime? startTime, int startProgress, int endProgress, CancellationToken cancellationToken)
    {
        var result = await InternalReindexAsync(workItem, progressCallbackAsync, startProgress, endProgress, startTime, cancellationToken).AnyContext();
        EnsureCopyCompleted(workItem, result);

        await progressCallbackAsync(endProgress + 1, $"Total: {result.Total:N0} Completed: {result.Completed:N0}").AnyContext();

        return result;
    }

    /// <summary>
    /// The number of settle checks made against a blocked source before giving up.
    /// </summary>
    /// <remarks>
    /// The block API drains writes. Repeated changes indicate an externally disturbed barrier, not a normal
    /// catch-up condition, and cannot be allowed to loop indefinitely.
    /// </remarks>
    private const int MAX_SETTLE_CHECKS = 5;

    /// <summary>Copies the entire settled, blocked source; timestamp and creation-time ranges are insufficient.</summary>
    /// <remarks>
    /// A scripted migration first clears its unpromoted destination so newly no-op or deleted records cannot
    /// survive from the live first pass. Full rescanning is deliberate and its cost scales with index size.
    /// </remarks>
    private async Task<ReindexResult> ConvergeCatchUpAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, CatchUpPlan catchUpPlan, CancellationToken cancellationToken)
    {
        if (catchUpPlan.SourceIsEmpty)
            _logger.LogInformation("Reindex {OldIndex} -> {NewIndex}: Source index was empty when planning; reconciling anyway in case it was written to before the block landed.",
                workItem.OldIndex, workItem.NewIndex);

        await EnsureSourceHasSettledAsync(workItem, progressCallbackAsync, cancellationToken).AnyContext();

        if (!String.IsNullOrEmpty(workItem.Script))
            await ClearDestinationForScriptAsync(workItem, progressCallbackAsync, cancellationToken).AnyContext();

        // startTime is intentionally not used to narrow this pass. See the remarks above.
        var result = await RunCatchUpPassAsync(workItem, progressCallbackAsync, null, 92, 95, cancellationToken).AnyContext();

        _logger.LogInformation("Reindex {OldIndex} -> {NewIndex}: Reconciled the copy against the blocked source ({Created:N0} created, {Updated:N0} rewritten of {Total:N0} matched).",
            workItem.OldIndex, workItem.NewIndex, result.Created, result.Updated, result.Total);
        return result;
    }

    /// <summary>Requires stable per-primary checkpoint vectors after the acknowledged write barrier.</summary>
    /// <remarks>The add-block API drains in-flight writes. Checkpoints detect a broken or externally removed barrier.</remarks>
    private async Task EnsureSourceHasSettledAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, CancellationToken cancellationToken)
    {
        ReindexSourceCheckpoint? previous = null;

        for (int check = 1; check <= MAX_SETTLE_CHECKS; check++)
        {
            await RefreshForCopyAsync(workItem, cancellationToken).AnyContext();
            ReindexSourceCheckpoint? current = await TryGetSourceCheckpointAsync(workItem.OldIndex, cancellationToken).AnyContext();

            if (current is null)
                throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                    $"Writes to {workItem.OldIndex} are blocked but its sequence numbers could not be read, so it could not be confirmed to have stopped changing. Refusing to promote {workItem.NewIndex}; {workItem.OldIndex} is unchanged and still serving the alias.");

            if (previous is not null && current.Matches(previous))
                return;

            if (previous is not null)
                _logger.LogInformation("Reindex {OldIndex} -> {NewIndex}: Source sequence number moved from {Previous:N0} to {Current:N0} after the write block was applied; re-checking before reconciling.",
                    workItem.OldIndex, workItem.NewIndex, previous, current);

            previous = current;

            // Keeps the caller's reindex lock renewed across the checks.
            await progressCallbackAsync(92, $"Waiting for {workItem.OldIndex} to settle before reconciling").AnyContext();
        }

        throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
            $"{workItem.OldIndex} kept changing across {MAX_SETTLE_CHECKS} checks even though writes to it are blocked, so the copy to {workItem.NewIndex} could not be proven complete. Refusing to promote {workItem.NewIndex}; {workItem.OldIndex} is unchanged and still serving the alias.");
    }

    /// <summary>
    /// The number of destination documents examined per batch when reconciling deletes.
    /// </summary>
    private const int DELETE_RECONCILE_BATCH_SIZE = 1000;

    /// <summary>Deletes destination identities absent from the blocked source, preserving custom routing.</summary>
    /// <remarks>
    /// Equal counts do not establish equal membership. Every destination batch is checked, and each lookup and
    /// deletion must succeed. Scripted copies instead rebuild an empty destination in their final pass.
    /// </remarks>
    private async Task ReconcileDeletesAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, CancellationToken cancellationToken)
    {
        if (!String.IsNullOrEmpty(workItem.Script))
            return;

        await RefreshForCopyAsync(workItem, cancellationToken).AnyContext();

        long destinationCount = await GetDocumentCountAsync(workItem.NewIndex, cancellationToken).AnyContext();

        long deleted = 0;
        long examined = 0;
        string? scrollId = null;

        try
        {
            while (true)
            {
                var (ids, nextScrollId) = await GetNextDestinationIdBatchAsync(workItem.NewIndex, scrollId, cancellationToken).AnyContext();
                scrollId = nextScrollId;

                if (ids.Count is 0)
                    break;

                examined += ids.Count;

                var missing = await GetIdsMissingFromSourceAsync(workItem.OldIndex, ids, cancellationToken).AnyContext();
                if (missing.Count > 0)
                {
                    await DeleteDocumentsAsync(workItem, missing, cancellationToken).AnyContext();
                    deleted += missing.Count;
                }

                // Reported every batch, not just at the end. Callers renew the reindex lock on each progress
                // report, so a long scan here would otherwise let the lock expire underneath the migration.
                await progressCallbackAsync(96, $"Reconciling deletes: examined {examined:N0} of {destinationCount:N0}, removed {deleted:N0}").AnyContext();
            }
        }
        finally
        {
            await ClearScrollAsync(scrollId, CancellationToken.None).AnyContext();
        }

        _logger.LogInformation("Reindex {OldIndex} -> {NewIndex}: Removed {Deleted:N0} document(s) from the destination that had been deleted from the source during the copy.",
            workItem.OldIndex, workItem.NewIndex, deleted);

        await RefreshForCopyAsync(workItem, cancellationToken).AnyContext();
        await progressCallbackAsync(96, $"Reconciled {deleted:N0} delete(s) that happened during the copy").AnyContext();
    }

    /// <summary>Reads a complete scroll page, preserving custom routing in every document key.</summary>
    private async Task<(List<ReindexDocumentBatch.Key> Ids, string? ScrollId)> GetNextDestinationIdBatchAsync(string index, string? scrollId, CancellationToken cancellationToken)
    {
        StringResponse response;
        if (scrollId is null)
        {
            response = await _client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.POST,
                $"/{Uri.EscapeDataString(index)}/_search?scroll={SCROLL_KEEP_ALIVE}&allow_partial_search_results=false",
                PostData.String(JsonSerializer.Serialize(new { size = DELETE_RECONCILE_BATCH_SIZE, _source = false, stored_fields = new[] { "_routing" }, sort = new[] { "_doc" } })), cancellationToken).AnyContext();
        }
        else
        {
            response = await _client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.POST, "/_search/scroll",
                PostData.String(JsonSerializer.Serialize(new { scroll = SCROLL_KEEP_ALIVE, scroll_id = scrollId })), cancellationToken).AnyContext();
        }
        using var document = ReindexResponse.Parse(response, "Reading destination document keys", cancellationToken);
        var root = document.RootElement;
        ReindexResponse.RequireCompleteSearch(root);
        string nextScroll = ReindexResponse.Text(root, "_scroll_id");
        var hits = ReindexResponse.Required(ReindexResponse.Required(root, "hits", JsonValueKind.Object), "hits", JsonValueKind.Array);
        if (hits.GetArrayLength() > 0 && String.IsNullOrEmpty(nextScroll))
            throw new RepositoryException("Destination search did not provide a scroll ID.");
        var keys = new List<ReindexDocumentBatch.Key>(hits.GetArrayLength());
        foreach (var hit in hits.EnumerateArray())
            keys.Add(ReindexDocumentBatch.ReadKey(hit));
        return (keys, nextScroll);
    }

    private const string SCROLL_KEEP_ALIVE = "2m";

    /// <summary>
    /// Releases the scroll context. Best effort: it expires on its own, so a failure here is not worth failing a
    /// migration that has otherwise succeeded.
    /// </summary>
    private async Task ClearScrollAsync(string? scrollId, CancellationToken cancellationToken)
    {
        if (scrollId is null)
            return;

        try
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            cancellationToken = cleanup.Token;
            await _client.Transport
                .RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.DELETE, "/_search/scroll",
                    PostData.String($"{{\"scroll_id\":{JsonSerializer.Serialize(scrollId)}}}"), cancellationToken)
                .AnyContext();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not clear the scroll used to reconcile deletes; it will expire on its own.");
        }
    }

    /// <summary>Only an explicit, error-free absence response authorizes deleting a destination key.</summary>
    /// <remarks>
    /// Request routing explicitly: with source retrieval disabled and no stored fields requested,
    /// Elasticsearch can skip its stored-field loader and omit routing even for a routed document.
    /// That omission must not be mistaken for a change of document identity.
    /// </remarks>
    private async Task<List<ReindexDocumentBatch.Key>> GetIdsMissingFromSourceAsync(string index, List<ReindexDocumentBatch.Key> ids, CancellationToken cancellationToken)
    {
        var docs = ids.Select(key =>
        {
            var item = new Dictionary<string, object> { ["_id"] = key.Id };
            if (key.Routing is not null)
                item["routing"] = key.Routing;
            return item;
        });
        var response = await _client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.POST,
            $"/{Uri.EscapeDataString(index)}/_mget?_source=false&stored_fields=_routing",
            PostData.String(JsonSerializer.Serialize(new { docs })), cancellationToken).AnyContext();
        using var document = ReindexResponse.Parse(response, "Reading source document presence", cancellationToken);
        return ReindexDocumentBatch.ReadMissing(index, ids, document.RootElement);
    }

    /// <summary>Deletes exact routed keys and validates every bulk result.</summary>
    private async Task DeleteDocumentsAsync(ReindexWorkItem workItem, List<ReindexDocumentBatch.Key> ids, CancellationToken cancellationToken)
    {
        if (ids.Count is 0)
            return;
        var request = new StringBuilder();
        foreach (var key in ids)
        {
            var metadata = new Dictionary<string, object> { ["_index"] = workItem.NewIndex, ["_id"] = key.Id };
            if (key.Routing is not null)
                metadata["routing"] = key.Routing;
            request.Append(JsonSerializer.Serialize(new { delete = metadata })).Append('\n');
        }
        var response = await _client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.POST, "/_bulk",
            PostData.String(request.ToString()), cancellationToken).AnyContext();
        using var document = ReindexResponse.Parse(response, "Deleting stale destination documents", cancellationToken);
        ReindexDocumentBatch.RequireSuccessfulDeletes(workItem.NewIndex, ids, document.RootElement);
    }

    private async Task<long> GetDocumentCountAsync(string index, CancellationToken cancellationToken)
    {
        var response = await _client.CountAsync<object>(d => d.Indices(index), cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        _logger.LogRequest(response);

        if (!response.IsValidResponse || response.Shards is null || response.Shards.Failed > 0
            || response.Shards.Total <= 0 || response.Shards.Successful != response.Shards.Total || response.Count < 0)
            throw new RepositoryException($"Error reading the complete document count for {index}: {response.ElasticsearchServerError}");

        return response.Count;
    }

    /// <summary>Requires exact output cardinality from the final blocked-source copy.</summary>
    private async Task EnsureDocumentCountsMatchAsync(ReindexWorkItem workItem, ReindexResult finalPass, Func<int, string?, Task> progressCallbackAsync, CancellationToken cancellationToken)
    {
        await RefreshForCopyAsync(workItem, cancellationToken).AnyContext();
        long sourceCount = await GetDocumentCountAsync(workItem.OldIndex, cancellationToken).AnyContext();
        long destinationCount = await GetDocumentCountAsync(workItem.NewIndex, cancellationToken).AnyContext();
        long expected = finalPass.Total - finalPass.Noops - finalPass.Deleted;
        if (sourceCount != finalPass.Total || expected < 0 || destinationCount != expected)
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                $"expected {expected} output documents from {sourceCount} source documents, but found {destinationCount}; refusing promotion");
        await progressCallbackAsync(98, "Verified final copy accounting and exact document counts").AnyContext();
    }

    /// <summary>Removes first-pass remnants before a scripted final pass can deliberately noop or delete them.</summary>
    private async Task ClearDestinationForScriptAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, CancellationToken cancellationToken)
    {
        await RefreshForCopyAsync(workItem, cancellationToken).AnyContext();
        string? scrollId = null;
        try
        {
            while (true)
            {
                var (keys, nextScrollId) = await GetNextDestinationIdBatchAsync(workItem.NewIndex, scrollId, cancellationToken).AnyContext();
                scrollId = nextScrollId;
                if (keys.Count is 0)
                    break;
                await DeleteDocumentsAsync(workItem, keys, cancellationToken).AnyContext();
                await progressCallbackAsync(92, "Clearing first-pass output before the scripted final copy").AnyContext();
            }
        }
        finally
        {
            await ClearScrollAsync(scrollId, CancellationToken.None).AnyContext();
        }
        await RefreshForCopyAsync(workItem, cancellationToken).AnyContext();
        if (await GetDocumentCountAsync(workItem.NewIndex, cancellationToken).AnyContext() is not 0)
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex, "scripted final-pass destination could not be emptied");
    }

    /// <summary>
    /// Compares source and destination document counts after the copy and reports whether the destination
    /// looks complete, which is the precondition for deleting the source.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is a coarse safety net, not proof of completeness. A count mismatch is advisory on this path. By the time it
    /// runs the aliases have already been switched, so writes - including hard deletes - land on the
    /// destination while the source is frozen. A legitimately complete reindex can therefore end up with fewer
    /// documents than its source, and treating that as a failure would ask callers to retry a reindex that
    /// would resurrect the deleted documents. In the other direction, documents written to the destination
    /// after the cutover can offset documents that genuinely failed to copy, hiding a real shortfall.
    /// </para>
    /// <para>
    /// Per-pass accounting is checked in
    /// <see cref="InternalReindexAsync"/>, which compares the copy task's own report of what it matched
    /// against what it did. That checks task completion, not consistency against concurrent application writes.
    /// </para>
    /// </remarks>
    /// <returns>
    /// <c>false</c> when the destination is short or a count could not be read, in which case the source is
    /// kept for inspection.
    /// </returns>
    private async Task<bool> VerifyDocumentCountsAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, CancellationToken cancellationToken)
    {
        if (workItem.OldIndex == workItem.NewIndex)
            return true;

        long sourceCount;
        long destinationCount;
        try
        {
            await RefreshForCopyAsync(workItem, cancellationToken).AnyContext();
            destinationCount = await GetDocumentCountAsync(workItem.NewIndex, cancellationToken).AnyContext();
            sourceCount = await GetDocumentCountAsync(workItem.OldIndex, cancellationToken).AnyContext();
        }
        catch (RepositoryException ex)
        {
            // The alias already moved, so retain the source rather than declaring partial refresh/count
            // evidence sufficient for cleanup. This advisory path does not certify live-write consistency.
            _logger.LogWarning(ex, "Could not verify the reindex of {OldIndex} -> {NewIndex}; keeping the source because refresh or count evidence was incomplete.", workItem.OldIndex, workItem.NewIndex);
            return false;
        }

        await progressCallbackAsync(98, $"Old Docs: {sourceCount} New Docs: {destinationCount}").AnyContext();
        if (destinationCount >= sourceCount)
            return true;

        long missing = sourceCount - destinationCount;
        // Scripts and writes through the already-switched alias can legitimately reduce output cardinality.
        _logger.LogWarning("Reindex of {OldIndex} -> {NewIndex} left the destination {MissingCount:N0} document(s) short ({OldCount:N0} -> {NewCount:N0}). This can be legitimate, so it is not treated as a failure, but {OldIndex} will not be deleted.",
            workItem.OldIndex, workItem.NewIndex, missing, sourceCount, destinationCount, workItem.OldIndex);

        return false;
    }

    private async Task DeleteOldIndexAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, CancellationToken cancellationToken)
    {
        var deleteIndexResponse = await _client.Indices.DeleteAsync(Indices.Index(workItem.OldIndex), cancellationToken).AnyContext();
        _logger.LogRequest(deleteIndexResponse);

        if (deleteIndexResponse.IsValidResponse)
        {
            await progressCallbackAsync(99, $"Deleted index: {workItem.OldIndex}").AnyContext();
            return;
        }

        _logger.LogWarning("Failed to delete old index {OldIndex}: {Error}", workItem.OldIndex, deleteIndexResponse.ElasticsearchServerError);
        await progressCallbackAsync(99, $"Failed to delete old index {workItem.OldIndex}: {deleteIndexResponse.ElasticsearchServerError}").AnyContext();
    }

    /// <summary>
    /// Throws when a copy pass did not copy every document, so an incomplete reindex can never be mistaken
    /// for a successful one by a caller that ignores logs.
    /// </summary>
    private void EnsureCopyCompleted(ReindexWorkItem workItem, ReindexResult result)
    {
        if (result.Outcome is ReindexOutcome.Completed)
            return;

        string reason = result.FailureReason ?? $"the copy stopped with outcome {result.Outcome}";
        _logger.LogError("Reindex of {OldIndex} -> {NewIndex} did not complete ({Outcome}): {Reason}. Total: {Total:N0} Completed: {Completed:N0} Failures: {Failures:N0}",
            workItem.OldIndex, workItem.NewIndex, result.Outcome, reason, result.Total, result.Completed, result.Failures);

        throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex, reason);
    }

    private async Task<ReindexResult> InternalReindexAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, int startProgress, int endProgress, DateTime? startTime, CancellationToken cancellationToken)
    {
        // Refresh before reading. Elasticsearch only makes writes visible to search once they are
        // refreshed (every second by default, and not at all for an index with no active searches). The copy's
        // source query is a search, so without this the copy can silently skip documents that were already
        // written. This runs for every pass, not just the catch-up pass.
        await RefreshForCopyAsync(workItem, cancellationToken).AnyContext();

        var query = GetResumeQuery(workItem.TimestampField, startTime);

        await using var taskLease = await ReindexTaskLease.AcquireAsync(_client, workItem, _logger, cancellationToken).AnyContext();
        var result = await _client.ReindexAsync(d =>
        {
            d.Source(src =>
            {
                src.Indices(workItem.OldIndex);
                if (query != null)
                    src.Query(query);
                if (workItem.ReindexBatchSize.HasValue)
                    src.Size(workItem.ReindexBatchSize.Value);
            });
            d.Dest(dest => dest.Index(workItem.NewIndex));
            d.Conflicts(Conflicts.Proceed);
            d.WaitForCompletion(false);
            d.RequestConfiguration(r => r.MaxRetries(0));

            if (workItem.ReindexRequestsPerSecond.HasValue)
                d.RequestsPerSecond(workItem.ReindexRequestsPerSecond.Value);

            if (!String.IsNullOrWhiteSpace(workItem.Script))
                d.Script(new Script { Source = workItem.Script });
        }, cancellationToken).AnyContext();

        if (result.Task is null)
        {
            // A definite API rejection did not start a task. Transport failures and missing task IDs remain
            // fenced by the durable dispatch intent because the request might have been accepted.
            if (result.ApiCallDetails?.HttpStatusCode is 400 or 401 or 403 or 404 && result.ElasticsearchServerError is not null)
                taskLease.MarkTerminal();
            string reason = result.ElasticsearchServerError?.Error?.Reason ?? "Unknown";
            _logger.LogError("Reindex failed to start - no task returned. Response valid: {IsValid}, Reason: {Reason}",
                result.IsValidResponse, reason);
            _logger.LogErrorRequest(result, "Reindex failed");

            return new ReindexResult
            {
                Outcome = ReindexOutcome.NotStarted,
                FailureReason = $"Elasticsearch did not return a reindex task; dispatch may be ambiguous ({reason})."
            };
        }

        await taskLease.RecordTaskAsync(result.Task.FullyQualifiedId).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        _logger.LogInformation("Reindex Task Id: {ReindexTaskId}", result.Task.FullyQualifiedId);
        _logger.LogRequest(result);
        long totalDocs = result.Total ?? 0;

        var outcome = ReindexOutcome.Abandoned;
        string? failureReason = null;
        TaskReindexResult? lastReindexResponse = null;
        int statusGetFails = 0;
        long lastExamined = 0;
        var noProgressTimeout = GetNoProgressTimeout(workItem);
        var sw = Stopwatch.StartNew();
        do
        {
            // Cancellation is a throwing concept everywhere in this method. Checking at the loop head means
            // there is exactly one cancellation exit, so the copy can never fall out of the loop and be
            // reported to the caller as a non-cancelled, unsuccessful-but-silent result.
            cancellationToken.ThrowIfCancellationRequested();

            var status = await _client.Tasks.GetAsync(CreateStatusRequest(result.Task.FullyQualifiedId), cancellationToken).AnyContext();
            cancellationToken.ThrowIfCancellationRequested();
            if (status.IsValidResponse)
            {
                if (status.Completed)
                    taskLease.MarkTerminal();
                _logger.LogRequest(status);
            }
            else
            {
                _logger.LogErrorRequest(status, "Error getting task status while reindexing: {OldIndex} -> {NewIndex}", workItem.OldIndex, workItem.NewIndex);
                statusGetFails++;

                if (statusGetFails > MAX_STATUS_FAILS)
                {
                    _logger.LogError("Failed to get the status {FailureCount} times in a row for reindex task {ReindexTaskId} reindexing {OldIndex} -> {NewIndex}",
                        statusGetFails, result.Task.FullyQualifiedId, workItem.OldIndex, workItem.NewIndex);
                    failureReason = $"Could not read the status of reindex task {result.Task.FullyQualifiedId} {statusGetFails} times in a row, so the copy could not be confirmed complete.";
                    break;
                }

                // Back off before retrying so a struggling cluster (e.g. rejecting requests due to
                // indexing pressure) isn't hammered with an immediate retry.
                await _timeProvider.Delay(GetStatusRetryDelay(statusGetFails), cancellationToken).AnyContext();
                continue;
            }

            statusGetFails = 0;

            // The reindex sub-response (the per-document failures and the created/updated/deleted/noop counters that
            // completeness is judged on) is only present in the raw body, so this must be readable. Treating an
            // unreadable body as "no failures" is exactly how a lossy reindex reports success.
            if (!TryReadReindexStatus(status, out var response, out string? readFailureReason))
            {
                outcome = ReindexOutcome.Abandoned;
                failureReason = readFailureReason;
                break;
            }

            if (response?.Error is not null)
            {
                _logger.LogError("Error reindex: {Type}, {Reason}, Cause: {CausedBy} Stack: {Stack}", response.Error.Type, response.Error.Reason, response.Error.CausedBy?.Reason, String.Join("\r\n", response.Error.ScriptStack ?? new List<string>()));
                outcome = ReindexOutcome.Failed;
                failureReason = $"The reindex task failed: {response.Error.Type}: {response.Error.Reason}";
                break;
            }

            lastReindexResponse = response?.Response;

            var taskStatus = TaskStatusValues.From(status.Task.Status, _logger);

            // restart the stop watch if there was progress made
            if (taskStatus.Examined > lastExamined)
                sw.Restart();
            lastExamined = taskStatus.Examined;

            string lastMessage = $"[{workItem.NewIndex}] Total: {taskStatus.Total:N0} Completed: {taskStatus.Converged:N0} VersionConflicts: {taskStatus.VersionConflicts:N0}";
            await progressCallbackAsync(CalculateProgress(taskStatus.Total, taskStatus.Converged, startProgress, endProgress), lastMessage).AnyContext();

            if (status.Completed && response?.Error is null)
            {
                // A completed task must have published its counters. Without them the accounting check below
                // has nothing to compare (total and completed both read 0) and would pass vacuously, reporting
                // a copy whose outcome was never actually read as a complete reindex. Fail closed instead: the
                // old index is kept and the caller is told, which is recoverable, whereas promoting an
                // unverified destination is not.
                if (lastReindexResponse is null)
                {
                    outcome = ReindexOutcome.Abandoned;
                    failureReason = "the reindex task reported completion but its status carried no reindex response, so the number of documents copied could not be verified";
                    break;
                }

                outcome = ReindexOutcome.Completed;
                break;
            }

            // waited longer than noProgressTimeout (extended beyond the 10 minute default when
            // ReindexRequestsPerSecond makes Elasticsearch's own inter-batch pause longer than that) with
            // no progress made
            if (sw.Elapsed > noProgressTimeout)
            {
                _logger.LogError("Timed out waiting for reindex {OldIndex} -> {NewIndex}. NoProgressTimeout: {NoProgressTimeout}", workItem.OldIndex, workItem.NewIndex, noProgressTimeout);
                failureReason = $"The reindex task examined no new documents for {noProgressTimeout}, so it was treated as stalled and abandoned.";
                break;
            }

            var timeToWait = TimeSpan.FromSeconds(totalDocs < 100000 ? 1 : 10);
            if (taskStatus.Total < 100)
                timeToWait = TimeSpan.FromMilliseconds(100);

            await _timeProvider.Delay(timeToWait, cancellationToken).AnyContext();
        } while (true);
        sw.Stop();

        if (outcome is not ReindexOutcome.Completed)
        {
            _logger.LogError("Reindex abandoned for {OldIndex} -> {NewIndex}. ReindexTaskId: {ReindexTaskId}, StatusFails: {StatusFails}, LastExamined: {LastExamined}, TotalDocs: {TotalDocs}, Elapsed: {Elapsed}",
                workItem.OldIndex, workItem.NewIndex, result.Task.FullyQualifiedId, statusGetFails, lastExamined, totalDocs, sw.Elapsed);

        }

        long failures = 0;
        if (lastReindexResponse?.Failures is { Count: > 0 })
        {
            _logger.LogError("Error while reindexing result");

            if (await CreateFailureIndexAsync(workItem, cancellationToken).AnyContext())
            {
                foreach (var failure in lastReindexResponse.Failures)
                {
                    await HandleFailureAsync(workItem, failure, cancellationToken).AnyContext();
                    failures++;
                }
            }
            // Documents that could not be copied mean the destination is short, whatever the task reported
            // about its own completion.
            var firstFailure = lastReindexResponse.Failures.First();
            outcome = ReindexOutcome.Failed;
            failureReason = $"{lastReindexResponse.Failures.Count:N0} document(s) failed to copy from {workItem.OldIndex} to {workItem.NewIndex}. First failure: {firstFailure.Cause?.Reason ?? "unknown"}";
        }

        long total = lastReindexResponse?.Total ?? 0;
        long versionConflicts = lastReindexResponse?.VersionConflicts ?? 0;
        long completed = checked((lastReindexResponse?.Created ?? 0) + (lastReindexResponse?.Updated ?? 0) + (lastReindexResponse?.Noops ?? 0) + (lastReindexResponse?.Deleted ?? 0));

        // The copy must account for every document it matched. This is the one completeness check that cannot
        // race with live traffic, because it compares the task's own report of what it matched against its own
        // report of what it did - unlike comparing index document counts, which the alias cutover makes
        // unreliable (see VerifyDocumentCountsAsync).
        if (outcome is ReindexOutcome.Completed && (decimal)completed + versionConflicts != total)
        {
            long unaccounted = total - (completed + versionConflicts);
            outcome = ReindexOutcome.Failed;
            failureReason = $"the copy task reported it finished but only accounted for {completed + versionConflicts:N0} of the {total:N0} document(s) it matched, leaving {unaccounted:N0} unaccounted for";
        }

        if (outcome is ReindexOutcome.Completed && (versionConflicts > 0 || lastReindexResponse?.TimedOut is true))
        {
            outcome = ReindexOutcome.Failed;
            failureReason = "the copy timed out or left version conflicts unresolved";
        }

        string message = $"Total: {total:N0} Completed: {completed:N0} VersionConflicts: {versionConflicts:N0}";
        await progressCallbackAsync(CalculateProgress(total, completed, startProgress, endProgress), message).AnyContext();
        return new ReindexResult
        {
            Total = total,
            Completed = completed,
            Created = lastReindexResponse?.Created ?? 0,
            Updated = lastReindexResponse?.Updated ?? 0,
            Deleted = lastReindexResponse?.Deleted ?? 0,
            Noops = lastReindexResponse?.Noops ?? 0,
            Failures = failures,
            Outcome = outcome,
            FailureReason = failureReason
        };
    }

    /// <summary>
    /// Builds the task-status request used to poll a running reindex, opting this single request into keeping
    /// the raw response body.
    /// </summary>
    /// <remarks>
    /// The reindex sub-response - the per-document <c>failures</c> array and the created/updated/deleted/noop counters
    /// that completeness is judged on - is not modeled by the client's typed <c>GetTasksResponse</c>, so it has
    /// to be read out of the raw JSON. The transport only retains that JSON when direct streaming is disabled,
    /// which is off by default, so without asking for it here failure detection silently degrades to "no
    /// failures found" on any normally-configured client and a lossy reindex reports success. Scoping it to
    /// this request keeps the buffering cost off every other call.
    /// </remarks>
    private static GetTasksRequest CreateStatusRequest(string taskId)
    {
        return new GetTasksRequest(taskId)
        {
            RequestConfiguration = new RequestConfiguration { DisableDirectStreaming = true }
        };
    }

    /// <summary>
    /// Reads the reindex sub-response out of a task-status response.
    /// </summary>
    /// <returns>
    /// <c>true</c> when the status was readable, with <paramref name="response"/> set to the reindex payload
    /// (which is legitimately <c>null</c> before Elasticsearch has published one). <c>false</c> when the body
    /// could not be read at all, meaning the copy's outcome is unknown and must not be reported as complete.
    /// </returns>
    private bool TryReadReindexStatus(GetTasksResponse status, out TaskWithReindexResponse? response, out string? failureReason)
    {
        response = null;
        failureReason = null;

        if (status.ApiCallDetails?.ResponseBodyInBytes is null)
        {
            _logger.LogError("Task status response for reindex task did not include a body, so document failures could not be read");
            failureReason = "The reindex task status was returned without a response body, so whether any documents failed to copy could not be determined.";
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(status.ApiCallDetails.ResponseBodyInBytes);
            var root = document.RootElement;
            if (root.ValueKind is not JsonValueKind.Object)
                throw new RepositoryException("Task response is not an object.");
            if (status.Completed && !root.TryGetProperty("error", out _))
            {
                var final = ReindexResponse.Required(root, "response", JsonValueKind.Object);
                foreach (string name in new[] { "total", "created", "updated", "deleted", "noops", "version_conflicts" })
                {
                    if (ReindexResponse.Number(final, name) < 0)
                        throw new RepositoryException("Task response contains a negative counter.");
                }
                ReindexResponse.Required(final, "failures", JsonValueKind.Array);
                if (!final.TryGetProperty("timed_out", out var timedOut) || timedOut.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                    throw new RepositoryException("Task response did not establish timeout status.");
            }
            response = status.DeserializeRaw<TaskWithReindexResponse>(_serializer);
            return response is not null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to deserialize the task status response while reindexing");
            failureReason = $"The reindex task status could not be parsed ({ex.Message}), so whether any documents failed to copy could not be determined.";
            return false;
        }
    }

    /// <summary>Requires a complete refresh of the source before any copy or verification.</summary>
    private async Task RefreshForCopyAsync(ReindexWorkItem workItem, CancellationToken cancellationToken)
    {
        var source = await _client.Indices.RefreshAsync(Indices.Index(workItem.OldIndex), cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        _logger.LogRequest(source);
        if (!source.IsValidResponse || source.Shards is null || source.Shards.Failed > 0 || source.Shards.Total <= 0 || source.Shards.Successful <= 0)
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                "the source refresh failed or returned incomplete shard results; the copy cannot see all acknowledged writes");

        if (String.Equals(workItem.OldIndex, workItem.NewIndex, StringComparison.Ordinal))
            return;

        // An absent destination is allowed only before its first copy.
        var destination = await _client.Indices.RefreshAsync(Indices.Index(workItem.NewIndex), cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        _logger.LogRequest(destination);
        if (destination.ApiCallDetails?.HttpStatusCode is 404)
            return;
        if (!destination.IsValidResponse || destination.Shards is null || destination.Shards.Failed > 0
            || destination.Shards.Total <= 0 || destination.Shards.Successful <= 0)
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                "the destination refresh failed or returned incomplete shard results");
    }

    private async Task<bool> CreateFailureIndexAsync(ReindexWorkItem workItem, CancellationToken cancellationToken)
    {
        string errorIndex = GetFailureIndexName(workItem.NewIndex);
        var existsResponse = await _client.Indices.ExistsAsync(errorIndex, cancellationToken).AnyContext();
        _logger.LogRequest(existsResponse);

        if (existsResponse.ApiCallDetails.HasSuccessfulStatusCode && existsResponse.Exists)
            return true;

        if (!existsResponse.ApiCallDetails.HasSuccessfulStatusCode && existsResponse.ApiCallDetails.HttpStatusCode is not 404)
        {
            _logger.LogErrorRequest(existsResponse, "Error checking if error index exists");
            return false;
        }

        // `dynamic: false` used to leave this index unsearchable, so the one artifact that records which
        // documents failed to copy could not be queried to find them. The diagnostic fields are mapped
        // explicitly instead, and the copied document body is stored but not indexed - it has an arbitrary,
        // per-repository shape, so indexing it risks mapping conflicts and field-count explosions in the very
        // index being relied on for recovery. It is still returned in _source when the failure is read.
        var createResponse = await _client.Indices.CreateAsync(errorIndex, d => d
            .Mappings(md => md
                .Dynamic(DynamicMapping.Strict)
                .Properties<object>(p => p
                    .Keyword("index")
                    .Keyword("source_index")
                    .Keyword("id")
                    .LongNumber("version")
                    .Keyword("routing")
                    .IntegerNumber("status")
                    .Boolean("found")
                    .Date("created_utc")
                    .Object("cause", o => o
                        .Properties(cp => cp
                            .Keyword("type")
                            .Text("reason")
                            .Text("stack_trace", t => t.Index(false))))
                    .Object("source", o => o.Enabled(false)))), cancellationToken).AnyContext();
        if (!createResponse.IsValidResponse)
        {
            _logger.LogErrorRequest(createResponse, "Unable to create error index");
            return false;
        }

        _logger.LogRequest(createResponse);
        return true;
    }

    private async Task HandleFailureAsync(ReindexWorkItem workItem, BulkIndexByScrollFailure failure, CancellationToken cancellationToken)
    {
        _logger.LogError("Error reindexing document {Index}/{Id}: [{Status}] {Message}", workItem.OldIndex, failure.Id, failure.Status, failure.Cause?.Reason);

        if (String.IsNullOrEmpty(failure.Id))
        {
            _logger.LogWarning("Skipping error document fetch: failure has no document Id");
            return;
        }

        var gr = await _client.GetAsync<object>(new GetRequest(workItem.OldIndex, failure.Id), cancellationToken).AnyContext();

        if (!gr.IsValidResponse)
        {
            _logger.LogErrorRequest(gr, "Error getting document {Index}/{Id}", workItem.OldIndex, failure.Id);
            return;
        }

        _logger.LogRequest(gr);
        var errorDocument = new ReindexFailure
        {
            Index = failure.Index,
            SourceIndex = workItem.OldIndex,
            Id = failure.Id,
            Version = gr.Version,
            Routing = gr.Routing,
            Source = gr.Source,
            Status = failure.Status,
            Found = gr.Found,
            CreatedUtc = _timeProvider.GetUtcNow().UtcDateTime,
            Cause = new ReindexFailureCause
            {
                Type = failure.Cause?.Type,
                Reason = failure.Cause?.Reason,
                StackTrace = failure.Cause?.StackTrace
            }
        };

        string errorIndex = GetFailureIndexName(workItem.NewIndex);

        // Keyed by source document id so a retried reindex overwrites the previous record for the same
        // document instead of accumulating a duplicate per attempt.
        var indexResponse = await _client.IndexAsync(errorDocument, i => i.Index(errorIndex).Id(failure.Id), cancellationToken).AnyContext();
        if (indexResponse.IsValidResponse)
            _logger.LogRequest(indexResponse);
        else
            _logger.LogErrorRequest(indexResponse, "Error indexing document {Index}/{Id}", errorIndex, gr.Id);
    }

    /// <summary>Reads complete alias definitions for the exact source, or refuses promotion.</summary>
    private async Task<Dictionary<string, AliasDefinition>> GetIndexAliasesAsync(string index, CancellationToken cancellationToken)
    {
        var response = await _client.Indices.GetAliasAsync(Indices.Index(index), cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        _logger.LogRequest(response);
        if (!response.IsValidResponse)
            throw new RepositoryException(response.GetErrorMessage($"Could not read source aliases for {index}"), response.OriginalException());
#if ELASTICSEARCH9
        var indices = response.Aliases;
#else
        var indices = response.Values;
#endif
        if (indices is null || indices.Count is not 1)
            throw new RepositoryException($"The alias response did not identify the exact source {index}");
        var entry = indices.First();
        if (!String.Equals(entry.Key, index) || entry.Value?.Aliases is null || entry.Value.Aliases.Any(a => a.Value is null))
            throw new RepositoryException($"Incomplete alias definitions for {index}; refusing to drop alias metadata");
        return entry.Value.Aliases.ToDictionary(a => a.Key, a => a.Value, StringComparer.Ordinal);
    }

    /// <summary>
    /// Moves every alias on the old index across to the new one in a single atomic update, which is the
    /// point at which the new index starts serving traffic.
    /// </summary>
    /// <exception cref="ReindexIncompleteException">
    /// Alias promotion was not confirmed. The server may have applied it; inspect the actual alias state.
    /// </exception>
    private async Task SwitchAliasesAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, int progress, CancellationToken cancellationToken)
    {
        var aliases = await GetIndexAliasesAsync(workItem.OldIndex, cancellationToken).AnyContext();

        var existingAliases = new HashSet<string>(aliases.Keys, StringComparer.Ordinal);

        // A default definition carries no metadata, which reproduces the plain add this used to build for the
        // primary alias when it wasn't already present on the old index.
        if (!String.IsNullOrEmpty(workItem.Alias))
            aliases.TryAdd(workItem.Alias, new AliasDefinition());

        if (aliases.Count is 0)
            return;

        var aliasActions = new List<IndexUpdateAliasesAction>();
        foreach (var (alias, definition) in aliases)
        {
            if (existingAliases.Contains(alias))
                aliasActions.Add(new IndexUpdateAliasesAction { Remove = new RemoveAction { Alias = alias, Index = workItem.OldIndex, MustExist = true } });
            aliasActions.Add(new IndexUpdateAliasesAction { Add = CreateAddAction(alias, definition, workItem.NewIndex) });
        }

        var bulkResponse = await _client.Indices.UpdateAliasesAsync(x => x.Actions(aliasActions), cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        if (!bulkResponse.IsValidResponse || !bulkResponse.Acknowledged)
        {
            _logger.LogErrorRequest(bulkResponse, "Error updating aliases during reindex");

            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                $"the documents were copied but the aliases ({String.Join(", ", aliases.Keys)}) could not be switched to the new index, the cutover outcome is unconfirmed; inspect both indexes and their aliases before recovery ({bulkResponse.ElasticsearchServerError})");
        }

        _logger.LogRequest(bulkResponse);
        await progressCallbackAsync(progress, $"Updated aliases: {String.Join(", ", aliases.Keys)} Remove: {workItem.OldIndex} Add: {workItem.NewIndex}").AnyContext();
    }

    /// <summary>
    /// Recreates an alias on the destination index, carrying its definition forward. Recreating a filtered
    /// alias without its filter would expose documents the alias was designed to hide, so the metadata is
    /// preserved rather than defaulted.
    /// </summary>
    private static AddAction CreateAddAction(string alias, AliasDefinition definition, string newIndex)
    {
        // Routing values need the null-safe conversion below because Routing's implicit conversion only
        // accepts a non-null string, unlike the other metadata which is nullable end to end.
        return new AddAction
        {
            Alias = alias,
            Index = newIndex,
            Filter = definition.Filter,
            IsHidden = definition.IsHidden,
            IsWriteIndex = definition.IsWriteIndex,
            IndexRouting = ToRouting(definition.IndexRouting),
            SearchRouting = ToRouting(definition.SearchRouting),
            Routing = ToRouting(definition.Routing)
        };

        static Routing? ToRouting(string? value) => value is null ? null : new Routing(value);
    }

    /// <summary>
    /// Builds the source query for a copy pass, or <c>null</c> to copy every document.
    /// </summary>
    /// <remarks>
    /// A pass is only ever narrowed by an <em>explicit</em> start time: <see cref="ReindexWorkItem.StartUtc"/>
    /// from the caller, or the timestamp the copy began at for the catch-up pass. It is deliberately never
    /// narrowed by inspecting the destination.
    /// <para>
    /// Progress used to be inferred by reading the newest timestamp already present in the destination and
    /// copying only documents at or after it. That is unsound, and it is the cause of the silent data loss this
    /// class was rewritten to prevent: Elasticsearch reindex copies in unordered doc order, so an interrupted
    /// pass leaves an arbitrary subset behind. If that subset happened to include the newest document, the
    /// watermark concluded there was nothing older left to copy and the run "succeeded" against a destination
    /// missing most of its documents. Sorting the copy is not a workaround either - reindex sort was deprecated
    /// in 7.6 and was never guaranteed to index in order. Recopying everything is the correct fail-safe: the
    /// source is the source of truth and reindex writes by document id, so a full recopy converges.
    /// </para>
    /// </remarks>
    private static Query? GetResumeQuery(string? timestampField, DateTime? startTime)
    {
        if (!startTime.HasValue)
            return null;

        return CreateRangeQuery(new QueryDescriptor<object>(), timestampField, startTime);
    }

    private static Query? CreateRangeQuery(QueryDescriptor<object> descriptor, string? timestampField, DateTime? startTime)
    {
        if (!startTime.HasValue)
            return descriptor;

        var start = startTime.Value;

        if (!String.IsNullOrEmpty(timestampField))
            return descriptor.Range(dr => dr.Date(drr => drr.Field(timestampField).Gte(start)));

        return descriptor.Range(dr => dr.Term(tr => tr.Field(ID_FIELD).Gte(ObjectId.GenerateNewId(start).ToString())));
    }

    private enum SampleIdStatus { Found, Empty, Failed }

    /// <summary>Reads a generation-bound vector for every primary shard, or reports missing evidence.</summary>
    private async Task<ReindexSourceCheckpoint?> TryGetSourceCheckpointAsync(string index, CancellationToken cancellationToken)
    {
        try
        {
            return await ReindexSourceCheckpoint.ReadAsync(_client, index, cancellationToken).AnyContext();
        }
        catch (RepositoryException ex)
        {
            _logger.LogWarning(ex, "Could not read complete source checkpoints for {Index}", index);
            return null;
        }
    }

    /// <summary>
    /// Reads the destination index's UUID, which identifies one physical generation of that index name.
    /// </summary>
    private async Task<string?> TryGetIndexUuidAsync(string index, CancellationToken cancellationToken)
    {
        var response = await _client.Indices.GetSettingsAsync((Indices)index, cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        _logger.LogRequest(response);
        if (response.ApiCallDetails?.HttpStatusCode is 404)
            return null;
        if (!response.IsValidResponse || response.Settings is null || response.Settings.Count is not 1)
            throw new RepositoryException($"Could not establish the exact index generation for {index}; refusing to infer missing completion evidence");
        var entry = response.Settings.First();
        string? uuid = entry.Value.Settings?.Index?.Uuid;
        if (!String.Equals(entry.Key, index) || String.IsNullOrEmpty(uuid))
            throw new RepositoryException($"The settings did not establish the exact index generation for {index}");
        return uuid;
    }

    /// <summary>
    /// Creates the completion-record index if it does not exist.
    /// </summary>
    /// <remarks>
    /// Mapped strictly so a shape change surfaces as a rejected write rather than as a record that silently
    /// fails to match later. Every field is a keyword because the record is only ever fetched by id.
    /// </remarks>
    private async Task<bool> EnsureCompletionIndexAsync(CancellationToken cancellationToken)
    {
        string index = GetCompletionIndexName();
        var existsResponse = await _client.Indices.ExistsAsync(index, cancellationToken).AnyContext();
        _logger.LogRequest(existsResponse);

        if (existsResponse.ApiCallDetails.HasSuccessfulStatusCode && existsResponse.Exists)
            return true;

        if (!existsResponse.ApiCallDetails.HasSuccessfulStatusCode && existsResponse.ApiCallDetails.HttpStatusCode is not 404)
        {
            _logger.LogErrorRequest(existsResponse, "Error checking if the reindex completion index exists");
            return false;
        }

        var createResponse = await _client.Indices.CreateAsync(index, d => d
            .Settings(s => s.NumberOfShards(1))
            .Mappings(md => md
                .Dynamic(DynamicMapping.Strict)
                .Properties<object>(p => p
                    .Keyword("alias")
                    .Keyword("source_index")
                    .Keyword("destination_index")
                    .Keyword("destination_uuid")
                    .Keyword("transformation")
                    .Date("completed_utc"))), cancellationToken).AnyContext();

        // A concurrent reindex may have created it between the check and the create.
        if (createResponse.IsValidResponse || createResponse.ElasticsearchServerError?.Error?.Type is "resource_already_exists_exception")
        {
            _logger.LogRequest(createResponse);
            return true;
        }

        _logger.LogErrorRequest(createResponse, "Unable to create the reindex completion index");
        return false;
    }

    /// <summary>
    /// Records that this migration finished, and refuses to continue if that record cannot be persisted.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Called only after the copy's own accounting confirmed completion, and before the queue item is
    /// acknowledged or the source is deleted. If the record cannot be written the migration is <em>not</em>
    /// reported as successful: without it, a redelivery has no way to distinguish this destination from one
    /// promoted by a failed attempt, and would be forced to treat a finished migration as an unknown outcome.
    /// </para>
    /// <para>
    /// The write is idempotent - same id, same content - so a retry after an ambiguous response overwrites
    /// rather than duplicating. An ambiguous response is resolved by reading the record back and checking it
    /// matches, rather than assuming either outcome.
    /// </para>
    /// </remarks>
    private async Task RecordCompletionAsync(ReindexWorkItem workItem, CancellationToken cancellationToken)
    {
        if (!await EnsureCompletionIndexAsync(cancellationToken).AnyContext())
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                "the copy finished but its completion could not be recorded, because the completion index could not be created");

        string? destinationUuid = await TryGetIndexUuidAsync(workItem.NewIndex, cancellationToken).AnyContext();
        if (String.IsNullOrEmpty(destinationUuid))
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                $"the copy finished but its completion could not be recorded, because the uuid of {workItem.NewIndex} could not be read");

        var completion = new ReindexCompletion
        {
            Alias = workItem.Alias,
            SourceIndex = workItem.OldIndex,
            DestinationIndex = workItem.NewIndex,
            DestinationUuid = destinationUuid,
            Transformation = GetTransformationFingerprint(workItem),
            CompletedUtc = _timeProvider.GetUtcNow().UtcDateTime
        };

        string id = GetCompletionId(workItem);
        var indexResponse = await _client.IndexAsync(completion, i => i
            .Index(GetCompletionIndexName())
            .Id(id)
            .Refresh(Refresh.True), cancellationToken).AnyContext();

        if (indexResponse.IsValidResponse)
        {
            _logger.LogRequest(indexResponse);
            return;
        }

        // The write may still have landed, so this is resolved by reading the exact record back rather than by
        // assuming it failed and rerunning the migration.
        _logger.LogWarning("The completion record write for {OldIndex} -> {NewIndex} returned an ambiguous response, verifying: {Error}",
            workItem.OldIndex, workItem.NewIndex, indexResponse.GetErrorMessage("Index failed"));

        if (await TryReadCompletionAsync(workItem, destinationUuid, cancellationToken).AnyContext())
            return;

        throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
            "the copy finished but its completion could not be recorded, so a later attempt could not tell this destination apart from one promoted by a failed attempt");
    }

    /// <summary>
    /// Returns whether a completion record exists that vouches for exactly this migration.
    /// </summary>
    /// <remarks>
    /// A record only counts when it matches this migration's identity, the destination's current uuid, and the
    /// transformation. A mismatch on any of those means the record describes a different generation or a
    /// different copy, and is treated as no evidence at all.
    /// </remarks>
    private async Task<bool> TryReadCompletionAsync(ReindexWorkItem workItem, string destinationUuid, CancellationToken cancellationToken)
    {
        var response = await _client.GetAsync<ReindexCompletion>(GetCompletionId(workItem),
            d => d.Index(GetCompletionIndexName()), cancellationToken).AnyContext();

        cancellationToken.ThrowIfCancellationRequested();
        if (!response.IsValidResponse && response.ApiCallDetails.HttpStatusCode is not 404)
            throw new RepositoryException(response.GetErrorMessage("Could not read completion evidence; refusing to infer that this migration may be replayed"), response.OriginalException());

        if (!response.Found || response.Source is null)
            return false;

        var completion = response.Source;
        if (!String.Equals(completion.Alias, workItem.Alias, StringComparison.Ordinal)
            || !String.Equals(completion.SourceIndex, workItem.OldIndex, StringComparison.Ordinal)
            || !String.Equals(completion.DestinationIndex, workItem.NewIndex, StringComparison.Ordinal))
            return false;
        if (!String.Equals(completion.DestinationUuid, destinationUuid, StringComparison.Ordinal))
        {
            _logger.LogWarning("The completion record for {NewIndex} names uuid {RecordedUuid}, but the index now has uuid {CurrentUuid}, so it describes an index generation that no longer exists.",
                workItem.NewIndex, completion.DestinationUuid, destinationUuid);
            return false;
        }

        string transformation = GetTransformationFingerprint(workItem);
        if (!String.Equals(completion.Transformation, transformation, StringComparison.Ordinal))
        {
            _logger.LogWarning("The completion record for {NewIndex} was written for a different transformation, so it does not vouch for this reindex.", workItem.NewIndex);
            return false;
        }

        return true;
    }

    /// <summary>
    /// Returns whether this migration has trustworthy evidence of completion.
    /// </summary>
    internal async Task<bool> HasCompletionEvidenceAsync(ReindexWorkItem workItem, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(workItem);

        string? destinationUuid = await TryGetIndexUuidAsync(workItem.NewIndex, cancellationToken).AnyContext();
        if (String.IsNullOrEmpty(destinationUuid))
            return false;

        return await TryReadCompletionAsync(workItem, destinationUuid, cancellationToken).AnyContext();
    }

    private sealed record SampleIdResult(SampleIdStatus Status, string? Id = null, string? Error = null, Exception? Exception = null);

    private async Task<SampleIdResult> GetSampleDocumentIdAsync(string index, CancellationToken cancellationToken)
    {
        var response = await _client.SearchAsync<IDictionary<string, object>>(d => d
            .Indices(index)
            .Source(new SourceConfig(false))
            .Size(1), cancellationToken
        ).AnyContext();

        _logger.LogRequest(response);

        cancellationToken.ThrowIfCancellationRequested();
        if (!response.IsValidResponse || response.TimedOut || response.Shards is null || response.Shards.Failed > 0)
            return new SampleIdResult(SampleIdStatus.Failed, Error: response.GetErrorMessage("Search failed or was incomplete"), Exception: response.OriginalException());

        if (!response.Hits.Any())
            return new SampleIdResult(SampleIdStatus.Empty);

        return new SampleIdResult(SampleIdStatus.Found, Id: response.Hits.First().Id);
    }

    private int CalculateProgress(long total, long completed, int startProgress = 0, int endProgress = 100)
    {
        if (total == 0) return startProgress;
        return startProgress + (int)((100 * (double)completed / total) * (((double)endProgress - startProgress) / 100));
    }

    private static readonly Func<int, TimeSpan> _statusRetryExponentialDelay = ResiliencePolicy.ExponentialDelay(TimeSpan.FromSeconds(1));
    private static readonly TimeSpan _maxStatusRetryDelay = TimeSpan.FromSeconds(30);

    // Any attempt count at or beyond this already saturates the exponential delay past _maxStatusRetryDelay
    // (2^(6-1) = 32s > 30s cap), so clamping here is purely overflow-safety headroom for Math.Pow, not a
    // behavioral limit. It intentionally isn't tied to MAX_STATUS_FAILS - those are separate concerns that
    // happen to share the same value today.
    private const int MaxAttemptsForDelayCalculation = 10;

    /// <summary>
    /// Computes the backoff delay before retrying a failed task status check, e.g. after Elasticsearch
    /// rejects the request due to indexing pressure (HTTP 429). Grows exponentially starting at 1 second,
    /// caps at 30 seconds so repeated failures don't hammer a struggling cluster, and applies +/-25% jitter
    /// (matching <see cref="ResiliencePolicy"/>'s own jitter formula) so multiple reindex operations failing
    /// at the same time due to a cluster-wide condition don't retry in lockstep.
    /// </summary>
    internal static TimeSpan GetStatusRetryDelay(int failedAttempts)
    {
        int clampedAttempts = Math.Clamp(failedAttempts, 1, MaxAttemptsForDelayCalculation);
        var delay = _statusRetryExponentialDelay(clampedAttempts);

        double offset = delay.TotalMilliseconds * 0.25;
        double jitteredMilliseconds = delay.TotalMilliseconds + (delay.TotalMilliseconds * 0.5 * Random.Shared.NextDouble() - offset);
        delay = TimeSpan.FromMilliseconds(Math.Max(0, jitteredMilliseconds));

        return delay > _maxStatusRetryDelay ? _maxStatusRetryDelay : delay;
    }

    private static readonly TimeSpan DefaultNoProgressTimeout = TimeSpan.FromMinutes(10);

    // Elasticsearch's own reindex API default for Source.Size when ReindexBatchSize isn't specified -
    // see https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex.
    private const int DefaultElasticsearchBatchSize = 1000;

    // Elasticsearch pauses roughly batchSize/requestsPerSecond between batches to honor the throttle. This
    // multiplier gives that pause headroom (write time, network latency) before treating it as a stall.
    private const int NoProgressTimeoutSafetyMultiplier = 3;

    /// <summary>
    /// Computes how long to wait for progress before treating a reindex as stalled and abandoning it.
    /// Defaults to 10 minutes. When <see cref="ReindexWorkItem.ReindexRequestsPerSecond"/> throttles the
    /// reindex, Elasticsearch pauses between batches to honor that rate, and the pause can exceed the
    /// default timeout for a low configured rate relative to the batch size. In that case the timeout is
    /// extended (with a safety margin) so a healthy, intentionally throttled reindex isn't mistaken for a
    /// stalled one and cancelled. The result is clamped to <see cref="TimeSpan.MaxValue"/> instead of
    /// overflowing for extreme (but otherwise valid) batch size/throttle combinations, such as an unbounded
    /// <see cref="ReindexWorkItem.ReindexBatchSize"/> paired with a very low
    /// <see cref="ReindexWorkItem.ReindexRequestsPerSecond"/>.
    /// </summary>
    internal static TimeSpan GetNoProgressTimeout(ReindexWorkItem workItem)
    {
        if (workItem.ReindexRequestsPerSecond is not > 0)
            return DefaultNoProgressTimeout;

        int effectiveBatchSize = workItem.ReindexBatchSize is > 0 ? workItem.ReindexBatchSize.Value : DefaultElasticsearchBatchSize;

        // Computed entirely in double (seconds) space and clamped before constructing a TimeSpan - an
        // extreme batch size/throttle combination (e.g. a very large ReindexBatchSize with a tiny
        // ReindexRequestsPerSecond) can otherwise overflow TimeSpan's ~29,000 year range and throw.
        double throttledTimeoutSeconds = effectiveBatchSize / (double)workItem.ReindexRequestsPerSecond.Value * NoProgressTimeoutSafetyMultiplier;
        if (throttledTimeoutSeconds >= TimeSpan.MaxValue.TotalSeconds)
            return TimeSpan.MaxValue;

        var throttledTimeout = TimeSpan.FromSeconds(throttledTimeoutSeconds);
        return throttledTimeout > DefaultNoProgressTimeout ? throttledTimeout : DefaultNoProgressTimeout;
    }

    private record ReindexResult
    {
        public long Total { get; init; }
        public long Completed { get; init; }

        /// <summary>Documents this pass wrote that did not exist in the destination.</summary>
        public long Created { get; init; }

        /// <summary>
        /// Documents this pass overwrote in the destination. Together with <see cref="Created"/> this is what
        /// convergence is measured on: a pass that creates and updates nothing found nothing left to copy.
        /// </summary>
        public long Updated { get; init; }

        public long Deleted { get; init; }
        public long Noops { get; init; }
        public long Failures { get; init; }
        public ReindexOutcome Outcome { get; init; }

        /// <summary>Why the copy did not complete, for the exception the caller receives.</summary>
        public string? FailureReason { get; init; }
    }

    /// <summary>
    /// Why a copy pass stopped. Replaces a single "succeeded" flag, which conflated "never started",
    /// "gave up waiting" and "copied some documents but not all" into one indistinguishable false.
    /// </summary>
    private enum ReindexOutcome
    {
        /// <summary>Elasticsearch reported the copy task finished and no document failed to copy.</summary>
        Completed,

        /// <summary>The copy task was never created, so nothing was copied.</summary>
        NotStarted,

        /// <summary>Waiting was given up on: the task stalled, or its status could not be read.</summary>
        Abandoned,

        /// <summary>The copy ran but documents failed to copy, or the task itself reported an error.</summary>
        Failed
    }

    internal record TaskWithReindexResponse
    {
        [JsonPropertyName("response")]
        public TaskReindexResult? Response { get; init; }

        [JsonPropertyName("error")]
        public TaskReindexError? Error { get; init; }
    }

    internal record TaskReindexError
    {
        [JsonPropertyName("type")]
        public string? Type { get; init; }

        [JsonPropertyName("reason")]
        public string? Reason { get; init; }

        [JsonPropertyName("script_stack")]
        public List<string>? ScriptStack { get; init; }

        [JsonPropertyName("caused_by")]
        public TaskCause? CausedBy { get; init; }
    }

    internal record TaskCause
    {
        [JsonPropertyName("type")]
        public string? Type { get; init; }

        [JsonPropertyName("reason")]
        public string? Reason { get; init; }
    }

    /// <summary>
    /// The reindex sub-response of a task status. Every member is mapped explicitly because Elasticsearch
    /// names these fields in snake_case while the configured serializer only matches case-insensitively -
    /// which does not bridge an underscore. A silently unbound counter here is not a cosmetic problem: it
    /// feeds the completeness check, so <c>version_conflicts</c> failing to bind made a healthy reindex look
    /// like it had left documents unaccounted for.
    /// </summary>
    internal record TaskReindexResult
    {
        [JsonPropertyName("total")]
        public long Total { get; init; }

        [JsonPropertyName("created")]
        public long Created { get; init; }

        [JsonPropertyName("updated")]
        public long Updated { get; init; }

        [JsonPropertyName("noops")]
        public long Noops { get; init; }

        [JsonPropertyName("deleted")]
        public long Deleted { get; init; }

        [JsonPropertyName("timed_out")]
        public bool TimedOut { get; init; }

        [JsonPropertyName("version_conflicts")]
        public long VersionConflicts { get; init; }

        [JsonPropertyName("failures")]
        public IReadOnlyCollection<BulkIndexByScrollFailure>? Failures { get; init; }
    }

    private record TaskStatusValues
    {
        private static readonly TaskStatusValues _empty = new();

        public long Total { get; init; }
        public long Created { get; init; }
        public long Updated { get; init; }
        public long Noops { get; init; }
        public long VersionConflicts { get; init; }
        public long Deleted { get; init; }

        /// <summary>Documents the copy actually changed. Drives reported progress.</summary>
        public long Converged => Created + Updated + Noops + Deleted;

        /// <summary>
        /// Documents the copy examined, including ones rejected as version conflicts. Drives the stall
        /// watchdog: a replay over a destination that already holds newer documents makes genuine progress
        /// while changing nothing, so conflicts must count here or healthy work reads as a stall.
        /// </summary>
        public long Examined => Converged + VersionConflicts;

        /// <summary>
        /// Reads the counters out of the task status, which is typed <c>object?</c> and arrives as either a
        /// <see cref="JsonElement"/> or an <see cref="IDictionary{TKey, TValue}"/> depending on serializer
        /// config. Unrecognized shapes yield zeroed counters so callers never have to null-check.
        /// </summary>
        public static TaskStatusValues From(object? status, ILogger logger)
        {
            Func<string, long>? read = status switch
            {
                JsonElement json => name => json.TryGetProperty(name, out var value) ? value.GetInt64() : 0,
                IDictionary<string, object> dict => name => dict.TryGetValue(name, out var value) ? Convert.ToInt64(value) : 0,
                _ => null
            };

            if (read is null)
            {
                if (status is not null)
                    logger.LogWarning("Unexpected task status type {StatusType}: {Status}", status.GetType().Name, status);

                return _empty;
            }

            return new TaskStatusValues
            {
                Total = read("total"),
                Created = read("created"),
                Updated = read("updated"),
                Noops = read("noops"),
                Deleted = read("deleted"),
                VersionConflicts = read("version_conflicts")
            };
        }
    }

    /// <summary>
    /// A per-document copy failure from a reindex task's <c>failures</c> array. Members are mapped explicitly
    /// for the same reason as <see cref="TaskReindexResult"/>: the configured serializer matches only
    /// case-insensitively, so anything Elasticsearch names in snake_case must be spelled out.
    /// </summary>
    internal record BulkIndexByScrollFailure
    {
        [JsonPropertyName("cause")]
        public Error? Cause { get; init; }

        [JsonPropertyName("id")]
        public string? Id { get; init; }

        [JsonPropertyName("index")]
        public string? Index { get; init; }

        [JsonPropertyName("status")]
        public int Status { get; init; }

        [JsonPropertyName("type")]
        public string? Type { get; init; }
    }
    /// <summary>Prevents an unrecorded stale migration from moving an alias backwards after a newer cutover.</summary>
    private async Task EnsureSourceOwnsAliasAsync(ReindexWorkItem workItem, CancellationToken cancellationToken)
    {
        var response = await _client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.GET,
            $"/_alias/{Uri.EscapeDataString(workItem.Alias)}", cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        if (response.ApiCallDetails?.HttpStatusCode is 404)
            return;
        using var document = ReindexResponse.Parse(response, "Checking source alias ownership", cancellationToken);
        var root = document.RootElement;
        if (!root.TryGetProperty(workItem.OldIndex, out var source)
            || !ReindexResponse.Required(source, "aliases", JsonValueKind.Object).TryGetProperty(workItem.Alias, out var alias)
            || alias.ValueKind is not JsonValueKind.Object)
            throw new ReindexCompletionUnknownException(workItem.Alias, workItem.OldIndex, workItem.NewIndex,
                "the alias exists but no longer belongs to this physical source; refusing a stale migration");
    }

    /// <summary>The migration owns only an unpromoted destination; unknown aliases are never disposable.</summary>
    private async Task EnsureDestinationNotServingTrafficAsync(ReindexWorkItem workItem, CancellationToken cancellationToken)
    {
        var response = await _client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.GET,
            $"/{Uri.EscapeDataString(workItem.NewIndex)}/_alias", cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        if (response.ApiCallDetails?.HttpStatusCode is 404)
            return;
        using var document = ReindexResponse.Parse(response, "Checking destination ownership", cancellationToken);
        var root = document.RootElement;
        var entry = ReindexResponse.Required(root, workItem.NewIndex, JsonValueKind.Object);
        var aliases = ReindexResponse.Required(entry, "aliases", JsonValueKind.Object);
        if (root.EnumerateObject().Count() is not 1 || aliases.EnumerateObject().Any())
            throw new ReindexCompletionUnknownException(workItem.Alias, workItem.OldIndex, workItem.NewIndex,
                "the destination is already aliased without matching completion evidence; refusing to recopy live data");
    }

}
