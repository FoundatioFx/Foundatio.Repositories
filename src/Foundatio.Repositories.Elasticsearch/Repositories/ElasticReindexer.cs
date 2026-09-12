using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
    private readonly IResiliencePolicyProvider _resiliencePolicyProvider;
    private readonly IResiliencePolicy _resiliencePolicy;
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

    /// <summary>
    /// Returns the stable id identifying a physical migration, so the same migration is recognizable across
    /// redelivery, process restart, and direct-versus-queued execution.
    /// </summary>
    /// <remarks>
    /// Keyed by the logical migration - alias plus source and destination index names - and explicitly not by
    /// an attempt id, delivery id, or job id, all of which change on redelivery and would make every retry
    /// look like a migration that had never run.
    /// </remarks>
    public static string GetCompletionId(ReindexWorkItem workItem)
    {
        ArgumentNullException.ThrowIfNull(workItem);

        return String.Concat(workItem.Alias, "|", workItem.OldIndex, "|", workItem.NewIndex);
    }

    /// <summary>
    /// Returns a fingerprint of the transformation the copy applies, so a completion record written for a
    /// different script cannot vouch for this migration.
    /// </summary>
    internal static string GetTransformationFingerprint(ReindexWorkItem workItem)
    {
        if (String.IsNullOrEmpty(workItem.Script))
            return "none";

        byte[] hash = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(workItem.Script));
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    public ElasticReindexer(ElasticsearchClient client, ITextSerializer serializer, ILogger? logger = null) : this(client, serializer, TimeProvider.System, logger)
    {
    }

    public ElasticReindexer(ElasticsearchClient client, ITextSerializer serializer, TimeProvider timeProvider, ILogger? logger = null) : this(client, serializer, timeProvider ?? TimeProvider.System, new ResiliencePolicyProvider(), logger ?? NullLogger.Instance)
    {
    }

    public ElasticReindexer(ElasticsearchClient client, ITextSerializer serializer, TimeProvider timeProvider, IResiliencePolicyProvider resiliencePolicyProvider, ILogger? logger = null)
    {
        _client = client;
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _timeProvider = timeProvider ?? TimeProvider.System;
        _resiliencePolicyProvider = resiliencePolicyProvider ?? new ResiliencePolicyProvider();
        _logger = logger ?? NullLogger.Instance;

        _resiliencePolicy = _resiliencePolicyProvider.GetPolicy<ElasticReindexer>(fallback => fallback.WithMaxAttempts(5).WithDelay(TimeSpan.FromSeconds(10)), _logger, _timeProvider);
    }

    /// <summary>
    /// Copies documents from the old index to the new one and moves the aliases across.
    /// </summary>
    /// <param name="workItem">Describes the source, destination, alias, and copy options.</param>
    /// <param name="progressCallbackAsync">Invoked with a percentage and a status message.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait for the server-side copy. Note this abandons the client's wait; the Elasticsearch
    /// <c>_reindex</c> task itself was started with <c>wait_for_completion=false</c> and continues running
    /// server-side.
    /// </param>
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

        // Determined before the copy so an unsupported configuration is refused cheaply rather than after an
        // expensive copy. This is a per-shard stats read, not a scan.
        var catchUpPlan = await PlanCatchUpAsync(workItem, cancellationToken).AnyContext();

        var firstPassResult = await InternalReindexAsync(workItem, progressCallbackAsync, 0, 90, workItem.StartUtc, cancellationToken).AnyContext();
        EnsureCopyCompleted(workItem, firstPassResult);

        await progressCallbackAsync(91, $"Total: {firstPassResult.Total:N0} Completed: {firstPassResult.Completed:N0}").AnyContext();

        // Enforced BEFORE any alias changes. When the catch-up pass cannot run, the only way to keep the
        // promise the cutover implies is to not make it: if the source changed during the copy, those changes
        // cannot be found again, so promoting the destination would silently serve an incomplete index. This
        // deliberately runs pre-Switch - turning the old post-cutover warning into a post-cutover exception
        // would report the failure without preventing it, since the alias has already moved.
        await EnsureCatchUpPossibleAsync(workItem, catchUpPlan, cancellationToken).AnyContext();

        if (workItem.OldIndex != workItem.NewIndex)
            await SwitchAliasesAsync(workItem, progressCallbackAsync, cancellationToken).AnyContext();

        if (catchUpPlan.CanCatchUp)
            await RunCatchUpPassAsync(workItem, progressCallbackAsync, startTime, cancellationToken).AnyContext();

        // Verify the destination isn't short of the source on every reindex, not only when the old index
        // happens to be scheduled for deletion - the shortfall gate used to live inside the delete branch, so
        // a DeleteOld = false reindex was never checked at all.
        bool countsVerified = await VerifyDocumentCountsAsync(workItem, progressCallbackAsync, cancellationToken).AnyContext();

        // Recorded here because every step that can fail the migration has now succeeded, and deliberately
        // before the queue item is acknowledged and before the source is deleted. Writing it any earlier would
        // make it evidence of a migration still in progress; writing it after cleanup would leave a finished
        // migration indistinguishable from one that failed after the cutover.
        if (workItem.OldIndex != workItem.NewIndex)
            await RecordCompletionAsync(workItem, cancellationToken).AnyContext();

        // Cleanup is deliberately after the completion record and is not allowed to undo it: deleting the
        // source is an optimization, and a failure there must not send a finished migration back to copying.
        if (countsVerified && workItem.DeleteOld && workItem.OldIndex != workItem.NewIndex)
            await DeleteOldIndexAsync(workItem, progressCallbackAsync, cancellationToken).AnyContext();

        await progressCallbackAsync(100, "Reindex complete").AnyContext();
    }

    /// <summary>
    /// Decides, before the copy starts, whether this reindex can catch up documents written to the source while
    /// the copy runs — and records the source's sequence number so a later change can be detected.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The catch-up pass narrows the source by time. With a <see cref="ReindexWorkItem.TimestampField"/> it
    /// queries that field directly; without one it slices by document id, which only works for ObjectId-format
    /// ids because their prefix encodes creation time. Any other id format leaves no way to find documents
    /// written during the copy.
    /// </para>
    /// <para>
    /// A sampled id is explicitly <em>not</em> treated as proof: ids are not required to be homogeneous, so one
    /// ObjectId does not establish that catch-up will find everything. It is used only in the negative
    /// direction — to recognize a source that definitely cannot be sliced by id — which is why a failed or
    /// empty sample leaves catch-up enabled rather than refusing. Deliberately no full scan: inferring id
    /// formats across a 500 GB index would cost more than the migration.
    /// </para>
    /// </remarks>
    private async Task<CatchUpPlan> PlanCatchUpAsync(ReindexWorkItem workItem, CancellationToken cancellationToken)
    {
        // Elasticsearch rejects a reindex whose source and destination are the same index, so this never
        // reaches the guard - the copy fails first. Exempting it keeps the guard from reading a sequence
        // number it cannot use and from ever masking that validation error with a misleading refusal.
        if (workItem.OldIndex == workItem.NewIndex)
            return new CatchUpPlan(CanCatchUp: !String.IsNullOrEmpty(workItem.TimestampField), InPlace: true);

        if (!String.IsNullOrEmpty(workItem.TimestampField))
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
                return new CatchUpPlan(CanCatchUp: false, SourceIsEmpty: true);

            case SampleIdStatus.Found when ObjectId.TryParse(sampleResult.Id!, out _):
                _logger.LogInformation("Reindex {OldIndex} -> {NewIndex}: Using ObjectId-based second pass (no TimestampField).", workItem.OldIndex, workItem.NewIndex);
                return new CatchUpPlan(CanCatchUp: true);

            case SampleIdStatus.Found:
                // The one case that genuinely cannot catch up. Record the source's sequence number so the
                // enforcement below can tell a static source (safe) from one being written to (not safe).
                long? maxSeqNo = await TryGetMaxSequenceNumberAsync(workItem.OldIndex, cancellationToken).AnyContext();
                _logger.LogInformation(
                    "Reindex {OldIndex} -> {NewIndex}: No TimestampField and IDs are not ObjectIds (sample: {SampleId}), so documents written during the copy cannot be caught up. The copy will only be promoted if the source does not change while it runs.",
                    workItem.OldIndex, workItem.NewIndex, sampleResult.Id);
                return new CatchUpPlan(CanCatchUp: false, StartingMaxSequenceNumber: maxSeqNo, SequenceNumberReadable: maxSeqNo.HasValue);

            default:
                _logger.LogWarning(sampleResult.Exception,
                    "Reindex {OldIndex} -> {NewIndex}: Failed to sample document ID ({Error}). Attempting ObjectId-based second pass anyway.",
                    workItem.OldIndex, workItem.NewIndex, sampleResult.Error);
                return new CatchUpPlan(CanCatchUp: true);
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
    /// Change is detected by the source's maximum sequence number, which advances on inserts, updates, and
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
    private async Task EnsureCatchUpPossibleAsync(ReindexWorkItem workItem, CatchUpPlan plan, CancellationToken cancellationToken)
    {
        if (plan.CanCatchUp || plan.SourceIsEmpty || plan.InPlace)
            return;

        if (!plan.SequenceNumberReadable)
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                $"{workItem.OldIndex} has no timestamp field and its document ids are not ObjectIds, so documents written during the copy cannot be caught up - and whether any were written could not be determined because the source's sequence numbers could not be read. Refusing to promote {workItem.NewIndex}. Add IHaveDates to the model, use ObjectId-format ids, or stop writes to {workItem.OldIndex} for the duration of the migration.");

        long? currentMaxSeqNo = await TryGetMaxSequenceNumberAsync(workItem.OldIndex, cancellationToken).AnyContext();
        if (currentMaxSeqNo is null)
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                $"{workItem.OldIndex} has no timestamp field and its document ids are not ObjectIds, so documents written during the copy cannot be caught up - and whether any were written could not be confirmed because the source's sequence numbers could not be re-read. Refusing to promote {workItem.NewIndex}.");

        if (currentMaxSeqNo > plan.StartingMaxSequenceNumber)
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                $"{workItem.OldIndex} was written to during the copy (sequence number advanced from {plan.StartingMaxSequenceNumber:N0} to {currentMaxSeqNo:N0}), and because it has no timestamp field and its document ids are not ObjectIds those changes cannot be caught up. Refusing to promote {workItem.NewIndex}; {workItem.OldIndex} is unchanged and still serving the alias. Add IHaveDates to the model, use ObjectId-format ids, or stop writes for the duration of the migration.");

        _logger.LogInformation("Reindex {OldIndex} -> {NewIndex}: Source was not written to during the copy (sequence number {SequenceNumber:N0}), so no catch-up is required.",
            workItem.OldIndex, workItem.NewIndex, currentMaxSeqNo);
    }

    /// <summary>
    /// Whether a reindex can catch up writes that land during the copy, plus the evidence needed to tell
    /// whether any did.
    /// </summary>
    private sealed record CatchUpPlan(
        bool CanCatchUp,
        bool SourceIsEmpty = false,
        long? StartingMaxSequenceNumber = null,
        bool SequenceNumberReadable = false,
        bool InPlace = false);

    /// <summary>
    /// Copies documents written to the source while the first pass was running. Throws if this pass does not
    /// complete, so a reindex that skipped the catch-up is never reported as successful.
    /// </summary>
    private async Task RunCatchUpPassAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, DateTime startTime, CancellationToken cancellationToken)
    {
        var result = await InternalReindexAsync(workItem, progressCallbackAsync, 92, 96, startTime, cancellationToken).AnyContext();
        EnsureCopyCompleted(workItem, result);

        await progressCallbackAsync(97, $"Total: {result.Total:N0} Completed: {result.Completed:N0}").AnyContext();
    }

    /// <summary>
    /// Compares source and destination document counts after the copy and reports whether the destination
    /// looks complete, which is the precondition for deleting the source.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is a coarse safety net, not proof of completeness, and deliberately never throws. By the time it
    /// runs the aliases have already been switched, so writes - including hard deletes - land on the
    /// destination while the source is frozen. A legitimately complete reindex can therefore end up with fewer
    /// documents than its source, and treating that as a failure would ask callers to retry a reindex that
    /// would resurrect the deleted documents. In the other direction, documents written to the destination
    /// after the cutover can offset documents that genuinely failed to copy, hiding a real shortfall.
    /// </para>
    /// <para>
    /// Completeness is established instead by the per-pass accounting in
    /// <see cref="InternalReindexAsync"/>, which compares the copy task's own report of what it matched
    /// against what it did and so cannot race with live traffic.
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

        var refreshResponse = await _client.Indices.RefreshAsync(Indices.Index(workItem.OldIndex).And(workItem.NewIndex), d => d.IgnoreUnavailable(), cancellationToken).AnyContext();
        _logger.LogRequest(refreshResponse);
        if (!refreshResponse.IsValidResponse)
            _logger.LogWarning("Failed to refresh indices before doc count comparison for {OldIndex} -> {NewIndex}: {Error}", workItem.OldIndex, workItem.NewIndex, refreshResponse.ElasticsearchServerError);

        var newDocCountResponse = await _client.CountAsync<object>(d => d.Indices(workItem.NewIndex), cancellationToken).AnyContext();
        _logger.LogRequest(newDocCountResponse);
        if (!newDocCountResponse.IsValidResponse)
            _logger.LogWarning("Failed to get new index doc count for {NewIndex}: {Error}", workItem.NewIndex, newDocCountResponse.ElasticsearchServerError);

        var oldDocCountResponse = await _client.CountAsync<object>(d => d.Indices(workItem.OldIndex), cancellationToken).AnyContext();
        _logger.LogRequest(oldDocCountResponse);
        if (!oldDocCountResponse.IsValidResponse)
            _logger.LogWarning("Failed to get old index doc count for {OldIndex}: {Error}", workItem.OldIndex, oldDocCountResponse.ElasticsearchServerError);

        await progressCallbackAsync(98, $"Old Docs: {oldDocCountResponse.Count} New Docs: {newDocCountResponse.Count}").AnyContext();

        if (!newDocCountResponse.IsValidResponse || !oldDocCountResponse.IsValidResponse)
        {
            _logger.LogWarning("Could not verify the reindex of {OldIndex} -> {NewIndex} was complete because a document count could not be read. Treating the reindex as unverified and keeping {OldIndex}.",
                workItem.OldIndex, workItem.NewIndex, workItem.OldIndex);
            return false;
        }

        if (newDocCountResponse.Count >= oldDocCountResponse.Count)
            return true;

        long missing = oldDocCountResponse.Count - newDocCountResponse.Count;

        // A script can legitimately drop documents (`ctx.op = "noop"`/`"delete"`), and so can a hard delete
        // through the already-switched alias, so this is reported rather than treated as a failure. Either way
        // the source is kept, so nothing is lost if the shortfall was real.
        _logger.LogWarning("Reindex of {OldIndex} -> {NewIndex} left the destination {MissingCount:N0} document(s) short ({OldCount:N0} -> {NewCount:N0}). This can be legitimate - a reindex script can drop documents, and documents deleted through the alias after the cutover are gone from the destination but still counted in the source - so it is not treated as a failure, but {OldIndex} will not be deleted.",
            workItem.OldIndex, workItem.NewIndex, missing, oldDocCountResponse.Count, newDocCountResponse.Count, workItem.OldIndex);

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

        var result = await _resiliencePolicy.ExecuteAsync(async ct =>
        {
            var response = await _client.ReindexAsync(d =>
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

                if (workItem.ReindexRequestsPerSecond.HasValue)
                    d.RequestsPerSecond(workItem.ReindexRequestsPerSecond.Value);

                if (!String.IsNullOrWhiteSpace(workItem.Script))
                    d.Script(new Script { Source = workItem.Script });
            }, ct).AnyContext();
            _logger.LogRequest(response);

            return response;
        }, cancellationToken).AnyContext();

        if (result.Task is null)
        {
            string reason = result.ElasticsearchServerError?.Error?.Reason ?? "Unknown";
            _logger.LogError("Reindex failed to start - no task returned. Response valid: {IsValid}, Reason: {Reason}",
                result.IsValidResponse, reason);
            _logger.LogErrorRequest(result, "Reindex failed");

            return new ReindexResult
            {
                Outcome = ReindexOutcome.NotStarted,
                FailureReason = $"Elasticsearch did not return a reindex task, so no documents were copied ({reason})."
            };
        }

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
            if (status.IsValidResponse)
            {
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

            // The reindex sub-response (the per-document failures and the created/updated/noop counters that
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

            await TryCancelTaskAsync(result.Task, workItem.OldIndex, workItem.NewIndex).AnyContext();
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
        long completed = (lastReindexResponse?.Created ?? 0) + (lastReindexResponse?.Updated ?? 0) + (lastReindexResponse?.Noops ?? 0);

        // The copy must account for every document it matched. This is the one completeness check that cannot
        // race with live traffic, because it compares the task's own report of what it matched against its own
        // report of what it did - unlike comparing index document counts, which the alias cutover makes
        // unreliable (see VerifyDocumentCountsAsync).
        if (outcome is ReindexOutcome.Completed && completed + versionConflicts < total)
        {
            long unaccounted = total - (completed + versionConflicts);
            outcome = ReindexOutcome.Failed;
            failureReason = $"the copy task reported it finished but only accounted for {completed + versionConflicts:N0} of the {total:N0} document(s) it matched, leaving {unaccounted:N0} unaccounted for";
        }

        string message = $"Total: {total:N0} Completed: {completed:N0} VersionConflicts: {versionConflicts:N0}";
        await progressCallbackAsync(CalculateProgress(total, completed, startProgress, endProgress), message).AnyContext();
        return new ReindexResult { Total = total, Completed = completed, Failures = failures, Outcome = outcome, FailureReason = failureReason };
    }

    /// <summary>
    /// Builds the task-status request used to poll a running reindex, opting this single request into keeping
    /// the raw response body.
    /// </summary>
    /// <remarks>
    /// The reindex sub-response - the per-document <c>failures</c> array and the created/updated/noop counters
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
            response = status.DeserializeRaw<TaskWithReindexResponse>(_serializer);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to deserialize the task status response while reindexing");
            failureReason = $"The reindex task status could not be parsed ({ex.Message}), so whether any documents failed to copy could not be determined.";
            return false;
        }
    }

    /// <summary>
    /// Makes already-written documents visible to search before a copy pass reads them. Failure is logged and
    /// tolerated: a refresh that did not happen means the pass may copy less than it could have, which the
    /// completeness verification at the end of the reindex catches.
    /// </summary>
    private async Task RefreshForCopyAsync(ReindexWorkItem workItem, CancellationToken cancellationToken)
    {
        var indices = workItem.OldIndex == workItem.NewIndex
            ? Indices.Index(workItem.OldIndex)
            : Indices.Index(workItem.OldIndex).And(workItem.NewIndex);

        // The destination does not exist yet on the first pass of some reindexes, and a refresh of a missing
        // index is a 404 rather than a no-op.
        var refreshResponse = await _client.Indices.RefreshAsync(indices, d => d.IgnoreUnavailable(), cancellationToken).AnyContext();
        _logger.LogRequest(refreshResponse);

        if (!refreshResponse.IsValidResponse)
            _logger.LogWarning("Failed to refresh {OldIndex} and {NewIndex} before copying, so the copy may read stale data: {Error}", workItem.OldIndex, workItem.NewIndex, refreshResponse.ElasticsearchServerError);
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

    /// <summary>
    /// Attempts to cancel the Elasticsearch server-side reindex task. Best-effort — failures are logged but not propagated.
    /// </summary>
    private async Task TryCancelTaskAsync(TaskId reindexTaskId, string oldIndex, string newIndex)
    {
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var response = await _client.Tasks.CancelAsync(c => c.TaskId(reindexTaskId), cts.Token).AnyContext();
            if (response.IsValidResponse)
            {
                _logger.LogRequest(response);
                _logger.LogInformation("Cancelled reindex task {ReindexTaskId} for {OldIndex} -> {NewIndex}", reindexTaskId.FullyQualifiedId, oldIndex, newIndex);
            }
            else
            {
                _logger.LogErrorRequest(response, "Failed to cancel reindex task {ReindexTaskId} for {OldIndex} -> {NewIndex}", reindexTaskId.FullyQualifiedId, oldIndex, newIndex);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Exception cancelling reindex task {ReindexTaskId} for {OldIndex} -> {NewIndex}", reindexTaskId.FullyQualifiedId, oldIndex, newIndex);
        }
    }

    /// <summary>
    /// Returns every alias on <paramref name="index"/> along with its definition, so the definitions can be
    /// recreated on the destination rather than silently defaulted.
    /// </summary>
    /// <exception cref="RepositoryException">
    /// The alias list could not be read. This is fatal by design: proceeding would move the primary alias
    /// while silently stranding every other alias on the old index, and would drop alias filters that may be
    /// the only thing scoping a shared index.
    /// </exception>
    private async Task<Dictionary<string, AliasDefinition>> GetIndexAliasesAsync(string index, CancellationToken cancellationToken)
    {
        var aliasesResponse = await _client.Indices.GetAliasAsync(Indices.Index(index), cancellationToken).AnyContext();
        _logger.LogRequest(aliasesResponse);

        var result = new Dictionary<string, AliasDefinition>(StringComparer.Ordinal);

        if (aliasesResponse.IsValidResponse)
        {
#if ELASTICSEARCH9
            var indices = aliasesResponse.Aliases;
#else
            var indices = aliasesResponse.Values;
#endif
            if (indices is null || indices.Count is 0)
                return result;

            // FirstOrDefault rather than SingleOrDefault: a response carrying more than one entry for the
            // requested index must not throw from inside the cutover path.
            var match = indices.FirstOrDefault(a => String.Equals(a.Key, index));
            if (match.Value?.Aliases is null)
                return result;

            foreach (var (alias, definition) in match.Value.Aliases)
                result[alias] = definition ?? new AliasDefinition();

            return result;
        }

        // A missing index has no aliases to move, which is not an error here.
        if (aliasesResponse.ApiCallDetails is { HttpStatusCode: 404 })
            return result;

        throw new RepositoryException(
            aliasesResponse.GetErrorMessage($"Error getting aliases for index {index}; refusing to move aliases without knowing what they are"),
            aliasesResponse.OriginalException());
    }

    /// <summary>
    /// Moves every alias on the old index across to the new one in a single atomic update, which is the
    /// point at which the new index starts serving traffic.
    /// </summary>
    /// <exception cref="ReindexIncompleteException">
    /// The alias update failed, so traffic is still being served by the old index.
    /// </exception>
    private async Task SwitchAliasesAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, CancellationToken cancellationToken)
    {
        var aliases = await GetIndexAliasesAsync(workItem.OldIndex, cancellationToken).AnyContext();

        // A default definition carries no metadata, which reproduces the plain add this used to build for the
        // primary alias when it wasn't already present on the old index.
        if (!String.IsNullOrEmpty(workItem.Alias))
            aliases.TryAdd(workItem.Alias, new AliasDefinition());

        if (aliases.Count is 0)
            return;

        var aliasActions = new List<IndexUpdateAliasesAction>();
        foreach (var (alias, definition) in aliases)
        {
            aliasActions.Add(new IndexUpdateAliasesAction { Remove = new RemoveAction { Alias = alias, Index = workItem.OldIndex } });
            aliasActions.Add(new IndexUpdateAliasesAction { Add = CreateAddAction(alias, definition, workItem.NewIndex) });
        }

        var bulkResponse = await _client.Indices.UpdateAliasesAsync(x => x.Actions(aliasActions), cancellationToken).AnyContext();
        if (!bulkResponse.IsValidResponse)
        {
            _logger.LogErrorRequest(bulkResponse, "Error updating aliases during reindex");

            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                $"the documents were copied but the aliases ({String.Join(", ", aliases.Keys)}) could not be switched to the new index, so traffic is still being served by {workItem.OldIndex} ({bulkResponse.ElasticsearchServerError})");
        }

        _logger.LogRequest(bulkResponse);
        await progressCallbackAsync(92, $"Updated aliases: {String.Join(", ", aliases.Keys)} Remove: {workItem.OldIndex} Add: {workItem.NewIndex}").AnyContext();
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

    /// <summary>
    /// Reads the source's highest assigned sequence number, which is the cheapest date-independent way to tell
    /// whether the source changed during a copy.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Elasticsearch assigns a monotonically increasing <c>_seq_no</c> to every write on a shard, so comparing
    /// the maximum across shards before and after a copy detects inserts, same-count updates, and deletes alike
    /// — none of which document counts reliably reveal. This is a per-shard stats read, not a scan, so it costs
    /// the same regardless of index size.
    /// </para>
    /// <para>
    /// Taken from shard-level stats rather than a <c>max</c> aggregation on <c>_seq_no</c>, because the
    /// aggregation only sees live documents: a delete leaves the searchable maximum unchanged while the shard's
    /// <c>max_seq_no</c> advances. Verified against a live cluster - after deleting a document the aggregation
    /// still reported 4 while the shard reported 5.
    /// </para>
    /// </remarks>
    /// <returns>The highest sequence number across the index's primary shards, or <c>null</c> if it could not be read.</returns>
    private async Task<long?> TryGetMaxSequenceNumberAsync(string index, CancellationToken cancellationToken)
    {
        var response = await _client.Indices.StatsAsync((Indices)index, d => d.Level(Level.Shards), cancellationToken).AnyContext();
        _logger.LogRequest(response);

        if (!response.IsValidResponse)
        {
            _logger.LogWarning("Could not read sequence numbers for {Index}: {Error}", index, response.GetErrorMessage("Stats failed"));
            return null;
        }

        long? max = null;
        foreach (var shards in response.Indices?.Values.SelectMany(i => i.Shards?.Values ?? []) ?? [])
        {
            foreach (var shard in shards)
            {
                if (shard.Routing?.Primary is not true || shard.SeqNo is null)
                    continue;

                long shardMax = shard.SeqNo.MaxSeqNo;
                if (max is null || shardMax > max)
                    max = shardMax;
            }
        }

        if (max is null)
            _logger.LogWarning("Sequence numbers were missing from the shard stats for {Index}", index);

        return max;
    }

    /// <summary>
    /// Reads the destination index's UUID, which identifies one physical generation of that index name.
    /// </summary>
    private async Task<string?> TryGetIndexUuidAsync(string index, CancellationToken cancellationToken)
    {
        var response = await _client.Indices.GetSettingsAsync((Indices)index, cancellationToken).AnyContext();
        _logger.LogRequest(response);

        if (!response.IsValidResponse)
        {
            _logger.LogWarning("Could not read the index uuid for {Index}: {Error}", index, response.GetErrorMessage("Get settings failed"));
            return null;
        }

        foreach (var state in response.Settings.Values)
        {
            string? uuid = state.Settings?.Index?.Uuid;
            if (!String.IsNullOrEmpty(uuid))
                return uuid;
        }

        _logger.LogWarning("The index uuid was missing from the settings for {Index}", index);
        return null;
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

        if (!response.IsValidResponse && response.ApiCallDetails.HttpStatusCode is not 404)
        {
            _logger.LogWarning("Could not read the completion record for {OldIndex} -> {NewIndex}: {Error}",
                workItem.OldIndex, workItem.NewIndex, response.GetErrorMessage("Get failed"));
            return false;
        }

        if (!response.Found || response.Source is null)
            return false;

        var completion = response.Source;
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

    private sealed record SampleIdResult(SampleIdStatus Status, string? Id = null, string? Error = null, Exception? Exception = null); private async Task<SampleIdResult> GetSampleDocumentIdAsync(string index, CancellationToken cancellationToken)
    {
        var response = await _client.SearchAsync<IDictionary<string, object>>(d => d
            .Indices(index)
            .Source(new SourceConfig(false))
            .Size(1), cancellationToken
        ).AnyContext();

        _logger.LogRequest(response);

        if (!response.IsValidResponse)
            return new SampleIdResult(SampleIdStatus.Failed, Error: response.GetErrorMessage("Search failed"), Exception: response.OriginalException());

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

        /// <summary>Documents the copy actually changed. Drives reported progress.</summary>
        public long Converged => Created + Updated + Noops;

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
}
