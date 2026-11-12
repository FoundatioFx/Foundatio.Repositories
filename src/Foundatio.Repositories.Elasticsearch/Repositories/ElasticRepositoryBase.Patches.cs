using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Bulk;
using Elastic.Clients.Elasticsearch.Core.MGet;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Extensions;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// The outcome of a batch of patches, after all retry rounds.
/// </summary>
internal sealed class PatchBatchResult<TDocument> where TDocument : class
{
    /// <summary>Patched documents (client-side patches), with <see cref="IVersioned.Version"/> set from the write response.</summary>
    public List<TDocument> ModifiedDocuments { get; } = [];

    /// <summary>Ids of documents that were written.</summary>
    public List<string> ModifiedIds { get; } = [];

    /// <summary>Documents that disappeared before they could be patched. They are skipped.</summary>
    public HashSet<string> MissingIds { get; } = [];

    /// <summary>Documents still conflicting after the retry budget was used.</summary>
    public HashSet<string> ConflictIds { get; } = [];

    /// <summary>Documents that failed with a transient error after the retry budget was used.</summary>
    public HashSet<string> RetryableIds { get; } = [];

    public HashSet<string> VersioningNotSupportedIds { get; } = [];
    public HashSet<string> FatalIds { get; } = [];
    public Dictionary<string, BulkItemError> Errors { get; } = [];
    public string? TransportError { get; set; }
    public Exception? TransportException { get; set; }
    public int ConflictsObserved { get; set; }
    public int Rounds { get; set; }

    public bool HasErrors => TransportError is not null || ConflictIds.Count > 0 || RetryableIds.Count > 0 || VersioningNotSupportedIds.Count > 0 || FatalIds.Count > 0;

    /// <summary>
    /// Adds the ids, failures and counters of another batch. Patched documents are not copied, to keep memory bounded.
    /// </summary>
    public void MergeFailures(PatchBatchResult<TDocument> other)
    {
        ModifiedIds.AddRange(other.ModifiedIds);
        MissingIds.UnionWith(other.MissingIds);
        ConflictIds.UnionWith(other.ConflictIds);
        RetryableIds.UnionWith(other.RetryableIds);
        VersioningNotSupportedIds.UnionWith(other.VersioningNotSupportedIds);
        FatalIds.UnionWith(other.FatalIds);
        foreach (var error in other.Errors)
            Errors[error.Key] = error.Value;
        TransportError ??= other.TransportError;
        TransportException ??= other.TransportException;
        ConflictsObserved += other.ConflictsObserved;
        Rounds = Math.Max(Rounds, other.Rounds);
    }
}

public abstract partial class ElasticRepositoryBase<T>
{
    private sealed record PendingWrite(string Id, string Index, FindHit<T> Hit, T Document, ElasticDocumentVersion Version);

    /// <summary>
    /// Applies a client-side patch (JSON patch, action patch or pipeline partial patch) to a batch of documents and writes
    /// them with one bulk request per round. Documents that conflict or fail with a transient error are re-read together
    /// with one multi-get, re-patched in memory and written again with their fresh version, for up to <c>Retry(n)</c> rounds
    /// with exponential backoff and jitter. Documents deleted in the meantime are skipped.
    /// </summary>
    /// <param name="hits">The documents to patch, with the version they were read at.</param>
    /// <param name="apply">Returns the patched document, or <c>null</c> when the patch does not change it.</param>
    /// <param name="operation">The operation name used in logs.</param>
    /// <param name="options">The command options.</param>
    /// <param name="refresh">The refresh mode for each bulk request.</param>
    /// <param name="retryConflicts"><c>false</c> to fail on the first conflict (used when an expected version was given).</param>
    private async Task<PatchBatchResult<T>> ApplyClientPatchesAsync(IReadOnlyCollection<FindHit<T>> hits, Func<FindHit<T>, T?> apply, string operation, ICommandOptions options, Refresh refresh, bool retryConflicts = true)
    {
        var result = new PatchBatchResult<T>();
        int budget = options.GetRetryCount();
        bool checkVersion = HasVersion && !options.ShouldSkipVersionCheck();
        IReadOnlyCollection<FindHit<T>> pending = hits;

        for (int round = 0; round <= budget && pending.Count > 0; round++)
        {
            result.Rounds = round + 1;
            var writes = new List<PendingWrite>(pending.Count);
            foreach (var hit in pending)
            {
                if (hit.Id is null || hit.Document is null)
                    continue;

                var version = hit.GetElasticVersion();
                if (checkVersion)
                    ThrowIfMissingVersion(version, hit.GetIndex() ?? ElasticIndex.Name, hit.Id);

                var patched = apply(hit);
                if (patched is null)
                    continue;

                writes.Add(new PendingWrite(hit.Id, hit.GetIndex() ?? ElasticIndex.GetIndex(hit.Id), hit, patched, version));
            }

            if (writes.Count is 0)
                break;

            var bulkRequest = new BulkRequest
            {
                Refresh = refresh,
                Operations = writes.Select(w =>
                {
                    var indexOperation = new BulkIndexOperation<T>(w.Document)
                    {
                        Id = w.Id,
                        Index = w.Index,
                        Pipeline = DefaultPipeline
                    };
                    if (w.Hit.Routing is not null)
                        indexOperation.Routing = w.Hit.Routing;
                    if (checkVersion)
                    {
                        indexOperation.IfSequenceNumber = w.Version.SequenceNumber;
                        indexOperation.IfPrimaryTerm = w.Version.PrimaryTerm;
                    }

                    return (IBulkOperation)indexOperation;
                }).ToList()
            };

            var response = await _client.BulkAsync(bulkRequest, DisposedCancellationToken).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());
            var bulkResult = BulkResult.From(response);

            var retryHits = new List<FindHit<T>>();
            if (bulkResult.HasTransportError)
            {
                bool transient = bulkResult.TransportStatusCode is null || ElasticWriteErrors.IsTransientStatus(bulkResult.TransportStatusCode);
                if (!transient || round >= budget)
                {
                    result.TransportError = bulkResult.TransportError;
                    result.TransportException = bulkResult.TransportException;
                    _logger.LogError("{Operation} bulk request failed after {Rounds} rounds: {Error}", operation, round + 1, bulkResult.TransportError);
                    break;
                }

                retryHits.AddRange(writes.Select(w => w.Hit));
            }
            else
            {
                var writesById = writes.ToDictionary(w => w.Id);
                foreach (var item in response.Items)
                {
                    if (item.Id is null || !item.IsValid || !writesById.TryGetValue(item.Id, out PendingWrite? write))
                        continue;

                    write.Document.SetDocumentVersion(item.GetElasticVersion());
                    result.ModifiedDocuments.Add(write.Document);
                    result.ModifiedIds.Add(item.Id);
                }

                result.MissingIds.UnionWith(bulkResult.MissingIds);
                result.FatalIds.UnionWith(bulkResult.FatalIds);
                result.VersioningNotSupportedIds.UnionWith(bulkResult.VersioningNotSupportedIds);
                foreach (var error in bulkResult.Errors)
                    result.Errors[error.Key] = error.Value;

                result.ConflictsObserved += bulkResult.ConflictIds.Count;
                bool lastRound = round >= budget;
                foreach (string id in bulkResult.ConflictIds)
                {
                    var error = bulkResult.Errors.GetValueOrDefault(id);
                    bool willRetry = retryConflicts && !lastRound;
                    LogVersionConflict(operation, error?.Index, id, round + 1, budget, writesById[id].Version.ToVersionString(), error?.Reason, willRetry ? "Retrying" : "Failed");
                    if (willRetry)
                        retryHits.Add(writesById[id].Hit);
                    else
                        result.ConflictIds.Add(id);
                }

                foreach (string id in bulkResult.RetryableIds)
                {
                    if (lastRound)
                        result.RetryableIds.Add(id);
                    else
                        retryHits.Add(writesById[id].Hit);
                }
            }

            if (retryHits.Count is 0)
                break;

            _logger.LogInformation("{Operation} round {Round} of {Budget}: {Modified} written, {Retrying} retrying ({Conflicts} conflicts), {Missing} skipped as deleted",
                operation, round + 1, budget + 1, result.ModifiedIds.Count, retryHits.Count, bulkResult.ConflictIds.Count, result.MissingIds.Count);

            await ElasticIndex.Configuration.TimeProvider.SafeDelay(GetRetryDelayWithJitter(round + 1), DisposedCancellationToken).AnyContext();
            if (DisposedCancellationToken.IsCancellationRequested)
                break;

            pending = await RefetchHitsAsync(retryHits, result, options).AnyContext();
        }

        if (result.ConflictIds.Count > 0 || result.RetryableIds.Count > 0)
        {
            _logger.LogError("{Operation} exhausted its retry budget of {Budget}: {Conflicts} documents still conflicting, {Retryable} still failing with transient errors",
                operation, budget, result.ConflictIds.Count, result.RetryableIds.Count);
        }

        return result;
    }

    /// <summary>
    /// Re-reads documents (with their current version) in one multi-get. Documents that no longer exist are added to
    /// <see cref="PatchBatchResult{TDocument}.MissingIds"/>.
    /// </summary>
    private async Task<IReadOnlyCollection<FindHit<T>>> RefetchHitsAsync(IReadOnlyCollection<FindHit<T>> hits, PatchBatchResult<T> result, ICommandOptions options)
    {
        var operations = hits.Select(h =>
        {
            var op = new MultiGetOperation(h.Id!) { Index = h.GetIndex() ?? ElasticIndex.GetIndex(h.Id!) };
            if (h.Routing is not null)
                op.Routing = h.Routing;
            return op;
        }).ToList();

        var response = await _client.MultiGetAsync<T>(new MultiGetRequestDescriptor().Docs(operations), DisposedCancellationToken).AnyContext();
        _logger.LogRequest(response, options.GetQueryLogLevel());
        if (!response.IsValidResponse)
            throw new DocumentException(response.GetErrorMessage("Error re-reading documents for patch retry"), response.OriginalException());

        var fresh = response.ToFindHits(_logger).ToList();
        var freshIds = fresh.Select(h => h.Id).ToHashSet();
        foreach (var hit in hits)
        {
            if (!freshIds.Contains(hit.Id))
                result.MissingIds.Add(hit.Id!);
        }

        return fresh;
    }
}
