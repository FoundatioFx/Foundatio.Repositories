using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Extensions;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch;

public abstract partial class ElasticRepositoryBase<T>
{
    /// <summary>
    /// A document targeted by a bulk Update API request.
    /// </summary>
    private readonly record struct UpdateTarget(string Id, string Index, string? Routing);

    /// <summary>
    /// Runs a script or partial patch through the bulk Update API. Elasticsearch re-runs the update on the latest document
    /// up to <c>Retry(n)</c> times on a conflict (<c>retry_on_conflict</c>). Items rejected with a transient error (HTTP 429,
    /// 502, 503, 504) are re-sent in later rounds with exponential backoff and jitter, up to the same budget.
    /// </summary>
    /// Documents that no longer exist (HTTP 404) are skipped when <paramref name="missingIsSkipped"/> is <c>true</c>; otherwise they are reported as failures.
    private async Task<PatchBatchResult<T>> ApplyServerPatchesAsync(IReadOnlyCollection<UpdateTarget> targets, IPatchOperation operation, string operationName, ICommandOptions options, Refresh refresh, bool missingIsSkipped)
    {
        var result = new PatchBatchResult<T>();
        int budget = options.GetRetryCount();
        IReadOnlyCollection<UpdateTarget> pending = targets;

        for (int round = 0; round <= budget && pending.Count > 0; round++)
        {
            result.Rounds = round + 1;
            var response = await _client.BulkAsync(b =>
            {
                b.Refresh(refresh);
                foreach (var target in pending)
                {
                    if (operation is ScriptPatch scriptOperation)
                    {
                        b.Update<T>(u =>
                        {
                            u.Id(target.Id)
                                .Index(target.Index)
                                .Script(s => s.Source(scriptOperation.Script).Params(scriptOperation.Params))
                                .RetriesOnConflict(budget);
                            if (target.Routing is not null)
                                u.Routing(target.Routing);
                        });
                    }
                    else if (operation is PartialPatch partialOperation)
                    {
                        // TODO: Null-valued properties are silently dropped by the ES client's SourceSerializer
                        // (elastic/elasticsearch-net#8763). Consumers must use ScriptPatch or JsonPatch to set fields to null.
                        b.Update<T, object>(u =>
                        {
                            u.Id(target.Id)
                                .Index(target.Index)
                                .Doc(partialOperation.Document)
                                .RetriesOnConflict(budget);
                            if (target.Routing is not null)
                                u.Routing(target.Routing);
                        });
                    }
                }
            }, DisposedCancellationToken).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());

            var bulkResult = BulkResult.From(response);
            bool lastRound = round >= budget;
            if (bulkResult.HasTransportError)
            {
                bool transient = bulkResult.TransportStatusCode is null || ElasticWriteErrors.IsTransientStatus(bulkResult.TransportStatusCode);
                if (!transient || lastRound)
                {
                    result.TransportError = bulkResult.TransportError;
                    result.TransportException = bulkResult.TransportException;
                    _logger.LogError("{Operation} bulk request failed after {Rounds} rounds: {Error}", operationName, round + 1, bulkResult.TransportError);
                    break;
                }

                await ElasticIndex.Configuration.TimeProvider.SafeDelay(GetRetryDelayWithJitter(round + 1), DisposedCancellationToken).AnyContext();
                continue;
            }

            foreach (string id in bulkResult.SuccessfulIds)
            {
                if (!bulkResult.NoopIds.Contains(id))
                    result.ModifiedIds.Add(id);
            }

            foreach (var error in bulkResult.Errors)
                result.Errors[error.Key] = error.Value;

            foreach (string id in bulkResult.FatalIds)
            {
                if (missingIsSkipped && bulkResult.Errors.GetValueOrDefault(id)?.Status is 404)
                    result.MissingIds.Add(id);
                else
                    result.FatalIds.Add(id);
            }

            result.MissingIds.UnionWith(bulkResult.MissingIds);
            result.VersioningNotSupportedIds.UnionWith(bulkResult.VersioningNotSupportedIds);
            result.ConflictsObserved += bulkResult.ConflictIds.Count;
            foreach (string id in bulkResult.ConflictIds)
            {
                var error = bulkResult.Errors.GetValueOrDefault(id);
                LogVersionConflict(operationName, error?.Index, id, budget + 1, budget, null, error?.Reason, "Failed after retry_on_conflict");
                result.ConflictIds.Add(id);
            }

            if (!bulkResult.HasRetryableErrors)
                break;

            if (lastRound)
            {
                result.RetryableIds.UnionWith(bulkResult.RetryableIds);
                _logger.LogError("{Operation} exhausted its retry budget of {Budget} with {Count} documents still failing with transient errors", operationName, budget, bulkResult.RetryableIds.Count);
                break;
            }

            _logger.LogWarning("{Operation} round {Round} of {Budget}: {Count} documents failed with transient errors, retrying", operationName, round + 1, budget + 1, bulkResult.RetryableIds.Count);
            pending = pending.Where(t => bulkResult.RetryableIds.Contains(t.Id)).ToList();
            await ElasticIndex.Configuration.TimeProvider.SafeDelay(GetRetryDelayWithJitter(round + 1), DisposedCancellationToken).AnyContext();
        }

        return result;
    }

    /// <summary>
    /// Throws for the failures left in a patch result. Call it only after successful documents have been invalidated in
    /// the cache and notified, so a partial failure never leaves stale cache entries.
    /// </summary>
    private static void ThrowForPatchErrors(PatchBatchResult<T> result, string operationLabel)
    {
        if (!result.HasErrors)
            return;

        if (result.TransportError is not null)
            throw new DocumentException(result.TransportError, result.TransportException);

        if (result.VersioningNotSupportedIds.Count > 0)
        {
            string index = result.Errors.GetValueOrDefault(result.VersioningNotSupportedIds.First())?.Index ?? "unknown";
            throw new DocumentVersioningNotSupportedException(ElasticWriteErrors.VersioningNotSupportedMessage(index));
        }

        int failed = result.ConflictIds.Count + result.RetryableIds.Count + result.FatalIds.Count;
        if (result.ConflictIds.Count > 0)
        {
            throw new VersionConflictDocumentException(
                $"Error {operationLabel} documents: {result.ConflictIds.Count} still conflicting after {result.Rounds} rounds ({failed} failed, {result.ModifiedIds.Count} succeeded). Ids: {FormatIds(result.ConflictIds)}");
        }

        var ids = result.RetryableIds.Concat(result.FatalIds).ToList();
        string? reason = ids.Select(id => result.Errors.GetValueOrDefault(id)?.Reason).FirstOrDefault(r => r is not null);
        throw new DocumentException($"Error {operationLabel} documents: {failed} failures ({result.ModifiedIds.Count} succeeded). Ids: {FormatIds(ids)}{(reason is null ? "" : $". First error: {reason}")}");
    }

    private static string FormatIds(IEnumerable<string> ids)
    {
        const int maxIds = 10;
        var list = ids.Take(maxIds + 1).ToList();
        return list.Count > maxIds ? String.Join(", ", list.Take(maxIds)) + ", ..." : String.Join(", ", list);
    }
}
