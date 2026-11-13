using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.MGet;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch;

public abstract partial class ElasticRepositoryBase<T>
{
    /// <summary>A document patched server-side, by the Update API or by update-by-query on its id. <see cref="Version"/> is
    /// the caller's <c>IfVersion</c>, or empty.</summary>
    private sealed record UpdateItem(DocumentKey Key, string? Routing, ElasticDocumentVersion Version) : IWriteItem;

    private ClientPatchApplier<T>? _clientPatchApplier;
    private ClientPatchApplier<T> ClientPatches => _clientPatchApplier ??= new ClientPatchApplier<T>(
        _client.ElasticsearchClientSettings.SourceSerializer,
        ApplyDateTracking,
        document =>
        {
            if (HasDateTracking)
                SetDocumentDates(document, ElasticIndex.Configuration.TimeProvider);
        });

    /// <summary>
    /// Client-side patch (<see cref="PatchStrategy.ClientSide"/>): patches each hit in memory and indexes it with
    /// <c>if_seq_no</c>/<c>if_primary_term</c> from the version it was read at, in grouped rounds.
    /// </summary>
    /// <remarks>
    /// Per document: an action patch that changes nothing ends as <see cref="WriteOutcome.Noop"/> without a write; a patch
    /// that throws ends as <see cref="WriteOutcome.Fatal"/>; a hit with no version when the write must be checked means the
    /// index has sequence numbers disabled (<see cref="WriteOutcome.VersioningNotSupported"/>). On a conflict all conflicting
    /// documents are re-read with one multi-get and re-patched for the next round, unless <paramref name="expectedVersion"/>
    /// is set: then the write is always checked against that version, a mismatch ends as
    /// <see cref="WriteOutcome.Conflict"/>, and the document is never re-read.
    /// </remarks>
    private async Task<WriteRunResult<IndexItem>> PatchClientSideAsync(IReadOnlyCollection<(string Id, FindHit<T> Hit)> hits, IPatchOperation operation, ElasticDocumentVersion? expectedVersion, ICommandOptions options, Refresh refresh, string operationName)
    {
        bool checkVersion = expectedVersion is not null || (HasVersion && !options.ShouldSkipVersionCheck());
        var decided = new List<WriteItemOutcome<IndexItem>>();
        var items = new List<IndexItem>();
        foreach (var (id, hit) in hits)
        {
            var (item, terminal) = PrepareClientPatch(id, hit, operation, expectedVersion, checkVersion);
            if (terminal is not null)
                decided.Add(new WriteItemOutcome<IndexItem>(item, terminal));
            else
                items.Add(item);
        }

        var prepareRetry = expectedVersion is null ? RepatchConflicts(operation, checkVersion, options) : null;
        var run = await WriteRunner.RunAsync(operationName, items, options.GetRetryCount(), SendIndexRound(options, refresh, allowSingleRequest: false), prepareRetry, DisposedCancellationToken).AnyContext();
        return WriteRunResult<IndexItem>.Merge(decided, run);
    }

    private (IndexItem Item, WriteItemResult? Terminal) PrepareClientPatch(string id, FindHit<T> hit, IPatchOperation operation, ElasticDocumentVersion? expectedVersion, bool checkVersion)
    {
        var key = new DocumentKey(hit.GetIndex() ?? ElasticIndex.GetIndex(id), id);
        var version = hit.GetElasticVersion();
        IndexItem Unwritten() => new(key, hit.Document ?? new T(), hit.Routing, version, false);

        if (expectedVersion is { } expected && version != expected)
            return (Unwritten(), new WriteItemResult(WriteOutcome.Conflict, 409, Reason: $"expected version {expected} but the document has version {version.ToVersionString() ?? "none"}"));

        if (checkVersion && version.IsEmpty)
            return (Unwritten(), new WriteItemResult(WriteOutcome.VersioningNotSupported, Reason: "The document was read without a version."));

        T? patched;
        try
        {
            patched = ClientPatches.Apply(hit, operation);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return (Unwritten(), new WriteItemResult(WriteOutcome.Fatal, Reason: ex.Message, Exception: ex));
        }

        if (patched is null)
            return (Unwritten(), new WriteItemResult(WriteOutcome.Noop));

        return (new IndexItem(key, patched, hit.Routing, checkVersion ? expectedVersion ?? version : ElasticDocumentVersion.Empty, false), null);
    }

    /// <summary>
    /// Re-reads conflicting documents with one multi-get and re-applies the patch. A document that no longer exists ends as
    /// <see cref="WriteOutcome.Missing"/> and an item error as <see cref="WriteOutcome.Fatal"/> (never as missing). When
    /// the multi-get itself fails with a transient error the stale items are sent again; the next conflict re-reads them.
    /// </summary>
    private PrepareConflictRetry<IndexItem> RepatchConflicts(IPatchOperation operation, bool checkVersion, ICommandOptions options)
    {
        return async (conflicts, ct) =>
        {
            var (response, items) = await MultiGetAsync(conflicts.Select(c => (c.Key, c.Routing)).ToList(), options, ct).AnyContext();
            if (items is null)
            {
                var failure = WriteOutcomeClassifier.ClassifyReadFailure(response.ApiCallDetails?.HttpStatusCode, response.OriginalException());
                if (failure is WriteOutcome.Transient)
                    return conflicts.Select(c => ((IndexItem?)c, (WriteItemResult?)null)).ToList();

                throw new Exceptions.DocumentException(response.GetErrorMessage("Error re-reading documents for patch retry"), response.OriginalException());
            }

            return conflicts.Select((conflict, index) =>
            {
                var item = items[index];
                if (item.IsError)
                    return (null, new WriteItemResult(WriteOutcome.Fatal, ErrorType: item.ErrorType, Reason: item.ErrorReason));

                if (item.Hit is not { } hit)
                    return ((IndexItem?)null, (WriteItemResult?)new WriteItemResult(WriteOutcome.Missing, 404));

                var (retry, terminal) = PrepareClientPatch(conflict.Key.Id, hit, operation, null, checkVersion);
                return terminal is null ? (retry, null) : (null, terminal);
            }).ToList();
        };
    }

    /// <summary>
    /// Reads documents by id for a client-side patch. Pipeline partial patches prefer real-time multi-get for each known
    /// index and routing, discovering only unresolved locations with a search. Other multi-id client patches retain their
    /// search semantics when multiple indexes or unknown parent routing require discovery.
    /// Documents that don't exist are returned as <see cref="WriteOutcome.Missing"/> failures.
    /// </summary>
    private async Task<(List<(string Id, FindHit<T> Hit)> Hits, List<WriteItemOutcome<IndexItem>> Failures)> ReadForPatchAsync(IReadOnlyCollection<Id> ids, ICommandOptions options, bool allowSearch = false, bool preferRealtime = false)
    {
        bool searchAll = allowSearch && !preferRealtime && (ElasticIndex.HasMultipleIndexes || (HasParent && ids.Any(i => i.Routing is null)));
        var discoveryIds = new List<Id>();
        var reads = new List<UpdateItem>();
        foreach (var id in ids)
        {
            if (searchAll || (allowSearch && preferRealtime && HasParent && id.Routing is null))
            {
                discoveryIds.Add(id);
                continue;
            }

            try
            {
                reads.Add(new UpdateItem(new DocumentKey(ElasticIndex.GetIndex(id), id.Value), id.Routing, ElasticDocumentVersion.Empty));
            }
            catch (ArgumentException ex) when (allowSearch && preferRealtime && ElasticIndex is DailyIndex && ex.ParamName is "id" && !ObjectId.TryParse(id.Value, out _))
            {
                // Time-series custom ids do not encode the date needed to resolve their index.
                discoveryIds.Add(id);
            }
        }

        var found = new List<(string Id, FindHit<T> Hit)>();
        var failures = new List<WriteItemOutcome<IndexItem>>();
        var authoritativeLocations = new HashSet<(string Index, string Id, string? Routing)>();
        var unresolvedFailures = new HashSet<Id>();
        if (reads.Count > 0)
        {
            var hits = new Dictionary<UpdateItem, FindHit<T>>(ReferenceEqualityComparer.Instance);
            var resolvedReads = new HashSet<UpdateItem>(ReferenceEqualityComparer.Instance);
            var run = await WriteRunner.RunAsync<UpdateItem>("Read", reads, options.GetRetryCount(), async (pending, ct) =>
            {
                var (response, items) = await MultiGetAsync(pending.Select(p => (p.Key, p.Routing)).ToList(), options, ct).AnyContext();
                if (items is null)
                {
                    var failure = new WriteItemResult(WriteOutcomeClassifier.ClassifyReadFailure(response.ApiCallDetails?.HttpStatusCode, response.OriginalException()), response.ApiCallDetails?.HttpStatusCode, Reason: response.GetErrorMessage("Error reading documents to patch"), Exception: response.OriginalException());
                    return Enumerable.Repeat(failure, pending.Count).ToList();
                }

                return pending.Select((read, index) =>
                {
                    var item = items[index];
                    if (item.Index is { Length: > 0 } physicalIndex)
                    {
                        authoritativeLocations.Add((physicalIndex, read.Key.Id, item.Hit?.Routing ?? read.Routing));
                        resolvedReads.Add(read);
                    }
                    if (item.IsError)
                        return new WriteItemResult(WriteOutcome.Fatal, ErrorType: item.ErrorType, Reason: item.ErrorReason);
                    if (item.Hit is null)
                        return new WriteItemResult(WriteOutcome.Missing, 404);

                    hits[read] = item.Hit;
                    return new WriteItemResult(WriteOutcome.Succeeded);
                }).ToList();
            }, null, DisposedCancellationToken).AnyContext();

            found.AddRange(run.Items.Where(i => i.Result.Outcome is WriteOutcome.Succeeded).Select(i => (i.Item.Key.Id, hits[i.Item])));
            foreach (var failure in run.Items.Where(i => i.Result.Outcome is not WriteOutcome.Succeeded))
            {
                failures.Add(new WriteItemOutcome<IndexItem>(new IndexItem(failure.Item.Key, new T(), failure.Item.Routing, ElasticDocumentVersion.Empty, false), failure.Result));
                if (!resolvedReads.Contains(failure.Item))
                    unresolvedFailures.Add(new Id(failure.Item.Key.Id, failure.Item.Routing));
            }
        }

        if (discoveryIds.Count > 0)
        {
            DisposedCancellationToken.ThrowIfCancellationRequested();
            var searched = await SearchForPatchAsync(discoveryIds, options).AnyContext();
            found.AddRange(searched.Where(h => !authoritativeLocations.Contains((h.Hit.GetIndex() ?? ElasticIndex.Name, h.Id, h.Hit.Routing))
                && !unresolvedFailures.Contains(new Id(h.Id, h.Hit.Routing))));
            found = found.DistinctBy(h => (h.Hit.GetIndex(), h.Id, h.Hit.Routing)).ToList();

            var foundIds = found.Select(h => h.Id).ToHashSet(StringComparer.Ordinal);
            var foundRoutedIds = found.Select(h => new Id(h.Id, h.Hit.Routing)).ToHashSet();
            failures.AddRange(discoveryIds.Where(i => i.Routing is null ? !foundIds.Contains(i.Value) : !foundRoutedIds.Contains(i))
                .Select(i => new WriteItemOutcome<IndexItem>(new IndexItem(new DocumentKey(ElasticIndex.Name, i.Value), new T(), i.Routing, ElasticDocumentVersion.Empty, false), new WriteItemResult(WriteOutcome.Missing, 404))));
        }

        return (found, failures);
    }

    private async Task<List<(string Id, FindHit<T> Hit)>> SearchForPatchAsync(IReadOnlyCollection<Id> ids, ICommandOptions options)
    {
        var findOptions = CreatePatchPagingOptions(options, fullSource: true).PageLimit(1000);
        var requestedIds = ids.ToHashSet();
        var unroutedIds = ids.Where(i => i.Routing is null).Select(i => i.Value).ToHashSet(StringComparer.Ordinal);
        var searched = new List<(string Id, FindHit<T> Hit)>();
        var results = await FindAsync(NewQuery().Id(ids.Select(i => i.Value)), findOptions).AnyContext();
        try
        {
            do
            {
                searched.AddRange(WithIds(results.Hits.Where(h => h.Document is not null))
                    .Where(h => unroutedIds.Contains(h.Id) || requestedIds.Contains(new Id(h.Id, h.Hit.Routing))));
            } while (await results.NextPageAsync().AnyContext());
        }
        finally
        {
            await CloseAbandonedPointInTimeAsync(results, findOptions).AnyContext();
        }

        return searched;
    }

    private static IEnumerable<(string Id, FindHit<T> Hit)> WithIds(IEnumerable<FindHit<T>> hits) =>
        hits.SelectMany(hit => hit.Id is { } id ? new[] { (id, hit) } : []);

    /// <summary>Returns the multi-get items in request order, or <c>null</c> items when the request failed as a whole.</summary>
    private async Task<(MultiGetResponse<T> Response, IReadOnlyList<MultiGetItem<T>>? Items)> MultiGetAsync(IReadOnlyList<(DocumentKey Key, string? Routing)> documents, ICommandOptions options, CancellationToken cancellationToken)
    {
        var operations = documents.Select(d => new MultiGetOperation(d.Key.Id) { Index = d.Key.Index, Routing = ToRouting(d.Routing) }).ToList();
        var response = await _client.MultiGetAsync<T>(new MultiGetRequestDescriptor().Docs(operations), cancellationToken).AnyContext();
        _logger.LogRequest(response, options.GetQueryLogLevel());
        if (!response.IsValidResponse || response.Docs.Count != documents.Count)
            return (response, null);

        return (response, response.ToMultiGetItems());
    }

    /// <summary>
    /// Server-side patch: the Update API (<see cref="PatchStrategy.UpdateApi"/>) or update-by-query per id
    /// (<see cref="PatchStrategy.UpdateByQueryPerId"/>).
    /// </summary>
    /// <remarks>
    /// The Update API re-runs the update on the latest document up to <c>Retry(n)</c> times (<c>retry_on_conflict</c>),
    /// so a 409 it still returns is terminal; with an expected version it sends <c>if_seq_no</c>/<c>if_primary_term</c>
    /// instead (Elasticsearch rejects both together). An update-by-query conflict wrote nothing for that one document, so
    /// it is re-run in the next round. Transient failures are re-sent unchanged in both.
    /// </remarks>
    private Task<WriteRunResult<UpdateItem>> PatchServerSideAsync(IReadOnlyList<UpdateItem> items, IPatchOperation operation, PatchStrategy strategy, ICommandOptions options, Refresh refresh, string operationName)
    {
        int budget = options.GetRetryCount();
        if (strategy is PatchStrategy.UpdateByQueryPerId)
        {
            return PatchByIdQueryAsync(items, (ScriptPatch)operation, refresh, budget, operationName, options);
        }

        return WriteRunner.RunAsync<UpdateItem>(operationName, items, budget, async (pending, ct) =>
        {
            if (pending.Count is 1)
                return [await UpdateSingleAsync(pending[0], operation, refresh, budget, options, ct).AnyContext()];

            var response = await WriteClient.BulkAsync(b =>
            {
                b.Refresh(refresh);
                foreach (var item in pending)
                {
                    if (operation is ScriptPatch scriptOperation)
                        b.Update<T>(u => ConfigureBulkUpdate(u.Id(item.Key.Id).Index(item.Key.Index).Script(s => s.Source(scriptOperation.Script).Params(scriptOperation.Params)), item, budget));
                    else
                        b.Update<T, object>(u => ConfigureBulkUpdate(u.Id(item.Key.Id).Index(item.Key.Index).Doc(((PartialPatch)operation).Document), item, budget));
                }
            }, ct).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());
            return BulkResult.From(response).ToWriteResults(pending.Count);
        }, null, DisposedCancellationToken);
    }

    /// <summary>
    /// Runs the update-by-query per id requests without refreshing, then refreshes the indexes that were written once, so a
    /// batch costs one refresh instead of one per document. The refresh also runs when the run ends in an exception after
    /// some documents were patched.
    /// </summary>
    private async Task<WriteRunResult<UpdateItem>> PatchByIdQueryAsync(IReadOnlyList<UpdateItem> items, ScriptPatch script, Refresh refresh, int budget, string operationName, ICommandOptions options)
    {
        var patchedIndexes = new ConcurrentDictionary<string, byte>();
        var partiallyPatchedIds = new List<string>();
        WriteRunResult<UpdateItem> run;
        try
        {
            run = await WriteRunner.RunAsync<UpdateItem>(operationName, items, budget, async (pending, ct) =>
            {
                var results = new List<WriteItemResult>(pending.Count);
                foreach (var item in pending)
                {
                    try
                    {
                        var (result, applied) = await UpdateByIdQueryAsync(item, script, options, ct).AnyContext();
                        if (applied)
                        {
                            patchedIndexes[item.Key.Index] = 0;
                            if (result.Outcome is not WriteOutcome.Succeeded)
                                partiallyPatchedIds.Add(item.Key.Id);
                        }

                        results.Add(result);
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        results.AddRange(Enumerable.Repeat(WriteItemResult.Cancelled, pending.Count - results.Count));
                        break;
                    }
                    catch (Exception ex)
                    {
                        var failure = new WriteItemResult(WriteOutcome.Fatal, Reason: ex.Message, Exception: ex);
                        results.AddRange(Enumerable.Repeat(failure, pending.Count - results.Count));
                        break;
                    }
                }

                return results;
            }, static (conflicts, _) => Task.FromResult<IReadOnlyList<(UpdateItem?, WriteItemResult?)>>(conflicts.Select(c => ((UpdateItem?)c, (WriteItemResult?)null)).ToList()), DisposedCancellationToken).AnyContext();
        }
        finally
        {
            if (refresh != Refresh.False && !patchedIndexes.IsEmpty)
                await RefreshPatchedIndexesAsync(patchedIndexes.Keys.ToArray()).AnyContext();
        }

        if (partiallyPatchedIds.Count > 0)
        {
            try
            {
                await OnPatchedAsync(partiallyPatchedIds, null, options).AnyContext();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error invalidating the cache or sending notifications after a partially completed update by query");
            }
        }

        return run;
    }

    private async Task RefreshPatchedIndexesAsync(string[] indexes)
    {
        try
        {
            var response = await _client.Indices.RefreshAsync(indexes).AnyContext();
            if (!response.IsValidResponse)
                _logger.LogErrorRequest(response, "Failed to refresh indices after patching");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error refreshing indices after patching: {Message}", ex.Message);
        }
    }

    private static Elastic.Clients.Elasticsearch.Core.Bulk.BulkUpdateOperationDescriptor<TDocument, TPartial> ConfigureBulkUpdate<TDocument, TPartial>(Elastic.Clients.Elasticsearch.Core.Bulk.BulkUpdateOperationDescriptor<TDocument, TPartial> descriptor, UpdateItem item, int budget)
    {
        if (item.Routing is not null)
            descriptor.Routing(item.Routing);

        // TODO: Null-valued properties of a PartialPatch are dropped by the client's source serializer
        // (elastic/elasticsearch-net#8763). Use ScriptPatch or JsonPatch to set fields to null.
        return item.Version.IsEmpty
            ? descriptor.RetriesOnConflict(budget)
            : descriptor.IfSequenceNumber(item.Version.SequenceNumber).IfPrimaryTerm(item.Version.PrimaryTerm);
    }

    private async Task<WriteItemResult> UpdateSingleAsync(UpdateItem item, IPatchOperation operation, Refresh refresh, int budget, ICommandOptions options, CancellationToken cancellationToken)
    {
        UpdateResponse<T> response;
        if (operation is ScriptPatch scriptOperation)
        {
            var request = new UpdateRequest<T, T>(item.Key.Index, item.Key.Id) { Script = new Script { Source = scriptOperation.Script, Params = scriptOperation.Params } };
            ConfigureUpdateRequest(request, item, refresh, budget);
            response = await WriteClient.UpdateAsync(request, cancellationToken).AnyContext();
        }
        else
        {
            var request = new UpdateRequest<T, object>(item.Key.Index, item.Key.Id) { Doc = ((PartialPatch)operation).Document };
            ConfigureUpdateRequest(request, item, refresh, budget);
            response = await WriteClient.UpdateAsync(request, cancellationToken).AnyContext();
        }

        _logger.LogRequest(response, options.GetQueryLogLevel());
        var result = ToWriteResult(response, ElasticDocumentVersion.Empty);
        return result.Outcome is WriteOutcome.Succeeded && response.Result is Result.NoOp ? result with { Outcome = WriteOutcome.Noop } : result;
    }

    private static void ConfigureUpdateRequest<TPartial>(UpdateRequest<T, TPartial> request, UpdateItem item, Refresh refresh, int budget)
    {
        request.Refresh = refresh;
        request.Routing = ToRouting(item.Routing);
        if (item.Version.IsEmpty)
        {
            request.RetryOnConflict = budget;
        }
        else
        {
            request.IfSeqNo = item.Version.SequenceNumber;
            request.IfPrimaryTerm = item.Version.PrimaryTerm;
        }
    }

    /// <summary>
    /// Runs a script patch through update-by-query on one id so the default pipeline is applied. A version conflict means
    /// the document changed during the request and nothing was written, so it is safe to run it again.
    /// </summary>
    private async Task<(WriteItemResult Result, bool Applied)> UpdateByIdQueryAsync(UpdateItem item, ScriptPatch scriptOperation, ICommandOptions options, CancellationToken cancellationToken)
    {
        var request = new UpdateByQueryRequest(item.Key.Index)
        {
            Query = new IdsQuery { Values = new Elastic.Clients.Elasticsearch.Ids([new Elastic.Clients.Elasticsearch.Id(item.Key.Id)]) },
            Script = new Script { Source = scriptOperation.Script, Params = scriptOperation.Params },
            Pipeline = DefaultPipeline,
            Conflicts = Conflicts.Proceed,
            Refresh = false,
            WaitForCompletion = true
        };
        if (item.Routing is not null)
            request.Routing = new Routing(item.Routing);

        var response = await WriteClient.UpdateByQueryAsync(request, cancellationToken).AnyContext();
        _logger.LogRequest(response, options.GetQueryLogLevel());
        bool applied = (response.Updated ?? 0) > 0 || (response.Deleted ?? 0) > 0;
        if (GetQueryFailureReason(response.TimedOut, response.Failures) is { } failure)
            return (new WriteItemResult(WriteOutcome.Fatal, response.ApiCallDetails?.HttpStatusCode, Reason: failure, Exception: response.OriginalException()), applied);

        if (!response.IsValidResponse)
        {
            var result = ToWriteResult(response, ElasticDocumentVersion.Empty);
            return (applied ? result with { Outcome = WriteOutcome.Fatal } : result, applied);
        }

        if ((response.VersionConflicts ?? 0) > 0)
            return (new WriteItemResult(applied ? WriteOutcome.Fatal : WriteOutcome.Conflict, 409, Reason: "The document changed during the update-by-query request."), applied);

        if ((response.Total ?? 0) is 0)
            return (new WriteItemResult(WriteOutcome.Missing, 404), false);

        return (new WriteItemResult(applied ? WriteOutcome.Succeeded : WriteOutcome.Noop), applied);
    }
}
