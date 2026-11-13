using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.MGet;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Utility;

namespace Foundatio.Repositories.Elasticsearch;

public abstract partial class ElasticRepositoryBase<T>
{
    /// <summary>A document patched server-side, by the Update API or by update-by-query on its id. <see cref="Version"/> is
    /// the caller's <c>IfVersion</c>, or empty.</summary>
    private sealed record UpdateItem(DocumentKey Key, string? Routing, ElasticDocumentVersion Version) : IWriteItem;

    /// <summary>A document patched client-side. <see cref="Document"/> is the patched document once it was written, and
    /// <c>null</c> for a document that was not written (not found, read failure, or a patch that changed nothing).</summary>
    private sealed record PatchItem(DocumentKey Key, string? Routing, ElasticDocumentVersion Version, T? Document = null) : IWriteItem;

    /// <summary>A document read for a patch: its key (concrete index and id) and the hit it was read as.</summary>
    private readonly record struct PatchSource(DocumentKey Key, FindHit<T> Hit);

    /// <summary>Keeps the first occurrence of each document (concrete index and id), so a patch is never applied twice to one document.</summary>
    private List<Id> DistinctIds(IEnumerable<Id> ids)
    {
        return ids.DistinctBy(id => (ElasticIndex.GetIndex(id), id.Value)).ToList();
    }

    /// <summary>Projects hits to patch sources, skipping hits without an id.</summary>
    private List<PatchSource> ToPatchSources(IEnumerable<FindHit<T>> hits)
    {
        var sources = new List<PatchSource>();
        foreach (var hit in hits)
        {
            if (hit.Id is { } id)
                sources.Add(new PatchSource(new DocumentKey(hit.GetIndex() ?? ElasticIndex.GetIndex(id), id), hit));
        }

        return sources;
    }

    private static List<T> PatchedDocuments(WriteRunResult<PatchItem> run) => run.Succeeded.Select(i => i.Document).OfType<T>().ToList();

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
    private async Task<WriteRunResult<PatchItem>> PatchClientSideAsync(IReadOnlyCollection<PatchSource> sources, IPatchOperation operation, ElasticDocumentVersion? expectedVersion, ICommandOptions options, Refresh refresh, string operationName)
    {
        bool checkVersion = expectedVersion is not null || (HasVersion && !options.ShouldSkipVersionCheck());
        var decided = new List<WriteItemOutcome<PatchItem>>();
        var items = new List<IndexItem>();
        foreach (var source in sources)
        {
            var (item, terminal) = PrepareClientPatch(source.Key, source.Hit, operation, expectedVersion, checkVersion);
            if (item is not null)
                items.Add(item);
            else
                decided.Add(new WriteItemOutcome<PatchItem>(new PatchItem(source.Key, source.Hit.Routing, source.Hit.GetElasticVersion()), terminal!));
        }

        var prepareRetry = expectedVersion is null ? RepatchConflicts(operation, checkVersion, options) : null;
        var run = await WriteRunner.RunAsync(operationName, items, options.GetRetryCount(), SendIndexRound(options, refresh, allowSingleRequest: false), prepareRetry, DisposedCancellationToken).AnyContext();
        var written = run.Items.Select(o => new WriteItemOutcome<PatchItem>(new PatchItem(o.Item.Key, o.Item.Routing, o.Item.Version, o.Item.Document), o.Result));
        return new WriteRunResult<PatchItem>([.. decided, .. written], run.Rounds);
    }

    /// <summary>Returns the item to write, or the terminal result when the document is not written.</summary>
    private (IndexItem? Item, WriteItemResult? Terminal) PrepareClientPatch(DocumentKey key, FindHit<T> hit, IPatchOperation operation, ElasticDocumentVersion? expectedVersion, bool checkVersion)
    {
        var version = hit.GetElasticVersion();
        if (expectedVersion is { } expected && version != expected)
            return (null, new WriteItemResult(WriteOutcome.Conflict, 409, Reason: $"expected version {expected} but the document has version {version.ToVersionString() ?? "none"}"));

        if (checkVersion && version.IsEmpty)
            return (null, new WriteItemResult(WriteOutcome.VersioningNotSupported, Reason: "The document was read without a version."));

        T? patched;
        try
        {
            patched = ClientPatches.Apply(hit, operation);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return (null, new WriteItemResult(WriteOutcome.Fatal, Reason: ex.Message, Exception: ex));
        }

        if (patched is null)
            return (null, new WriteItemResult(WriteOutcome.Noop));

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
                var failure = WriteOutcomeClassifier.ClassifyRequestFailure(response.ApiCallDetails?.HttpStatusCode, response.OriginalException());
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

                var (retry, terminal) = PrepareClientPatch(hit, operation, null, checkVersion);
                return terminal is null ? (retry, null) : (null, terminal);
            }).ToList();
        };
    }

    /// <summary>
    /// Reads documents by id for a client-side patch. Ids whose index can be computed are read with a multi-get (transient
    /// failures retried within the budget); several ids with multiple indexes or parent routing are found with a search
    /// (as before), and a single id is always read in real time from its index.
    /// Documents that don't exist are returned as <see cref="WriteOutcome.Missing"/> failures.
    /// </summary>
    private async Task<(List<FindHit<T>> Hits, List<WriteItemOutcome<IndexItem>> Failures)> ReadForPatchAsync(IReadOnlyCollection<Id> ids, ICommandOptions options)
    {
        if (ids.Count > 1 && (ElasticIndex.HasMultipleIndexes || (HasParent && ids.Any(i => i.Routing is null))))
        {
            var findOptions = options.Clone().PageLimit(1000).Cache(false);
            var searched = new List<FindHit<T>>();
            var results = await FindAsync(NewQuery().Id(ids.Select(i => i.Value)), findOptions).AnyContext();
            do
            {
                searched.AddRange(results.Hits.Where(h => h.Id is not null && h.Document is not null));
            } while (await results.NextPageAsync().AnyContext());

            var foundIds = searched.Select(h => h.Id!).ToHashSet();
            var missing = ids.Where(i => !foundIds.Contains(i.Value))
                .Select(i => new WriteItemOutcome<IndexItem>(new IndexItem(new DocumentKey(ElasticIndex.Name, i.Value), new T(), i.Routing, ElasticDocumentVersion.Empty, false), new WriteItemResult(WriteOutcome.Missing, 404)))
                .ToList();
            return (searched, missing);
        }

        var hits = new Dictionary<UpdateItem, FindHit<T>>(ReferenceEqualityComparer.Instance);
        var reads = ids.Select(id => new UpdateItem(new DocumentKey(ElasticIndex.GetIndex(id), id.Value), id.Routing, ElasticDocumentVersion.Empty)).ToList();
        var run = await WriteRunner.RunAsync<UpdateItem>("Read", reads, options.GetRetryCount(), async (pending, ct) =>
        {
            var (response, items) = await MultiGetAsync(pending.Select(p => (p.Key, p.Routing)).ToList(), options, ct).AnyContext();
            if (items is null)
            {
                var failure = new WriteItemResult(WriteOutcomeClassifier.ClassifyRequestFailure(response.ApiCallDetails?.HttpStatusCode, response.OriginalException()), response.ApiCallDetails?.HttpStatusCode, Reason: response.GetErrorMessage("Error reading documents to patch"), Exception: response.OriginalException());
                return Enumerable.Repeat(failure, pending.Count).ToList();
            }

            return pending.Select((read, index) =>
            {
                var item = items[index];
                if (item.IsError)
                    return new WriteItemResult(WriteOutcome.Fatal, ErrorType: item.ErrorType, Reason: item.ErrorReason);
                if (item.Hit is null)
                    return new WriteItemResult(WriteOutcome.Missing, 404);

                hits[read] = item.Hit;
                return new WriteItemResult(WriteOutcome.Succeeded);
            }).ToList();
        }, null, DisposedCancellationToken).AnyContext();

        var found = run.Items.Where(i => i.Result.Outcome is WriteOutcome.Succeeded).Select(i => hits[i.Item]).ToList();
        var failures = run.Items.Where(i => i.Result.Outcome is not WriteOutcome.Succeeded)
            .Select(i => new WriteItemOutcome<IndexItem>(new IndexItem(i.Item.Key, new T(), i.Item.Routing, ElasticDocumentVersion.Empty, false), i.Result))
            .ToList();
        return (found, failures);
    }

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
            var script = (ScriptPatch)operation;
            return WriteRunner.RunAsync<UpdateItem>(operationName, items, budget, async (pending, ct) =>
            {
                var results = new List<WriteItemResult>(pending.Count);
                foreach (var item in pending)
                {
                    try
                    {
                        results.Add(await UpdateByIdQueryAsync(item, script, refresh, options, ct).AnyContext());
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
            }, static (conflicts, _) => Task.FromResult<IReadOnlyList<(UpdateItem?, WriteItemResult?)>>(conflicts.Select(c => ((UpdateItem?)c, (WriteItemResult?)null)).ToList()), DisposedCancellationToken);
        }

        return WriteRunner.RunAsync<UpdateItem>(operationName, items, budget, async (pending, ct) =>
        {
            if (pending.Count is 1)
                return [await UpdateSingleAsync(pending[0], operation, refresh, budget, options, ct).AnyContext()];

            var response = await _client.BulkAsync(b =>
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
            response = await _client.UpdateAsync(request, cancellationToken).AnyContext();
        }
        else
        {
            var request = new UpdateRequest<T, object>(item.Key.Index, item.Key.Id) { Doc = ((PartialPatch)operation).Document };
            ConfigureUpdateRequest(request, item, refresh, budget);
            response = await _client.UpdateAsync(request, cancellationToken).AnyContext();
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
    private async Task<WriteItemResult> UpdateByIdQueryAsync(UpdateItem item, ScriptPatch scriptOperation, Refresh refresh, ICommandOptions options, CancellationToken cancellationToken)
    {
        var request = new UpdateByQueryRequest(item.Key.Index)
        {
            Query = new IdsQuery { Values = new Elastic.Clients.Elasticsearch.Ids([new Elastic.Clients.Elasticsearch.Id(item.Key.Id)]) },
            Script = new Script { Source = scriptOperation.Script, Params = scriptOperation.Params },
            Pipeline = DefaultPipeline,
            Refresh = refresh != Refresh.False,
            WaitForCompletion = true
        };
        if (item.Routing is not null)
            request.Routing = new Routing(item.Routing);

        var response = await _client.UpdateByQueryAsync(request, cancellationToken).AnyContext();
        _logger.LogRequest(response, options.GetQueryLogLevel());
        if (!response.IsValidResponse)
            return ToWriteResult(response, ElasticDocumentVersion.Empty);

        if ((response.VersionConflicts ?? 0) > 0)
            return new WriteItemResult(WriteOutcome.Conflict, 409, Reason: "The document changed during the update-by-query request.");

        if ((response.Total ?? 0) is 0)
            return new WriteItemResult(WriteOutcome.Missing, 404);

        return new WriteItemResult((response.Updated ?? 0) > 0 || (response.Deleted ?? 0) > 0 ? WriteOutcome.Succeeded : WriteOutcome.Noop);
    }
}
