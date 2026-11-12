using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Bulk;
using Elastic.Transport.Products.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Resilience;
using Microsoft.Extensions.Logging;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Utility;

namespace Foundatio.Repositories.Elasticsearch;

public abstract partial class ElasticRepositoryBase<T>
{
    /// <summary>A document to index: created (<c>op_type=create</c>), indexed with <c>if_seq_no</c>/<c>if_primary_term</c>
    /// from <see cref="Version"/>, or indexed unconditionally when <see cref="Version"/> is empty.</summary>
    private sealed record IndexItem(DocumentKey Key, T Document, string? Routing, ElasticDocumentVersion Version, bool IsCreate, FindHit<T>? Hit = null) : IWriteItem;

    /// <summary>A document to delete, with <c>if_seq_no</c>/<c>if_primary_term</c> when <see cref="Version"/> is set.</summary>
    private sealed record DeleteItem(DocumentKey Key, T Document, string? Routing, ElasticDocumentVersion Version) : IWriteItem;

    private WriteRoundRunner? _writeRunner;
    private WriteRoundRunner WriteRunner => _writeRunner ??= CreateWriteRunner();

    /// <summary>
    /// Document writes retry with <see cref="WriteRoundRunner"/> (<c>Retry(n)</c> and the transient classification), not the
    /// repository's <see cref="IResiliencePolicy"/>: retrying a write the policy can't classify (a 502, a timeout) could apply
    /// it twice. A custom policy is still used for everything else, so this only warns.
    /// </summary>
    private WriteRoundRunner CreateWriteRunner()
    {
        if (_resiliencePolicy is not ResiliencePolicy)
            _logger.LogWarning("Repository {Repository} has a custom resilience policy ({Policy}); document writes don't use it and retry with Retry(n) on HTTP 429/503 and unsent requests only", GetType().Name, _resiliencePolicy.GetType().Name);

        return new WriteRoundRunner(_logger, ElasticIndex.Configuration.TimeProvider);
    }

    private IndexItem CreateIndexItem(T document, bool isCreateOperation, ICommandOptions options)
    {
        var key = new DocumentKey(ElasticIndex.GetIndex(document), document.Id);
        string? routing = GetParentIdFunc?.Invoke(document);
        if (isCreateOperation || !HasVersion || options.ShouldSkipVersionCheck())
            return new IndexItem(key, document, routing, ElasticDocumentVersion.Empty, isCreateOperation);

        // A versioned document without a version has never been read, so it must not exist yet: create it.
        var version = ((IVersioned)document).GetElasticVersion();
        return new IndexItem(key, document, routing, version, IsCreate: version.IsEmpty);
    }

    /// <summary>Indexes documents in rounds; a single document uses the index API, several use one bulk request per round.</summary>
    private async Task<WriteRunResult<IndexItem>> IndexDocumentsAsync(IReadOnlyCollection<T> documents, bool isCreateOperation, ICommandOptions options)
    {
        if (ElasticIndex.HasMultipleIndexes)
        {
            foreach (var documentGroup in documents.GroupBy(ElasticIndex.GetIndex))
                await ElasticIndex.EnsureIndexAsync(documentGroup.First()).AnyContext();
        }
        else
        {
            await ElasticIndex.EnsureIndexAsync(null).AnyContext();
        }

        var items = documents.Select(d => CreateIndexItem(d, isCreateOperation, options)).ToList();
        string operation = isCreateOperation ? "Add" : "Save";
        return await WriteRunner.RunAsync(operation, items, options.GetRetryCount(), SendIndexRound(options, Refresh: options.GetRefreshMode(DefaultConsistency), allowSingleRequest: true), null, DisposedCancellationToken).AnyContext();
    }

    private SendWriteRound<IndexItem> SendIndexRound(ICommandOptions options, Refresh Refresh, bool allowSingleRequest)
    {
        return async (pending, ct) =>
        {
            IReadOnlyList<WriteItemResult> results;
            if (allowSingleRequest && pending.Count is 1)
            {
                var item = pending[0];
                var request = new IndexRequest<T>(item.Document, item.Key.Index, item.Key.Id)
                {
                    OpType = item.IsCreate ? OpType.Create : OpType.Index,
                    Pipeline = DefaultPipeline,
                    Refresh = Refresh,
                    Routing = ToRouting(item.Routing)
                };
                if (!item.Version.IsEmpty)
                {
                    request.IfSeqNo = item.Version.SequenceNumber;
                    request.IfPrimaryTerm = item.Version.PrimaryTerm;
                }

                var response = await _client.IndexAsync(request, ct).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());
                results = [ToWriteResult(response, response.IsValidResponse ? response.GetElasticVersion() : ElasticDocumentVersion.Empty)];
            }
            else
            {
                var request = new BulkRequest { Refresh = Refresh, Operations = pending.Select(CreateBulkIndexOperation).ToList() };
                var response = await _client.BulkAsync(request, ct).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());
                results = BulkResult.From(response).ToWriteResults(pending.Count);
            }

            for (int index = 0; index < pending.Count; index++)
            {
                if (results[index].Outcome is WriteOutcome.Succeeded)
                    pending[index].Document.SetDocumentVersion(results[index].Version);
            }

            return results;
        };
    }

    private IBulkOperation CreateBulkIndexOperation(IndexItem item)
    {
        if (item.IsCreate)
            return new BulkCreateOperation<T>(item.Document) { Id = item.Key.Id, Index = item.Key.Index, Routing = ToRouting(item.Routing), Pipeline = DefaultPipeline };

        var operation = new BulkIndexOperation<T>(item.Document) { Id = item.Key.Id, Index = item.Key.Index, Routing = ToRouting(item.Routing), Pipeline = DefaultPipeline };
        if (!item.Version.IsEmpty)
        {
            operation.IfSequenceNumber = item.Version.SequenceNumber;
            operation.IfPrimaryTerm = item.Version.PrimaryTerm;
        }

        return operation;
    }

    /// <summary>
    /// Deletes documents in rounds. A document that is already gone counts as removed. Versioned documents are deleted with
    /// <c>if_seq_no</c>/<c>if_primary_term</c> unless the check is skipped; a document with no version can't be checked
    /// (a delete can't fall back to create), so it throws before anything is deleted.
    /// </summary>
    private List<DeleteItem> CreateDeleteItems(IReadOnlyCollection<T> documents, ICommandOptions options)
    {
        bool checkVersion = HasVersion && !options.ShouldSkipVersionCheck();
        return documents.Select(d =>
        {
            var key = new DocumentKey(ElasticIndex.GetIndex(d), d.Id);
            var version = checkVersion ? ((IVersioned)d).GetElasticVersion() : ElasticDocumentVersion.Empty;
            if (checkVersion && version.IsEmpty)
                throw new DocumentException($"Document {key} has no version, so it can't be removed with a version check. Remove the document as it was loaded, use SkipVersionCheck or RemoveAsync(id), or check that index {key.Index} has sequence numbers enabled.");

            return new DeleteItem(key, d, GetParentIdFunc?.Invoke(d), version);
        }).ToList();
    }

    private async Task<WriteRunResult<DeleteItem>> DeleteDocumentsAsync(IReadOnlyList<DeleteItem> items, ICommandOptions options)
    {
        var refresh = options.GetRefreshMode(DefaultConsistency);
        return await WriteRunner.RunAsync<DeleteItem>("Remove", items, options.GetRetryCount(), async (pending, ct) =>
        {
            if (pending.Count is 1)
            {
                var item = pending[0];
                var request = new DeleteRequest(item.Key.Index, item.Key.Id) { Refresh = refresh, Routing = ToRouting(item.Routing) };
                if (!item.Version.IsEmpty)
                {
                    request.IfSeqNo = item.Version.SequenceNumber;
                    request.IfPrimaryTerm = item.Version.PrimaryTerm;
                }

                var response = await _client.DeleteAsync(request, ct).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());
                return [ToWriteResult(response, ElasticDocumentVersion.Empty)];
            }

            var bulk = new BulkRequest
            {
                Refresh = refresh,
                Operations = pending.Select(item =>
                {
                    var operation = new BulkDeleteOperation(item.Key.Id) { Index = item.Key.Index, Routing = ToRouting(item.Routing) };
                    if (!item.Version.IsEmpty)
                    {
                        operation.IfSequenceNumber = item.Version.SequenceNumber;
                        operation.IfPrimaryTerm = item.Version.PrimaryTerm;
                    }

                    return (IBulkOperation)operation;
                }).ToList()
            };
            var bulkResponse = await _client.BulkAsync(bulk, ct).AnyContext();
            _logger.LogRequest(bulkResponse, options.GetQueryLogLevel());
            return BulkResult.From(bulkResponse).ToWriteResults(pending.Count);
        }, null, DisposedCancellationToken).AnyContext();
    }

    private static Routing? ToRouting(string? routing) => routing is null ? null : new Routing(routing);

    /// <summary>Classifies a single-document response the same way bulk items are classified.</summary>
    private static WriteItemResult ToWriteResult(ElasticsearchResponse response, ElasticDocumentVersion version)
    {
        if (response.IsValidResponse)
            return new WriteItemResult(WriteOutcome.Succeeded, response.ApiCallDetails?.HttpStatusCode, Version: version);

        int? status = response.ApiCallDetails?.HttpStatusCode;
        var error = response.ElasticsearchServerError?.Error;
        var exception = response.OriginalException();
        string reason = response.GetErrorMessage("Error writing document");
        if (error is null && status is not 404)
            return new WriteItemResult(WriteOutcomeClassifier.ClassifyRequestFailure(status, exception), status, null, reason, exception);

        return new WriteItemResult(WriteOutcomeClassifier.Classify(status, error?.Type, error?.Reason), status, error?.Type, error?.Reason ?? reason, exception);
    }
}
