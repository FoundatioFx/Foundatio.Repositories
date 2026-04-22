using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Bulk;
using Elastic.Transport.Extensions;
using Foundatio.Messaging;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Queries;
using Foundatio.Repositories.Utility;
using Foundatio.Resilience;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Tasks = Elastic.Clients.Elasticsearch.Tasks;

namespace Foundatio.Repositories.Elasticsearch;

public abstract class ElasticRepositoryBase<T> : ElasticReadOnlyRepositoryBase<T>, ISearchableRepository<T> where T : class, IIdentity, new()
{
    protected readonly IMessagePublisher? _messagePublisher;

    protected ElasticRepositoryBase(IIndex index) : base(index)
    {
        _messagePublisher = index.Configuration.MessageBus;
        NotificationsEnabled = _messagePublisher != null;

        if (_idField is not null)
            AddRequiredField(_idField);

        if (HasCreatedDate)
            AddRequiredField(e => ((IHaveCreatedDate)e).CreatedUtc);

        if (HasDates && _updatedUtcField is not null)
            AddRequiredField(_updatedUtcField);

        if (HasCustomFields)
        {
            AddDefaultExclude("Idx");
            DocumentsChanging.AddHandler(OnCustomFieldsDocumentsChanging);
            BeforeQuery.AddHandler(OnCustomFieldsBeforeQuery);
        }
    }

    /// <summary>
    /// Gets whether this repository supports automatic date tracking for patch operations.
    /// When <c>true</c>, <see cref="SetDocumentDates"/>, <see cref="GetUpdatedUtcFieldPath"/>, and
    /// the <see cref="ApplyDateTracking(ScriptPatch)"/> overloads are active.
    /// Override to <c>true</c> for custom date field scenarios (e.g., nested metadata dates).
    /// </summary>
    protected virtual bool HasDateTracking => HasDates;
    protected string? DefaultPipeline { get; set; } = null;
    protected bool AutoCreateCustomFields { get; set; } = false;

    public Task<T> AddAsync(T document, CommandOptionsDescriptor<T> options)
    {
        return AddAsync(document, options?.Configure());
    }

    public async Task<T> AddAsync(T document, ICommandOptions? options = null)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));

        await AddAsync([document], options).AnyContext();
        return document;
    }

    public Task AddAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options)
    {
        return AddAsync(documents, options?.Configure());
    }

    public virtual async Task AddAsync(IEnumerable<T> documents, ICommandOptions? options = null)
    {
        var docs = documents?.ToList();
        if (docs == null || docs.Any(d => d == null))
            throw new ArgumentNullException(nameof(documents));

        if (docs.Count == 0)
            return;

        options = ConfigureOptions(options?.As<T>());

        await OnDocumentsAddingAsync(docs, options).AnyContext();

        if (options.ShouldValidate())
            foreach (var doc in docs)
                await ValidateAndThrowAsync(doc).AnyContext();

        var result = await IndexDocumentsAsync(docs, true, options).AnyContext();

        var successDocs = result.IsSuccess ? docs : docs.Where(d => result.SuccessfulIds.Contains(d.Id)).ToList();
        if (successDocs.Count > 0)
        {
            await OnDocumentsAddedAsync(successDocs, options).AnyContext();
            if (IsCacheEnabled && options.ShouldUseCache())
                await AddDocumentsToCacheAsync(successDocs, options, false).AnyContext();
        }

        if (result.HasErrors)
            ThrowForBulkErrors(result, isCreateOperation: true);
    }

    protected virtual Task ValidateAndThrowAsync(T document) { return Task.CompletedTask; }

    public Task<T> SaveAsync(T document, CommandOptionsDescriptor<T> options)
    {
        return SaveAsync(document, options?.Configure());
    }

    public async Task<T> SaveAsync(T document, ICommandOptions? options = null)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));

        await SaveAsync(new[] { document }, options).AnyContext();
        return document;
    }

    public Task SaveAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options)
    {
        return SaveAsync(documents, options?.Configure());
    }

    public virtual async Task SaveAsync(IEnumerable<T> documents, ICommandOptions? options = null)
    {
        var docs = documents?.ToList();
        if (docs == null || docs.Any(d => d == null))
            throw new ArgumentNullException(nameof(documents));

        if (docs.Count == 0)
            return;

        string[] ids = docs.Where(d => !String.IsNullOrEmpty(d.Id)).Select(d => d.Id).ToArray();
        if (ids.Length < docs.Count)
            throw new ArgumentException("Id must be set when calling Save.");

        options = ConfigureOptions(options?.As<T>());

        var originalDocuments = await GetOriginalDocumentsAsync(ids, options).AnyContext();
        await OnDocumentsSavingAsync(docs, originalDocuments, options).AnyContext();

        if (options.ShouldValidate())
            foreach (var doc in docs)
                await ValidateAndThrowAsync(doc).AnyContext();

        var result = await IndexDocumentsAsync(docs, false, options).AnyContext();

        var successDocs = result.IsSuccess ? docs : docs.Where(d => result.SuccessfulIds.Contains(d.Id)).ToList();
        if (successDocs.Count > 0)
        {
            var successOriginals = result.IsSuccess
                ? originalDocuments
                : originalDocuments.Where(o => result.SuccessfulIds.Contains(o.Id)).ToList();
            await OnDocumentsSavedAsync(successDocs, successOriginals, options).AnyContext();
            if (IsCacheEnabled && options.ShouldUseCache())
                await AddDocumentsToCacheAsync(successDocs, options, false).AnyContext();
        }

        if (result.HasErrors)
            ThrowForBulkErrors(result, isCreateOperation: false);
    }

    public Task<bool> PatchAsync(Id id, IPatchOperation operation, CommandOptionsDescriptor<T> options)
    {
        return PatchAsync(id, operation, options?.Configure());
    }

    public virtual async Task<bool> PatchAsync(Id id, IPatchOperation operation, ICommandOptions? options = null)
    {
        if (String.IsNullOrEmpty(id.Value))
            throw new ArgumentNullException(nameof(id));

        ArgumentNullException.ThrowIfNull(operation);

        if (operation is JsonPatch { Patch: null or { Operations: null or { Count: 0 } } })
            return false;

        if (operation is ActionPatch<T> { Actions: null or { Count: 0 } })
            return false;

        await ElasticIndex.EnsureIndexAsync(id).AnyContext();

        options = ConfigureOptions(options?.As<T>());

        if (operation is ScriptPatch scriptPatchOp)
            operation = ApplyDateTracking(scriptPatchOp);
        else if (operation is PartialPatch partialPatchOp)
            operation = ApplyDateTracking(partialPatchOp);

        bool modified = true;

        if (operation is ScriptPatch scriptOperation)
        {
            // ScriptPatch: noop detection requires the script to explicitly set ctx.op = 'none'.
            // Simply reassigning the same value is treated as a modification by Elasticsearch.
            if (!String.IsNullOrEmpty(DefaultPipeline))
            {
                var request = new UpdateByQueryRequest(ElasticIndex.GetIndex(id))
                {
                    Query = new Elastic.Clients.Elasticsearch.QueryDsl.IdsQuery
                    {
                        Values = new Elastic.Clients.Elasticsearch.Ids(new[] { new Elastic.Clients.Elasticsearch.Id(id.Value) })
                    },
                    Conflicts = Conflicts.Proceed,
                    Script = new Script { Source = scriptOperation.Script, Params = scriptOperation.Params },
                    Pipeline = DefaultPipeline,
                    Refresh = options.GetRefreshMode(DefaultConsistency) != Refresh.False,
                    WaitForCompletion = true
                };
                if (id.Routing != null)
                    request.Routing = new Elastic.Clients.Elasticsearch.Routing(id.Routing);

                var response = await _client.UpdateByQueryAsync(request).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());

                if (!response.IsValidResponse)
                {
                    if (response.ApiCallDetails is { HttpStatusCode: 404 })
                        throw new DocumentNotFoundException(id);

                    throw new DocumentException(response.GetErrorMessage($"Error patching document {ElasticIndex.GetIndex(id)}/{id.Value}"), response.OriginalException());
                }

                modified = (response.Noops ?? 0) is 0;
            }
            else
            {
                var request = new UpdateRequest<T, T>(ElasticIndex.GetIndex(id), id.Value)
                {
                    Script = new Script { Source = scriptOperation.Script, Params = scriptOperation.Params },
                    RetryOnConflict = options.GetRetryCount(),
                    Refresh = options.GetRefreshMode(DefaultConsistency)
                };
                if (id.Routing != null)
                    request.Routing = id.Routing;

                var response = await _client.UpdateAsync(request).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());

                if (!response.IsValidResponse)
                {
                    if (response.ApiCallDetails is { HttpStatusCode: 404 })
                        throw new DocumentNotFoundException(id);

                    if (response.ApiCallDetails is { HttpStatusCode: 409 })
                        throw new VersionConflictDocumentException(
                            response.GetErrorMessage($"Error patching document {ElasticIndex.GetIndex(id)}/{id.Value}"),
                            response.OriginalException());

                    throw new DocumentException(response.GetErrorMessage($"Error patching document {ElasticIndex.GetIndex(id)}/{id.Value}"), response.OriginalException());
                }

                modified = response.Result is not Result.NoOp;
            }
        }
        else if (operation is PartialPatch partialOperation)
        {
            // PartialPatch: Elasticsearch's detect_noop (enabled by default) reports noop when no
            // field values change. However, ApplyDateTracking injects UpdatedUtc for IHaveDates
            // models, which typically prevents noop detection since the timestamp always changes.
            if (!String.IsNullOrEmpty(DefaultPipeline))
            {
                // Pipeline path uses get-merge-reindex, so a write always occurs (like JsonPatch).
                var policy = _resiliencePolicy;
                if (options.HasRetryCount())
                {
                    if (policy is ResiliencePolicy resiliencePolicy)
                        policy = resiliencePolicy.Clone(options.GetRetryCount());
                    else
                        _logger.LogWarning("Unable to override resilience policy max attempts");
                }

                await policy.ExecuteAsync(async ct =>
                {
                    var getRequest = new GetRequest(ElasticIndex.GetIndex(id), id.Value);
                    if (id.Routing != null)
                        getRequest.Routing = id.Routing;

                    var response = await _client.GetAsync<T>(getRequest, ct).AnyContext();
                    _logger.LogRequest(response, options.GetQueryLogLevel());
                    if (!response.IsValidResponse)
                    {
                        if (!response.Found)
                            throw new DocumentNotFoundException(id);

                        throw new DocumentException(response.GetErrorMessage($"Error patching document {ElasticIndex.GetIndex(id)}/{id.Value}"), response.OriginalException());
                    }

                    var sourceJson = _client.ElasticsearchClientSettings.SourceSerializer.SerializeToString(response.Source);
                    var sourceNode = JsonNode.Parse(sourceJson);
                    var partialJson = _client.ElasticsearchClientSettings.SourceSerializer.SerializeToString(partialOperation.Document);
                    var partialNode = JsonNode.Parse(partialJson);

                    if (sourceNode is JsonObject sourceObj && partialNode is JsonObject partialObj)
                    {
                        foreach (var prop in partialObj)
                            sourceObj[prop.Key] = prop.Value?.DeepClone();
                    }

                    if (sourceNode is null)
                        throw new DocumentException("Failed to parse source document JSON for partial merge.");

                    using var mergedStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(sourceNode.ToJsonString()));
                    var mergedDocument = _client.ElasticsearchClientSettings.SourceSerializer.Deserialize<T>(mergedStream);

                    var indexRequest = new IndexRequest<T>(mergedDocument, ElasticIndex.GetIndex(id), id.Value)
                    {
                        Pipeline = DefaultPipeline,
                        Refresh = options.GetRefreshMode(DefaultConsistency)
                    };
                    if (id.Routing != null)
                        indexRequest.Routing = id.Routing;

                    if (HasVersion && !options.ShouldSkipVersionCheck())
                    {
                        indexRequest.IfSeqNo = response.SeqNo;
                        indexRequest.IfPrimaryTerm = response.PrimaryTerm;
                    }

                    var indexResponse = await _client.IndexAsync(indexRequest, ct).AnyContext();
                    _logger.LogRequest(indexResponse, options.GetQueryLogLevel());

                    if (!indexResponse.IsValidResponse)
                    {
                        if (indexResponse.ElasticsearchServerError?.Status == 409)
                            throw new VersionConflictDocumentException(indexResponse.GetErrorMessage("Error saving document"), indexResponse.OriginalException());

                        throw new DocumentException(indexResponse.GetErrorMessage("Error saving document"), indexResponse.OriginalException());
                    }
                }).AnyContext();
            }
            else
            {
                var request = new UpdateRequest<T, object>(ElasticIndex.GetIndex(id), id.Value)
                {
                    Doc = partialOperation.Document,
                    RetryOnConflict = options.GetRetryCount()
                };
                if (id.Routing != null)
                    request.Routing = id.Routing;
                request.Refresh = options.GetRefreshMode(DefaultConsistency);

                var response = await _client.UpdateAsync(request).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());

                if (!response.IsValidResponse)
                {
                    if (response.ApiCallDetails is { HttpStatusCode: 404 })
                        throw new DocumentNotFoundException(id);

                    if (response.ApiCallDetails is { HttpStatusCode: 409 })
                        throw new VersionConflictDocumentException(
                            response.GetErrorMessage($"Error patching document {ElasticIndex.GetIndex(id)}/{id.Value}"),
                            response.OriginalException());

                    throw new DocumentException(response.GetErrorMessage($"Error patching document {ElasticIndex.GetIndex(id)}/{id.Value}"), response.OriginalException());
                }

                modified = response.Result is not Result.NoOp;
            }
        }
        else if (operation is JsonPatch jsonOperation)
        {
            // JsonPatch uses get-modify-reindex, so a write always occurs regardless of whether
            // field values actually changed. Noop detection is not possible without content comparison.
            var policy = _resiliencePolicy;
            if (options.HasRetryCount())
            {
                if (policy is ResiliencePolicy resiliencePolicy)
                    policy = resiliencePolicy.Clone(options.GetRetryCount());
                else
                    _logger.LogWarning("Unable to override resilience policy max attempts");
            }

            await policy.ExecuteAsync(async ct =>
            {
                var request = new GetRequest(ElasticIndex.GetIndex(id), id.Value);
                if (id.Routing != null)
                    request.Routing = id.Routing;

                var response = await _client.GetAsync<T>(request, ct).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());
                if (!response.IsValidResponse)
                {
                    if (!response.Found)
                        throw new DocumentNotFoundException(id);

                    throw new DocumentException(response.GetErrorMessage($"Error patching document {ElasticIndex.GetIndex(id)}/{id.Value}"), response.OriginalException());
                }

                // Serialize to JSON string, apply patch, deserialize back
                // Using System.Text.Json.Nodes.JsonNode since Elastic.Clients.Elasticsearch uses System.Text.Json exclusively
                var json = _client.ElasticsearchClientSettings.SourceSerializer.SerializeToString(response.Source);
                JsonNode? target = JsonNode.Parse(json);
                if (target is null)
                    throw new DocumentException("Document JSON parsed to null; cannot apply JSON patch.");

                new JsonPatcher().Patch(ref target, jsonOperation.Patch);

                ApplyDateTracking(target);

                if (target is null)
                    throw new DocumentException("JSON patch produced a null document.");

                using var patchStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(target.ToJsonString()));
                var patchedDocument = _client.ElasticsearchClientSettings.SourceSerializer.Deserialize<T>(patchStream);

                var indexRequest = new IndexRequest<T>(patchedDocument, ElasticIndex.GetIndex(id), id.Value)
                {
                    Pipeline = DefaultPipeline,
                    Refresh = options.GetRefreshMode(DefaultConsistency)
                };
                if (id.Routing != null)
                    indexRequest.Routing = id.Routing;

                if (HasVersion && !options.ShouldSkipVersionCheck())
                {
                    indexRequest.IfSeqNo = response.SeqNo;
                    indexRequest.IfPrimaryTerm = response.PrimaryTerm;
                }

                var updateResponse = await _client.IndexAsync(indexRequest, ct).AnyContext();
                _logger.LogRequest(updateResponse, options.GetQueryLogLevel());

                if (!updateResponse.IsValidResponse)
                {
                    if (updateResponse.ElasticsearchServerError?.Status is 409)
                        throw new VersionConflictDocumentException(updateResponse.GetErrorMessage("Error saving document"), updateResponse.OriginalException());

                    throw new DocumentException(updateResponse.GetErrorMessage("Error saving document"), updateResponse.OriginalException());
                }
            }).AnyContext();
        }
        else if (operation is ActionPatch<T> actionPatch)
        {
            var policy = _resiliencePolicy;
            if (options.HasRetryCount())
            {
                if (policy is ResiliencePolicy resiliencePolicy)
                    policy = resiliencePolicy.Clone(options.GetRetryCount());
                else
                    _logger.LogWarning("Unable to override resilience policy max attempts");
            }

            T? modifiedDocument = null;
            await policy.ExecuteAsync(async ct =>
            {
                modifiedDocument = null;
                modified = true;
                var request = new GetRequest(ElasticIndex.GetIndex(id), id.Value);
                if (id.Routing != null)
                    request.Routing = id.Routing;
                var response = await _client.GetAsync<T>(request, ct).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());

                if (!response.IsValidResponse)
                {
                    if (!response.Found)
                        throw new DocumentNotFoundException(id);

                    throw new DocumentException(response.GetErrorMessage($"Error patching document {ElasticIndex.GetIndex(id)}/{id.Value}"), response.OriginalException());
                }

                if (response.Source is IVersioned versionedDoc && response.PrimaryTerm.HasValue)
                    versionedDoc.Version = response.GetElasticVersion();

                if (response.Source is not { } source)
                    throw new DocumentException($"Document {ElasticIndex.GetIndex(id)}/{id.Value} returned no source");

                bool actionModified = false;
                foreach (var action in actionPatch.Actions)
                    actionModified |= action?.Invoke(source) ?? false;

                if (!actionModified)
                {
                    modified = false;
                    return;
                }

                if (HasDateTracking)
                    SetDocumentDates(source, ElasticIndex.Configuration.TimeProvider);

                modifiedDocument = source;
                await IndexDocumentsAsync([source], false, options).AnyContext();
            }).AnyContext();

            if (modified && modifiedDocument is not null)
            {
                await OnDocumentsChangedAsync(ChangeType.Saved, [modifiedDocument], options).AnyContext();

                if (options.ShouldNotify())
                    await PublishChangeTypeMessageAsync(ChangeType.Saved, modifiedDocument).AnyContext();

                return true;
            }
        }
        else
        {
            throw new ArgumentException("Unknown operation type", nameof(operation));
        }

        // ScriptPatch, PartialPatch, and single-doc JsonPatch invalidate by ID only because
        // the modified document is not available client-side. ActionPatch handles its own
        // document-based invalidation above and returns early.
        // TODO: Single-doc JsonPatch could deserialize the JToken to T for document-based invalidation.
        if (modified)
        {
            await OnDocumentsChangedAsync(ChangeType.Saved, EmptyList, options).AnyContext();
            if (IsCacheEnabled)
                await InvalidateCacheAsync(id).AnyContext();

            if (options.ShouldNotify())
                await PublishChangeTypeMessageAsync(ChangeType.Saved, id).AnyContext();
        }

        return modified;
    }

    public Task<long> PatchAsync(Ids ids, IPatchOperation operation, CommandOptionsDescriptor<T> options)
    {
        return PatchAsync(ids, operation, options?.Configure());
    }

    public virtual async Task<long> PatchAsync(Ids ids, IPatchOperation operation, ICommandOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(ids);
        ArgumentNullException.ThrowIfNull(operation);

        if (ids is { Count: 0 })
            return 0;

        options = ConfigureOptions(options?.As<T>());

        if (ids.Count is 1)
            return await PatchAsync(ids[0], operation, options).AnyContext() ? 1 : 0;

        if (operation is ScriptPatch scriptPatchOp)
            operation = ApplyDateTracking(scriptPatchOp);
        else if (operation is PartialPatch partialPatchOp)
            operation = ApplyDateTracking(partialPatchOp);

        if (operation is JsonPatch)
            return await PatchAllAsync(NewQuery().Id(ids), operation, options).AnyContext();

        if (operation is ActionPatch<T>)
            return await PatchAllAsync(NewQuery().Id(ids), operation, options).AnyContext();

        if (operation is not ScriptPatch and not PartialPatch)
            throw new ArgumentException("Unknown operation type", nameof(operation));

        if (!String.IsNullOrEmpty(DefaultPipeline))
        {
            long pipelineModified = 0;
            foreach (var id in ids)
            {
                if (await PatchAsync(id, operation, options).AnyContext())
                    pipelineModified++;
            }
            return pipelineModified;
        }

        var bulkResponse = await _client.BulkAsync(b =>
        {
            b.Refresh(options.GetRefreshMode(DefaultConsistency));
            foreach (var id in ids)
            {
                if (operation is ScriptPatch scriptOperation)
                    b.Update<T>(u =>
                    {
                        u.Id(id.Value)
                          .Index(ElasticIndex.GetIndex(id))
                          .Script(s => s.Source(scriptOperation.Script).Params(scriptOperation.Params))
                          .RetriesOnConflict(options.GetRetryCount());

                        if (id.Routing != null)
                            u.Routing(id.Routing);
                    });
                else if (operation is PartialPatch partialOperation)
                    b.Update<T, object>(u =>
                    {
                        u.Id(id.Value)
                            .Index(ElasticIndex.GetIndex(id))
                            // TODO: Null-valued properties are silently dropped by the ES client's SourceSerializer
                            // (elastic/elasticsearch-net#8763). Consumers must use ScriptPatch or JsonPatch to set fields to null.
                            .Doc(partialOperation.Document)
                            .RetriesOnConflict(options.GetRetryCount());

                        if (id.Routing != null)
                            u.Routing(id.Routing);
                    });
            }
        }).AnyContext();
        _logger.LogRequest(bulkResponse, options.GetQueryLogLevel());

        var result = BulkResult.From(bulkResponse);

        var modifiedIds = result.IsSuccess
            ? ids.Where(id => !result.NoopIds.Contains(id.Value)).ToList()
            : ids.Where(id => result.SuccessfulIds.Contains(id.Value) && !result.NoopIds.Contains(id.Value)).ToList();
        if (modifiedIds.Count > 0)
        {
            // ScriptPatch/PartialPatch execute server-side; the modified document is not available.
            // TODO: Invalidate by documents instead of IDs to support custom cache invalidation overrides.
            await OnDocumentsChangedAsync(ChangeType.Saved, EmptyList, options).AnyContext();
            if (IsCacheEnabled)
                await InvalidateCacheAsync(modifiedIds.Select(id => id.Value)).AnyContext();

            if (options.ShouldNotify())
            {
                var tasks = new List<Task>(modifiedIds.Count);
                foreach (var id in modifiedIds)
                    tasks.Add(PublishChangeTypeMessageAsync(ChangeType.Saved, id));

                await Task.WhenAll(tasks).AnyContext();
            }
        }

        if (result.HasErrors)
            ThrowForBulkErrors(result, operationLabel: "patching");

        return result.ModifiedCount;
    }

    public Task RemoveAsync(Id id, CommandOptionsDescriptor<T> options)
    {
        return RemoveAsync(id, options?.Configure());
    }

    public Task RemoveAsync(Id id, ICommandOptions? options = null)
    {
        if (String.IsNullOrEmpty(id))
            throw new ArgumentNullException(nameof(id));

        return RemoveAsync((Ids)id, options);
    }

    public Task RemoveAsync(Ids ids, CommandOptionsDescriptor<T> options)
    {
        return RemoveAsync(ids, options?.Configure());
    }

    public async Task RemoveAsync(Ids ids, ICommandOptions? options = null)
    {
        if (ids == null)
            throw new ArgumentNullException(nameof(ids));

        options = ConfigureOptions(options?.As<T>());
        if (IsCacheEnabled)
            options = options.ReadCache();

        // TODO: If not OriginalsEnabled then just delete by id
        // TODO: Delete by id using GetIndexById and id.Routing if its a child doc
        var documents = await GetByIdsAsync(ids, options).AnyContext();
        if (documents == null)
            return;

        await RemoveAsync(documents, options).AnyContext();
    }

    public Task RemoveAsync(T document, CommandOptionsDescriptor<T> options)
    {
        return RemoveAsync(document, options?.Configure());
    }

    public virtual Task RemoveAsync(T document, ICommandOptions? options = null)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));

        return RemoveAsync(new[] { document }, options);
    }

    public Task RemoveAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options)
    {
        return RemoveAsync(documents, options?.Configure());
    }

    public virtual async Task RemoveAsync(IEnumerable<T> documents, ICommandOptions? options = null)
    {
        var docs = documents?.ToList();
        if (docs == null || docs.Any(d => d == null))
            throw new ArgumentNullException(nameof(documents));

        if (docs.Count == 0)
            return;

        if (ElasticIndex.HasMultipleIndexes)
        {
            foreach (var documentGroup in docs.GroupBy(ElasticIndex.GetIndex))
                await ElasticIndex.EnsureIndexAsync(documentGroup.First()).AnyContext();
        }
        else
        {
            await ElasticIndex.EnsureIndexAsync(null).AnyContext();
        }

        options = ConfigureOptions(options?.As<T>());
        if (IsCacheEnabled && options.HasCacheKey())
            throw new ArgumentException("Cache key can't be set when calling RemoveAsync");

        await OnDocumentsRemovingAsync(docs, options).AnyContext();

        if (docs.Count == 1)
        {
            var document = docs.Single();
            var request = new DeleteRequest(ElasticIndex.GetIndex(document), document.Id)
            {
                Refresh = options.GetRefreshMode(DefaultConsistency)
            };

            if (GetParentIdFunc is not null)
                request.Routing = GetParentIdFunc(document);

            var response = await _client.DeleteAsync(request).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());

            if (!response.IsValidResponse && response.ApiCallDetails.HttpStatusCode != 404)
            {
                throw new DocumentException(response.GetErrorMessage($"Error removing document {ElasticIndex.GetIndex(document)}/{document.Id}"), response.OriginalException());
            }
        }
        else
        {
            var response = await _client.BulkAsync(bulk =>
            {
                bulk.Refresh(options.GetRefreshMode(DefaultConsistency));
                foreach (var doc in docs)
                    bulk.Delete<T>(d =>
                    {
                        d.Id(doc.Id).Index(ElasticIndex.GetIndex(doc));

                        if (GetParentIdFunc is not null)
                            d.Routing(GetParentIdFunc(doc));
                    });
            }).AnyContext();

            _logger.LogRequest(response, options.GetQueryLogLevel());

            var result = BulkResult.From(response);

            var successDocs = result.IsSuccess ? docs : docs.Where(d => result.SuccessfulIds.Contains(d.Id)).ToList();
            if (successDocs.Count > 0)
                await OnDocumentsRemovedAsync(successDocs, options).AnyContext();

            if (result.HasErrors)
                ThrowForBulkErrors(result, operationLabel: "removing");

            return;
        }

        await OnDocumentsRemovedAsync(docs, options).AnyContext();
    }

    public Task<long> RemoveAllAsync(CommandOptionsDescriptor<T>? options)
    {
        return RemoveAllAsync(options?.Configure());
    }

    public virtual async Task<long> RemoveAllAsync(ICommandOptions? options = null)
    {
        long count = await RemoveAllAsync(NewQuery(), options).AnyContext();

        if (IsCacheEnabled && count > 0)
            await Cache.RemoveAllAsync().AnyContext();

        return count;
    }

    public Task<long> PatchAllAsync(RepositoryQueryDescriptor<T> query, IPatchOperation operation, CommandOptionsDescriptor<T>? options = null)
    {
        return PatchAllAsync(query.Configure(), operation, options?.Configure());
    }

    /// <summary>
    /// Patches all documents matching the query. Script and partial patches use Elasticsearch's
    /// update-by-query when caching is disabled; otherwise they fall back to batch processing.
    /// JsonPatch and ActionPatch always use batch processing with conflict retry.
    /// </summary>
    public virtual async Task<long> PatchAllAsync(IRepositoryQuery query, IPatchOperation operation, ICommandOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(operation);

        if (operation is JsonPatch { Patch: null or { Operations: null or { Count: 0 } } })
            return 0;

        if (operation is ActionPatch<T> { Actions: null or { Count: 0 } })
            return 0;

        if (!ElasticIndex.HasMultipleIndexes)
            await ElasticIndex.EnsureIndexAsync(null).AnyContext();

        options = ConfigureOptions(options?.As<T>());

        if (operation is ScriptPatch scriptPatchOp)
            operation = ApplyDateTracking(scriptPatchOp);
        else if (operation is PartialPatch partialPatchOp)
            operation = ApplyDateTracking(partialPatchOp);

        long affectedRecords = 0;
        if (operation is JsonPatch jsonOperation)
        {
            var patcher = new JsonPatcher();
            long modifiedRecords = 0;
            await BatchProcessAsync(query, async results =>
            {
                var processedDocs = new Dictionary<string, T>(results.Hits.Count);
                var bulkResult = await _client.BulkAsync(b =>
                {
                    b.Refresh(options.GetRefreshMode(DefaultConsistency));
                    foreach (var h in results.Hits)
                    {
                        // Using System.Text.Json.Nodes.JsonNode since Elastic.Clients.Elasticsearch uses System.Text.Json exclusively
                        var json = _client.ElasticsearchClientSettings.SourceSerializer.SerializeToString(h.Document);
                        JsonNode? target = JsonNode.Parse(json);
                        if (target is null)
                            throw new DocumentException("Document JSON parsed to null; cannot apply JSON patch.");

                        patcher.Patch(ref target, jsonOperation.Patch);
                        if (target is null)
                            throw new DocumentException("JSON patch produced a null document.");

                        using var docStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(target.ToJsonString()));
                        var doc = _client.ElasticsearchClientSettings.SourceSerializer.Deserialize<T>(docStream);

                        if (HasDateTracking)
                            SetDocumentDates(doc, ElasticIndex.Configuration.TimeProvider);

                        if (h.Id is null || doc is null)
                            throw new DocumentException("Bulk JSON patch requires hit Id and deserialized document.");

                        processedDocs[h.Id] = doc;
                        var elasticVersion = h.GetElasticVersion();

                        b.Index(doc, i =>
                        {
                            i.Id(h.Id!)
                             .Index(h.GetIndex()!);
                            if (h.Routing is not null)
                                i.Routing(h.Routing);
                            if (DefaultPipeline is not null)
                                i.Pipeline(DefaultPipeline);

                            if (HasVersion)
                            {
                                i.IfPrimaryTerm(elasticVersion.PrimaryTerm);
                                i.IfSequenceNumber(elasticVersion.SequenceNumber);
                            }
                        });
                    }
                }).AnyContext();

                var retriedIds = new List<string>();
                if (bulkResult.IsValidResponse)
                {
                    _logger.LogRequest(bulkResult, options.GetQueryLogLevel());
                }
                else
                {
                    var retryResult = await HandleBulkPatchErrorsAsync(bulkResult, results, "JsonPatch", operation, options).AnyContext();
                    modifiedRecords += retryResult.Count;
                    retriedIds = retryResult.Ids;
                }

                var successfulIds = new HashSet<string>(bulkResult.Items?.Where(i => i.IsValid).Select(i => i.Id!) ?? []);
                var modifiedDocuments = successfulIds.Select(id => processedDocs.GetValueOrDefault(id)).Where(d => d is not null).Select(d => d!).ToList();
                modifiedRecords += modifiedDocuments.Count;

                if (modifiedDocuments.Count > 0 || retriedIds.Count > 0)
                {
                    if (IsCacheEnabled)
                        await InvalidateCacheAsync(modifiedDocuments).AnyContext();

                    try
                    {
                        var callbackIds = successfulIds.Concat(retriedIds).ToList();
                        options.GetUpdatedIdsCallback()?.Invoke(callbackIds);
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error calling updated ids callback: {Message}", ex.Message);
                    }
                }

                return true;
            }, options.Clone()).AnyContext();

            affectedRecords += modifiedRecords;
        }
        else if (operation is ActionPatch<T> actionOperation)
        {
            long modifiedRecords = 0;
            await BatchProcessAsync(query, async results =>
            {
                var modifiedHits = new List<FindHit<T>>(results.Hits.Count);
                foreach (var h in results.Hits)
                {
                    if (h.Document is null)
                        continue;

                    bool actionModified = false;
                    foreach (var action in actionOperation.Actions)
                        actionModified |= action?.Invoke(h.Document) ?? false;

                    if (!actionModified)
                        continue;

                    if (HasDateTracking)
                        SetDocumentDates(h.Document, ElasticIndex.Configuration.TimeProvider);

                    modifiedHits.Add(h);
                }

                if (modifiedHits.Count is 0)
                    return true;

                var bulkResult = await _client.BulkAsync(b =>
                {
                    b.Refresh(options.GetRefreshMode(DefaultConsistency));
                    foreach (var h in modifiedHits)
                    {
                        var elasticVersion = h.GetElasticVersion();

                        b.Index(h.Document, i =>
                        {
                            i.Id(h.Id!)
                                .Index(h.GetIndex()!);
                            if (h.Routing is not null)
                                i.Routing(h.Routing);
                            if (DefaultPipeline is not null)
                                i.Pipeline(DefaultPipeline);

                            if (HasVersion)
                            {
                                i.IfPrimaryTerm(elasticVersion.PrimaryTerm);
                                i.IfSequenceNumber(elasticVersion.SequenceNumber);
                            }
                        });
                    }
                }).AnyContext();

                var retriedIds = new List<string>();
                if (bulkResult.IsValidResponse)
                {
                    _logger.LogRequest(bulkResult, options.GetQueryLogLevel());
                }
                else
                {
                    var retryResult = await HandleBulkPatchErrorsAsync(bulkResult, results, "ActionPatch", operation, options).AnyContext();
                    modifiedRecords += retryResult.Count;
                    retriedIds = retryResult.Ids;
                }

                var successfulIds = new HashSet<string>(bulkResult.Items?.Where(i => i.IsValid).Select(i => i.Id!) ?? []);
                var modifiedDocuments = modifiedHits.Where(h => successfulIds.Contains(h.Id!)).ToList();
                modifiedRecords += modifiedDocuments.Count;

                if (modifiedDocuments.Count > 0 || retriedIds.Count > 0)
                {
                    if (IsCacheEnabled)
                        await InvalidateCacheAsync(modifiedDocuments.Select(h => h.Document!)).AnyContext();

                    try
                    {
                        var callbackIds = modifiedDocuments.Select(h => h.Id!).Concat(retriedIds).ToList();
                        options.GetUpdatedIdsCallback()?.Invoke(callbackIds);
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error calling updated ids callback: {Message}", ex.Message);
                    }
                }

                return true;
            }, options.Clone()).AnyContext();

            affectedRecords += modifiedRecords;
        }
        else
        {
            if (operation is not ScriptPatch and not PartialPatch)
                throw new ArgumentException("Unknown operation type", nameof(operation));

            if (!IsCacheEnabled && operation is ScriptPatch scriptOperation)
            {
                var request = new UpdateByQueryRequest(Indices.Index(String.Join(",", ElasticIndex.GetIndexesByQuery(query))))
                {
                    Query = await ElasticIndex.QueryBuilder.BuildQueryAsync(query, options, new SearchRequestDescriptor<T>()).AnyContext(),
                    Conflicts = Conflicts.Proceed,
                    Script = new Script { Source = scriptOperation.Script, Params = scriptOperation.Params },
                    Pipeline = DefaultPipeline,
                    Version = HasVersion,
                    Refresh = options.GetRefreshMode(DefaultConsistency) != Refresh.False,
                    IgnoreUnavailable = true,
                    WaitForCompletion = false
                };

                var response = await _client.UpdateByQueryAsync(request).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());
                if (!response.IsValidResponse)
                {
                    throw new DocumentException(response.GetErrorMessage("Error occurred while patching by query"), response.OriginalException());
                }

                if (response.Task is null)
                    throw new DocumentException("Update by query did not return a task identifier.", response.OriginalException());

                var taskId = response.Task.ToString();
                int attempts = 0;
                do
                {
                    attempts++;
                    var taskRequest = new Tasks.GetTasksRequest(taskId) { WaitForCompletion = false };
                    var taskStatus = await _client.Tasks.GetAsync(taskRequest).AnyContext();
                    _logger.LogRequest(taskStatus, options.GetQueryLogLevel());

                    if (!taskStatus.IsValidResponse)
                    {
                        if (taskStatus.ApiCallDetails.HttpStatusCode.GetValueOrDefault() == 404)
                        {
                            _logger.LogWarning("Task {TaskId} not found (404), treating as completed", taskId);
                            break;
                        }

                        _logger.LogError("Error getting task status for {TaskId}: {Error}", taskId, taskStatus.ElasticsearchServerError);
                        if (attempts >= 20)
                            throw new DocumentException($"Failed to get task status for {taskId} after {attempts} attempts");

                        var retryDelay = TimeSpan.FromSeconds(attempts <= 5 ? 1 : 5);
                        await ElasticIndex.Configuration.TimeProvider.SafeDelay(retryDelay, DisposedCancellationToken).AnyContext();
                        continue;
                    }

                    // Extract status values from the raw JSON. The Status property is object? and may be
                    // deserialized as JsonElement or IDictionary<string, object> depending on serializer config.
                    long? created = null, updated = null, deleted = null, versionConflicts = null, total = null;
                    if (taskStatus.Task.Status is JsonElement jsonElement)
                    {
                        total = jsonElement.TryGetProperty("total", out var totalProp) ? totalProp.GetInt64() : 0;
                        created = jsonElement.TryGetProperty("created", out var createdProp) ? createdProp.GetInt64() : 0;
                        updated = jsonElement.TryGetProperty("updated", out var updatedProp) ? updatedProp.GetInt64() : 0;
                        deleted = jsonElement.TryGetProperty("deleted", out var deletedProp) ? deletedProp.GetInt64() : 0;
                        versionConflicts = jsonElement.TryGetProperty("version_conflicts", out var conflictsProp) ? conflictsProp.GetInt64() : 0;
                    }
                    else if (taskStatus.Task.Status is IDictionary<string, object> dict)
                    {
                        total = dict.TryGetValue("total", out var totalVal) ? Convert.ToInt64(totalVal) : 0;
                        created = dict.TryGetValue("created", out var createdVal) ? Convert.ToInt64(createdVal) : 0;
                        updated = dict.TryGetValue("updated", out var updatedVal) ? Convert.ToInt64(updatedVal) : 0;
                        deleted = dict.TryGetValue("deleted", out var deletedVal) ? Convert.ToInt64(deletedVal) : 0;
                        versionConflicts = dict.TryGetValue("version_conflicts", out var conflictsVal) ? Convert.ToInt64(conflictsVal) : 0;
                    }

                    if (taskStatus.Completed)
                    {
                        if (taskStatus.Error is not null)
                            throw new DocumentException($"Script operation task ({taskId}) failed: {taskStatus.Error.Type} - {taskStatus.Error.Reason}", taskStatus.OriginalException());

                        if (versionConflicts > 0)
                            _logger.LogWarning("Script operation task ({TaskId}) completed with {Conflicts} version conflicts", taskId, versionConflicts);
                        else
                            _logger.LogInformation("Script operation task ({TaskId}) completed: Created: {Created} Updated: {Updated} Deleted: {Deleted} Conflicts: {Conflicts} Total: {Total}", taskId, created, updated, deleted, versionConflicts, total);

                        affectedRecords += (created ?? 0) + (updated ?? 0) + (deleted ?? 0);
                        break;
                    }

                    _logger.LogDebug("Checking script operation task ({TaskId}) status: Created: {Created} Updated: {Updated} Deleted: {Deleted} Conflicts: {Conflicts} Total: {Total}", taskId, created, updated, deleted, versionConflicts, total);
                    var delay = TimeSpan.FromSeconds(attempts <= 5 ? 1 : 5);
                    await ElasticIndex.Configuration.TimeProvider.SafeDelay(delay, DisposedCancellationToken).AnyContext();
                } while (!DisposedCancellationToken.IsCancellationRequested);
            }
            else
            {
                if (HasIdentity && !query.GetIncludes().Contains(_idField!.Value))
                    query.Include(_idField!.Value);

                long modifiedInBatch = 0;
                await BatchProcessAsync(query, async results =>
                {
                    var bulkResult = await _client.BulkAsync(b =>
                    {
                        if (DefaultPipeline is not null)
                            b.Pipeline(DefaultPipeline);
                        b.Refresh(options.GetRefreshMode(DefaultConsistency));

                        foreach (var h in results.Hits)
                        {
                            if (operation is PartialPatch partialOp && !String.IsNullOrEmpty(DefaultPipeline))
                            {
                                var sourceJson = _client.ElasticsearchClientSettings.SourceSerializer.SerializeToString(h.Document);
                                var sourceNode = JsonNode.Parse(sourceJson);
                                var partialJson = _client.ElasticsearchClientSettings.SourceSerializer.SerializeToString(partialOp.Document);
                                var partialNode = JsonNode.Parse(partialJson);

                                if (sourceNode is JsonObject sourceObj && partialNode is JsonObject partialObj)
                                {
                                    foreach (var prop in partialObj)
                                        sourceObj[prop.Key] = prop.Value?.DeepClone();
                                }

                                if (sourceNode is null)
                                    throw new DocumentException("Failed to parse source document JSON for partial merge.");

                                using var mergedStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(sourceNode.ToJsonString()));
                                var mergedDoc = _client.ElasticsearchClientSettings.SourceSerializer.Deserialize<T>(mergedStream);
                                var elasticVersion = h.GetElasticVersion();

                                b.Index(mergedDoc, i =>
                                {
                                    i.Id(h.Id!)
                                        .Index(h.GetIndex()!);
                                    if (h.Routing is not null)
                                        i.Routing(h.Routing);
                                    if (DefaultPipeline is not null)
                                        i.Pipeline(DefaultPipeline);

                                    if (HasVersion)
                                    {
                                        i.IfPrimaryTerm(elasticVersion.PrimaryTerm);
                                        i.IfSequenceNumber(elasticVersion.SequenceNumber);
                                    }
                                });
                            }
                            else if (operation is ScriptPatch sp)
                                b.Update<T>(u =>
                                {
                                    u.Id(h.Id!)
                                        .Index(h.GetIndex()!)
                                        .Script(s => s.Source(sp.Script).Params(sp.Params))
                                        .RetriesOnConflict(options.GetRetryCount());
                                    if (h.Routing is not null)
                                        u.Routing(h.Routing);
                                });
                            else if (operation is PartialPatch pp)
                                // TODO: Null-valued properties are silently dropped by the ES client's SourceSerializer
                                // (elastic/elasticsearch-net#8763). Consumers must use ScriptPatch or JsonPatch to set fields to null.
                                b.Update<T, object>(u =>
                                {
                                    u.Id(h.Id!)
                                        .Index(h.GetIndex()!)
                                        .Doc(pp.Document)
                                        .RetriesOnConflict(options.GetRetryCount());
                                    if (h.Routing is not null)
                                        u.Routing(h.Routing);
                                });
                        }
                    }).AnyContext();

                    if (bulkResult.IsValidResponse)
                    {
                        _logger.LogRequest(bulkResult, options.GetQueryLogLevel());
                    }
                    else
                    {
                        _logger.LogErrorRequest(bulkResult, "Error occurred while bulk updating");
                        return false;
                    }

                    var result = BulkResult.From(bulkResult);
                    var updatedIds = results.Hits
                        .Where(h => result.SuccessfulIds.Contains(h.Id!) && !result.NoopIds.Contains(h.Id!))
                        .Select(h => h.Id!).ToList();
                    modifiedInBatch += updatedIds.Count;

                    if (IsCacheEnabled && updatedIds.Count > 0)
                    {
                        // TODO: Invalidate by documents instead of IDs to support custom cache invalidation overrides.
                        await InvalidateCacheAsync(updatedIds).AnyContext();
                    }

                    try
                    {
                        if (updatedIds.Count > 0)
                            options.GetUpdatedIdsCallback()?.Invoke(updatedIds);
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error calling updated ids callback: {Message}", ex.Message);
                    }

                    return true;
                }, options.Clone()).AnyContext();

                affectedRecords += modifiedInBatch;
            }
        }

        if (affectedRecords > 0)
        {
            // When called via PatchAsync(Ids), query has IDs and this duplicates per-batch invalidation
            // (harmless/idempotent). When called directly with a filter query, GetIds() is empty (no-op).
            if (IsCacheEnabled)
                await InvalidateCacheByQueryAsync(query.As<T>()).AnyContext();

            // Empty list: batch callbacks handle per-document cache invalidation for ActionPatch/JsonPatch.
            // This fires the DocumentsChanged event so subscribers know documents were modified.
            await OnDocumentsChangedAsync(ChangeType.Saved, EmptyList, options).AnyContext();
            await SendQueryNotificationsAsync(ChangeType.Saved, query, options).AnyContext();
        }

        return affectedRecords;
    }

    public Task<long> RemoveAllAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T>? options = null)
    {
        return RemoveAllAsync(query.Configure(), options?.Configure());
    }

    public virtual async Task<long> RemoveAllAsync(IRepositoryQuery query, ICommandOptions? options = null)
    {
        options = ConfigureOptions(options?.As<T>());
        bool hasRemoveListeners = DocumentsChanging.HasHandlers || DocumentsChanged.HasHandlers || DocumentsRemoving.HasHandlers || DocumentsRemoved.HasHandlers;
        if (hasRemoveListeners || (IsCacheEnabled && options.ShouldUseCache(true)))
        {
            var includes = query.GetIncludes();
            foreach (var field in RequiredFields.Select(f => f.Value))
                if (field is not null && !includes.Contains(field))
                    query.Include(field);

            if (!options.HasPageLimit())
                options.PageLimit(1000);

            var removeOptions = options.Clone();
            if (removeOptions.GetConsistency() != Consistency.Eventual)
                removeOptions.Consistency(Consistency.Eventual);

            return await BatchProcessAsync(query, async results =>
            {
                await RemoveAsync(results.Documents, removeOptions).AnyContext();
                return true;
            }, options).AnyContext();
        }

        var response = await _client.DeleteByQueryAsync(new DeleteByQueryRequest(ElasticIndex.Name)
        {
            Refresh = options.GetRefreshMode(DefaultConsistency) != Refresh.False,
            Conflicts = Conflicts.Proceed,
            Query = await ElasticIndex.QueryBuilder.BuildQueryAsync(query, options, new SearchRequestDescriptor<T>()).AnyContext()
        }).AnyContext();
        _logger.LogRequest(response, options.GetQueryLogLevel());

        if (!response.IsValidResponse)
        {
            throw new DocumentException(response.GetErrorMessage("Error removing documents"), response.OriginalException());
        }

        if (response.Deleted.HasValue && response.Deleted > 0)
        {
            if (IsCacheEnabled)
                await InvalidateCacheByQueryAsync(query.As<T>()).AnyContext();

            await OnDocumentsRemovedAsync(EmptyList, options).AnyContext();
            await SendQueryNotificationsAsync(ChangeType.Removed, query, options).AnyContext();
        }

        if (response.Total != response.Deleted)
            _logger.LogWarning("RemoveAll: {Deleted} of {Total} records were removed ({Conflicts} version conflicts)", response.Deleted, response.Total, response.VersionConflicts);

        return response.Deleted ?? 0;
    }

    public Task<long> BatchProcessAsync(RepositoryQueryDescriptor<T> query, Func<FindResults<T>, Task<bool>> processFunc, CommandOptionsDescriptor<T>? options = null)
    {
        return BatchProcessAsync(query.Configure(), processFunc, options?.Configure());
    }

    public Task<long> BatchProcessAsync(IRepositoryQuery query, Func<FindResults<T>, Task<bool>> processFunc, ICommandOptions? options = null)
    {
        return BatchProcessAsAsync(query, processFunc, options);
    }

    public Task<long> BatchProcessAsAsync<TResult>(RepositoryQueryDescriptor<T> query, Func<FindResults<TResult>, Task<bool>> processFunc, CommandOptionsDescriptor<T>? options = null) where TResult : class, new()
    {
        return BatchProcessAsAsync<TResult>(query.Configure(), processFunc, options?.Configure());
    }

    public virtual async Task<long> BatchProcessAsAsync<TResult>(IRepositoryQuery query, Func<FindResults<TResult>, Task<bool>> processFunc, ICommandOptions? options = null)
        where TResult : class, new()
    {
        if (processFunc == null)
            throw new ArgumentNullException(nameof(processFunc));

        if (!ElasticIndex.HasMultipleIndexes)
            await ElasticIndex.EnsureIndexAsync(null).AnyContext();

        options = ConfigureOptions(options?.As<T>());
        if (!options.ShouldUseSnapshotPaging())
            options.SearchAfterPaging();
        if (!options.HasPageLimit())
            options.PageLimit(500);

        long recordsProcessed = 0;
        var results = await FindAsAsync<TResult>(query, options).AnyContext();
        do
        {
            if (results.Hits.Count == 0)
                break;

            if (!await processFunc(results).AnyContext())
            {
                _logger.LogTrace("Aborted batch processing");
                break;
            }

            recordsProcessed += results.Documents.Count;
        } while (await results.NextPageAsync().AnyContext());

        if (options.GetConsistency() != Consistency.Eventual)
            await RefreshForConsistency(query, options).AnyContext();

        _logger.LogTrace("{Processed} records processed", recordsProcessed);
        return recordsProcessed;
    }

    /// <summary>
    /// Registers a field that must always be included when <see cref="RemoveAllAsync(IRepositoryQuery, ICommandOptions)"/>
    /// fetches documents for deletion. This ensures critical fields (needed for cache invalidation,
    /// notifications, or event handlers) are returned even when the caller has specified a restricted
    /// include set. <c>Id</c> and <c>CreatedUtc</c> (when applicable) are registered automatically.
    /// </summary>
    /// <remarks>
    /// To ensure a field is included in <em>all</em> source-filtered operations (not just removes),
    /// use <see cref="ElasticReadOnlyRepositoryBase{T}.AddRequiredField(string)"/> instead.
    /// </remarks>
    [Obsolete("Use AddRequiredField instead. This method will be removed in a future major version.")]
    protected void AddPropertyRequiredForRemove(string field)
    {
        AddRequiredField(field);
    }

    /// <inheritdoc cref="AddPropertyRequiredForRemove(string)"/>
    [Obsolete("Use AddRequiredField instead. This method will be removed in a future major version.")]
    protected void AddPropertyRequiredForRemove(Lazy<string> field)
    {
        AddRequiredField(field);
    }

    /// <inheritdoc cref="AddPropertyRequiredForRemove(string)"/>
    [Obsolete("Use AddRequiredField instead. This method will be removed in a future major version.")]
    protected void AddPropertyRequiredForRemove(Expression<Func<T, object?>> objectPath)
    {
        AddRequiredField(objectPath);
    }

    /// <inheritdoc cref="AddPropertyRequiredForRemove(string)"/>
    [Obsolete("Use AddRequiredField instead. This method will be removed in a future major version.")]
    protected void AddPropertyRequiredForRemove(params Expression<Func<T, object?>>[] objectPaths)
    {
        AddRequiredField(objectPaths);
    }

    protected virtual async Task OnCustomFieldsBeforeQuery(object sender, BeforeQueryEventArgs<T> args)
    {
        var tenantKey = GetTenantKey(args.Query);
        if (String.IsNullOrEmpty(tenantKey))
            return;

        var definitionRepo = ElasticIndex.Configuration.CustomFieldDefinitionRepository;
        if (definitionRepo is null)
        {
            _logger.LogWarning("CustomFieldDefinitionRepository is not configured for index {IndexName}; custom field query resolution skipped", ElasticIndex.Name);
            return;
        }

        var fieldMapping = await definitionRepo.GetFieldMappingAsync(EntityTypeName, tenantKey).AnyContext();
        var mapping = fieldMapping.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.GetIdxName(), StringComparer.OrdinalIgnoreCase);

        args.Options.QueryFieldResolver(mapping.ToHierarchicalFieldResolver("idx."));
    }

    protected virtual async Task OnCustomFieldsDocumentsChanging(object sender, DocumentsChangeEventArgs<T> args)
    {
        var definitionRepo = ElasticIndex.Configuration.CustomFieldDefinitionRepository;
        if (definitionRepo is null)
        {
            _logger.LogWarning("CustomFieldDefinitionRepository is not configured for index {IndexName}; clearing Idx for all documents", ElasticIndex.Name);

            foreach (var doc in args.Documents.Select(d => d.Value))
            {
                var idx = GetDocumentIdx(doc);
                idx.Clear();
            }

            return;
        }

        var tenantGroups = args.Documents.Select(d => d.Value).GroupBy(GetDocumentTenantKey).Where(g => g.Key != null).ToList();

        foreach (var tenant in tenantGroups)
        {
            var fieldDefinitions = await definitionRepo.GetFieldMappingAsync(EntityTypeName, tenant.Key!).AnyContext();

            foreach (var doc in tenant)
            {
                var idx = GetDocumentIdx(doc);
                idx.Clear();

                var customFields = GetDocumentCustomFields(doc);

                foreach (var customField in customFields)
                {
                    if (customField.Value is null)
                        continue;

                    if (!fieldDefinitions.TryGetValue(customField.Key, out var fieldDefinition))
                    {
                        fieldDefinition = await HandleUnmappedCustomField(doc, customField.Key, customField.Value, fieldDefinitions).AnyContext();
                        if (fieldDefinition == null)
                            continue;

                        fieldDefinitions[customField.Key] = fieldDefinition;
                    }

                    if (!ElasticIndex.CustomFieldTypes.TryGetValue(fieldDefinition.IndexType, out var fieldType))
                    {
                        _logger.LogWarning("Field type {IndexType} is not configured for this index {IndexName} for custom field {CustomFieldName}", fieldDefinition.IndexType, ElasticIndex.Name, customField.Key);
                        continue;
                    }

                    var result = await fieldType.ProcessValueAsync(doc, customField.Value, fieldDefinition).AnyContext();

                    SetDocumentCustomField(doc, customField.Key, result.Value);

                    object? idxValue = result.Idx ?? result.Value;
                    if (idxValue is not null)
                        idx[fieldDefinition.GetIdxName()] = idxValue;

                    if (result.IsCustomFieldDefinitionModified)
                        await definitionRepo.SaveAsync(fieldDefinition).AnyContext();
                }

                foreach (var alwaysProcessField in fieldDefinitions.Values.Where(f => f.ProcessMode == CustomFieldProcessMode.AlwaysProcess))
                {
                    if (!ElasticIndex.CustomFieldTypes.TryGetValue(alwaysProcessField.IndexType, out var fieldType))
                    {
                        _logger.LogWarning("Field type {IndexType} is not configured for this index {IndexName} for custom field {CustomFieldName}", alwaysProcessField.IndexType, ElasticIndex.Name, alwaysProcessField.Name);
                        continue;
                    }

                    object? value = GetDocumentCustomField(doc, alwaysProcessField.Name);
                    var result = await fieldType.ProcessValueAsync(doc, value, alwaysProcessField).AnyContext();

                    SetDocumentCustomField(doc, alwaysProcessField.Name, result.Value);

                    object? alwaysIdxValue = result.Idx ?? result.Value;
                    if (alwaysIdxValue is not null)
                        idx[alwaysProcessField.GetIdxName()] = alwaysIdxValue;

                    if (result.IsCustomFieldDefinitionModified)
                        await definitionRepo.SaveAsync(alwaysProcessField).AnyContext();
                }
            }
        }
    }

    protected virtual async Task<CustomFieldDefinition?> HandleUnmappedCustomField(T document, string name, object value, IDictionary<string, CustomFieldDefinition> existingFields)
    {
        if (!AutoCreateCustomFields)
            return null;

        var tenantKey = GetDocumentTenantKey(document);
        if (String.IsNullOrEmpty(tenantKey))
            return null;

        var definitionRepo = ElasticIndex.Configuration.CustomFieldDefinitionRepository;
        if (definitionRepo is null)
            throw new RepositoryException("Custom field definition repository is not configured.");

        return await definitionRepo.AddFieldAsync(EntityTypeName, tenantKey, name, StringFieldType.IndexType).AnyContext();
    }

    /// <summary>
    /// Gets the tenant key from the document for custom field tenant-scoped lookups.
    /// </summary>
    protected string? GetDocumentTenantKey(T document)
    {
        return document switch
        {
            IHaveCustomFields f => f.GetTenantKey(),
            IHaveVirtualCustomFields v => v.GetTenantKey(),
            _ => null
        };
    }

    /// <summary>
    /// Gets the custom fields dictionary from the document (<c>Data</c> for <see cref="IHaveCustomFields"/>,
    /// or the virtual fields collection for <see cref="IHaveVirtualCustomFields"/>).
    /// </summary>
    protected IDictionary<string, object?> GetDocumentCustomFields(T document)
    {
        ArgumentNullException.ThrowIfNull(document);

        return document switch
        {
            IHaveCustomFields f => f.Data,
            IHaveVirtualCustomFields v => v.GetCustomFields(),
            _ => throw new RepositoryException($"Document type {document.GetType().Name} does not implement IHaveCustomFields or IHaveVirtualCustomFields.")
        };
    }

    /// <summary>
    /// Sets a custom field value on the document. If <paramref name="value"/> is <c>null</c>,
    /// the field is removed via <see cref="RemoveDocumentCustomField"/> to prevent stale data.
    /// </summary>
    protected void SetDocumentCustomField(T document, string name, object? value)
    {
        ArgumentNullException.ThrowIfNull(document);

        if (value is null)
        {
            RemoveDocumentCustomField(document, name);
            return;
        }

        switch (document)
        {
            case IHaveCustomFields f:
                f.Data[name] = value;
                return;
            case IHaveVirtualCustomFields v:
                v.SetCustomField(name, value);
                return;
            default:
                throw new RepositoryException($"Document type {document.GetType().Name} does not implement IHaveCustomFields or IHaveVirtualCustomFields.");
        }
    }

    /// <summary>
    /// Removes a custom field from the document's data store. For <see cref="IHaveCustomFields"/>
    /// documents this removes the key from <c>Data</c>; for <see cref="IHaveVirtualCustomFields"/>
    /// it delegates to <see cref="IHaveVirtualCustomFields.RemoveCustomField"/>.
    /// </summary>
    protected void RemoveDocumentCustomField(T document, string name)
    {
        ArgumentNullException.ThrowIfNull(document);

        switch (document)
        {
            case IHaveCustomFields f:
                f.Data.Remove(name);
                return;
            case IHaveVirtualCustomFields v:
                v.RemoveCustomField(name);
                return;
            default:
                throw new RepositoryException($"Document type {document.GetType().Name} does not implement IHaveCustomFields or IHaveVirtualCustomFields.");
        }
    }

    /// <summary>
    /// Gets a single custom field value by name from the document.
    /// </summary>
    protected object? GetDocumentCustomField(T document, string name)
    {
        ArgumentNullException.ThrowIfNull(document);

        return document switch
        {
            IHaveCustomFields f => f.Data.GetValueOrDefault(name),
            IHaveVirtualCustomFields v => v.GetCustomField(name),
            _ => throw new RepositoryException($"Document type {document.GetType().Name} does not implement IHaveCustomFields or IHaveVirtualCustomFields.")
        };
    }

    /// <summary>
    /// Gets the indexed custom field values dictionary (<c>Idx</c>) from the document.
    /// </summary>
    protected IDictionary<string, object> GetDocumentIdx(T document)
    {
        ArgumentNullException.ThrowIfNull(document);

        return document switch
        {
            IHaveCustomFields f => f.Idx,
            IHaveVirtualCustomFields v => v.Idx,
            _ => throw new RepositoryException($"Document type {document.GetType().Name} does not implement IHaveCustomFields or IHaveVirtualCustomFields.")
        };
    }

    protected virtual string? GetTenantKey(IRepositoryQuery query)
    {
        return null;
    }

    public AsyncEvent<DocumentsEventArgs<T>> DocumentsAdding { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

    private static string GetDocumentJoinKey(T? document)
    {
        ArgumentNullException.ThrowIfNull(document);

        if (String.IsNullOrEmpty(document.Id))
            throw new ArgumentException("Document is missing an Id before save.", nameof(document));

        return document.Id;
    }

    private async Task OnDocumentsAddingAsync(IReadOnlyCollection<T> documents, ICommandOptions options)
    {
        if (HasDateTracking)
        {
            var timeProvider = ElasticIndex.Configuration.TimeProvider;

            foreach (var document in documents)
                SetDocumentDates(document, timeProvider);
        }
        else if (HasCreatedDate)
        {
            documents.OfType<IHaveCreatedDate>().SetCreatedDates(ElasticIndex.Configuration.TimeProvider);
        }

        if (DocumentsAdding is { HasHandlers: true })
            await DocumentsAdding.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this, options)).AnyContext();

        documents.EnsureIds(ElasticIndex.CreateDocumentId, ElasticIndex.Configuration.TimeProvider);

        await OnDocumentsChangingAsync(ChangeType.Added, documents, options).AnyContext();
    }

    public AsyncEvent<DocumentsEventArgs<T>> DocumentsAdded { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

    private async Task OnDocumentsAddedAsync(IReadOnlyCollection<T> documents, ICommandOptions options)
    {
        if (DocumentsAdded is { HasHandlers: true })
            await DocumentsAdded.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this, options)).AnyContext();

        var modifiedDocs = documents.Select(d => new ModifiedDocument<T>(d, null)).ToList();
        await OnDocumentsChangedAsync(ChangeType.Added, modifiedDocs, options).AnyContext();
        await SendNotificationsAsync(ChangeType.Added, modifiedDocs, options).AnyContext();
    }

    public AsyncEvent<ModifiedDocumentsEventArgs<T>> DocumentsSaving { get; } = new AsyncEvent<ModifiedDocumentsEventArgs<T>>();

    private async Task OnDocumentsSavingAsync(IReadOnlyCollection<T> documents, IReadOnlyCollection<T> originalDocuments, ICommandOptions options)
    {
        if (documents.Count == 0)
            return;

        if (HasDateTracking)
        {
            var timeProvider = ElasticIndex.Configuration.TimeProvider;

            foreach (var document in documents)
                SetDocumentDates(document, timeProvider);
        }

        documents.EnsureIds(ElasticIndex.CreateDocumentId, ElasticIndex.Configuration.TimeProvider);

        var modifiedDocs = originalDocuments.FullOuterJoin(
            documents, GetDocumentJoinKey, GetDocumentJoinKey,
            (original, modified, id) => new { Id = id, Original = original, Modified = modified }).Select(m =>
            {
                T current = m.Modified ?? m.Original ?? throw new DocumentException($"Full outer join produced no document for id {m.Id}.");
                return new ModifiedDocument<T>(current, m.Modified is not null ? m.Original : null);
            }).ToList();

        if (DocumentsSaving is { HasHandlers: true })
            await DocumentsSaving.InvokeAsync(this, new ModifiedDocumentsEventArgs<T>(modifiedDocs, this, options)).AnyContext();

        await OnDocumentsChangingAsync(ChangeType.Saved, modifiedDocs, options).AnyContext();
    }

    public AsyncEvent<ModifiedDocumentsEventArgs<T>> DocumentsSaved { get; } = new AsyncEvent<ModifiedDocumentsEventArgs<T>>();

    private async Task OnDocumentsSavedAsync(IReadOnlyCollection<T> documents, IReadOnlyCollection<T> originalDocuments, ICommandOptions options)
    {
        var modifiedDocs = originalDocuments.FullOuterJoin(
            documents, GetDocumentJoinKey, GetDocumentJoinKey,
            (original, modified, id) => new { Id = id, Original = original, Modified = modified }).Select(m =>
            {
                T current = m.Modified ?? m.Original ?? throw new DocumentException($"Full outer join produced no document for id {m.Id}.");
                return new ModifiedDocument<T>(current, m.Modified is not null ? m.Original : null);
            }).ToList();

        if (SupportsSoftDeletes && IsCacheEnabled)
        {
            string[] deletedIds = modifiedDocs.Where(d => ((ISupportSoftDeletes)d.Value).IsDeleted).Select(m => m.Value.Id).ToArray();
            if (deletedIds.Length > 0)
                await Cache.ListAddAsync("deleted", deletedIds, TimeSpan.FromSeconds(30)).AnyContext();

            string[] undeletedIds = modifiedDocs.Where(d => ((ISupportSoftDeletes)d.Value).IsDeleted == false).Select(m => m.Value.Id).ToArray();
            if (undeletedIds.Length > 0)
                await Cache.ListRemoveAsync("deleted", undeletedIds).AnyContext();
        }

        if (DocumentsSaved is { HasHandlers: true })
            await DocumentsSaved.InvokeAsync(this, new ModifiedDocumentsEventArgs<T>(modifiedDocs, this, options)).AnyContext();

        await OnDocumentsChangedAsync(ChangeType.Saved, modifiedDocs, options).AnyContext();
        await SendNotificationsAsync(ChangeType.Saved, modifiedDocs, options).AnyContext();
    }

    public AsyncEvent<DocumentsEventArgs<T>> DocumentsRemoving { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

    private async Task OnDocumentsRemovingAsync(IReadOnlyCollection<T> documents, ICommandOptions options)
    {
        if (DocumentsRemoving is { HasHandlers: true })
            await DocumentsRemoving.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this, options)).AnyContext();

        await OnDocumentsChangingAsync(ChangeType.Removed, documents, options).AnyContext();
    }

    public AsyncEvent<DocumentsEventArgs<T>> DocumentsRemoved { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

    private async Task OnDocumentsRemovedAsync(IReadOnlyCollection<T> documents, ICommandOptions options)
    {
        if (DocumentsRemoved is { HasHandlers: true })
            await DocumentsRemoved.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this, options)).AnyContext();

        await OnDocumentsChangedAsync(ChangeType.Removed, documents, options).AnyContext();
        await SendNotificationsAsync(ChangeType.Removed, documents, options).AnyContext();
    }

    public AsyncEvent<DocumentsChangeEventArgs<T>> DocumentsChanging { get; } = new AsyncEvent<DocumentsChangeEventArgs<T>>();

    private Task OnDocumentsChangingAsync(ChangeType changeType, IReadOnlyCollection<T> documents, ICommandOptions options)
    {
        if (DocumentsChanging == null || !DocumentsChanging.HasHandlers)
            return Task.CompletedTask;

        return OnDocumentsChangingAsync(changeType, documents.Select(d => new ModifiedDocument<T>(d, null)).ToList(), options);
    }

    private Task OnDocumentsChangingAsync(ChangeType changeType, IReadOnlyCollection<ModifiedDocument<T>> documents, ICommandOptions options)
    {
        if (DocumentsChanging == null || !DocumentsChanging.HasHandlers)
            return Task.CompletedTask;

        return DocumentsChanging.InvokeAsync(this, new DocumentsChangeEventArgs<T>(changeType, documents, this, options));
    }

    public AsyncEvent<DocumentsChangeEventArgs<T>> DocumentsChanged { get; } = new AsyncEvent<DocumentsChangeEventArgs<T>>();

    private Task OnDocumentsChangedAsync(ChangeType changeType, IReadOnlyCollection<T> documents, ICommandOptions options)
    {
        return OnDocumentsChangedAsync(changeType, documents.Select(d => new ModifiedDocument<T>(d, null)).ToList(), options);
    }

    private async Task OnDocumentsChangedAsync(ChangeType changeType, IReadOnlyCollection<ModifiedDocument<T>> documents, ICommandOptions options)
    {
        await InvalidateCacheAsync(documents, changeType).AnyContext();

        if (DocumentsChanged == null || !DocumentsChanged.HasHandlers)
            return;

        await DocumentsChanged.InvokeAsync(this, new DocumentsChangeEventArgs<T>(changeType, documents, this, options)).AnyContext();
    }

    private async Task<IReadOnlyCollection<T>> GetOriginalDocumentsAsync(Ids ids, ICommandOptions options)
    {
        if (!options.GetOriginalsEnabled(OriginalsEnabled) || ids.Count is 0)
            return EmptyList;

        var originals = options.GetOriginals<T>().ToList();
        foreach (var original in originals)
            ids.RemoveAll(id => id.Value == original.Id);

        originals.AddRange(await GetByIdsAsync(ids, o => options.Clone().ReadCache().As<T>()).AnyContext());

        return originals.AsReadOnly();
    }

    private async Task<BulkResult> IndexDocumentsAsync(IReadOnlyCollection<T> documents, bool isCreateOperation, ICommandOptions options)
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

        if (documents.Count == 1)
            return await IndexSingleDocumentAsync(documents.Single(), isCreateOperation, options).AnyContext();

        return await IndexDocumentsBulkAsync(documents, isCreateOperation, options).AnyContext();
    }

    private async Task<BulkResult> IndexSingleDocumentAsync(T document, bool isCreateOperation, ICommandOptions options)
    {
        var response = await _client.IndexAsync(document, i =>
        {
            i.OpType(isCreateOperation ? OpType.Create : OpType.Index);
            if (DefaultPipeline is not null)
                i.Pipeline(DefaultPipeline);
            i.Refresh(options.GetRefreshMode(DefaultConsistency));

            if (GetParentIdFunc is not null)
                i.Routing(GetParentIdFunc(document));

            i.Index(ElasticIndex.GetIndex(document));

            if (HasVersion && !isCreateOperation && !options.ShouldSkipVersionCheck())
            {
                var elasticVersion = ((IVersioned)document).GetElasticVersion();
                i.IfPrimaryTerm(elasticVersion.PrimaryTerm);
                i.IfSeqNo(elasticVersion.SequenceNumber);
            }
        }).AnyContext();
        _logger.LogRequest(response, options.GetQueryLogLevel());

        if (!response.IsValidResponse)
        {
            string message = $"Error {(isCreateOperation ? "adding" : "saving")} document";
            if (response.ElasticsearchServerError?.Status is 409)
                throw isCreateOperation
                    ? new DuplicateDocumentException(response.GetErrorMessage(message), response.OriginalException())
                    : new VersionConflictDocumentException(response.GetErrorMessage(message), response.OriginalException());

            throw new DocumentException(response.GetErrorMessage(message), response.OriginalException());
        }

        if (HasVersion)
        {
            var versionDoc = (IVersioned)document;
            var elasticVersion = response.GetElasticVersion();
            versionDoc.Version = elasticVersion;
        }

        return BulkResult.Empty;
    }

    private async Task<BulkResult> IndexDocumentsBulkAsync(IReadOnlyCollection<T> documents, bool isCreateOperation, ICommandOptions options)
    {
        IReadOnlyCollection<T> docsToIndex = documents;
        var allSuccessfulIds = new HashSet<string>();
        var allConflictIds = new HashSet<string>();
        var allFatalIds = new HashSet<string>();
        BulkResult result = BulkResult.Empty;

        for (int attempt = 0; attempt < 4; attempt++)
        {
            var bulkRequest = new BulkRequest();
            var list = docsToIndex.Select(d =>
            {
                var routing = GetParentIdFunc?.Invoke(d);
                var index = ElasticIndex.GetIndex(d);

                if (isCreateOperation)
                {
                    var createOperation = new BulkCreateOperation<T>(d) { Pipeline = DefaultPipeline };
                    if (routing != null)
                        createOperation.Routing = routing;
                    createOperation.Index = index;
                    return (IBulkOperation)createOperation;
                }
                else
                {
                    var indexOperation = new BulkIndexOperation<T>(d) { Pipeline = DefaultPipeline };
                    if (routing != null)
                        indexOperation.Routing = routing;
                    indexOperation.Index = index;
                    if (HasVersion && !options.ShouldSkipVersionCheck())
                    {
                        var elasticVersion = ((IVersioned)d).GetElasticVersion();
                        indexOperation.IfSequenceNumber = elasticVersion.SequenceNumber;
                        indexOperation.IfPrimaryTerm = elasticVersion.PrimaryTerm;
                    }
                    return (IBulkOperation)indexOperation;
                }
            }).ToList();
            bulkRequest.Operations = list;
            bulkRequest.Refresh = options.GetRefreshMode(DefaultConsistency);

            var response = await _client.BulkAsync(bulkRequest).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());

            result = BulkResult.From(response);

            if (result.HasTransportError)
            {
                return new BulkResult
                {
                    SuccessfulIds = allSuccessfulIds,
                    ConflictIds = allConflictIds,
                    FatalIds = allFatalIds,
                    TransportError = result.TransportError,
                    TransportException = result.TransportException
                };
            }

            if (HasVersion)
            {
                var documentsById = new Dictionary<string, T>();
                foreach (var d in docsToIndex.Where(d => !String.IsNullOrEmpty(d.Id)))
                    documentsById.TryAdd(d.Id, d);
                foreach (var hit in response.Items)
                {
                    if (!hit.IsValid)
                        continue;

                    if (!documentsById.TryGetValue(hit.Id!, out var document))
                        continue;

                    var versionDoc = (IVersioned)document;
                    var elasticVersion = hit.GetElasticVersion();
                    versionDoc.Version = elasticVersion;
                }
            }

            allSuccessfulIds.UnionWith(result.SuccessfulIds);
            allConflictIds.UnionWith(result.ConflictIds);
            allFatalIds.UnionWith(result.FatalIds);

            if (!result.HasRetryableErrors || attempt >= 3)
                break;

            docsToIndex = docsToIndex.Where(d => result.RetryableIds.Contains(d.Id)).ToList();
            var delay = TimeSpan.FromSeconds(Math.Pow(2, attempt));
            await ElasticIndex.Configuration.TimeProvider.SafeDelay(delay, DisposedCancellationToken).AnyContext();

            if (DisposedCancellationToken.IsCancellationRequested)
                break;
        }

        return new BulkResult
        {
            SuccessfulIds = allSuccessfulIds,
            ConflictIds = allConflictIds,
            RetryableIds = result.RetryableIds,
            FatalIds = allFatalIds
        };
    }

    private async Task<(long Count, List<string> Ids)> HandleBulkPatchErrorsAsync(BulkResponse bulkResponse, FindResults<T> results, string patchType, IPatchOperation operation, ICommandOptions options)
    {
        var result = BulkResult.From(bulkResponse);
        if (result.HasTransportError || result.FatalIds.Count > 0 || result.RetryableIds.Count > 0)
        {
            _logger.LogErrorRequest(bulkResponse, "Error occurred while bulk updating");
            ThrowForBulkErrors(result, operationLabel: "patching");
        }

        var retriedIds = new List<string>();
        if (result.HasConflicts)
        {
            _logger.LogRequest(bulkResponse, options.GetQueryLogLevel());
            _logger.LogInformation("Bulk {PatchType} had {ConflictCount} version conflicts, re-fetching and retrying", patchType, result.ConflictIds.Count);

            foreach (var hit in results.Hits)
            {
                if (hit.Id is null || !result.ConflictIds.Contains(hit.Id))
                    continue;

                if (await PatchAsync(new Id(hit.Id, hit.Routing), operation, options).AnyContext())
                    retriedIds.Add(hit.Id);
            }
        }

        return (retriedIds.Count, retriedIds);
    }

    private static void ThrowForBulkErrors(BulkResult result, bool isCreateOperation = false, string? operationLabel = null)
    {
        if (result.IsSuccess)
            return;

        if (result.HasTransportError)
        {
            if (result.TransportException is not null)
                throw new DocumentException(result.TransportError ?? "Unknown transport error", result.TransportException);

            throw new DocumentException(result.TransportError ?? "Unknown transport error");
        }

        string label = operationLabel ?? (isCreateOperation ? "adding" : "saving");
        int totalErrors = result.ConflictIds.Count + result.RetryableIds.Count + result.FatalIds.Count;

        if (result.HasConflicts)
        {
            if (isCreateOperation)
                throw new DuplicateDocumentException($"Error {label} documents: {result.ConflictIds.Count} duplicates ({totalErrors} total errors)");
            throw new VersionConflictDocumentException($"Error {label} documents: {result.ConflictIds.Count} conflicts ({totalErrors} total errors)");
        }

        throw new DocumentException($"Error {label} documents: {totalErrors} failures");
    }

    /// <summary>
    /// Gets or sets whether entity change notifications are published to the message bus.
    /// When enabled, <see cref="Foundatio.Repositories.Models.EntityChanged"/> messages are published
    /// after add, save, and remove operations. Defaults to <c>true</c> if a message bus is configured.
    /// </summary>
    protected bool NotificationsEnabled { get; set; }

    /// <summary>
    /// Gets or sets whether original document state is tracked during save operations.
    /// When enabled, the original document is preserved before modifications, allowing
    /// change detection (e.g., for soft delete transitions). Defaults to <c>false</c>.
    /// </summary>
    protected bool OriginalsEnabled { get; set; }

    /// <summary>
    /// Gets or sets whether notifications for multiple documents are batched together.
    /// When enabled, bulk operations may consolidate notifications. Defaults to <c>false</c>.
    /// </summary>
    public bool BatchNotifications { get; set; }

    private TimeSpan? _notificationDeliveryDelay;

    /// <summary>
    /// Gets or sets the delivery delay for entity change notifications.
    /// When set, notifications are delayed by the specified duration before being delivered
    /// to subscribers. This can be useful to allow Elasticsearch indexing to complete before
    /// consumers read the updated documents. Defaults to <c>null</c> (immediate delivery).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Warning:</b> Only set a delay if your message bus implementation supports delayed delivery.
    /// Message buses that do not support delayed delivery may silently drop messages, resulting in
    /// message loss. The in-memory message bus supports delayed delivery, but other implementations
    /// may not.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is negative.</exception>
    protected TimeSpan? NotificationDeliveryDelay
    {
        get => _notificationDeliveryDelay;
        set
        {
            if (value.HasValue && value.Value < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(value), "Delivery delay cannot be negative.");
            _notificationDeliveryDelay = value;
        }
    }

    private Task SendNotificationsAsync(ChangeType changeType, IReadOnlyCollection<T> documents, ICommandOptions options)
    {
        return SendNotificationsAsync(changeType, documents.Select(d => new ModifiedDocument<T>(d, null)).ToList(), options);
    }

    protected virtual Task SendQueryNotificationsAsync(ChangeType changeType, IRepositoryQuery query, ICommandOptions options)
    {
        if (!NotificationsEnabled || !options.ShouldNotify())
            return Task.CompletedTask;

        var delay = NotificationDeliveryDelay;
        var ids = query.GetIds();
        if (ids.Count > 0)
        {
            var tasks = new List<Task>(ids.Count);
            foreach (string id in ids)
            {
                tasks.Add(PublishMessageAsync(new EntityChanged
                {
                    ChangeType = changeType,
                    Id = id,
                    Type = EntityTypeName
                }, delay));
            }

            return Task.WhenAll(tasks);
        }

        return PublishMessageAsync(new EntityChanged
        {
            ChangeType = changeType,
            Type = EntityTypeName
        }, delay);
    }

    protected virtual Task SendNotificationsAsync(ChangeType changeType, IReadOnlyCollection<ModifiedDocument<T>> documents, ICommandOptions options)
    {
        if (!NotificationsEnabled || !options.ShouldNotify())
            return Task.CompletedTask;

        var delay = NotificationDeliveryDelay;
        if (documents.Count == 0)
            return PublishChangeTypeMessageAsync(changeType, (T?)null, delay);

        var tasks = new List<Task>(documents.Count);
        if (BatchNotifications && documents.Count > 1)
        {
            // TODO: This needs to support batch notifications
            if (!SupportsSoftDeletes || changeType != ChangeType.Saved)
            {
                foreach (var doc in documents.Select(d => d.Value))
                    tasks.Add(PublishChangeTypeMessageAsync(changeType, doc, delay));

                return Task.WhenAll(tasks);
            }

            bool allDeleted = documents.All(d => d.Original != null && ((ISupportSoftDeletes)d.Original).IsDeleted == false && ((ISupportSoftDeletes)d.Value).IsDeleted);
            foreach (var doc in documents.Select(d => d.Value))
                tasks.Add(PublishChangeTypeMessageAsync(allDeleted ? ChangeType.Removed : changeType, doc, delay));

            return Task.WhenAll(tasks);
        }

        if (!SupportsSoftDeletes)
        {
            foreach (var d in documents)
                tasks.Add(PublishChangeTypeMessageAsync(changeType, d.Value, delay));

            return Task.WhenAll(tasks);
        }

        foreach (var d in documents)
        {
            var docChangeType = changeType;
            if (d.Original != null)
            {
                var document = (ISupportSoftDeletes)d.Value;
                var original = (ISupportSoftDeletes)d.Original;
                if (original.IsDeleted == false && document.IsDeleted)
                    docChangeType = ChangeType.Removed;
            }

            tasks.Add(PublishChangeTypeMessageAsync(docChangeType, d.Value, delay));
        }

        return Task.WhenAll(tasks);
    }

    protected virtual Task PublishChangeTypeMessageAsync(ChangeType changeType, T? document, TimeSpan? delay)
    {
        return PublishChangeTypeMessageAsync(changeType, document, null, delay);
    }

    protected virtual Task PublishChangeTypeMessageAsync(ChangeType changeType, T? document, IDictionary<string, object?>? data = null, TimeSpan? delay = null)
    {
        return PublishChangeTypeMessageAsync(changeType, document?.Id, data, delay);
    }

    protected virtual Task PublishChangeTypeMessageAsync(ChangeType changeType, string? id, IDictionary<string, object?>? data = null, TimeSpan? delay = null)
    {
        if (!NotificationsEnabled)
            return Task.CompletedTask;

        return PublishMessageAsync(new EntityChanged
        {
            ChangeType = changeType,
            Id = id,
            Type = EntityTypeName,
            Data = new DataDictionary(data)
        }, delay);
    }

    protected virtual async Task PublishMessageAsync(EntityChanged message, TimeSpan? delay = null)
    {
        if (!NotificationsEnabled || _messagePublisher == null)
            return;

        if (BeforePublishEntityChanged is { HasHandlers: true })
        {
            var eventArgs = new BeforePublishEntityChangedEventArgs<T>(this, message);
            await BeforePublishEntityChanged.InvokeAsync(this, eventArgs).AnyContext();
            if (eventArgs.Cancel)
                return;
        }

        await _messagePublisher.PublishAsync(message, new MessageOptions { DeliveryDelay = delay }).AnyContext();
    }

    public AsyncEvent<BeforePublishEntityChangedEventArgs<T>> BeforePublishEntityChanged { get; } = new AsyncEvent<BeforePublishEntityChangedEventArgs<T>>();

    /// <summary>
    /// Returns the Elasticsearch dot-path for the updated timestamp field (e.g., <c>"updatedUtc"</c>
    /// or <c>"metaData.dateUpdatedUtc"</c>). Called only when <see cref="HasDateTracking"/> is <c>true</c>.
    /// </summary>
    /// <exception cref="RepositoryException">
    /// Thrown when <see cref="HasDateTracking"/> is <c>true</c> but no field path is available.
    /// Override this method when using custom date fields.
    /// </exception>
    protected virtual string GetUpdatedUtcFieldPath()
    {
        if (HasDates)
            return _updatedUtcField!.Value;

        throw new RepositoryException(
            $"{GetType().Name} has HasDateTracking=true but does not implement IHaveDates. Override GetUpdatedUtcFieldPath() to return the Elasticsearch field path for your updated timestamp.");
    }

    /// <summary>
    /// Sets date properties on the document for Add, Save, and ActionPatch/JsonPatch-bulk operations.
    /// Override to handle custom date fields (e.g., <c>IHaveDateMetaData.MetaData.DateUpdatedUtc</c>).
    /// </summary>
    protected virtual void SetDocumentDates(T document, TimeProvider timeProvider)
    {
        if (document is IHaveDates datesDoc)
        {
            var utcNow = timeProvider.GetUtcNow().UtcDateTime;

            if (datesDoc.CreatedUtc == DateTime.MinValue || datesDoc.CreatedUtc > utcNow)
                datesDoc.CreatedUtc = utcNow;

            datesDoc.UpdatedUtc = utcNow;
        }
        else if (document is IHaveCreatedDate createdDoc)
        {
            var utcNow = timeProvider.GetUtcNow().UtcDateTime;

            if (createdDoc.CreatedUtc == DateTime.MinValue || createdDoc.CreatedUtc > utcNow)
                createdDoc.CreatedUtc = utcNow;
        }
    }

    /// <summary>
    /// Injects the updated timestamp into a <see cref="ScriptPatch"/> by appending a Painless assignment
    /// and adding the timestamp as a script parameter. Skips injection if the caller already provided
    /// the parameter (logged at Debug level).
    /// </summary>
    protected virtual ScriptPatch ApplyDateTracking(ScriptPatch script)
    {
        if (!HasDateTracking)
            return script;

        var fieldPath = GetUpdatedUtcFieldPath();
        var lastDotIndex = fieldPath.LastIndexOf('.');
        var paramKey = lastDotIndex >= 0 ? fieldPath[(lastDotIndex + 1)..] : fieldPath;

        if (script.Params is { } existingParams && existingParams.ContainsKey(paramKey))
        {
            _logger.LogDebug("Skipping automatic {FieldPath} injection; caller provided {ParamKey}", fieldPath, paramKey);
            return script;
        }

        var scriptSuffix = BuildNestedAssignmentScript(fieldPath, paramKey);
        var patchParams = script.Params is { } existing
            ? new Dictionary<string, object>(existing)
            : new Dictionary<string, object>();
        patchParams[paramKey] = ElasticIndex.Configuration.TimeProvider.GetUtcNow().UtcDateTime;

        // Date tracking is appended AFTER the user's script. This is safe for noop detection
        // because ES evaluates ctx.op after the entire script executes — if the user set
        // ctx.op = 'none', the write is skipped regardless of source modifications.
        return new ScriptPatch($"{script.Script} {scriptSuffix}") { Params = patchParams };
    }

    /// <summary>
    /// Injects the updated timestamp into a <see cref="PartialPatch"/> by adding the field to the
    /// serialized document. Skips injection if the caller already provided the field (logged at Debug level).
    /// </summary>
    protected virtual PartialPatch ApplyDateTracking(PartialPatch patch)
    {
        if (!HasDateTracking)
            return patch;

        var fieldPath = GetUpdatedUtcFieldPath();
        var serialized = _client.ElasticsearchClientSettings.SourceSerializer.SerializeToString(patch.Document);
        var partialDoc = JsonNode.Parse(serialized);

        if (partialDoc is not JsonObject partialObject)
            return patch;

        if (GetNestedJsonNode(partialObject, fieldPath) is not null)
        {
            _logger.LogDebug("Skipping automatic {FieldPath} injection; caller already provided it", fieldPath);
            return patch;
        }

        SetNestedJsonNodeValue(partialObject, fieldPath,
            JsonValue.Create(ElasticIndex.Configuration.TimeProvider.GetUtcNow().UtcDateTime));

        return new PartialPatch(ToDictionary(partialObject));
    }

    /// <summary>
    /// Sets the updated timestamp on a <see cref="JsonNode"/> document (used by single-doc JsonPatch).
    /// Supports nested dot-path fields.
    /// </summary>
    protected virtual void ApplyDateTracking(JsonNode target)
    {
        if (!HasDateTracking)
            return;

        var fieldPath = GetUpdatedUtcFieldPath();

        SetNestedJsonNodeValue(target, fieldPath,
            JsonValue.Create(ElasticIndex.Configuration.TimeProvider.GetUtcNow().UtcDateTime));
    }

    private static string BuildNestedAssignmentScript(string fieldPath, string paramKey)
    {
        var dotIndex = fieldPath.IndexOf('.');
        if (dotIndex < 0)
            return $"ctx._source.{fieldPath} = params.{paramKey};";

        var sb = new System.Text.StringBuilder();
        var prefix = "ctx._source";
        var remaining = fieldPath.AsSpan();

        while (true)
        {
            dotIndex = remaining.IndexOf('.');
            if (dotIndex < 0)
                break;

            var segment = remaining[..dotIndex];
            sb.Append("if (").Append(prefix).Append('.').Append(segment)
              .Append(" == null) { ").Append(prefix).Append('.').Append(segment)
              .Append(" = [:]; } ");
            prefix = $"{prefix}.{segment}";
            remaining = remaining[(dotIndex + 1)..];
        }

        sb.Append(prefix).Append('.').Append(remaining)
          .Append(" = params.").Append(paramKey).Append(';');
        return sb.ToString();
    }

    private static JsonNode? GetNestedJsonNode(JsonNode? node, string dotPath)
    {
        var remaining = dotPath.AsSpan();

        while (remaining.Length > 0)
        {
            var dotIndex = remaining.IndexOf('.');
            var segment = dotIndex >= 0 ? remaining[..dotIndex] : remaining;

            if (node is not JsonObject obj)
                return null;

            node = obj[segment.ToString()];
            if (node is null)
                return null;

            remaining = dotIndex >= 0 ? remaining[(dotIndex + 1)..] : ReadOnlySpan<char>.Empty;
        }

        return node;
    }

    private static void SetNestedJsonNodeValue(JsonNode node, string dotPath, JsonNode value)
    {
        var remaining = dotPath.AsSpan();

        while (true)
        {
            var dotIndex = remaining.IndexOf('.');

            if (dotIndex < 0)
            {
                if (node is JsonObject leaf)
                    leaf[remaining.ToString()] = value;

                return;
            }

            var segment = remaining[..dotIndex].ToString();

            if (node is not JsonObject current)
                return;

            var next = current[segment];

            if (next is not JsonObject)
            {
                next = new JsonObject();
                current[segment] = next;
            }

            node = next;
            remaining = remaining[(dotIndex + 1)..];
        }
    }

    private static Dictionary<string, object> ToDictionary(JsonObject obj)
    {
        var dict = new Dictionary<string, object>(obj.Count);

        foreach (var property in obj)
            dict[property.Key] = ToValue(property.Value)!;

        return dict;
    }

    private static object? ToValue(JsonNode? node)
    {
        return node switch
        {
            JsonObject nested => ToDictionary(nested),
            JsonArray array => array.Select(ToValue).ToList(),
            JsonValue val => val.Deserialize<object>(),
            null => null,
            _ => node.Deserialize<object>()
        };
    }
}
