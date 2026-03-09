using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Elasticsearch.Net;
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
using Nest;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Elasticsearch;

public abstract class ElasticRepositoryBase<T> : ElasticReadOnlyRepositoryBase<T>, ISearchableRepository<T> where T : class, IIdentity, new()
{
    protected readonly IMessagePublisher _messagePublisher;
    private readonly List<Lazy<Field>> _propertiesRequiredForRemove = new();

    protected ElasticRepositoryBase(IIndex index) : base(index)
    {
        _messagePublisher = index.Configuration.MessageBus;
        NotificationsEnabled = _messagePublisher != null;

        AddPropertyRequiredForRemove(_idField);
        if (HasCreatedDate)
            AddPropertyRequiredForRemove(e => ((IHaveCreatedDate)e).CreatedUtc);

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
    protected string DefaultPipeline { get; set; } = null;
    protected bool AutoCreateCustomFields { get; set; } = false;

    #region IRepository

    public Task<T> AddAsync(T document, CommandOptionsDescriptor<T> options)
    {
        return AddAsync(document, options.Configure());
    }

    public async Task<T> AddAsync(T document, ICommandOptions options = null)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));

        await AddAsync([document], options).AnyContext();
        return document;
    }

    public Task AddAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options)
    {
        return AddAsync(documents, options.Configure());
    }

    public virtual async Task AddAsync(IEnumerable<T> documents, ICommandOptions options = null)
    {
        var docs = documents?.ToList();
        if (docs == null || docs.Any(d => d == null))
            throw new ArgumentNullException(nameof(documents));

        if (docs.Count == 0)
            return;

        options = ConfigureOptions(options.As<T>());

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
        return SaveAsync(document, options.Configure());
    }

    public async Task<T> SaveAsync(T document, ICommandOptions options = null)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));

        await SaveAsync(new[] { document }, options).AnyContext();
        return document;
    }

    public Task SaveAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options)
    {
        return SaveAsync(documents, options.Configure());
    }

    public virtual async Task SaveAsync(IEnumerable<T> documents, ICommandOptions options = null)
    {
        var docs = documents?.ToList();
        if (docs == null || docs.Any(d => d == null))
            throw new ArgumentNullException(nameof(documents));

        if (docs.Count == 0)
            return;

        string[] ids = docs.Where(d => !String.IsNullOrEmpty(d.Id)).Select(d => d.Id).ToArray();
        if (ids.Length < docs.Count)
            throw new ArgumentException("Id must be set when calling Save.");

        options = ConfigureOptions(options.As<T>());

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
        return PatchAsync(id, operation, options.Configure());
    }

    public virtual async Task<bool> PatchAsync(Id id, IPatchOperation operation, ICommandOptions options = null)
    {
        if (String.IsNullOrEmpty(id.Value))
            throw new ArgumentNullException(nameof(id));

        if (operation == null)
            throw new ArgumentNullException(nameof(operation));

        if (operation is JsonPatch { Patch: null or { Operations.Count: 0 } })
            return false;

        if (operation is ActionPatch<T> { Actions: null or { Count: 0 } })
            return false;

        await ElasticIndex.EnsureIndexAsync(id).AnyContext();

        options = ConfigureOptions(options.As<T>());

        if (operation is ScriptPatch scriptPatchOp)
            operation = ApplyDateTracking(scriptPatchOp);
        else if (operation is PartialPatch partialPatchOp)
            operation = ApplyDateTracking(partialPatchOp);

        bool modified = true;

        if (operation is ScriptPatch scriptOperation)
        {
            // ScriptPatch: noop detection requires the script to explicitly set ctx.op = 'none'.
            // Simply reassigning the same value is treated as a modification by Elasticsearch.
            // TODO: Figure out how to specify a pipeline here.
            var request = new UpdateRequest<T, T>(ElasticIndex.GetIndex(id), id.Value)
            {
                Script = new InlineScript(scriptOperation.Script) { Params = scriptOperation.Params },
                RetryOnConflict = options.GetRetryCount(),
                Refresh = options.GetRefreshMode(DefaultConsistency)
            };
            if (id.Routing != null)
                request.Routing = id.Routing;

            var response = await _client.UpdateAsync(request).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());

            if (!response.IsValid)
            {
                if (response.ApiCall is { HttpStatusCode: 404 })
                    throw new DocumentNotFoundException(id);

                if (response.ApiCall is { HttpStatusCode: 409 })
                    throw new VersionConflictDocumentException(
                        response.GetErrorMessage($"Error patching document {ElasticIndex.GetIndex(id)}/{id.Value}"),
                        response.OriginalException);

                throw new DocumentException(
                    response.GetErrorMessage($"Error patching document {ElasticIndex.GetIndex(id)}/{id.Value}"),
                    response.OriginalException);
            }

            modified = response.Result is not Result.Noop;
        }
        else if (operation is PartialPatch partialOperation)
        {
            // PartialPatch: Elasticsearch's detect_noop (enabled by default) reports noop when no
            // field values change. However, ApplyDateTracking injects UpdatedUtc for IHaveDates
            // models, which typically prevents noop detection since the timestamp always changes.
            // TODO: Figure out how to specify a pipeline here.
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

            if (!response.IsValid)
            {
                if (response.ApiCall is { HttpStatusCode: 404 })
                    throw new DocumentNotFoundException(id);

                if (response.ApiCall is { HttpStatusCode: 409 })
                    throw new VersionConflictDocumentException(
                        response.GetErrorMessage($"Error patching document {ElasticIndex.GetIndex(id)}/{id.Value}"),
                        response.OriginalException);

                throw new DocumentException(
                    response.GetErrorMessage($"Error patching document {ElasticIndex.GetIndex(id)}/{id.Value}"),
                    response.OriginalException);
            }

            modified = response.Result is not Result.Noop;
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
                var response = await _client.LowLevel.GetAsync<GetResponse<IDictionary<string, object>>>(ElasticIndex.GetIndex(id), id.Value, new GetRequestParameters { Routing = id.Routing }, ct).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());
                if (!response.IsValid)
                {
                    if (!response.Found)
                        throw new DocumentNotFoundException(id);

                    throw new DocumentException(
                        response.GetErrorMessage($"Error patching document {ElasticIndex.GetIndex(id)}/{id.Value}"),
                        response.OriginalException);
                }

                var jObject = JObject.FromObject(response.Source);
                var target = (JToken)jObject;
                new JsonPatcher().Patch(ref target, jsonOperation.Patch);

                ApplyDateTracking(target);

                var indexParameters = new IndexRequestParameters
                {
                    Pipeline = DefaultPipeline,
                    Refresh = options.GetRefreshMode(DefaultConsistency)
                };
                if (id.Routing != null)
                    indexParameters.Routing = id.Routing;

                if (HasVersion && !options.ShouldSkipVersionCheck())
                {
                    indexParameters.IfSequenceNumber = response.SequenceNumber;
                    indexParameters.IfPrimaryTerm = response.PrimaryTerm;
                }

                var updateResponse = await _client.LowLevel.IndexAsync<VoidResponse>(ElasticIndex.GetIndex(id), id.Value, PostData.String(target.ToString()), indexParameters, ct).AnyContext();
                _logger.LogRequest(updateResponse, options.GetQueryLogLevel());

                if (!updateResponse.Success)
                {
                    if (updateResponse.HttpStatusCode is 409)
                        throw new VersionConflictDocumentException(updateResponse.GetErrorMessage("Error saving document"), updateResponse.OriginalException);

                    throw new DocumentException(updateResponse.GetErrorMessage("Error saving document"), updateResponse.OriginalException);
                }
            });
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

            await policy.ExecuteAsync(async ct =>
            {
                var request = new GetRequest(ElasticIndex.GetIndex(id), id.Value);
                if (id.Routing != null)
                    request.Routing = id.Routing;
                var response = await _client.GetAsync<T>(request, ct).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());

                if (!response.IsValid)
                {
                    if (!response.Found)
                        throw new DocumentNotFoundException(id);

                    throw new DocumentException(
                        response.GetErrorMessage($"Error patching document {ElasticIndex.GetIndex(id)}/{id.Value}"),
                        response.OriginalException);
                }

                if (response.Source is IVersioned versionedDoc && response.PrimaryTerm.HasValue)
                    versionedDoc.Version = response.GetElasticVersion();

                // Snapshot before applying actions so we can detect noop
                var beforeJson = _client.ConnectionSettings.SourceSerializer.SerializeToString(response.Source);

                foreach (var action in actionPatch.Actions)
                    action?.Invoke(response.Source);

                var afterJson = _client.ConnectionSettings.SourceSerializer.SerializeToString(response.Source);
                if (String.Equals(beforeJson, afterJson, StringComparison.Ordinal))
                {
                    modified = false;
                    return;
                }

                if (HasDateTracking)
                    SetDocumentDates(response.Source, ElasticIndex.Configuration.TimeProvider);

                await IndexDocumentsAsync([response.Source], false, options).AnyContext();
            });
        }
        else
        {
            throw new ArgumentException("Unknown operation type", nameof(operation));
        }

        // TODO: Find a good way to invalidate cache and send changed notification
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
        return PatchAsync(ids, operation, options.Configure());
    }

    public virtual async Task<long> PatchAsync(Ids ids, IPatchOperation operation, ICommandOptions options = null)
    {
        ArgumentNullException.ThrowIfNull(ids);
        ArgumentNullException.ThrowIfNull(operation);

        if (ids is { Count: 0 })
            return 0;

        options = ConfigureOptions(options.As<T>());

        if (ids.Count == 1)
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

        var bulkResponse = await _client.BulkAsync(b =>
        {
            b.Refresh(options.GetRefreshMode(DefaultConsistency));
            foreach (var id in ids)
            {
                b.Pipeline(DefaultPipeline);

                if (operation is ScriptPatch scriptOperation)
                    b.Update<T>(u =>
                    {
                        u.Id(id.Value)
                          .Index(ElasticIndex.GetIndex(id))
                          .Script(s => s.Source(scriptOperation.Script).Params(scriptOperation.Params))
                          .RetriesOnConflict(options.GetRetryCount());

                        if (id.Routing != null)
                            u.Routing(id.Routing);

                        return u;
                    });
                else if (operation is PartialPatch partialOperation)
                    b.Update<T, object>(u =>
                    {
                        u.Id(id.Value)
                            .Index(ElasticIndex.GetIndex(id))
                            .Doc(partialOperation.Document)
                            .RetriesOnConflict(options.GetRetryCount());

                        if (id.Routing != null)
                            u.Routing(id.Routing);

                        return u;
                    });
            }

            return b;
        }).AnyContext();
        _logger.LogRequest(bulkResponse, options.GetQueryLogLevel());

        var result = BulkResult.From(bulkResponse);

        var modifiedIds = result.IsSuccess
            ? ids.Where(id => !result.NoopIds.Contains(id.Value)).ToList()
            : ids.Where(id => result.SuccessfulIds.Contains(id.Value) && !result.NoopIds.Contains(id.Value)).ToList();
        if (modifiedIds.Count > 0)
        {
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
        return RemoveAsync(id, options.Configure());
    }

    public Task RemoveAsync(Id id, ICommandOptions options = null)
    {
        if (String.IsNullOrEmpty(id))
            throw new ArgumentNullException(nameof(id));

        return RemoveAsync((Ids)id, options);
    }

    public Task RemoveAsync(Ids ids, CommandOptionsDescriptor<T> options)
    {
        return RemoveAsync(ids, options.Configure());
    }

    public async Task RemoveAsync(Ids ids, ICommandOptions options = null)
    {
        if (ids == null)
            throw new ArgumentNullException(nameof(ids));

        options = ConfigureOptions(options.As<T>());
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
        return RemoveAsync(document, options.Configure());
    }

    public virtual Task RemoveAsync(T document, ICommandOptions options = null)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));

        return RemoveAsync(new[] { document }, options);
    }

    public Task RemoveAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options)
    {
        return RemoveAsync(documents, options.Configure());
    }

    public virtual async Task RemoveAsync(IEnumerable<T> documents, ICommandOptions options = null)
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

        options = ConfigureOptions(options.As<T>());
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

            if (!response.IsValid && response.ApiCall.HttpStatusCode != 404)
            {
                throw new DocumentException(response.GetErrorMessage($"Error removing document {ElasticIndex.GetIndex(document)}/{document.Id}"), response.OriginalException);
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

                        return d;
                    });

                return bulk;
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

    public Task<long> RemoveAllAsync(CommandOptionsDescriptor<T> options)
    {
        return RemoveAllAsync(options.Configure());
    }

    public virtual async Task<long> RemoveAllAsync(ICommandOptions options = null)
    {
        long count = await RemoveAllAsync(NewQuery(), options);

        if (IsCacheEnabled && count > 0)
            await Cache.RemoveAllAsync().AnyContext();

        return count;
    }

    #endregion

    #region ISearchableRepository

    public Task<long> PatchAllAsync(RepositoryQueryDescriptor<T> query, IPatchOperation operation, CommandOptionsDescriptor<T> options = null)
    {
        return PatchAllAsync(query.Configure(), operation, options.Configure());
    }

    /// <summary>
    /// Patches all documents matching the query. Script and partial patches use Elasticsearch's
    /// update-by-query when caching is disabled; otherwise they fall back to batch processing.
    /// JsonPatch and ActionPatch always use batch processing with conflict retry.
    /// </summary>
    public virtual async Task<long> PatchAllAsync(IRepositoryQuery query, IPatchOperation operation, ICommandOptions options = null)
    {
        if (operation == null)
            throw new ArgumentNullException(nameof(operation));

        if (operation is JsonPatch { Patch: null or { Operations: null or { Count: 0 } } })
            return 0;

        if (operation is ActionPatch<T> { Actions: null or { Count: 0 } })
            return 0;

        if (!ElasticIndex.HasMultipleIndexes)
            await ElasticIndex.EnsureIndexAsync(null).AnyContext();

        options = ConfigureOptions(options.As<T>());

        if (operation is ScriptPatch scriptPatchOp)
            operation = ApplyDateTracking(scriptPatchOp);
        else if (operation is PartialPatch partialPatchOp)
            operation = ApplyDateTracking(partialPatchOp);

        long affectedRecords = 0;
        if (operation is JsonPatch jsonOperation)
        {
            var patcher = new JsonPatcher();
            affectedRecords += await BatchProcessAsync(query, async results =>
            {
                var bulkResult = await _client.BulkAsync(b =>
                {
                    b.Refresh(options.GetRefreshMode(DefaultConsistency));
                    foreach (var h in results.Hits)
                    {
                        string json = _client.ConnectionSettings.SourceSerializer.SerializeToString(h.Document);
                        var target = JToken.Parse(json);
                        patcher.Patch(ref target, jsonOperation.Patch);
                        var doc = _client.ConnectionSettings.SourceSerializer.Deserialize<T>(new MemoryStream(System.Text.Encoding.UTF8.GetBytes(target.ToString())));

                        if (HasDateTracking)
                            SetDocumentDates(doc, ElasticIndex.Configuration.TimeProvider);

                        var elasticVersion = h.GetElasticVersion();

                        b.Index<T>(i =>
                        {
                            i.Document(doc)
                             .Id(h.Id)
                             .Routing(h.Routing)
                             .Index(h.GetIndex())
                             .Pipeline(DefaultPipeline);

                            if (HasVersion)
                            {
                                i.IfPrimaryTerm(elasticVersion.PrimaryTerm);
                                i.IfSequenceNumber(elasticVersion.SequenceNumber);
                            }

                            return i;
                        });
                    }

                    return b;
                }).AnyContext();

                if (bulkResult.IsValid)
                {
                    _logger.LogRequest(bulkResult, options.GetQueryLogLevel());
                }
                else if (!await HandleBulkPatchErrorsAsync(bulkResult, results, "JsonPatch", operation, options).AnyContext())
                {
                    return false;
                }

                // JsonPatch uses Index API (get-modify-reindex), not Update API, so ES does not
                // report noop status. All processed documents are treated as modified.
                var updatedIds = results.Hits.Select(h => h.Id).ToList();
                if (IsCacheEnabled)
                    await InvalidateCacheAsync(updatedIds).AnyContext();

                try
                {
                    options.GetUpdatedIdsCallback()?.Invoke(updatedIds);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error calling updated ids callback");
                }

                return true;
            }, options.Clone()).AnyContext();
        }
        else if (operation is ActionPatch<T> actionOperation)
        {
            affectedRecords += await BatchProcessAsync(query, async results =>
            {
                var bulkResult = await _client.BulkAsync(b =>
                {
                    b.Refresh(options.GetRefreshMode(DefaultConsistency));
                    foreach (var h in results.Hits)
                    {
                        foreach (var action in actionOperation.Actions)
                            action?.Invoke(h.Document);

                        if (HasDateTracking)
                            SetDocumentDates(h.Document, ElasticIndex.Configuration.TimeProvider);

                        var elasticVersion = h.GetElasticVersion();

                        b.Index<T>(i =>
                        {
                            i.Document(h.Document)
                                .Id(h.Id)
                                .Routing(h.Routing)
                                .Index(h.GetIndex())
                                .Pipeline(DefaultPipeline);

                            if (HasVersion)
                            {
                                i.IfPrimaryTerm(elasticVersion.PrimaryTerm);
                                i.IfSequenceNumber(elasticVersion.SequenceNumber);
                            }

                            return i;
                        });
                    }

                    return b;
                }).AnyContext();

                if (bulkResult.IsValid)
                {
                    _logger.LogRequest(bulkResult, options.GetQueryLogLevel());
                }
                else if (!await HandleBulkPatchErrorsAsync(bulkResult, results, "ActionPatch", operation, options).AnyContext())
                {
                    return false;
                }

                // ActionPatch bulk path: noop detection per document is not yet implemented
                // for batch processing. Single-doc PatchAsync does compare before/after, but here
                // all documents are indexed. Future optimization could skip unchanged documents.
                var updatedIds = results.Hits.Select(h => h.Id).ToList();
                if (IsCacheEnabled)
                    await InvalidateCacheAsync(updatedIds).AnyContext();

                try
                {
                    options.GetUpdatedIdsCallback()?.Invoke(updatedIds);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error calling updated ids callback");
                }

                return true;
            }, options.Clone()).AnyContext();
        }
        else
        {
            if (operation is not ScriptPatch and not PartialPatch)
                throw new ArgumentException("Unknown operation type", nameof(operation));

            if (!IsCacheEnabled && operation is ScriptPatch scriptOperation)
            {
                var request = new UpdateByQueryRequest(Indices.Index(String.Join(",", ElasticIndex.GetIndexesByQuery(query))))
                {
                    Query = await ElasticIndex.QueryBuilder.BuildQueryAsync(query, options, new SearchDescriptor<T>()).AnyContext(),
                    Conflicts = Conflicts.Proceed,
                    Script = new InlineScript(scriptOperation.Script) { Params = scriptOperation.Params },
                    Pipeline = DefaultPipeline,
                    Version = HasVersion,
                    Refresh = options.GetRefreshMode(DefaultConsistency) != Refresh.False,
                    IgnoreUnavailable = true,
                    WaitForCompletion = false
                };

                var response = await _client.UpdateByQueryAsync(request).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());
                if (!response.IsValid)
                {
                    throw new DocumentException(response.GetErrorMessage("Error occurred while patching by query"), response.OriginalException);
                }

                var taskId = response.Task;
                int attempts = 0;
                do
                {
                    attempts++;
                    var taskStatus = await _client.Tasks.GetTaskAsync(taskId, t => t.WaitForCompletion(false)).AnyContext();
                    _logger.LogRequest(taskStatus, options.GetQueryLogLevel());

                    if (!taskStatus.IsValid)
                    {
                        if (taskStatus.ApiCall.HttpStatusCode.GetValueOrDefault() == 404)
                        {
                            _logger.LogWarning("Task {TaskId} not found (404), treating as completed", taskId);
                            break;
                        }

                        _logger.LogError("Error getting task status for {TaskId}: {Error}", taskId, taskStatus.ServerError);
                        if (attempts >= 20)
                            throw new DocumentException($"Failed to get task status for {taskId} after {attempts} attempts");

                        var retryDelay = TimeSpan.FromSeconds(attempts <= 5 ? 1 : 5);
                        await ElasticIndex.Configuration.TimeProvider.SafeDelay(retryDelay, DisposedCancellationToken).AnyContext();
                        continue;
                    }

                    var status = taskStatus.Task.Status;
                    if (taskStatus.Completed)
                    {
                        if (status.VersionConflicts > 0)
                            _logger.LogWarning("Script operation task ({TaskId}) completed with {Conflicts} version conflicts", taskId, status.VersionConflicts);
                        else
                            _logger.LogInformation("Script operation task ({TaskId}) completed: Created: {Created} Updated: {Updated} Deleted: {Deleted} Conflicts: {Conflicts} Total: {Total}", taskId, status.Created, status.Updated, status.Deleted, status.VersionConflicts, status.Total);

                        affectedRecords += status.Created + status.Updated + status.Deleted;
                        break;
                    }

                    _logger.LogDebug("Checking script operation task ({TaskId}) status: Created: {Created} Updated: {Updated} Deleted: {Deleted} Conflicts: {Conflicts} Total: {Total}", taskId, status.Created, status.Updated, status.Deleted, status.VersionConflicts, status.Total);
                    var delay = TimeSpan.FromSeconds(attempts <= 5 ? 1 : 5);
                    await ElasticIndex.Configuration.TimeProvider.SafeDelay(delay, DisposedCancellationToken).AnyContext();
                } while (!DisposedCancellationToken.IsCancellationRequested);
            }
            else
            {
                if (HasIdentity && !query.GetIncludes().Contains(_idField.Value))
                    query.Include(_idField.Value);

                // TODO: BatchProcessAsync returns total documents processed (including noops).
                // The PatchAllAsync return value for this cached path may overcount modified
                // documents. Consider tracking modified count via updatedIds accumulation.
                affectedRecords += await BatchProcessAsync(query, async results =>
                {
                    var bulkResult = await _client.BulkAsync(b =>
                    {
                        b.Pipeline(DefaultPipeline);
                        b.Refresh(options.GetRefreshMode(DefaultConsistency));

                        foreach (var h in results.Hits)
                        {
                            if (operation is ScriptPatch sp)
                                b.Update<T>(u => u
                                    .Id(h.Id)
                                    .Routing(h.Routing)
                                    .Index(h.GetIndex())
                                    .Script(s => s.Source(sp.Script).Params(sp.Params))
                                    .RetriesOnConflict(options.GetRetryCount()));
                            else if (operation is PartialPatch pp)
                                b.Update<T, object>(u => u.Id(h.Id)
                                    .Routing(h.Routing)
                                    .Index(h.GetIndex())
                                    .Doc(pp.Document)
                                    .RetriesOnConflict(options.GetRetryCount()));
                        }

                        return b;
                    }).AnyContext();

                    if (bulkResult.IsValid)
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
                        .Where(h => !result.NoopIds.Contains(h.Id))
                        .Select(h => h.Id).ToList();
                    if (IsCacheEnabled && updatedIds.Count > 0)
                    {
                        await InvalidateCacheAsync(updatedIds).AnyContext();
                    }

                    try
                    {
                        if (updatedIds.Count > 0)
                            options.GetUpdatedIdsCallback()?.Invoke(updatedIds);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error calling updated ids callback");
                    }

                    return true;
                }, options.Clone()).AnyContext();
            }
        }

        if (affectedRecords > 0)
        {
            if (IsCacheEnabled)
                await InvalidateCacheByQueryAsync(query.As<T>()).AnyContext();

            await OnDocumentsChangedAsync(ChangeType.Saved, EmptyList, options).AnyContext();
            await SendQueryNotificationsAsync(ChangeType.Saved, query, options).AnyContext();
        }

        return affectedRecords;
    }

    public Task<long> RemoveAllAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null)
    {
        return RemoveAllAsync(query.Configure(), options.Configure());
    }

    public virtual async Task<long> RemoveAllAsync(IRepositoryQuery query, ICommandOptions options = null)
    {
        options = ConfigureOptions(options.As<T>());
        bool hasRemoveListeners = DocumentsChanging.HasHandlers || DocumentsChanged.HasHandlers || DocumentsRemoving.HasHandlers || DocumentsRemoved.HasHandlers;
        if (hasRemoveListeners || (IsCacheEnabled && options.ShouldUseCache(true)))
        {
            var includes = query.GetIncludes();
            foreach (var field in _propertiesRequiredForRemove.Select(f => f.Value))
                if (field != null && !includes.Contains(field))
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
            Query = await ElasticIndex.QueryBuilder.BuildQueryAsync(query, options, new SearchDescriptor<T>()).AnyContext()
        }).AnyContext();
        _logger.LogRequest(response, options.GetQueryLogLevel());

        if (!response.IsValid)
        {
            throw new DocumentException(response.GetErrorMessage("Error removing documents"), response.OriginalException);
        }

        if (response.Deleted > 0)
        {
            if (IsCacheEnabled)
                await InvalidateCacheByQueryAsync(query.As<T>()).AnyContext();

            await OnDocumentsRemovedAsync(EmptyList, options).AnyContext();
            await SendQueryNotificationsAsync(ChangeType.Removed, query, options).AnyContext();
        }

        if (response.Total != response.Deleted)
            _logger.LogWarning("RemoveAll: {Deleted} of {Total} records were removed ({Conflicts} version conflicts)", response.Deleted, response.Total, response.VersionConflicts);

        return response.Deleted;
    }

    public Task<long> BatchProcessAsync(RepositoryQueryDescriptor<T> query, Func<FindResults<T>, Task<bool>> processFunc, CommandOptionsDescriptor<T> options = null)
    {
        return BatchProcessAsync(query.Configure(), processFunc, options.Configure());
    }

    public Task<long> BatchProcessAsync(IRepositoryQuery query, Func<FindResults<T>, Task<bool>> processFunc, ICommandOptions options = null)
    {
        return BatchProcessAsAsync(query, processFunc, options);
    }

    public Task<long> BatchProcessAsAsync<TResult>(RepositoryQueryDescriptor<T> query, Func<FindResults<TResult>, Task<bool>> processFunc, CommandOptionsDescriptor<T> options = null) where TResult : class, new()
    {
        return BatchProcessAsAsync<TResult>(query.Configure(), processFunc, options.Configure());
    }

    public virtual async Task<long> BatchProcessAsAsync<TResult>(IRepositoryQuery query, Func<FindResults<TResult>, Task<bool>> processFunc, ICommandOptions options = null)
        where TResult : class, new()
    {
        if (processFunc == null)
            throw new ArgumentNullException(nameof(processFunc));

        if (!ElasticIndex.HasMultipleIndexes)
            await ElasticIndex.EnsureIndexAsync(null).AnyContext();

        options = ConfigureOptions(options.As<T>());
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

    #endregion

    /// <summary>
    /// Registers a field that must always be included when <see cref="RemoveAllAsync(IRepositoryQuery, ICommandOptions)"/>
    /// fetches documents for deletion. This ensures critical fields (needed for cache invalidation,
    /// notifications, or event handlers) are returned even when the caller has specified a restricted
    /// include set. <c>Id</c> and <c>CreatedUtc</c> (when applicable) are registered automatically.
    /// </summary>
    protected void AddPropertyRequiredForRemove(string field)
    {
        _propertiesRequiredForRemove.Add(new Lazy<Field>(() => field));
    }

    /// <inheritdoc cref="AddPropertyRequiredForRemove(string)"/>
    protected void AddPropertyRequiredForRemove(Lazy<string> field)
    {
        _propertiesRequiredForRemove.Add(new Lazy<Field>(() => field.Value));
    }

    /// <inheritdoc cref="AddPropertyRequiredForRemove(string)"/>
    protected void AddPropertyRequiredForRemove(Expression<Func<T, object>> objectPath)
    {
        _propertiesRequiredForRemove.Add(new Lazy<Field>(() => Infer.PropertyName(objectPath)));
    }

    /// <inheritdoc cref="AddPropertyRequiredForRemove(string)"/>
    protected void AddPropertyRequiredForRemove(params Expression<Func<T, object>>[] objectPaths)
    {
        _propertiesRequiredForRemove.AddRange(objectPaths.Select(o => new Lazy<Field>(() => Infer.PropertyName(o))));
    }

    protected virtual async Task OnCustomFieldsBeforeQuery(object sender, BeforeQueryEventArgs<T> args)
    {
        var tenantKey = GetTenantKey(args.Query);
        if (String.IsNullOrEmpty(tenantKey))
            return;

        var fieldMapping = await ElasticIndex.Configuration.CustomFieldDefinitionRepository.GetFieldMappingAsync(EntityTypeName, tenantKey);
        var mapping = fieldMapping.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.GetIdxName(), StringComparer.OrdinalIgnoreCase);

        args.Options.QueryFieldResolver(mapping.ToHierarchicalFieldResolver("idx."));
    }

    protected virtual async Task OnCustomFieldsDocumentsChanging(object sender, DocumentsChangeEventArgs<T> args)
    {
        var tenantGroups = args.Documents.Select(d => d.Value).GroupBy(GetDocumentTenantKey).Where(g => g.Key != null).ToList();

        foreach (var tenant in tenantGroups)
        {
            var fieldDefinitions = await ElasticIndex.Configuration.CustomFieldDefinitionRepository.GetFieldMappingAsync(EntityTypeName, tenant.Key);

            foreach (var doc in tenant)
            {
                var idx = GetDocumentIdx(doc);
                if (idx == null)
                    continue;

                idx.Clear();

                var customFields = GetDocumentCustomFields(doc);
                if (customFields == null)
                    continue;

                foreach (var customField in customFields)
                {
                    if (!fieldDefinitions.TryGetValue(customField.Key, out var fieldDefinition))
                    {
                        fieldDefinition = await HandleUnmappedCustomField(doc, customField.Key, customField.Value, fieldDefinitions);
                        if (fieldDefinition == null)
                            continue;

                        fieldDefinitions[customField.Key] = fieldDefinition;
                    }

                    if (!ElasticIndex.CustomFieldTypes.TryGetValue(fieldDefinition.IndexType, out var fieldType))
                    {
                        _logger.LogWarning("Field type {IndexType} is not configured for this index {IndexName} for custom field {CustomFieldName}", fieldDefinition.IndexType, ElasticIndex.Name, customField.Key);
                        continue;
                    }

                    var result = await fieldType.ProcessValueAsync(doc, customField.Value, fieldDefinition);
                    SetDocumentCustomField(doc, customField.Key, result.Value);
                    idx[fieldDefinition.GetIdxName()] = result.Idx ?? result.Value;

                    if (result.IsCustomFieldDefinitionModified)
                        await ElasticIndex.Configuration.CustomFieldDefinitionRepository.SaveAsync(fieldDefinition).AnyContext();
                }

                foreach (var alwaysProcessField in fieldDefinitions.Values.Where(f => f.ProcessMode == CustomFieldProcessMode.AlwaysProcess))
                {
                    if (!ElasticIndex.CustomFieldTypes.TryGetValue(alwaysProcessField.IndexType, out var fieldType))
                    {
                        _logger.LogWarning("Field type {IndexType} is not configured for this index {IndexName} for custom field {CustomFieldName}", alwaysProcessField.IndexType, ElasticIndex.Name, alwaysProcessField.Name);
                        continue;
                    }

                    object value = GetDocumentCustomField(doc, alwaysProcessField.Name);
                    var result = await fieldType.ProcessValueAsync(doc, value, alwaysProcessField);
                    SetDocumentCustomField(doc, alwaysProcessField.Name, result.Value);
                    idx[alwaysProcessField.GetIdxName()] = result.Idx ?? result.Value;

                    if (result.IsCustomFieldDefinitionModified)
                        await ElasticIndex.Configuration.CustomFieldDefinitionRepository.SaveAsync(alwaysProcessField).AnyContext();
                }
            }
        }
    }

    protected virtual async Task<CustomFieldDefinition> HandleUnmappedCustomField(T document, string name, object value, IDictionary<string, CustomFieldDefinition> existingFields)
    {
        if (!AutoCreateCustomFields)
            return null;

        var tenantKey = GetDocumentTenantKey(document);
        if (String.IsNullOrEmpty(tenantKey))
            return null;

        return await ElasticIndex.Configuration.CustomFieldDefinitionRepository.AddFieldAsync(EntityTypeName, GetDocumentTenantKey(document), name, StringFieldType.IndexType);
    }

    protected string GetDocumentTenantKey(T document)
    {
        return document switch
        {
            IHaveCustomFields f => f.GetTenantKey(),
            IHaveVirtualCustomFields v => v.GetTenantKey(),
            _ => null
        };
    }

    protected IDictionary<string, object> GetDocumentCustomFields(T document)
    {
        return document switch
        {
            IHaveCustomFields f => f.Data,
            IHaveVirtualCustomFields v => v.GetCustomFields(),
            _ => null
        };
    }

    protected void SetDocumentCustomField(T document, string name, object value)
    {
        switch (document)
        {
            case IHaveCustomFields f:
                f.Data[name] = value;
                return;
            case IHaveVirtualCustomFields v:
                v.SetCustomField(name, value);
                return;
        }
    }

    protected object GetDocumentCustomField(T document, string name)
    {
        return document switch
        {
            IHaveCustomFields f => f.Data.GetValueOrDefault(name),
            IHaveVirtualCustomFields v => v.GetCustomField(name),
            _ => null,
        };
    }

    protected IDictionary<string, object> GetDocumentIdx(T document)
    {
        return document switch
        {
            IHaveCustomFields f => f.Idx,
            IHaveVirtualCustomFields v => v.Idx,
            _ => null,
        };
    }

    protected virtual string GetTenantKey(IRepositoryQuery query)
    {
        return null;
    }

    #region Events

    public AsyncEvent<DocumentsEventArgs<T>> DocumentsAdding { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

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
            documents, cf => cf.Id, cf => cf.Id,
            (original, modified, id) => new { Id = id, Original = original, Modified = modified }).Select(m => new ModifiedDocument<T>(m.Modified, m.Original)).ToList();

        if (DocumentsSaving is { HasHandlers: true })
            await DocumentsSaving.InvokeAsync(this, new ModifiedDocumentsEventArgs<T>(modifiedDocs, this, options)).AnyContext();

        await OnDocumentsChangingAsync(ChangeType.Saved, modifiedDocs, options).AnyContext();
    }

    public AsyncEvent<ModifiedDocumentsEventArgs<T>> DocumentsSaved { get; } = new AsyncEvent<ModifiedDocumentsEventArgs<T>>();

    private async Task OnDocumentsSavedAsync(IReadOnlyCollection<T> documents, IReadOnlyCollection<T> originalDocuments, ICommandOptions options)
    {
        var modifiedDocs = originalDocuments.FullOuterJoin(
            documents, cf => cf.Id, cf => cf.Id,
            (original, modified, id) => new { Id = id, Original = original, Modified = modified }).Select(m => new ModifiedDocument<T>(m.Modified, m.Original)).ToList();

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

    #endregion

    private async Task<IReadOnlyCollection<T>> GetOriginalDocumentsAsync(Ids ids, ICommandOptions options = null)
    {
        if (!options.GetOriginalsEnabled(OriginalsEnabled) || ids.Count == 0)
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
            i.Pipeline(DefaultPipeline);
            i.Refresh(options.GetRefreshMode(DefaultConsistency));

            if (GetParentIdFunc is not null)
                i.Routing(GetParentIdFunc(document));

            i.Index(ElasticIndex.GetIndex(document));

            if (HasVersion && !isCreateOperation && !options.ShouldSkipVersionCheck())
            {
                var elasticVersion = ((IVersioned)document).GetElasticVersion();
                i.IfPrimaryTerm(elasticVersion.PrimaryTerm);
                i.IfSequenceNumber(elasticVersion.SequenceNumber);
            }

            return i;
        }).AnyContext();
        _logger.LogRequest(response, options.GetQueryLogLevel());

        if (!response.IsValid)
        {
            string message = $"Error {(isCreateOperation ? "adding" : "saving")} document";
            if (response.ServerError?.Status is 409)
                throw isCreateOperation
                    ? new DuplicateDocumentException(response.GetErrorMessage(message), response.OriginalException)
                    : new VersionConflictDocumentException(response.GetErrorMessage(message), response.OriginalException);

            throw new DocumentException(response.GetErrorMessage(message), response.OriginalException);
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
                IBulkOperation baseOperation;
                if (isCreateOperation)
                {
                    baseOperation = new BulkCreateOperation<T>(d) { Pipeline = DefaultPipeline };
                }
                else
                {
                    var indexOperation = new BulkIndexOperation<T>(d) { Pipeline = DefaultPipeline };
                    if (HasVersion && !options.ShouldSkipVersionCheck())
                    {
                        var elasticVersion = ((IVersioned)d).GetElasticVersion();
                        indexOperation.IfSequenceNumber = elasticVersion.SequenceNumber;
                        indexOperation.IfPrimaryTerm = elasticVersion.PrimaryTerm;
                    }
                    baseOperation = indexOperation;
                }

                if (GetParentIdFunc is not null)
                    baseOperation.Routing = GetParentIdFunc(d);
                baseOperation.Index = ElasticIndex.GetIndex(d);

                return baseOperation;
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

                    if (!documentsById.TryGetValue(hit.Id, out var document))
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

    private async Task<bool> HandleBulkPatchErrorsAsync(BulkResponse bulkResponse, FindResults<T> results, string patchType, IPatchOperation operation, ICommandOptions options)
    {
        var result = BulkResult.From(bulkResponse);
        if (result.HasTransportError || result.FatalIds.Count > 0 || result.RetryableIds.Count > 0)
        {
            _logger.LogErrorRequest(bulkResponse, "Error occurred while bulk updating");
            return false;
        }

        if (result.HasConflicts)
        {
            _logger.LogRequest(bulkResponse, options.GetQueryLogLevel());
            _logger.LogInformation("Bulk {PatchType} had {ConflictCount} version conflicts, re-fetching and retrying", patchType, result.ConflictIds.Count);

            var conflictHits = results.Hits.Where(h => result.ConflictIds.Contains(h.Id)).ToList();
            foreach (var hit in conflictHits)
                await PatchAsync(new Id(hit.Id, hit.Routing), operation, options).AnyContext();
        }

        return true;
    }

    private static void ThrowForBulkErrors(BulkResult result, bool isCreateOperation = false, string operationLabel = null)
    {
        if (result.IsSuccess)
            return;

        if (result.HasTransportError)
            throw new DocumentException(result.TransportError, result.TransportException);

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
            return PublishChangeTypeMessageAsync(changeType, null, delay);

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

    protected virtual Task PublishChangeTypeMessageAsync(ChangeType changeType, T document, TimeSpan? delay)
    {
        return PublishChangeTypeMessageAsync(changeType, document, null, delay);
    }

    protected virtual Task PublishChangeTypeMessageAsync(ChangeType changeType, T document, IDictionary<string, object> data = null, TimeSpan? delay = null)
    {
        return PublishChangeTypeMessageAsync(changeType, document?.Id, data, delay);
    }

    protected virtual Task PublishChangeTypeMessageAsync(ChangeType changeType, string id, IDictionary<string, object> data = null, TimeSpan? delay = null)
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
            return _updatedUtcField.Value;

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
        var serialized = _client.ConnectionSettings.SourceSerializer.SerializeToString(patch.Document);
        var partialDoc = JToken.Parse(serialized);

        if (partialDoc is not JObject partialObject)
            return patch;

        if (GetNestedJToken(partialObject, fieldPath) is not null)
        {
            _logger.LogDebug("Skipping automatic {FieldPath} injection; caller already provided it", fieldPath);
            return patch;
        }

        SetNestedJTokenValue(partialObject, fieldPath,
            JToken.FromObject(ElasticIndex.Configuration.TimeProvider.GetUtcNow().UtcDateTime));

        return new PartialPatch(ToDictionary(partialObject));
    }

    /// <summary>
    /// Sets the updated timestamp on a <see cref="JToken"/> document (used by single-doc JsonPatch).
    /// Supports nested dot-path fields.
    /// </summary>
    protected virtual void ApplyDateTracking(JToken target)
    {
        if (!HasDateTracking)
            return;

        var fieldPath = GetUpdatedUtcFieldPath();

        SetNestedJTokenValue(target, fieldPath,
            JToken.FromObject(ElasticIndex.Configuration.TimeProvider.GetUtcNow().UtcDateTime));
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

    private static JToken GetNestedJToken(JToken token, string dotPath)
    {
        var remaining = dotPath.AsSpan();

        while (remaining.Length > 0)
        {
            var dotIndex = remaining.IndexOf('.');
            var segment = dotIndex >= 0 ? remaining[..dotIndex] : remaining;

            if (token is not JObject obj)
                return null;

            token = obj[segment.ToString()];
            if (token is null)
                return null;

            remaining = dotIndex >= 0 ? remaining[(dotIndex + 1)..] : ReadOnlySpan<char>.Empty;
        }

        return token;
    }

    private static void SetNestedJTokenValue(JToken token, string dotPath, JToken value)
    {
        var remaining = dotPath.AsSpan();

        while (true)
        {
            var dotIndex = remaining.IndexOf('.');

            if (dotIndex < 0)
            {
                if (token is JObject leaf)
                    leaf[remaining.ToString()] = value;

                return;
            }

            var segment = remaining[..dotIndex].ToString();

            if (token is not JObject current)
                return;

            var next = current[segment];

            if (next is not JObject)
            {
                next = new JObject();
                current[segment] = next;
            }

            token = next;
            remaining = remaining[(dotIndex + 1)..];
        }
    }

    private static Dictionary<string, object> ToDictionary(JObject obj)
    {
        var dict = new Dictionary<string, object>(obj.Count);

        foreach (var property in obj.Properties())
            dict[property.Name] = ToValue(property.Value);

        return dict;
    }

    private static object ToValue(JToken token)
    {
        return token switch
        {
            JObject nested => ToDictionary(nested),
            JArray array => array.Select(ToValue).ToList(),
            JValue value => value.Value,
            _ => token.ToObject<object>()
        };
    }
}
