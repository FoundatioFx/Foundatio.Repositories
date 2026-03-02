using System;
using System.Collections.Generic;
using System.Diagnostics;
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

        await IndexDocumentsAsync(docs, true, options).AnyContext();

        await OnDocumentsAddedAsync(docs, options).AnyContext();
        if (IsCacheEnabled && options.ShouldUseCache())
            await AddDocumentsToCacheAsync(docs, options, false).AnyContext();
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

        await IndexDocumentsAsync(docs, false, options).AnyContext();

        await OnDocumentsSavedAsync(docs, originalDocuments, options).AnyContext();
        if (IsCacheEnabled && options.ShouldUseCache())
            await AddDocumentsToCacheAsync(docs, options, false).AnyContext();
    }

    public Task PatchAsync(Id id, IPatchOperation operation, CommandOptionsDescriptor<T> options)
    {
        return PatchAsync(id, operation, options.Configure());
    }

    public virtual async Task PatchAsync(Id id, IPatchOperation operation, ICommandOptions options = null)
    {
        if (String.IsNullOrEmpty(id.Value))
            throw new ArgumentNullException(nameof(id));

        if (operation == null)
            throw new ArgumentNullException(nameof(operation));

        await ElasticIndex.EnsureIndexAsync(id).AnyContext();

        options = ConfigureOptions(options.As<T>());

        if (operation is ScriptPatch scriptOperation)
        {
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

                throw new DocumentException(
                    response.GetErrorMessage($"Error patching document {ElasticIndex.GetIndex(id)}/{id.Value}"),
                    response.OriginalException);
            }
        }
        else if (operation is PartialPatch partialOperation)
        {
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

                throw new DocumentException(
                    response.GetErrorMessage($"Error patching document {ElasticIndex.GetIndex(id)}/{id.Value}"),
                    response.OriginalException);
            }
        }
        else if (operation is JsonPatch jsonOperation)
        {
            if (jsonOperation.Patch == null || jsonOperation.Patch.Operations.Count == 0)
                return;

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

                var response = await _client.LowLevel.GetAsync<GetResponse<IDictionary<string, object>>>(ElasticIndex.GetIndex(id), id.Value, ctx: ct).AnyContext();
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
                    if (response.ServerError?.Status == 409)
                        throw new VersionConflictDocumentException(response.GetErrorMessage("Error saving document"), response.OriginalException);

                    throw new DocumentException(response.GetErrorMessage("Error saving document"), response.OriginalException);
                }
            });
        }
        else if (operation is ActionPatch<T> actionPatch)
        {
            if (actionPatch.Actions == null || actionPatch.Actions.Count == 0)
                return;

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

                foreach (var action in actionPatch.Actions)
                    action?.Invoke(response.Source);

                await IndexDocumentsAsync([response.Source], false, options);
            });
        }
        else
        {
            throw new ArgumentException("Unknown operation type", nameof(operation));
        }

        // TODO: Find a good way to invalidate cache and send changed notification
        await OnDocumentsChangedAsync(ChangeType.Saved, EmptyList, options).AnyContext();
        if (IsCacheEnabled)
            await InvalidateCacheAsync(id).AnyContext();

        if (options.ShouldNotify())
            await PublishChangeTypeMessageAsync(ChangeType.Saved, id).AnyContext();
    }

    public Task PatchAsync(Ids ids, IPatchOperation operation, CommandOptionsDescriptor<T> options)
    {
        return PatchAsync(ids, operation, options.Configure());
    }

    public virtual async Task PatchAsync(Ids ids, IPatchOperation operation, ICommandOptions options = null)
    {
        if (ids == null)
            throw new ArgumentNullException(nameof(ids));

        if (operation == null)
            throw new ArgumentNullException(nameof(operation));

        if (ids.Count == 0)
            return;

        options = ConfigureOptions(options.As<T>());
        if (ids.Count == 1)
        {
            await PatchAsync(ids[0], operation, options).AnyContext();
            return;
        }

        if (operation is JsonPatch)
        {
            await PatchAllAsync(NewQuery().Id(ids), operation, options).AnyContext();
            return;
        }

        var scriptOperation = operation as ScriptPatch;
        var partialOperation = operation as PartialPatch;
        if (scriptOperation == null && partialOperation == null)
            throw new ArgumentException("Unknown operation type", nameof(operation));

        var bulkResponse = await _client.BulkAsync(b =>
        {
            b.Refresh(options.GetRefreshMode(DefaultConsistency));
            foreach (var id in ids)
            {
                b.Pipeline(DefaultPipeline);

                if (scriptOperation != null)
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
                else if (partialOperation != null)
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

        // TODO: Is there a better way to handle failures?
        if (!bulkResponse.IsValid)
        {
            throw new DocumentException(bulkResponse.GetErrorMessage("Error bulk patching documents"), bulkResponse.OriginalException);
        }

        // TODO: Find a good way to invalidate cache and send changed notification
        await OnDocumentsChangedAsync(ChangeType.Saved, EmptyList, options).AnyContext();
        if (IsCacheEnabled)
            await InvalidateCacheAsync(ids.Select(id => id.Value)).AnyContext();

        if (options.ShouldNotify())
        {
            var tasks = new List<Task>(ids.Count);
            foreach (var id in ids)
                tasks.Add(PublishChangeTypeMessageAsync(ChangeType.Saved, id));

            await Task.WhenAll(tasks).AnyContext();
        }
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

            if (GetParentIdFunc != null)
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

                        if (GetParentIdFunc != null)
                            d.Routing(GetParentIdFunc(doc));

                        return d;
                    });

                return bulk;
            }).AnyContext();

            _logger.LogRequest(response, options.GetQueryLogLevel());
            if (!response.IsValid)
            {
                throw new DocumentException(response.GetErrorMessage("Error bulk removing documents"), response.OriginalException);
            }
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
            await Cache.RemoveAllAsync();

        return count;
    }

    #endregion

    #region ISearchableRepository

    public Task<long> PatchAllAsync(RepositoryQueryDescriptor<T> query, IPatchOperation operation, CommandOptionsDescriptor<T> options = null)
    {
        return PatchAllAsync(query.Configure(), operation, options.Configure());
    }

    /// <summary>
    /// Script patches will not invalidate the cache or send notifications.
    /// Partial patches will not
    /// </summary>
    public virtual async Task<long> PatchAllAsync(IRepositoryQuery query, IPatchOperation operation, ICommandOptions options = null)
    {
        if (operation == null)
            throw new ArgumentNullException(nameof(operation));

        if (!ElasticIndex.HasMultipleIndexes)
            await ElasticIndex.EnsureIndexAsync(null).AnyContext();

        options = ConfigureOptions(options.As<T>());

        long affectedRecords = 0;
        if (operation is JsonPatch jsonOperation)
        {
            if (jsonOperation.Patch?.Operations == null || jsonOperation.Patch.Operations.Count == 0)
                return 0;

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
                else
                {
                    if (bulkResult.ItemsWithErrors.All(i => i.Status == 409))
                    {
                        _logger.LogRequest(bulkResult, options.GetQueryLogLevel());

                        string[] ids = bulkResult.ItemsWithErrors.Where(i => i.Status == 409).Select(i => i.Id).ToArray();
                        await PatchAsync(ids, operation, options);
                    }
                    else
                    {
                        _logger.LogErrorRequest(bulkResult, "Error occurred while bulk updating");
                        return false;
                    }
                }

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
            if (actionOperation.Actions == null || actionOperation.Actions.Count == 0)
                return 0;

            affectedRecords += await BatchProcessAsync(query, async results =>
            {
                var bulkResult = await _client.BulkAsync(b =>
                {
                    b.Refresh(options.GetRefreshMode(DefaultConsistency));
                    foreach (var h in results.Hits)
                    {
                        foreach (var action in actionOperation.Actions)
                            action?.Invoke(h.Document);

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
                else
                {
                    if (bulkResult.ItemsWithErrors.All(i => i.Status == 409))
                    {
                        _logger.LogRequest(bulkResult, options.GetQueryLogLevel());

                        string[] ids = bulkResult.ItemsWithErrors.Where(i => i.Status == 409).Select(i => i.Id).ToArray();
                        await PatchAsync(ids, operation, options);
                    }
                    else
                    {
                        _logger.LogErrorRequest(bulkResult, "Error occurred while bulk updating");
                        return false;
                    }
                }

                var updatedIds = results.Hits.Select(h => h.Id).ToList();
                if (IsCacheEnabled)
                {
                    await InvalidateCacheAsync(updatedIds).AnyContext();
                }

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
            var scriptOperation = operation as ScriptPatch;
            var partialOperation = operation as PartialPatch;
            if (scriptOperation == null && partialOperation == null)
                throw new ArgumentException("Unknown operation type", nameof(operation));

            if (!IsCacheEnabled && scriptOperation != null)
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
                        await ElasticIndex.Configuration.TimeProvider.Delay(retryDelay).AnyContext();
                        continue;
                    }

                    var status = taskStatus.Task.Status;
                    if (taskStatus.Completed)
                    {
                        // TODO: need to check to see if the task failed or completed successfully. Throw if it failed.
                        _logger.LogInformation("Script operation task ({TaskId}) completed: Created: {Created} Updated: {Updated} Deleted: {Deleted} Conflicts: {Conflicts} Total: {Total}", taskId, status.Created, status.Updated, status.Deleted, status.VersionConflicts, status.Total);
                        affectedRecords += status.Created + status.Updated + status.Deleted;
                        break;
                    }

                    _logger.LogDebug("Checking script operation task ({TaskId}) status: Created: {Created} Updated: {Updated} Deleted: {Deleted} Conflicts: {Conflicts} Total: {Total}", taskId, status.Created, status.Updated, status.Deleted, status.VersionConflicts, status.Total);
                    var delay = TimeSpan.FromSeconds(attempts <= 5 ? 1 : 5);
                    await ElasticIndex.Configuration.TimeProvider.Delay(delay).AnyContext();
                } while (true);
            }
            else
            {
                if (HasIdentity && !query.GetIncludes().Contains(_idField.Value))
                    query.Include(_idField.Value);

                affectedRecords += await BatchProcessAsync(query, async results =>
                {
                    var bulkResult = await _client.BulkAsync(b =>
                    {
                        b.Pipeline(DefaultPipeline);
                        b.Refresh(options.GetRefreshMode(DefaultConsistency));

                        foreach (var h in results.Hits)
                        {
                            if (scriptOperation != null)
                                b.Update<T>(u => u
                                    .Id(h.Id)
                                    .Routing(h.Routing)
                                    .Index(h.GetIndex())
                                    .Script(s => s.Source(scriptOperation.Script).Params(scriptOperation.Params))
                                    .RetriesOnConflict(options.GetRetryCount()));
                            else if (partialOperation != null)
                                b.Update<T, object>(u => u.Id(h.Id)
                                    .Routing(h.Routing)
                                    .Index(h.GetIndex())
                                    .Doc(partialOperation.Document));
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

                    var updatedIds = results.Hits.Select(h => h.Id).ToList();
                    if (IsCacheEnabled)
                    {
                        // TODO: Add cache invalidation for documents.
                        await InvalidateCacheAsync(updatedIds).AnyContext();
                    }

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
        }

        if (affectedRecords > 0)
        {
            if (IsCacheEnabled)
                await InvalidateCacheByQueryAsync(query.As<T>());

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
                await InvalidateCacheByQueryAsync(query.As<T>());

            await OnDocumentsRemovedAsync(EmptyList, options).AnyContext();
            await SendQueryNotificationsAsync(ChangeType.Removed, query, options).AnyContext();
        }

        Debug.Assert(response.Total == response.Deleted, "All records were not removed");
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
                        await ElasticIndex.Configuration.CustomFieldDefinitionRepository.SaveAsync(fieldDefinition);
                }

                foreach (var alwaysProcessField in fieldDefinitions.Values.Where(f => f.ProcessMode == CustomFieldProcessMode.AlwaysProcess))
                {
                    if (!ElasticIndex.CustomFieldTypes.TryGetValue(alwaysProcessField.IndexType, out var fieldType))
                    {
                        _logger.LogWarning("Field type {IndexType} is not configured for this index {IndexName} for custom field {CustomFieldName}", alwaysProcessField.IndexType, ElasticIndex.Name, alwaysProcessField.Name);
                        continue;
                    }

                    var value = GetDocumentCustomField(doc, alwaysProcessField.Name);
                    var result = await fieldType.ProcessValueAsync(doc, value, alwaysProcessField);
                    SetDocumentCustomField(doc, alwaysProcessField.Name, result.Value);
                    idx[alwaysProcessField.GetIdxName()] = result.Idx ?? result.Value;

                    if (result.IsCustomFieldDefinitionModified)
                        await ElasticIndex.Configuration.CustomFieldDefinitionRepository.SaveAsync(alwaysProcessField);
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
        if (HasDates)
            documents.OfType<IHaveDates>().SetDates(ElasticIndex.Configuration.TimeProvider);
        else if (HasCreatedDate)
            documents.OfType<IHaveCreatedDate>().SetCreatedDates(ElasticIndex.Configuration.TimeProvider);

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

        if (HasDates)
            documents.Cast<IHaveDates>().SetDates(ElasticIndex.Configuration.TimeProvider);

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

    private async Task IndexDocumentsAsync(IReadOnlyCollection<T> documents, bool isCreateOperation, ICommandOptions options)
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
        {
            var document = documents.Single();
            var response = await _client.IndexAsync(document, i =>
            {
                i.OpType(isCreateOperation ? OpType.Create : OpType.Index);
                i.Pipeline(DefaultPipeline);
                i.Refresh(options.GetRefreshMode(DefaultConsistency));

                if (GetParentIdFunc != null)
                    i.Routing(GetParentIdFunc(document));
                //i.Routing(GetParentIdFunc != null ? GetParentIdFunc(document) : document.Id);

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
                if (isCreateOperation && response.ServerError?.Status == 409)
                    throw new DuplicateDocumentException(response.GetErrorMessage(message), response.OriginalException);
                else if (!isCreateOperation && response.ServerError?.Status == 409)
                    throw new VersionConflictDocumentException(response.GetErrorMessage(message), response.OriginalException);

                throw new DocumentException(response.GetErrorMessage(message), response.OriginalException);
            }

            if (HasVersion)
            {
                var versionDoc = (IVersioned)document;
                var elasticVersion = response.GetElasticVersion();
                versionDoc.Version = elasticVersion;
            }
        }
        else
        {
            var bulkRequest = new BulkRequest();
            var list = documents.Select(d =>
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

                if (GetParentIdFunc != null)
                    baseOperation.Routing = GetParentIdFunc(d);
                baseOperation.Index = ElasticIndex.GetIndex(d);

                return baseOperation;
            }).ToList();
            bulkRequest.Operations = list;
            bulkRequest.Refresh = options.GetRefreshMode(DefaultConsistency);

            var response = await _client.BulkAsync(bulkRequest).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());

            if (HasVersion)
            {
                var documentsById = new Dictionary<string, T>();
                foreach (var d in documents.Where(d => !String.IsNullOrEmpty(d.Id)))
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

            var allErrors = response.ItemsWithErrors.ToList();
            if (allErrors.Count > 0)
            {
                var retryableIds = allErrors.Where(e => e.Status == 429 || e.Status == 503).Select(e => e.Id).ToList();
                if (retryableIds.Count > 0)
                {
                    var docs = documents.Where(d => retryableIds.Contains(d.Id)).ToList();
                    await IndexDocumentsAsync(docs, isCreateOperation, options).AnyContext();

                    // return as all recoverable items were retried.
                    if (allErrors.Count == retryableIds.Count)
                        return;
                }
            }

            if (!response.IsValid)
            {
                if (isCreateOperation && allErrors.Any(e => e.Status == 409))
                    throw new DuplicateDocumentException(response.GetErrorMessage("Error adding duplicate documents"), response.OriginalException);
                else if (allErrors.Any(e => e.Status == 409))
                    throw new VersionConflictDocumentException(response.GetErrorMessage("Error saving documents"), response.OriginalException);

                throw new DocumentException(response.GetErrorMessage($"Error {(isCreateOperation ? "adding" : "saving")} documents"), response.OriginalException);
            }
        }
        // 429 // 503
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
}
