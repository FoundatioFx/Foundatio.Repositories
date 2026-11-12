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

public abstract partial class ElasticRepositoryBase<T> : ElasticReadOnlyRepositoryBase<T>, ISearchableRepository<T> where T : class, IIdentity, new()
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

    public Task<T> AddAsync(T document, CommandOptionsDescriptor<T>? options)
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

    public Task AddAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T>? options)
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

    public Task<T> SaveAsync(T document, CommandOptionsDescriptor<T>? options)
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

    public Task SaveAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T>? options)
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

    public Task<bool> PatchAsync(Id id, IPatchOperation operation, CommandOptionsDescriptor<T>? options)
    {
        return PatchAsync(id, operation, options?.Configure());
    }

    /// <summary>
    /// Patches one document by id.
    /// </summary>
    /// <remarks>
    /// <para><b>Script and partial patches</b> use the Elasticsearch Update API. Elasticsearch re-runs the update on the latest
    /// document up to <c>Retry(n)</c> times when a concurrent write wins (<c>retry_on_conflict</c>, default 10).</para>
    /// <para><b>JSON and action patches</b> (and partial patches when a default pipeline is configured) read the document,
    /// apply the patch in memory and write it back with <c>if_seq_no</c>/<c>if_primary_term</c> for versioned documents. On a
    /// conflict the document is read and patched again, up to <c>Retry(n)</c> times.</para>
    /// <para>Pass <c>IfVersion(version)</c> to require the stored document to still have that version. The patch then fails
    /// with a <see cref="VersionConflictDocumentException"/> instead of retrying. <c>IfVersion</c> is not supported for script
    /// patches when a default pipeline is configured (update-by-query cannot take an expected version).</para>
    /// <para>Transient errors (HTTP 429, 502, 503 and 504) are retried up to <c>Retry(n)</c> times.</para>
    /// </remarks>
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
        var expectedVersion = GetExpectedVersion(options);

        if (operation is ScriptPatch scriptPatchOp)
            operation = ApplyDateTracking(scriptPatchOp);
        else if (operation is PartialPatch partialPatchOp)
            operation = ApplyDateTracking(partialPatchOp);

        bool modified;
        if (operation is ScriptPatch scriptOperation && !String.IsNullOrEmpty(DefaultPipeline))
        {
            if (expectedVersion is not null)
                throw new ArgumentException("IfVersion is not supported for ScriptPatch when a default pipeline is configured.", nameof(options));

            modified = await PatchScriptWithPipelineAsync(id, scriptOperation, options).AnyContext();
        }
        else if (operation is ScriptPatch || (operation is PartialPatch && String.IsNullOrEmpty(DefaultPipeline)))
        {
            modified = await PatchWithUpdateApiAsync(id, operation, expectedVersion, options).AnyContext();
        }
        else if (operation is JsonPatch or ActionPatch<T> or PartialPatch)
        {
            return await PatchClientSideAsync(id, operation, expectedVersion, options).AnyContext();
        }
        else
        {
            throw new ArgumentException("Unknown operation type", nameof(operation));
        }

        // Script and partial patches run server-side, so the modified document is not available client-side.
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

    private static ElasticDocumentVersion? GetExpectedVersion(ICommandOptions options)
    {
        if (!options.HasExpectedVersion())
            return null;

        var version = ElasticDocumentVersion.Parse(options.GetExpectedVersion());
        if (version.IsEmpty)
            throw new ArgumentException($"IfVersion value \"{options.GetExpectedVersion()}\" is not a valid document version.", nameof(options));

        return version;
    }

    private async Task<bool> PatchWithUpdateApiAsync(Id id, IPatchOperation operation, ElasticDocumentVersion? expectedVersion, ICommandOptions options)
    {
        string index = ElasticIndex.GetIndex(id);
        var policy = CreateWritePolicy(options, retryConflicts: false);
        return await policy.ExecuteAsync(async ct =>
        {
            UpdateResponse<T> response;
            if (operation is ScriptPatch scriptOperation)
            {
                var request = new UpdateRequest<T, T>(index, id.Value)
                {
                    Script = new Script { Source = scriptOperation.Script, Params = scriptOperation.Params },
                    Refresh = options.GetRefreshMode(DefaultConsistency)
                };
                ConfigureUpdateRequest(request, id, expectedVersion, options);
                response = await _client.UpdateAsync(request, ct).AnyContext();
            }
            else
            {
                var request = new UpdateRequest<T, object>(index, id.Value)
                {
                    Doc = ((PartialPatch)operation).Document,
                    Refresh = options.GetRefreshMode(DefaultConsistency)
                };
                ConfigureUpdateRequest(request, id, expectedVersion, options);
                response = await _client.UpdateAsync(request, ct).AnyContext();
            }

            _logger.LogRequest(response, options.GetQueryLogLevel());
            if (!response.IsValidResponse)
            {
                if (response.ApiCallDetails is { HttpStatusCode: 404 })
                    throw new DocumentNotFoundException(id);

                var exception = CreateWriteException(response, "patching", index, id.Value);
                if (exception is VersionConflictDocumentException)
                    LogVersionConflict(operation.GetType().Name, index, id.Value, 1, expectedVersion is null ? options.GetRetryCount() : 0, expectedVersion?.ToVersionString(), response.ElasticsearchServerError?.Error?.Reason, "Failed");

                throw exception;
            }

            return response.Result is not Result.NoOp;
        }, DisposedCancellationToken).AnyContext();
    }

    /// <summary>
    /// Elasticsearch rejects <c>if_seq_no</c> combined with <c>retry_on_conflict</c>, so an expected version replaces
    /// the server-side conflict retries.
    /// </summary>
    private static void ConfigureUpdateRequest<TPartial>(UpdateRequest<T, TPartial> request, Id id, ElasticDocumentVersion? expectedVersion, ICommandOptions options)
    {
        if (id.Routing != null)
            request.Routing = id.Routing;

        if (expectedVersion is { } version)
        {
            request.IfSeqNo = version.SequenceNumber;
            request.IfPrimaryTerm = version.PrimaryTerm;
        }
        else
        {
            request.RetryOnConflict = options.GetRetryCount();
        }
    }

    /// <summary>
    /// Runs a script patch through update-by-query so the default pipeline is applied. Elasticsearch skips the write when
    /// the document changed during the request (a version conflict), so the request is retried up to <c>Retry(n)</c> times.
    /// </summary>
    private async Task<bool> PatchScriptWithPipelineAsync(Id id, ScriptPatch scriptOperation, ICommandOptions options)
    {
        string index = ElasticIndex.GetIndex(id);
        var policy = CreateWritePolicy(options, retryConflicts: true);
        return await policy.ExecuteAsync(async ct =>
        {
            var request = new UpdateByQueryRequest(index)
            {
                Query = new Elastic.Clients.Elasticsearch.QueryDsl.IdsQuery
                {
                    Values = new Elastic.Clients.Elasticsearch.Ids(new[] { new Elastic.Clients.Elasticsearch.Id(id.Value) })
                },
                Script = new Script { Source = scriptOperation.Script, Params = scriptOperation.Params },
                Pipeline = DefaultPipeline,
                Refresh = options.GetRefreshMode(DefaultConsistency) != Refresh.False,
                WaitForCompletion = true
            };
            if (id.Routing != null)
                request.Routing = new Elastic.Clients.Elasticsearch.Routing(id.Routing);

            var response = await _client.UpdateByQueryAsync(request, ct).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());

            if (!response.IsValidResponse)
            {
                if (response.ApiCallDetails is { HttpStatusCode: 404 })
                    throw new DocumentNotFoundException(id);

                throw CreateWriteException(response, "patching", index, id.Value);
            }

            if ((response.VersionConflicts ?? 0) > 0)
            {
                LogVersionConflict("ScriptPatch", index, id.Value, 1, options.GetRetryCount(), null, null, "Retrying");
                throw new VersionConflictDocumentException($"Version conflict patching document {index}/{id.Value}");
            }

            if ((response.Total ?? 0) is 0)
                throw new DocumentNotFoundException(id);

            return (response.Updated ?? 0) > 0 || (response.Deleted ?? 0) > 0;
        }, DisposedCancellationToken).AnyContext();
    }

    /// <summary>
    /// Reads one document, applies a JSON, action or pipeline partial patch in memory and writes it back version-checked,
    /// re-reading and re-applying on conflicts.
    /// </summary>
    private async Task<bool> PatchClientSideAsync(Id id, IPatchOperation operation, ElasticDocumentVersion? expectedVersion, ICommandOptions options)
    {
        string index = ElasticIndex.GetIndex(id);
        var readPolicy = CreateWritePolicy(options, retryConflicts: false);
        var hit = await readPolicy.ExecuteAsync(async ct =>
        {
            var request = new GetRequest(index, id.Value);
            if (id.Routing != null)
                request.Routing = id.Routing;

            var response = await _client.GetAsync<T>(request, ct).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());
            if (!response.Found)
                throw new DocumentNotFoundException(id);

            if (!response.IsValidResponse)
                throw CreateWriteException(response, "patching", index, id.Value);

            return response.ToFindHit();
        }, DisposedCancellationToken).AnyContext();

        if (expectedVersion is { } expected && hit.GetElasticVersion() != expected)
        {
            LogVersionConflict(operation.GetType().Name, index, id.Value, 1, 0, expected.ToVersionString(), null, "Failed");
            throw new VersionConflictDocumentException($"Version conflict patching document {index}/{id.Value}: expected version {expected} but found {hit.Version ?? "none"}");
        }

        bool actionModified = operation is not ActionPatch<T>;
        var result = await ApplyClientPatchesAsync([hit], h =>
        {
            var patched = ApplyClientPatch(h, operation, singleDocument: true);
            if (operation is ActionPatch<T> && patched is not null)
                actionModified = true;
            return patched;
        }, operation.GetType().Name, options, options.GetRefreshMode(DefaultConsistency), retryConflicts: expectedVersion is null).AnyContext();

        if (result.MissingIds.Contains(id.Value) && result.ModifiedIds.Count is 0)
            throw new DocumentNotFoundException(id);

        if (result.ModifiedDocuments.Count > 0)
        {
            await OnDocumentsChangedAsync(ChangeType.Saved, result.ModifiedDocuments, options).AnyContext();
            if (IsCacheEnabled)
                await InvalidateCacheAsync(id).AnyContext();

            if (options.ShouldNotify())
                await PublishChangeTypeMessageAsync(ChangeType.Saved, result.ModifiedDocuments[0]).AnyContext();
        }

        if (result.HasErrors)
        {
            if (result.ConflictIds.Count > 0)
                throw new VersionConflictDocumentException($"Version conflict patching document {index}/{id.Value} after {result.Rounds} attempts");

            ThrowForPatchErrors(result, "patching");
        }

        return actionModified && result.ModifiedDocuments.Count > 0;
    }

    /// <summary>
    /// Applies a client-side patch to a copy of the hit's document. Returns <c>null</c> when an action patch makes no change.
    /// The version stays the one the document was read at; the write response sets the new version.
    /// </summary>
    private T? ApplyClientPatch(FindHit<T> hit, IPatchOperation operation, bool singleDocument)
    {
        var source = hit.Document ?? throw new DocumentException($"Document {hit.GetIndex()}/{hit.Id} returned no source");
        var serializer = _client.ElasticsearchClientSettings.SourceSerializer;
        T? patched;

        switch (operation)
        {
            case JsonPatch jsonOperation:
            {
                // Version is not part of _source, so a patch targeting /version has no effect.
                JsonNode? target = JsonNode.Parse(serializer.SerializeToString(source))
                    ?? throw new DocumentException("Document JSON parsed to null; cannot apply JSON patch.");
                new JsonPatcher().Patch(ref target, jsonOperation.Patch);
                if (target is null)
                    throw new DocumentException("JSON patch produced a null document.");

                if (singleDocument)
                    ApplyDateTracking(target);

                using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(target.ToJsonString()));
                patched = serializer.Deserialize<T>(stream) ?? throw new DocumentException("JSON patch produced a null document.");
                if (!singleDocument && HasDateTracking)
                    SetDocumentDates(patched, ElasticIndex.Configuration.TimeProvider);
                break;
            }
            case ActionPatch<T> actionPatch:
            {
                bool actionModified = false;
                foreach (var action in actionPatch.Actions)
                    actionModified |= action?.Invoke(source) ?? false;

                if (!actionModified)
                    return null;

                if (HasDateTracking)
                    SetDocumentDates(source, ElasticIndex.Configuration.TimeProvider);
                patched = source;
                break;
            }
            case PartialPatch partialOperation:
            {
                var sourceNode = JsonNode.Parse(serializer.SerializeToString(source));
                var partialNode = JsonNode.Parse(serializer.SerializeToString(partialOperation.Document));
                if (sourceNode is JsonObject sourceObj && partialNode is JsonObject partialObj)
                {
                    foreach (var prop in partialObj)
                        sourceObj[prop.Key] = prop.Value?.DeepClone();
                }

                if (sourceNode is null)
                    throw new DocumentException("Failed to parse source document JSON for partial merge.");

                using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(sourceNode.ToJsonString()));
                patched = serializer.Deserialize<T>(stream) ?? throw new DocumentException("Partial merge produced a null document.");
                break;
            }
            default:
                throw new ArgumentException("Unknown operation type", nameof(operation));
        }

        patched.SetDocumentVersion(hit.GetElasticVersion());
        return patched;
    }

    public Task<long> PatchAsync(Ids ids, IPatchOperation operation, CommandOptionsDescriptor<T>? options)
    {
        return PatchAsync(ids, operation, options?.Configure());
    }

    /// <summary>
    /// Patches several documents by id.
    /// </summary>
    /// <remarks>
    /// <para>Script and partial patches use one bulk Update API request; Elasticsearch re-runs each update on the latest
    /// document up to <c>Retry(n)</c> times on a conflict. JSON, action and pipeline partial patches are applied per batch
    /// and conflicting documents are re-read with one multi-get, re-patched and written again in grouped rounds, up to
    /// <c>Retry(n)</c> rounds. Items rejected with HTTP 429, 502, 503 or 504 are retried in the same rounds.</para>
    /// <para>Successful documents are invalidated in the cache and notified before a <see cref="VersionConflictDocumentException"/>
    /// or <see cref="DocumentException"/> is thrown for the documents that still failed. <c>IfVersion</c> is only supported
    /// when patching a single document.</para>
    /// </remarks>
    public virtual async Task<long> PatchAsync(Ids ids, IPatchOperation operation, ICommandOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(ids);
        ArgumentNullException.ThrowIfNull(operation);

        if (ids is { Count: 0 })
            return 0;

        options = ConfigureOptions(options?.As<T>());

        if (ids.Count is 1)
            return await PatchAsync(ids[0], operation, options).AnyContext() ? 1 : 0;

        if (options.HasExpectedVersion())
            throw new ArgumentException("IfVersion is only supported when patching a single document.", nameof(options));

        if (operation is JsonPatch or ActionPatch<T> || (operation is PartialPatch && !String.IsNullOrEmpty(DefaultPipeline)))
            return await PatchAllAsync(NewQuery().Id(ids), operation, options).AnyContext();

        if (operation is not ScriptPatch and not PartialPatch)
            throw new ArgumentException("Unknown operation type", nameof(operation));

        if (operation is ScriptPatch scriptPatchOp)
            operation = ApplyDateTracking(scriptPatchOp);
        else if (operation is PartialPatch partialPatchOp)
            operation = ApplyDateTracking(partialPatchOp);

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

        var targets = ids.Select(id => new UpdateTarget(id.Value, ElasticIndex.GetIndex(id), id.Routing)).ToList();
        var result = await ApplyServerPatchesAsync(targets, operation, operation.GetType().Name, options, options.GetRefreshMode(DefaultConsistency), missingIsSkipped: false).AnyContext();

        if (result.ModifiedIds.Count > 0)
        {
            // ScriptPatch/PartialPatch execute server-side; the modified document is not available.
            // TODO: Invalidate by documents instead of IDs to support custom cache invalidation overrides.
            await OnDocumentsChangedAsync(ChangeType.Saved, EmptyList, options).AnyContext();
            if (IsCacheEnabled)
                await InvalidateCacheAsync(result.ModifiedIds).AnyContext();

            if (options.ShouldNotify())
            {
                var modifiedIds = ids.Where(id => result.ModifiedIds.Contains(id.Value)).ToList();
                var tasks = new List<Task>(modifiedIds.Count);
                foreach (var id in modifiedIds)
                    tasks.Add(PublishChangeTypeMessageAsync(ChangeType.Saved, id));

                await Task.WhenAll(tasks).AnyContext();
            }
        }

        ThrowForPatchErrors(result, "patching");

        return result.ModifiedIds.Count;
    }


    public Task RemoveAsync(Id id, CommandOptionsDescriptor<T>? options)
    {
        return RemoveAsync(id, options?.Configure());
    }

    public Task RemoveAsync(Id id, ICommandOptions? options = null)
    {
        if (String.IsNullOrEmpty(id))
            throw new ArgumentNullException(nameof(id));

        return RemoveAsync((Ids)id, options);
    }

    public Task RemoveAsync(Ids ids, CommandOptionsDescriptor<T>? options)
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

        // The caller did not supply versions, so documents loaded here (possibly from the cache) are removed without a version check.
        await RemoveAsync(documents, options.Clone().SkipVersionCheck()).AnyContext();
    }

    public Task RemoveAsync(T document, CommandOptionsDescriptor<T>? options)
    {
        return RemoveAsync(document, options?.Configure());
    }

    public virtual Task RemoveAsync(T document, ICommandOptions? options = null)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));

        return RemoveAsync(new[] { document }, options);
    }

    public Task RemoveAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T>? options)
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

        bool checkVersion = HasVersion && !options.ShouldSkipVersionCheck();
        if (docs.Count == 1)
        {
            var document = docs.Single();
            string index = ElasticIndex.GetIndex(document);
            var elasticVersion = checkVersion ? ((IVersioned)document).GetElasticVersion() : ElasticDocumentVersion.Empty;
            var policy = CreateWritePolicy(options, retryConflicts: false);
            await policy.ExecuteAsync(async ct =>
            {
                var request = new DeleteRequest(index, document.Id)
                {
                    Refresh = options.GetRefreshMode(DefaultConsistency)
                };

                if (GetParentIdFunc is not null)
                    request.Routing = GetParentIdFunc(document);

                if (!elasticVersion.IsEmpty)
                {
                    request.IfSeqNo = elasticVersion.SequenceNumber;
                    request.IfPrimaryTerm = elasticVersion.PrimaryTerm;
                }

                var response = await _client.DeleteAsync(request, ct).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());

                if (response.IsValidResponse || response.ApiCallDetails.HttpStatusCode is 404)
                    return;

                string? reason = response.ElasticsearchServerError?.Error?.Reason;
                if (response.ApiCallDetails.HttpStatusCode is 409 && ElasticWriteErrors.IsMissingDocumentConflict(reason))
                    return;

                var exception = CreateWriteException(response, "removing", index, document.Id);
                if (exception is VersionConflictDocumentException)
                    LogVersionConflict("Remove", index, document.Id, 1, 0, elasticVersion.ToVersionString(), reason, "Failed");

                throw exception;
            }, DisposedCancellationToken).AnyContext();
        }
        else
        {
            IReadOnlyCollection<T> docsToDelete = docs;
            var allSuccessfulDocs = new List<T>(docs.Count);
            var allFatalIds = new HashSet<string>();
            var allConflictIds = new HashSet<string>();
            BulkResult result = BulkResult.Empty;
            int retryBudget = options.GetRetryCount();

            for (int attempt = 0; attempt <= retryBudget; attempt++)
            {
                var response = await _client.BulkAsync(bulk =>
                {
                    bulk.Refresh(options.GetRefreshMode(DefaultConsistency));
                    foreach (var doc in docsToDelete)
                        bulk.Delete<T>(d =>
                        {
                            d.Id(doc.Id).Index(ElasticIndex.GetIndex(doc));

                            if (GetParentIdFunc is not null)
                                d.Routing(GetParentIdFunc(doc));

                            var elasticVersion = checkVersion ? ((IVersioned)doc).GetElasticVersion() : ElasticDocumentVersion.Empty;
                            if (!elasticVersion.IsEmpty)
                            {
                                d.IfSequenceNumber(elasticVersion.SequenceNumber);
                                d.IfPrimaryTerm(elasticVersion.PrimaryTerm);
                            }
                        });
                }, DisposedCancellationToken).AnyContext();

                _logger.LogRequest(response, options.GetQueryLogLevel());

                result = BulkResult.From(response);

                var successDocs = result.IsSuccess
                    ? docsToDelete.ToList()
                    : docsToDelete.Where(d => result.SuccessfulIds.Contains(d.Id)).ToList();
                allSuccessfulDocs.AddRange(successDocs);
                allFatalIds.UnionWith(result.FatalIds);
                allFatalIds.UnionWith(result.VersioningNotSupportedIds);
                allConflictIds.UnionWith(result.ConflictIds);
                foreach (string id in result.ConflictIds)
                    LogVersionConflict("BulkRemove", result.Errors.GetValueOrDefault(id)?.Index, id, attempt + 1, retryBudget, null, result.Errors.GetValueOrDefault(id)?.Reason, "Failed");

                if (!result.HasRetryableErrors || attempt >= retryBudget)
                    break;

                _logger.LogWarning("Bulk remove had {RetryableCount} transient item failures, retry {Attempt} of {Budget}", result.RetryableIds.Count, attempt + 1, retryBudget);
                docsToDelete = docsToDelete.Where(d => result.RetryableIds.Contains(d.Id)).ToList();
                await ElasticIndex.Configuration.TimeProvider.SafeDelay(GetRetryDelayWithJitter(attempt + 1), DisposedCancellationToken).AnyContext();

                if (DisposedCancellationToken.IsCancellationRequested)
                    break;
            }

            if (allSuccessfulDocs.Count > 0)
                await OnDocumentsRemovedAsync(allSuccessfulDocs, options).AnyContext();

            var finalResult = result with { FatalIds = allFatalIds, ConflictIds = allConflictIds, VersioningNotSupportedIds = new HashSet<string>() };
            if (finalResult.HasErrors)
                ThrowForBulkErrors(finalResult, operationLabel: "removing");

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
    /// Patches all documents matching the query.
    /// </summary>
    /// <remarks>
    /// <para><b>Snapshot semantics.</b> Matching documents are read in batches from a point-in-time snapshot of the query, so
    /// documents that start matching after the operation begins are not patched, and documents changed by another writer
    /// after they were read are handled per patch type as described below. Running the same <c>PatchAllAsync</c> in parallel
    /// patches documents more than once; make scripts idempotent (for example <c>ctx._source.count = params.value</c>
    /// instead of <c>ctx._source.count++</c>) when the operation can be retried or run concurrently.</para>
    /// <para><b>Script and partial patches</b> use the bulk Update API per batch. Elasticsearch re-runs the update on the latest
    /// document up to <c>Retry(n)</c> times (<c>retry_on_conflict</c>, default 10), so concurrent changes are never lost.
    /// Update-by-query is used only when requested with <c>UseUpdateByQuery()</c> and the cache is disabled; it skips
    /// documents changed after its snapshot and throws a <see cref="VersionConflictDocumentException"/> with the count
    /// unless <c>ProceedOnQueryConflicts()</c> is set.</para>
    /// <para><b>JSON, action and pipeline partial patches</b> are applied in memory and written with <c>if_seq_no</c> for
    /// versioned documents. Conflicting documents are re-read together with one multi-get, re-patched and written again in
    /// grouped rounds (up to <c>Retry(n)</c>). <c>SkipVersionCheck()</c> writes without the check.</para>
    /// <para>Documents deleted while the operation runs are skipped. Successful documents are invalidated in the cache and
    /// notified as each batch completes; documents that still fail are reported at the end in a
    /// <see cref="VersionConflictDocumentException"/> or <see cref="DocumentException"/>.</para>
    /// </remarks>
    public virtual async Task<long> PatchAllAsync(IRepositoryQuery query, IPatchOperation operation, ICommandOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(operation);

        if (operation is JsonPatch { Patch: null or { Operations: null or { Count: 0 } } })
            return 0;

        if (operation is ActionPatch<T> { Actions: null or { Count: 0 } })
            return 0;

        if (operation is not (ScriptPatch or PartialPatch or JsonPatch or ActionPatch<T>))
            throw new ArgumentException("Unknown operation type", nameof(operation));

        if (!ElasticIndex.HasMultipleIndexes)
            await ElasticIndex.EnsureIndexAsync(null).AnyContext();

        options = ConfigureOptions(options?.As<T>());
        if (options.HasExpectedVersion())
            throw new ArgumentException("IfVersion is only supported when patching a single document.", nameof(options));

        if (operation is ScriptPatch scriptPatchOp)
            operation = ApplyDateTracking(scriptPatchOp);
        else if (operation is PartialPatch partialPatchOp)
            operation = ApplyDateTracking(partialPatchOp);

        long affectedRecords = 0;
        long queryConflicts = 0;
        bool notifiedPerBatch = false;
        var failures = new PatchBatchResult<T>();
        string operationName = $"PatchAll {operation.GetType().Name}";

        if (operation is ScriptPatch scriptOperation && options.ShouldUseUpdateByQuery() && !IsCacheEnabled)
        {
            (affectedRecords, queryConflicts) = await PatchAllWithUpdateByQueryAsync(query, scriptOperation, options).AnyContext();
        }
        else if (operation is JsonPatch or ActionPatch<T> || (operation is PartialPatch && !String.IsNullOrEmpty(DefaultPipeline)))
        {
            await BatchProcessAsync(query, async results =>
            {
                var batch = await ApplyClientPatchesAsync(results.Hits, h => ApplyClientPatch(h, operation, singleDocument: false), operationName, options, Refresh.False).AnyContext();
                affectedRecords += batch.ModifiedIds.Count;
                if (batch.ModifiedIds.Count > 0)
                    notifiedPerBatch = true;

                await OnBatchPatchedAsync(batch.ModifiedIds, batch.ModifiedDocuments, options).AnyContext();
                failures.MergeFailures(batch);
                return true;
            }, options.Clone()).AnyContext();
        }
        else
        {
            if (HasIdentity && !query.GetIncludes().Contains(_idField!.Value))
                query.Include(_idField!.Value);

            await BatchProcessAsync(query, async results =>
            {
                var targets = results.Hits
                    .Where(h => h.Id is not null)
                    .Select(h => new UpdateTarget(h.Id!, h.GetIndex() ?? ElasticIndex.GetIndex(h.Id!), h.Routing))
                    .ToList();

                var batch = await ApplyServerPatchesAsync(targets, operation, operationName, options, Refresh.False, missingIsSkipped: true).AnyContext();
                affectedRecords += batch.ModifiedIds.Count;
                if (batch.ModifiedIds.Count > 0)
                    notifiedPerBatch = true;

                // TODO: Invalidate by documents instead of IDs to support custom cache invalidation overrides.
                await OnBatchPatchedAsync(batch.ModifiedIds, null, options).AnyContext();
                failures.MergeFailures(batch);
                return true;
            }, options.Clone()).AnyContext();
        }

        if (affectedRecords > 0)
        {
            if (IsCacheEnabled)
                await InvalidateCacheByQueryAsync(query.As<T>()).AnyContext();

            // Empty list: batch callbacks handle per-document cache invalidation for client-side patches.
            // This fires the DocumentsChanged event so subscribers know documents were modified.
            await OnDocumentsChangedAsync(ChangeType.Saved, EmptyList, options).AnyContext();

            if (!notifiedPerBatch)
                await SendQueryNotificationsAsync(ChangeType.Saved, query, options).AnyContext();
        }

        if (queryConflicts > 0)
        {
            if (!options.ShouldProceedOnQueryConflicts())
                throw new VersionConflictDocumentException($"Update by query patched {affectedRecords} documents and skipped {queryConflicts} documents that changed during the operation (version conflicts).");

            _logger.LogWarning("Update by query patched {Affected} documents and skipped {Conflicts} documents with version conflicts", affectedRecords, queryConflicts);
        }

        if (failures.MissingIds.Count > 0)
            _logger.LogInformation("{Operation} skipped {Count} documents that were deleted while the operation ran", operationName, failures.MissingIds.Count);

        ThrowForPatchErrors(failures, "patching");

        return affectedRecords;
    }

    /// <summary>
    /// Invalidates the cache, sends notifications and calls the updated-ids callback once for the documents patched in a batch.
    /// </summary>
    private async Task OnBatchPatchedAsync(IReadOnlyCollection<string> modifiedIds, IReadOnlyCollection<T>? modifiedDocuments, ICommandOptions options)
    {
        if (modifiedIds.Count is 0)
            return;

        if (IsCacheEnabled)
        {
            if (modifiedDocuments is { Count: > 0 })
                await InvalidateCacheAsync(modifiedDocuments).AnyContext();
            else
                await InvalidateCacheAsync(modifiedIds).AnyContext();
        }

        await SendNotificationsAsync(ChangeType.Saved, modifiedIds, options).AnyContext();

        try
        {
            options.GetUpdatedIdsCallback()?.Invoke(modifiedIds.ToList());
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error calling updated ids callback: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Runs a script patch as an asynchronous update-by-query task and waits for it. Returns the number of documents
    /// changed and the number skipped because they changed after the query snapshot was taken.
    /// </summary>
    private async Task<(long Affected, long Conflicts)> PatchAllWithUpdateByQueryAsync(IRepositoryQuery query, ScriptPatch scriptOperation, ICommandOptions options)
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

        var response = await _client.UpdateByQueryAsync(request, DisposedCancellationToken).AnyContext();
        _logger.LogRequest(response, options.GetQueryLogLevel());
        if (!response.IsValidResponse)
            throw new DocumentException(response.GetErrorMessage("Error occurred while patching by query"), response.OriginalException());

        if (response.Task is null)
            throw new DocumentException("Update by query did not return a task identifier.", response.OriginalException());

        var taskId = response.Task.ToString();
        int attempts = 0;
        do
        {
            attempts++;
            var taskRequest = new Tasks.GetTasksRequest(taskId) { WaitForCompletion = false };
            var taskStatus = await _client.Tasks.GetAsync(taskRequest, DisposedCancellationToken).AnyContext();
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

            var status = ReadTaskStatus(taskStatus.Task.Status);
            if (taskStatus.Completed)
            {
                if (taskStatus.Error is not null)
                    throw new DocumentException($"Script operation task ({taskId}) failed: {taskStatus.Error.Type} - {taskStatus.Error.Reason}", taskStatus.OriginalException());

                _logger.LogInformation("Script operation task ({TaskId}) completed: Created: {Created} Updated: {Updated} Deleted: {Deleted} Conflicts: {Conflicts} Total: {Total}", taskId, status.Created, status.Updated, status.Deleted, status.VersionConflicts, status.Total);
                return (status.Created + status.Updated + status.Deleted, status.VersionConflicts);
            }

            _logger.LogDebug("Checking script operation task ({TaskId}) status: Created: {Created} Updated: {Updated} Deleted: {Deleted} Conflicts: {Conflicts} Total: {Total}", taskId, status.Created, status.Updated, status.Deleted, status.VersionConflicts, status.Total);
            var delay = TimeSpan.FromSeconds(attempts <= 5 ? 1 : 5);
            await ElasticIndex.Configuration.TimeProvider.SafeDelay(delay, DisposedCancellationToken).AnyContext();
        } while (!DisposedCancellationToken.IsCancellationRequested);

        return (0, 0);
    }

    private readonly record struct QueryTaskStatus(long Total, long Created, long Updated, long Deleted, long VersionConflicts);

    /// <summary>
    /// Reads task status counters. The status is <c>object?</c> and may be a <see cref="JsonElement"/> or a dictionary
    /// depending on the serializer configuration.
    /// </summary>
    private static QueryTaskStatus ReadTaskStatus(object? status)
    {
        return status switch
        {
            JsonElement element => new QueryTaskStatus(
                element.TryGetProperty("total", out var total) ? total.GetInt64() : 0,
                element.TryGetProperty("created", out var created) ? created.GetInt64() : 0,
                element.TryGetProperty("updated", out var updated) ? updated.GetInt64() : 0,
                element.TryGetProperty("deleted", out var deleted) ? deleted.GetInt64() : 0,
                element.TryGetProperty("version_conflicts", out var conflicts) ? conflicts.GetInt64() : 0),
            IDictionary<string, object> dict => new QueryTaskStatus(
                dict.TryGetValue("total", out var total) ? Convert.ToInt64(total) : 0,
                dict.TryGetValue("created", out var created) ? Convert.ToInt64(created) : 0,
                dict.TryGetValue("updated", out var updated) ? Convert.ToInt64(updated) : 0,
                dict.TryGetValue("deleted", out var deleted) ? Convert.ToInt64(deleted) : 0,
                dict.TryGetValue("version_conflicts", out var conflicts) ? Convert.ToInt64(conflicts) : 0),
            _ => default
        };
    }

    public Task<long> RemoveAllAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T>? options = null)
    {
        return RemoveAllAsync(query.Configure(), options?.Configure());
    }

    /// <summary>
    /// Removes all documents matching the query.
    /// </summary>
    /// <remarks>
    /// <para>When the cache is enabled or remove handlers are registered, matching documents are read in batches and removed
    /// by id without a version check. Otherwise a server-side delete-by-query runs against a snapshot of the matching
    /// documents: a document changed by another writer after the snapshot is skipped as a version conflict. Deleting is
    /// idempotent, so the query is re-run up to <c>Retry(n)</c> more times (default 10) with backoff until no conflicts remain.
    /// If conflicts are still left, a <see cref="VersionConflictDocumentException"/> is thrown unless
    /// <c>ProceedOnQueryConflicts()</c> is set. Documents that start matching after the last pass are not removed.</para>
    /// </remarks>
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

            var removeOptions = options.Clone().SkipVersionCheck();
            if (removeOptions.GetConsistency(DefaultConsistency) != Consistency.Eventual)
                removeOptions.Consistency(Consistency.Eventual);

            return await BatchProcessAsync(query, async results =>
            {
                await RemoveAsync(results.Documents, removeOptions).AnyContext();
                return true;
            }, options).AnyContext();
        }

        // Elasticsearch's delete-by-query snapshots document versions when it begins and skips any document
        // whose version changed before the delete executes (counted as a version conflict via Conflicts.Proceed).
        // Unlike single-document updates, delete-by-query does not support retry_on_conflict, so we re-run the
        // query at the application level until no conflicts remain or the retry budget is exhausted.
        var esQuery = await ElasticIndex.QueryBuilder.BuildQueryAsync(query, options, new SearchRequestDescriptor<T>()).AnyContext();
        var indices = Indices.Index(String.Join(",", ElasticIndex.GetIndexesByQuery(query)));
        int maxAttempts = options.GetRetryCount() + 1;
        int attempt = 0;
        long totalDeleted = 0;
        long lastConflicts;

        do
        {
            attempt++;
            var response = await _client.DeleteByQueryAsync(new DeleteByQueryRequest(indices)
            {
                // Force a refresh on retry passes so the next snapshot observes the resolved versions and converges.
                Refresh = attempt > 1 || options.GetRefreshMode(DefaultConsistency) != Refresh.False,
                Conflicts = Conflicts.Proceed,
                IgnoreUnavailable = true,
                Query = esQuery
            }, DisposedCancellationToken).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());

            if (!response.IsValidResponse)
                throw new DocumentException(response.GetErrorMessage("Error removing documents"), response.OriginalException());

            totalDeleted += response.Deleted ?? 0;
            lastConflicts = response.VersionConflicts ?? 0;

            if (lastConflicts > 0 && attempt < maxAttempts)
            {
                _logger.LogWarning("RemoveAll pass {Attempt} of {MaxAttempts} skipped {Conflicts} documents with version conflicts, retrying", attempt, maxAttempts, lastConflicts);
                await ElasticIndex.Configuration.TimeProvider.SafeDelay(GetRetryDelayWithJitter(attempt), DisposedCancellationToken).AnyContext();

                // Refresh right before the next pass so its snapshot sees the versions written by concurrent writers.
                var refreshResponse = await _client.Indices.RefreshAsync(indices, cancellationToken: DisposedCancellationToken).AnyContext();
                if (!refreshResponse.IsValidResponse)
                    _logger.LogErrorRequest(refreshResponse, "Failed to refresh indices before delete-by-query retry");
            }
        }
        while (lastConflicts > 0 && attempt < maxAttempts && !DisposedCancellationToken.IsCancellationRequested);

        if (totalDeleted > 0)
        {
            if (IsCacheEnabled)
                await InvalidateCacheByQueryAsync(query.As<T>()).AnyContext();

            await OnDocumentsRemovedAsync(EmptyList, options).AnyContext();
            await SendQueryNotificationsAsync(ChangeType.Removed, query, options).AnyContext();
        }

        if (lastConflicts > 0)
        {
            if (!options.ShouldProceedOnQueryConflicts())
                throw new VersionConflictDocumentException($"RemoveAll removed {totalDeleted} documents but {lastConflicts} documents still had version conflicts after {attempt} passes.");

            _logger.LogWarning("RemoveAll: {Deleted} records removed after {Attempts} attempts ({Conflicts} unresolved version conflicts)", totalDeleted, attempt, lastConflicts);
        }

        return totalDeleted;
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
        var policy = CreateWritePolicy(options, retryConflicts: false);
        await policy.ExecuteAsync(async _ => await IndexSingleDocumentCoreAsync(document, isCreateOperation, options).AnyContext(), DisposedCancellationToken).AnyContext();
        return BulkResult.Empty;
    }

    /// <summary>
    /// Indexes one document without retries. Version-checked writes send <c>if_seq_no</c>/<c>if_primary_term</c> from
    /// <see cref="IVersioned.Version"/> and update it from the response.
    /// </summary>
    private async Task IndexSingleDocumentCoreAsync(T document, bool isCreateOperation, ICommandOptions options)
    {
        string index = ElasticIndex.GetIndex(document);
        var elasticVersion = HasVersion && !isCreateOperation && !options.ShouldSkipVersionCheck()
            ? ((IVersioned)document).GetElasticVersion()
            : ElasticDocumentVersion.Empty;

        if (HasVersion && !isCreateOperation && !options.ShouldSkipVersionCheck())
            ThrowIfMissingVersion(elasticVersion, index, document.Id);

        var response = await _client.IndexAsync(document, i =>
        {
            i.OpType(isCreateOperation ? OpType.Create : OpType.Index);
            if (DefaultPipeline is not null)
                i.Pipeline(DefaultPipeline);
            i.Refresh(options.GetRefreshMode(DefaultConsistency));

            if (GetParentIdFunc is not null)
                i.Routing(GetParentIdFunc(document));

            i.Index(index);

            if (!elasticVersion.IsEmpty)
            {
                i.IfPrimaryTerm(elasticVersion.PrimaryTerm);
                i.IfSeqNo(elasticVersion.SequenceNumber);
            }
        }, DisposedCancellationToken).AnyContext();
        _logger.LogRequest(response, options.GetQueryLogLevel());

        if (!response.IsValidResponse)
        {
            var exception = CreateWriteException(response, isCreateOperation ? "adding" : "saving", index, document.Id, isCreateOperation);
            if (exception is VersionConflictDocumentException)
                LogVersionConflict(isCreateOperation ? "Add" : "Save", index, document.Id, 1, 0, elasticVersion.ToVersionString(), response.ElasticsearchServerError?.Error?.Reason, "Failed");

            throw exception;
        }

        if (HasVersion)
            document.SetDocumentVersion(response.GetElasticVersion());
    }

    private async Task<BulkResult> IndexDocumentsBulkAsync(IReadOnlyCollection<T> documents, bool isCreateOperation, ICommandOptions options)
    {
        bool checkVersion = HasVersion && !isCreateOperation && !options.ShouldSkipVersionCheck();
        if (checkVersion)
        {
            foreach (var document in documents)
                ThrowIfMissingVersion(((IVersioned)document).GetElasticVersion(), ElasticIndex.GetIndex(document), document.Id);
        }

        IReadOnlyCollection<T> docsToIndex = documents;
        var allSuccessfulIds = new HashSet<string>();
        var allConflictIds = new HashSet<string>();
        var allFatalIds = new HashSet<string>();
        var allUnsupportedIds = new HashSet<string>();
        var allErrors = new Dictionary<string, BulkItemError>();
        BulkResult result = BulkResult.Empty;
        int retryBudget = options.GetRetryCount();

        for (int attempt = 0; attempt <= retryBudget; attempt++)
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

                var indexOperation = new BulkIndexOperation<T>(d) { Pipeline = DefaultPipeline };
                if (routing != null)
                    indexOperation.Routing = routing;
                indexOperation.Index = index;
                if (checkVersion)
                {
                    var elasticVersion = ((IVersioned)d).GetElasticVersion();
                    indexOperation.IfSequenceNumber = elasticVersion.SequenceNumber;
                    indexOperation.IfPrimaryTerm = elasticVersion.PrimaryTerm;
                }

                return (IBulkOperation)indexOperation;
            }).ToList();
            bulkRequest.Operations = list;
            bulkRequest.Refresh = options.GetRefreshMode(DefaultConsistency);

            var response = await _client.BulkAsync(bulkRequest, DisposedCancellationToken).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());

            result = BulkResult.From(response);

            if (result.HasTransportError)
            {
                bool transient = result.TransportStatusCode is null || ElasticWriteErrors.IsTransientStatus(result.TransportStatusCode);
                if (transient && attempt < retryBudget)
                {
                    _logger.LogWarning("Transient bulk {Operation} failure (status {StatusCode}), retry {Attempt} of {Budget}", isCreateOperation ? "add" : "save", result.TransportStatusCode, attempt + 1, retryBudget);
                    await ElasticIndex.Configuration.TimeProvider.SafeDelay(GetRetryDelayWithJitter(attempt + 1), DisposedCancellationToken).AnyContext();
                    continue;
                }

                return new BulkResult
                {
                    SuccessfulIds = allSuccessfulIds,
                    ConflictIds = allConflictIds,
                    FatalIds = allFatalIds,
                    VersioningNotSupportedIds = allUnsupportedIds,
                    Errors = allErrors,
                    TransportError = result.TransportError,
                    TransportException = result.TransportException,
                    TransportStatusCode = result.TransportStatusCode
                };
            }

            if (HasVersion)
            {
                var documentsById = new Dictionary<string, T>();
                foreach (var d in docsToIndex.Where(d => !String.IsNullOrEmpty(d.Id)))
                    documentsById.TryAdd(d.Id, d);
                foreach (var hit in response.Items)
                {
                    if (!hit.IsValid || hit.Id is null || !documentsById.TryGetValue(hit.Id, out var document))
                        continue;

                    document.SetDocumentVersion(hit.GetElasticVersion());
                }
            }

            allSuccessfulIds.UnionWith(result.SuccessfulIds);
            allConflictIds.UnionWith(result.ConflictIds);
            allConflictIds.UnionWith(result.MissingIds);
            allFatalIds.UnionWith(result.FatalIds);
            allUnsupportedIds.UnionWith(result.VersioningNotSupportedIds);
            foreach (var error in result.Errors)
                allErrors[error.Key] = error.Value;

            foreach (string id in result.ConflictIds)
                LogVersionConflict(isCreateOperation ? "BulkAdd" : "BulkSave", result.Errors.GetValueOrDefault(id)?.Index, id, attempt + 1, retryBudget, null, result.Errors.GetValueOrDefault(id)?.Reason, "Failed");

            if (!result.HasRetryableErrors || attempt >= retryBudget)
                break;

            _logger.LogWarning("Bulk {Operation} had {RetryableCount} transient item failures, retry {Attempt} of {Budget}", isCreateOperation ? "add" : "save", result.RetryableIds.Count, attempt + 1, retryBudget);
            docsToIndex = docsToIndex.Where(d => result.RetryableIds.Contains(d.Id)).ToList();
            await ElasticIndex.Configuration.TimeProvider.SafeDelay(GetRetryDelayWithJitter(attempt + 1), DisposedCancellationToken).AnyContext();

            if (DisposedCancellationToken.IsCancellationRequested)
                break;
        }

        if (allUnsupportedIds.Count > 0)
            _logger.LogError("Bulk {Operation} failed for {Count} documents because the index has sequence numbers disabled", isCreateOperation ? "add" : "save", allUnsupportedIds.Count);

        return new BulkResult
        {
            SuccessfulIds = allSuccessfulIds,
            ConflictIds = allConflictIds,
            RetryableIds = result.RetryableIds,
            FatalIds = allFatalIds,
            VersioningNotSupportedIds = allUnsupportedIds,
            Errors = allErrors
        };
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
        int totalErrors = result.ConflictIds.Count + result.RetryableIds.Count + result.FatalIds.Count + result.VersioningNotSupportedIds.Count;

        if (result.VersioningNotSupportedIds.Count > 0)
        {
            string index = result.Errors.GetValueOrDefault(result.VersioningNotSupportedIds.First())?.Index ?? "unknown";
            throw new DocumentVersioningNotSupportedException(ElasticWriteErrors.VersioningNotSupportedMessage(index));
        }

        if (result.HasConflicts)
        {
            if (isCreateOperation)
                throw new DuplicateDocumentException($"Error {label} documents: {result.ConflictIds.Count} duplicates ({totalErrors} total errors)");
            throw new VersionConflictDocumentException($"Error {label} documents: {result.ConflictIds.Count} conflicts ({totalErrors} total errors). Ids: {FormatIds(result.ConflictIds)}");
        }

        string? reason = result.Errors.Values.Select(e => e.Reason).FirstOrDefault(r => r is not null);
        throw new DocumentException($"Error {label} documents: {totalErrors} failures{(reason is null ? "" : $". First error: {reason}")}");
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

    private Task SendNotificationsAsync(ChangeType changeType, IReadOnlyCollection<string> ids, ICommandOptions options)
    {
        if (!NotificationsEnabled || !options.ShouldNotify())
            return Task.CompletedTask;

        var delay = NotificationDeliveryDelay;
        if (ids.Count is 0)
        {
            return PublishMessageAsync(new EntityChanged
            {
                ChangeType = changeType,
                Type = EntityTypeName
            }, delay);
        }

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
