using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
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
        _writeRunner = new Lazy<WriteRoundRunner>(CreateWriteRunner, LazyThreadSafetyMode.ExecutionAndPublication);
        _lazyWriteClient = new Lazy<ElasticsearchClient>(CreateWriteClient, LazyThreadSafetyMode.ExecutionAndPublication);
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

    /// <summary>
    /// Adds documents with <c>op_type=create</c>. A document whose id already exists fails with a
    /// <see cref="DuplicateDocumentException"/>. HTTP 429, structured Elasticsearch 503 errors and unsent requests are retried up to <c>Retry(n)</c> times.
    /// Documents that were added are notified and cached before the exception for the others is thrown.
    /// <c>IfVersion</c> is not supported (a new document has no version to check) and throws an <see cref="ArgumentException"/>.
    /// </summary>
    public virtual async Task AddAsync(IEnumerable<T> documents, ICommandOptions? options = null)
    {
        var docs = documents?.ToList();
        if (docs == null || docs.Any(d => d == null))
            throw new ArgumentNullException(nameof(documents));

        if (docs.Count == 0)
            return;

        options = ConfigureOptions(options?.As<T>());
        ThrowIfExpectedVersion(options);

        await OnDocumentsAddingAsync(docs, options).AnyContext();

        if (options.ShouldValidate())
            foreach (var doc in docs)
                await ValidateAndThrowAsync(doc).AnyContext();

        var run = await IndexDocumentsAsync(docs, true, options).AnyContext();
        var successDocs = run.Succeeded.Select(i => i.Document).ToList();
        if (successDocs.Count > 0)
        {
            await OnDocumentsAddedAsync(successDocs, options).AnyContext();
            if (IsCacheEnabled && options.ShouldUseCache())
                await AddDocumentsToCacheAsync(successDocs, options, false).AnyContext();
        }

        WriteExceptionMapper.ThrowIfFailed(run, "adding", MissingDocumentPolicy.Conflict, i => i.IsCreate);
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

    /// <summary>
    /// Saves documents. An <see cref="IVersioned"/> document is written with <c>if_seq_no</c>/<c>if_primary_term</c> from
    /// its <see cref="IVersioned.Version"/> and fails with a <see cref="VersionConflictDocumentException"/> if the stored
    /// document changed or was deleted. One with no version has never been read, so it is created (<c>op_type=create</c>)
    /// and fails with a <see cref="DuplicateDocumentException"/> if the id already exists. <c>SkipVersionCheck()</c>, and
    /// documents that don't implement <see cref="IVersioned"/>, are indexed unconditionally.
    /// </summary>
    /// <remarks>HTTP 429, structured Elasticsearch 503 errors and unsent requests are retried up to <c>Retry(n)</c> times; conflicts are never retried.
    /// Documents that were saved are notified and cached before the exception for the others is thrown. <c>IfVersion</c> is
    /// not supported and throws an <see cref="ArgumentException"/>: a save checks the <see cref="IVersioned.Version"/> of the
    /// document passed in.</remarks>
    public virtual async Task SaveAsync(IEnumerable<T> documents, ICommandOptions? options = null)
    {
        var docs = documents?.ToList();
        if (docs == null || docs.Any(d => d == null))
            throw new ArgumentNullException(nameof(documents));

        if (docs.Count == 0)
            return;

        if (docs.Any(d => String.IsNullOrEmpty(d.Id)))
            throw new ArgumentException("Id must be set when calling Save.");

        options = ConfigureOptions(options?.As<T>());
        ThrowIfExpectedVersion(options);

        var originalDocuments = await GetOriginalDocumentsAsync(docs, options).AnyContext();
        await OnDocumentsSavingAsync(docs, originalDocuments, options).AnyContext();

        if (options.ShouldValidate())
            foreach (var doc in docs)
                await ValidateAndThrowAsync(doc).AnyContext();

        var run = await IndexDocumentsAsync(docs, false, options).AnyContext();
        var successDocs = run.Succeeded.Select(i => i.Document).ToList();
        if (successDocs.Count > 0)
        {
            var successKeys = successDocs.Select(GetDocumentLocationKey).ToHashSet();
            var successOriginals = originalDocuments.Where(o => successKeys.Contains(GetDocumentLocationKey(o))).ToList();
            await OnDocumentsSavedAsync(successDocs, successOriginals, options).AnyContext();
            if (IsCacheEnabled && options.ShouldUseCache())
                await AddDocumentsToCacheAsync(successDocs, options, false).AnyContext();
        }

        WriteExceptionMapper.ThrowIfFailed(run, "saving", MissingDocumentPolicy.Conflict, i => i.IsCreate);
    }

    public Task<bool> PatchAsync(Id id, IPatchOperation operation, CommandOptionsDescriptor<T>? options)
    {
        return PatchAsync(id, operation, options?.Configure());
    }

    /// <summary>
    /// Patches one document by id. The <see cref="PatchStrategy"/> depends on the patch type and default pipeline; see
    /// the versioning guide for the full state machine.
    /// </summary>
    /// <remarks>
    /// <para><b>Script and partial patches</b> use the Update API; Elasticsearch re-runs the update on the latest document up
    /// to <c>Retry(n)</c> times (<c>retry_on_conflict</c>, default 10). With a default pipeline a script patch runs as
    /// update-by-query on the id, re-run on a conflict.</para>
    /// <para><b>JSON and action patches</b> (and partial patches with a default pipeline) read the document, patch it in
    /// memory and write it back with <c>if_seq_no</c>/<c>if_primary_term</c> for versioned documents, re-reading on conflicts.</para>
    /// <para><c>IfVersion(version)</c> requires the stored document to have that version: every write is checked against
    /// it (also for entities that don't implement <see cref="IVersioned"/>), the document is never re-read, and a mismatch
    /// throws a <see cref="VersionConflictDocumentException"/>. It can't be combined with <c>SkipVersionCheck()</c>, or used
    /// for a script patch with a default pipeline.</para>
    /// <para>HTTP 429, structured Elasticsearch 503 errors and unsent requests are retried up to <c>Retry(n)</c> times. A missing document throws a
    /// <see cref="DocumentNotFoundException"/>. Client-side patches preserve the read's concurrency metadata regardless
    /// of the model's JSON representation of <see cref="IVersioned.Version"/>.</para>
    /// </remarks>
    public virtual async Task<bool> PatchAsync(Id id, IPatchOperation operation, ICommandOptions? options = null)
    {
        if (String.IsNullOrEmpty(id.Value))
            throw new ArgumentNullException(nameof(id));

        ArgumentNullException.ThrowIfNull(operation);
        if (IsEmptyPatch(operation))
            return false;

        options = ConfigureOptions(options?.As<T>());
        var expectedVersion = GetExpectedVersion(options);
        operation = PreparePatch(operation);
        var strategy = PatchStrategyResolver.Resolve<T>(operation, !String.IsNullOrEmpty(DefaultPipeline), false);
        if (strategy is PatchStrategy.UpdateByQueryPerId && expectedVersion is not null)
            throw new ArgumentException("IfVersion is not supported for ScriptPatch when a default pipeline is configured.", nameof(options));

        await ElasticIndex.EnsureIndexAsync(id).AnyContext();

        var refresh = options.GetRefreshMode(DefaultConsistency);
        string operationName = operation.GetType().Name;
        if (strategy is PatchStrategy.ClientSide)
        {
            var (hits, readFailures) = await ReadForPatchAsync([id], options).AnyContext();
            var run = WriteRunResult<IndexItem>.Merge(readFailures, await PatchClientSideAsync(hits, operation, expectedVersion, options, refresh, operationName).AnyContext());
            var patched = run.Succeeded.Select(i => i.Document).ToList();
            if (patched.Count > 0)
            {
                await OnDocumentsChangedAsync(ChangeType.Saved, patched, options).AnyContext();
                if (IsCacheEnabled)
                    await InvalidateCacheAsync(id.Value).AnyContext();

                if (options.ShouldNotify())
                    await PublishChangeTypeMessageAsync(ChangeType.Saved, patched[0]).AnyContext();
            }

            WriteExceptionMapper.ThrowIfFailed(run, "patching", MissingDocumentPolicy.NotFound);
            return patched.Count > 0;
        }

        var item = new UpdateItem(new DocumentKey(ElasticIndex.GetIndex(id), id.Value), id.Routing, expectedVersion ?? ElasticDocumentVersion.Empty);
        var serverRun = await PatchServerSideAsync([item], operation, strategy, options, refresh, operationName).AnyContext();
        bool modified = serverRun.Succeeded.Count > 0;
        if (modified)
        {
            // Server-side patches don't return the document.
            await OnDocumentsChangedAsync(ChangeType.Saved, EmptyList, options).AnyContext();
            if (IsCacheEnabled)
                await InvalidateCacheAsync(id.Value).AnyContext();

            if (options.ShouldNotify())
                await PublishChangeTypeMessageAsync(ChangeType.Saved, id.Value).AnyContext();
        }

        WriteExceptionMapper.ThrowIfFailed(serverRun, "patching", MissingDocumentPolicy.NotFound);
        return modified;
    }

    private static bool IsEmptyPatch(IPatchOperation operation)
    {
        return operation is JsonPatch { Patch: null or { Operations: null or { Count: 0 } } } or ActionPatch<T> { Actions: null or { Count: 0 } };
    }

    /// <summary>Validates the operation type and adds the updated date to script and partial patches.</summary>
    private IPatchOperation PreparePatch(IPatchOperation operation)
    {
        return operation switch
        {
            ScriptPatch script => ApplyDateTracking(script),
            PartialPatch partial => ApplyDateTracking(partial),
            JsonPatch or ActionPatch<T> => operation,
            _ => throw new ArgumentException("Unknown operation type", nameof(operation))
        };
    }

    private static ElasticDocumentVersion? GetExpectedVersion(ICommandOptions options)
    {
        if (!options.HasExpectedVersion())
            return null;

        if (options.ShouldSkipVersionCheck())
            throw new ArgumentException("IfVersion can't be combined with SkipVersionCheck.", nameof(options));

        var version = ElasticDocumentVersion.Parse(options.GetExpectedVersion());
        if (version.IsEmpty)
            throw new ArgumentException($"IfVersion value \"{options.GetExpectedVersion()}\" is not a valid document version.", nameof(options));

        return version;
    }

    private static void ThrowIfExpectedVersion(ICommandOptions options)
    {
        if (options.HasExpectedVersion())
            throw new ArgumentException("IfVersion applies to PatchAsync(id) only. SaveAsync and RemoveAsync use the Version of the document passed in; to check an expected version when patching several documents, patch each one by id.", nameof(options));
    }

    public Task<long> PatchAsync(Ids ids, IPatchOperation operation, CommandOptionsDescriptor<T>? options)
    {
        return PatchAsync(ids, operation, options?.Configure());
    }

    /// <summary>
    /// Patches several documents by id, with the same <see cref="PatchStrategy"/> as <see cref="PatchAsync(Id, IPatchOperation, ICommandOptions)"/>.
    /// </summary>
    /// <remarks>
    /// <para>Script and partial patches use one bulk Update API request per round. JSON, action and pipeline partial patches
    /// read the documents, write them with one bulk request, and re-read conflicting documents together in grouped rounds,
    /// up to <c>Retry(n)</c>. JSON and action patches search to preserve repository query and soft-delete filters;
    /// ids excluded from that initial selection are skipped.
    /// Pipeline partial patches use real-time multi-get for known index and routing locations, searching only unresolved
    /// locations. Script patches with a default pipeline run update-by-query per id.</para>
    /// <para>Repeated id and routing inputs are deduplicated. Search-discovered documents in different indexes remain separate matches;
    /// the same id with different routing values is retained because those values can identify different documents.</para>
    /// <para>Every document ends with an outcome. Successful documents are invalidated in the cache and notified before a
    /// <see cref="VersionConflictDocumentException"/>, <see cref="DocumentNotFoundException"/> or <see cref="DocumentException"/>
    /// is thrown for the rest. <c>IfVersion</c> is not supported here, even for one id.</para>
    /// </remarks>
    public virtual async Task<long> PatchAsync(Ids ids, IPatchOperation operation, ICommandOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(ids);
        ArgumentNullException.ThrowIfNull(operation);

        options = ConfigureOptions(options?.As<T>());
        ThrowIfExpectedVersion(options);
        if (ids is { Count: 0 } || IsEmptyPatch(operation))
            return 0;

        if (ids.Count is 1)
            return await PatchAsync(ids[0], operation, options).AnyContext() ? 1 : 0;

        ids = new Ids(ids.Distinct());

        operation = PreparePatch(operation);
        if (!ElasticIndex.HasMultipleIndexes)
            await ElasticIndex.EnsureIndexAsync(null).AnyContext();

        var strategy = PatchStrategyResolver.Resolve<T>(operation, !String.IsNullOrEmpty(DefaultPipeline), false);
        var refresh = options.GetRefreshMode(DefaultConsistency);
        string operationName = operation.GetType().Name;
        if (strategy is PatchStrategy.ClientSide)
        {
            var (hits, readFailures) = await ReadForPatchAsync(ids, options, allowSearch: true, preferRealtime: operation is PartialPatch).AnyContext();
            var run = WriteRunResult<IndexItem>.Merge(readFailures, await PatchClientSideAsync(hits, operation, null, options, refresh, operationName, useDocumentDates: true).AnyContext());
            var patched = run.Succeeded;
            await OnPatchedAsync(patched.Select(i => i.Key.Id).ToList(), patched.Select(i => i.Document).ToList(), options).AnyContext();
            WriteExceptionMapper.ThrowIfFailed(run, "patching", MissingDocumentPolicy.NotFound);
            return patched.Count;
        }

        var items = ids.Select(id => new UpdateItem(new DocumentKey(ElasticIndex.GetIndex(id), id.Value), id.Routing, ElasticDocumentVersion.Empty)).ToList();
        var serverRun = await PatchServerSideAsync(items, operation, strategy, options, refresh, operationName).AnyContext();
        var modifiedIds = serverRun.Succeeded.Select(i => i.Key.Id).ToList();
        await OnPatchedAsync(modifiedIds, null, options).AnyContext();
        WriteExceptionMapper.ThrowIfFailed(serverRun, "patching", MissingDocumentPolicy.NotFound);
        return modifiedIds.Count;
    }

    /// <summary>
    /// Fires the change event, invalidates the cache, sends notifications and calls the updated-ids callback once for
    /// documents that were patched. Server-side patches pass no documents.
    /// </summary>
    private async Task OnPatchedAsync(IReadOnlyCollection<string> modifiedIds, IReadOnlyCollection<T>? modifiedDocuments, ICommandOptions options)
    {
        if (modifiedIds.Count is 0)
            return;

        await OnDocumentsChangedAsync(ChangeType.Saved, modifiedDocuments ?? EmptyList, options).AnyContext();
        if (IsCacheEnabled)
        {
            if (modifiedDocuments is { Count: > 0 })
                await InvalidateCacheAsync(modifiedDocuments).AnyContext();
            else
                await InvalidateCacheAsync(modifiedIds).AnyContext();
        }

        if (modifiedDocuments is { Count: > 0 })
            await SendNotificationsAsync(ChangeType.Saved, modifiedDocuments, options).AnyContext();
        else
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

    /// <summary>Removes documents by id, without a version check (the caller supplied no versions). <c>IfVersion</c> is not supported and throws an <see cref="ArgumentException"/>.</summary>
    public async Task RemoveAsync(Ids ids, ICommandOptions? options = null)
    {
        if (ids == null)
            throw new ArgumentNullException(nameof(ids));

        options = ConfigureOptions(options?.As<T>());
        ThrowIfExpectedVersion(options);
        if (IsCacheEnabled)
            options = options.ReadCache();

        // TODO: If not OriginalsEnabled then just delete by id
        // TODO: Delete by id using GetIndexById and id.Routing if its a child doc
        var documents = await GetByIdsAsync(ids, options).AnyContext();
        if (documents == null)
            return;

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

    /// <summary>
    /// Removes documents. <see cref="IVersioned"/> documents are deleted with <c>if_seq_no</c>/<c>if_primary_term</c> unless
    /// <c>SkipVersionCheck()</c> is set, and fail with a <see cref="VersionConflictDocumentException"/> if they changed. A
    /// versioned document with no version can't be checked (a delete has no create fallback), so it throws a
    /// <see cref="DocumentException"/> before anything is deleted. A document that is already gone is not an error: it is
    /// evicted from the cache but, because this call didn't delete it, <see cref="IRepository{T}.DocumentsRemoved"/>,
    /// <see cref="IRepository{T}.DocumentsChanged"/> and the <c>EntityChanged</c> notification are raised only for the
    /// documents this call actually deleted.
    /// </summary>
    /// <remarks>HTTP 429, structured Elasticsearch 503 errors and unsent requests are retried up to <c>Retry(n)</c> times. Deleted documents are notified
    /// before the exception for the others is thrown. <c>IfVersion</c> is not supported and throws an
    /// <see cref="ArgumentException"/>: a remove checks the <see cref="IVersioned.Version"/> of the document passed in.</remarks>
    public virtual async Task RemoveAsync(IEnumerable<T> documents, ICommandOptions? options = null)
    {
        var docs = documents?.ToList();
        if (docs == null || docs.Any(d => d == null))
            throw new ArgumentNullException(nameof(documents));

        if (docs.Count == 0)
            return;

        options = ConfigureOptions(options?.As<T>());
        ThrowIfExpectedVersion(options);
        if (IsCacheEnabled && options.HasCacheKey())
            throw new ArgumentException("Cache key can't be set when calling RemoveAsync");

        var items = CreateDeleteItems(docs, options);
        await OnDocumentsRemovingAsync(docs, options).AnyContext();
        await EnsureIndexesAsync(docs).AnyContext();

        var run = await DeleteDocumentsAsync(items, options).AnyContext();
        var removed = run.Succeeded.Select(i => i.Document).ToList();
        if (removed.Count > 0)
            await OnDocumentsRemovedAsync(removed, options).AnyContext();

        var alreadyGone = run.With(WriteOutcome.Missing).Select(i => i.Item.Document).ToList();
        if (alreadyGone.Count > 0 && IsCacheEnabled)
            await InvalidateCacheAsync(alreadyGone).AnyContext();

        WriteExceptionMapper.ThrowIfFailed(run, "removing", MissingDocumentPolicy.Success);
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
    /// documents that start matching after the operation begins are not patched. Running the same <c>PatchAllAsync</c> in
    /// parallel patches documents more than once; make scripts idempotent (for example <c>ctx._source.count = params.value</c>
    /// instead of <c>ctx._source.count++</c>) when the operation can be retried or run concurrently.</para>
    /// <para><b>Script and partial patches</b> use the bulk Update API per batch, re-run by Elasticsearch on the latest
    /// document up to <c>Retry(n)</c> times (<c>retry_on_conflict</c>, default 10). With a default pipeline, script patches
    /// run update-by-query on each document id, refreshing conflicted indexes before a retry. Written indexes share a
    /// final refresh unless consistency is eventual. Update-by-query over the
    /// whole query is used only with <c>UseUpdateByQuery()</c> and the cache disabled; it skips documents changed after its
    /// snapshot and throws a <see cref="VersionConflictDocumentException"/> with the count unless
    /// <c>ProceedOnQueryConflicts()</c> is set.</para>
    /// <para><b>JSON, action and pipeline partial patches</b> are applied in memory and written with <c>if_seq_no</c> for
    /// versioned documents. Conflicting documents are re-read with one multi-get, re-patched and written again in grouped
    /// rounds (up to <c>Retry(n)</c>). <c>SkipVersionCheck()</c> writes without the check.</para>
    /// <para>Every document ends with an outcome. Documents deleted while the operation runs are skipped. Successful
    /// documents are invalidated in the cache and notified as each batch completes; documents that still fail are reported
    /// at the end in a <see cref="VersionConflictDocumentException"/> or <see cref="DocumentException"/>. If the repository
    /// is disposed, the remaining batches are not processed and an <see cref="OperationCanceledException"/> is thrown after
    /// the completed documents are notified. <c>IfVersion</c> is not supported.</para>
    /// </remarks>
    public virtual async Task<long> PatchAllAsync(IRepositoryQuery query, IPatchOperation operation, ICommandOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(operation);
        options = ConfigureOptions(options?.As<T>());
        ThrowIfExpectedVersion(options);
        if (IsEmptyPatch(operation))
            return 0;

        operation = PreparePatch(operation);

        if (!ElasticIndex.HasMultipleIndexes)
            await ElasticIndex.EnsureIndexAsync(null).AnyContext();

        var strategy = PatchStrategyResolver.Resolve<T>(operation, !String.IsNullOrEmpty(DefaultPipeline), options.ShouldUseUpdateByQuery() && !IsCacheEnabled);
        string operationName = $"PatchAll {operation.GetType().Name}";
        long affectedRecords = 0;
        long queryConflicts = 0;
        long skippedMissing = 0;
        WriteRunResult<IndexItem>? clientFailures = null;
        WriteRunResult<UpdateItem>? serverFailures = null;

        if (strategy is PatchStrategy.UpdateByQueryBulk)
        {
            string taskId = await StartPatchAllByQueryAsync(query, (ScriptPatch)operation, options).AnyContext();
            try
            {
                (affectedRecords, queryConflicts) = await WaitForPatchAllByQueryAsync(taskId, options).AnyContext();
            }
            catch (Exception ex) when (ex is DocumentException or OperationCanceledException)
            {
                await NotifyPatchedByQuerySafelyAsync(query, options).AnyContext();
                throw;
            }
        }
        else if (strategy is PatchStrategy.ClientSide)
        {
            await BatchProcessAsync(query, async results =>
            {
                var run = await PatchClientSideAsync(WithIds(results.Hits).ToList(), operation, null, options, Refresh.False, operationName, useDocumentDates: true).AnyContext();
                var patched = run.Succeeded;
                affectedRecords += patched.Count;
                skippedMissing += run.Count(WriteOutcome.Missing);
                await OnPatchedAsync(patched.Select(i => i.Key.Id).ToList(), patched.Select(i => i.Document).ToList(), options).AnyContext();
                clientFailures = WriteRunResult<IndexItem>.Accumulate(clientFailures, run);
                return run.Count(WriteOutcome.Cancelled) is 0;
            }, CreatePatchPagingOptions(options, fullSource: true)).AnyContext();
        }
        else
        {
            if (HasIdentity && !query.GetIncludes().Contains(_idField!.Value))
                query.Include(_idField!.Value);

            await BatchProcessAsync(query, async results =>
            {
                var items = WithIds(results.Hits)
                    .Select(h => new UpdateItem(new DocumentKey(h.Hit.GetIndex() ?? ElasticIndex.GetIndex(h.Id), h.Id), h.Hit.Routing, ElasticDocumentVersion.Empty))
                    .ToList();

                var run = await PatchServerSideAsync(items, operation, strategy, options, Refresh.False, operationName).AnyContext();
                var modifiedIds = run.Succeeded.Select(i => i.Key.Id).ToList();
                affectedRecords += modifiedIds.Count;
                skippedMissing += run.Count(WriteOutcome.Missing);
                await OnPatchedAsync(modifiedIds, null, options).AnyContext();
                serverFailures = WriteRunResult<UpdateItem>.Accumulate(serverFailures, run);
                return run.Count(WriteOutcome.Cancelled) is 0;
            }, CreatePatchPagingOptions(options)).AnyContext();
        }

        if (strategy is PatchStrategy.UpdateByQueryBulk && affectedRecords > 0)
            await NotifyPatchedByQueryAsync(query, options).AnyContext();

        if (queryConflicts > 0)
        {
            if (!options.ShouldProceedOnQueryConflicts())
                throw new VersionConflictDocumentException($"Update by query patched {affectedRecords} documents and skipped {queryConflicts} documents that changed during the operation (version conflicts).");

            _logger.LogWarning("Update by query patched {Affected} documents and skipped {Conflicts} documents with version conflicts", affectedRecords, queryConflicts);
        }

        if (skippedMissing > 0)
            _logger.LogInformation("{Operation} skipped {Count} documents that were deleted while the operation ran", operationName, skippedMissing);

        if (clientFailures is not null)
            WriteExceptionMapper.ThrowIfFailed(clientFailures, "patching", MissingDocumentPolicy.Skip);
        if (serverFailures is not null)
            WriteExceptionMapper.ThrowIfFailed(serverFailures, "patching", MissingDocumentPolicy.Skip);

        DisposedCancellationToken.ThrowIfCancellationRequested();
        return affectedRecords;
    }

    private static ICommandOptions CreatePatchPagingOptions(ICommandOptions options, bool fullSource = false)
    {
        var pagingOptions = options.Clone().Cache(false);
        if (fullSource)
            pagingOptions.BuildOption(FullDocumentSourceKey, true);
        if (!pagingOptions.ShouldUseSnapshotPaging())
            pagingOptions.SearchAfterPaging(SearchAfterPagingMode.PointInTime);

        return pagingOptions;
    }

    private async Task NotifyPatchedByQueryAsync(IRepositoryQuery query, ICommandOptions options)
    {
        if (IsCacheEnabled)
            await InvalidateCacheByQueryAsync(query.As<T>()).AnyContext();

        await OnDocumentsChangedAsync(ChangeType.Saved, EmptyList, options).AnyContext();
        await SendQueryNotificationsAsync(ChangeType.Saved, query, options).AnyContext();
    }

    /// <summary>
    /// Invalidates and notifies after an update-by-query task whose outcome is unknown (it may already have patched
    /// documents). A failure here is logged so it never replaces the exception that is about to be rethrown.
    /// </summary>
    private async Task NotifyPatchedByQuerySafelyAsync(IRepositoryQuery query, ICommandOptions options)
    {
        try
        {
            await NotifyPatchedByQueryAsync(query, options).AnyContext();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error invalidating the cache or sending notifications after an update by query with an unknown outcome");
        }
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
    /// <para>Documents removed by completed passes are always invalidated and notified before a failure is thrown; a failed
    /// pass throws a <see cref="DocumentException"/> and disposal throws an <see cref="OperationCanceledException"/>.</para>
    /// <para>The batched path returns the number of matching documents processed for removal, including documents that
    /// disappear before their delete runs. It stops at the first failed batch. Delete-by-query returns actual deletes.</para>
    /// </remarks>
    public virtual async Task<long> RemoveAllAsync(IRepositoryQuery query, ICommandOptions? options = null)
    {
        options = ConfigureOptions(options?.As<T>());
        ThrowIfExpectedVersion(options);
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

        var (deleted, conflicts, run) = await DeleteByQueryAsync(query, options).AnyContext();
        if (deleted > 0)
        {
            if (IsCacheEnabled)
                await InvalidateCacheByQueryAsync(query.As<T>()).AnyContext();

            await OnDocumentsRemovedAsync(EmptyList, options).AnyContext();
            await SendQueryNotificationsAsync(ChangeType.Removed, query, options).AnyContext();
        }

        if (run.Count(WriteOutcome.Conflict) > 0)
        {
            if (!options.ShouldProceedOnQueryConflicts())
                throw new VersionConflictDocumentException($"RemoveAll removed {deleted} documents but {conflicts} documents still had version conflicts after {run.Rounds} passes.");

            _logger.LogWarning("RemoveAll: {Deleted} records removed after {Passes} passes ({Conflicts} unresolved version conflicts)", deleted, run.Rounds, conflicts);
            return deleted;
        }

        WriteExceptionMapper.ThrowIfFailed(run, "removing", MissingDocumentPolicy.Success);
        return deleted;
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
        try
        {
            do
            {
                if (results.Hits.Count is 0)
                    break;

                if (!await processFunc(results).AnyContext())
                {
                    _logger.LogTrace("Aborted batch processing");
                    break;
                }

                recordsProcessed += results.Documents.Count;
            } while (await results.NextPageAsync().AnyContext());
        }
        finally
        {
            await CloseAbandonedPointInTimeAsync(results, options).AnyContext();
        }

        await RefreshForConsistency(query, options).AnyContext();

        _logger.LogTrace("{Processed} records processed", recordsProcessed);
        return recordsProcessed;
    }

    private async Task CloseAbandonedPointInTimeAsync<TResult>(FindResults<TResult> results, ICommandOptions options) where TResult : class
    {
        if (!results.HasMore || !options.IsRepoOwnedPointInTime())
            return;

        options.PointInTimeId(null);
        try
        {
            if (!await ClosePointInTimeAsync(results).AnyContext())
                _logger.LogWarning("Failed to close repository-owned point in time after paging stopped; it will expire after its keep-alive window");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error closing repository-owned point in time after paging stopped");
        }
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

    private (string Index, string? Routing, string Id) GetDocumentLocationKey(T? document)
    {
        ArgumentNullException.ThrowIfNull(document);

        string id = GetDocumentJoinKey(document);
        return (ElasticIndex.GetIndex(document), GetParentIdFunc?.Invoke(document), id);
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
            documents, GetDocumentLocationKey, GetDocumentLocationKey,
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
            documents, GetDocumentLocationKey, GetDocumentLocationKey,
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

    private async Task<IReadOnlyCollection<T>> GetOriginalDocumentsAsync(IReadOnlyCollection<T> documents, ICommandOptions options)
    {
        if (!options.GetOriginalsEnabled(OriginalsEnabled) || documents.Count is 0)
            return EmptyList;

        var originals = options.GetOriginals<T>().ToList();
        var originalKeys = originals.Select(GetDocumentLocationKey).ToHashSet();
        var remainingDocuments = documents.Where(d => !originalKeys.Contains(GetDocumentLocationKey(d))).DistinctBy(GetDocumentLocationKey).ToList();
        if (remainingDocuments.Count is 0)
            return originals.AsReadOnly();

        if (!HasParent && !ElasticIndex.HasMultipleIndexes)
        {
            var ids = new Ids(remainingDocuments.Select(d => d.Id));
            originals.AddRange(await GetByIdsAsync(ids, o => options.Clone().ReadCache().As<T>()).AnyContext());
            return originals.AsReadOnly();
        }

        var locations = remainingDocuments.Select(d => (Key: new DocumentKey(ElasticIndex.GetIndex(d), d.Id), Routing: GetParentIdFunc?.Invoke(d))).ToList();
        var readOptions = ConfigureOptions(options.Clone().ReadCache().As<T>());
        await OnBeforeGetAsync(new Ids(locations.Select(d => new Id(d.Key.Id, d.Routing)).Distinct()), readOptions, typeof(T)).AnyContext();
        var (response, items) = await MultiGetAsync(locations, readOptions, DisposedCancellationToken).AnyContext();
        DisposedCancellationToken.ThrowIfCancellationRequested();
        if (items is null)
            throw new DocumentException(response.GetErrorMessage("Error getting original documents"), response.OriginalException());

        var errors = items.Where(i => i.IsError).ToList();
        if (errors.Count > 0)
        {
            var first = errors[0];
            throw new DocumentException($"Error getting {errors.Count} original documents. First error for {first.Index}/{first.Id}: {first.ErrorType}: {first.ErrorReason}");
        }

        originals.AddRange(items.Select(i => i.Hit?.Document).OfType<T>());

        return originals.AsReadOnly();
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
            return PublishChangeTypeMessageAsync(changeType, (string?)null, delay: delay);

        var tasks = new List<Task>(ids.Count);
        foreach (string id in ids)
            tasks.Add(PublishChangeTypeMessageAsync(changeType, id, delay: delay));

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
