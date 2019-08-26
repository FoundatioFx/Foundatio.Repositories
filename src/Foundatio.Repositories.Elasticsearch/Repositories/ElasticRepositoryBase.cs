using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Elasticsearch.Net;
using FluentValidation;
using Nest;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Messaging;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.JsonPatch;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;
using Foundatio.Utility;
using Newtonsoft.Json.Linq;
using Foundatio.Repositories.Options;
using Microsoft.Extensions.Logging;
using System.Linq.Expressions;
using System.Threading;

namespace Foundatio.Repositories.Elasticsearch {
    public abstract class ElasticRepositoryBase<T> : ElasticReadOnlyRepositoryBase<T>, IElasticRepository<T> where T : class, IIdentity, new() {
        protected readonly IValidator<T> _validator;
        protected readonly IMessagePublisher _messagePublisher;
        private readonly List<Lazy<Field>> _propertiesRequiredForRemove = new List<Lazy<Field>>();

        protected ElasticRepositoryBase(IIndex index, IValidator<T> validator = null) : base(index) {
            _validator = validator;
            _messagePublisher = index.Configuration.MessageBus;
            NotificationsEnabled = _messagePublisher != null;

            AddPropertyRequiredForRemove(_idField);
            if (HasCreatedDate)
                AddPropertyRequiredForRemove(e => ((IHaveCreatedDate)e).CreatedUtc);
        }

        protected string DefaultPipeline { get; set; } = null;
        protected Consistency DefaultConsistency { get; set; } = Consistency.Eventual;

        public virtual async Task<T> AddAsync(T document, ICommandOptions options = null) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            await AddAsync(new[] { document }, options).AnyContext();
            return document;
        }

        public virtual async Task AddAsync(IEnumerable<T> documents, ICommandOptions options = null) {
            var docs = documents?.ToList();
            if (docs == null || docs.Any(d => d == null))
                throw new ArgumentNullException(nameof(documents));

            if (docs.Count == 0)
                return;

            options = ConfigureOptions(options);
            await OnDocumentsAddingAsync(docs, options).AnyContext();

            if (_validator != null)
                foreach (var doc in docs)
                    await _validator.ValidateAndThrowAsync(doc).AnyContext();

            await IndexDocumentsAsync(docs, true, options).AnyContext();

            await OnDocumentsAddedAsync(docs, options).AnyContext();
            await AddToCacheAsync(docs, options).AnyContext();
        }

        public virtual async Task<T> SaveAsync(T document, ICommandOptions options = null) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            await SaveAsync(new[] { document }, options).AnyContext();
            return document;
        }

        public virtual async Task SaveAsync(IEnumerable<T> documents, ICommandOptions options = null) {
            var docs = documents?.ToList();
            if (docs == null || docs.Any(d => d == null))
                throw new ArgumentNullException(nameof(documents));

            if (docs.Count == 0)
                return;

            string[] ids = docs.Where(d => !String.IsNullOrEmpty(d.Id)).Select(d => d.Id).ToArray();
            if (ids.Length < docs.Count)
                throw new ApplicationException("Id must be set when calling Save.");

            options = ConfigureOptions(options);

            var originalDocuments = await GetOriginalDocumentsAsync(ids, options).AnyContext();
            await OnDocumentsSavingAsync(docs, originalDocuments, options).AnyContext();

            if (_validator != null)
                foreach (var doc in docs)
                    await _validator.ValidateAndThrowAsync(doc).AnyContext();

            await IndexDocumentsAsync(docs, false, options).AnyContext();

            await OnDocumentsSavedAsync(docs, originalDocuments, options).AnyContext();
            await AddToCacheAsync(docs, options).AnyContext();
        }

        private async Task<IReadOnlyCollection<T>> GetOriginalDocumentsAsync(Ids ids, ICommandOptions options) {
            if (!options.GetOriginalsEnabled(OriginalsEnabled) || ids.Count == 0)
                return EmptyList;

            var originals = options.GetOriginals<T>().ToList();
            foreach (var original in originals)
                ids.RemoveAll(id => id.Value == original.Id);

            originals.AddRange(await GetByIdsAsync(ids, options.Clone().ReadCache()).AnyContext());

            return originals.AsReadOnly();
        }

        public virtual async Task PatchAsync(Id id, IPatchOperation operation, ICommandOptions options = null) {
            if (String.IsNullOrEmpty(id.Value))
                throw new ArgumentNullException(nameof(id));

            if (operation == null)
                throw new ArgumentNullException(nameof(operation));

            options = ConfigureOptions(options);

            if (operation is ScriptPatch scriptOperation) {
                // TODO: Figure out how to specify a pipeline here.
                var request = new UpdateRequest<T, T>(ElasticIndex.GetIndex(id), id.Value) {
                    Script = new InlineScript(scriptOperation.Script) { Params = scriptOperation.Params },
                    RetryOnConflict = 10,
                    Refresh = options.GetRefreshMode(DefaultConsistency)
                };
                if (id.Routing != null)
                    request.Routing = id.Routing;

                var response = await _client.UpdateAsync(request).AnyContext();

                if (response.IsValid) {
                    _logger.LogTraceRequest(response);
                } else {
                    _logger.LogErrorRequest(response, "Error patching document {Index}/{Id}", ElasticIndex.GetIndex(id), id.Value);
                    throw new ApplicationException(response.GetErrorMessage(), response.OriginalException);
                }
            } else if (operation is Models.JsonPatch jsonOperation) {
                var request = new GetRequest(ElasticIndex.GetIndex(id), id.Value);
                if (id.Routing != null)
                    request.Routing = id.Routing;

                var response = await _client.LowLevel.GetAsync<GetResponse<IDictionary<string, object>>>(ElasticIndex.GetIndex(id), id.Value).AnyContext();
                var jobject = JObject.FromObject(response.Source);
                if (response.IsValid) {
                    _logger.LogTraceRequest(response);
                } else {
                    _logger.LogErrorRequest(response, "Error patching document {Index}/{Id}", ElasticIndex.GetIndex(id), id.Value);
                    throw new ApplicationException(response.GetErrorMessage(), response.OriginalException);
                }

                var target = (JToken)jobject;
                new JsonPatcher().Patch(ref target, jsonOperation.Patch);

                var indexParameters = new IndexRequestParameters {
                    Pipeline = DefaultPipeline,
                    Refresh = options.GetRefreshMode(DefaultConsistency)
                };
                if (id.Routing != null)
                    indexParameters.Routing = id.Routing;
                
                var updateResponse = await _client.LowLevel.IndexAsync<VoidResponse>(ElasticIndex.GetIndex(id), id.Value, PostData.String(target.ToString()), indexParameters, default(CancellationToken)).AnyContext();

                if (updateResponse.Success) {
                    _logger.LogTraceRequest(updateResponse);
                } else {
                    _logger.LogErrorRequest(updateResponse, "Error patching document {Index}/{Id} with {Pipeline}", ElasticIndex.GetIndex(id), id.Value, DefaultPipeline);
                    throw new ApplicationException(updateResponse.GetErrorMessage(), updateResponse.OriginalException);
                }
            } else if (operation is PartialPatch partialOperation) {
                // TODO: Figure out how to specify a pipeline here.
                var request = new UpdateRequest<T, object>(ElasticIndex.GetIndex(id), id.Value) {
                    Doc = partialOperation.Document,
                    RetryOnConflict = 10
                };
                if (id.Routing != null)
                    request.Routing = id.Routing;
                request.Refresh = options.GetRefreshMode(DefaultConsistency);

                var response = await _client.UpdateAsync(request).AnyContext();

                if (response.IsValid) {
                    _logger.LogTraceRequest(response);
                } else {
                    _logger.LogErrorRequest(response, "Error patching document {Index}/{Id}", ElasticIndex.GetIndex(id), id.Value);
                    throw new ApplicationException(response.GetErrorMessage(), response.OriginalException);
                }
            } else {
                throw new ArgumentException("Unknown operation type", nameof(operation));
            }

            // TODO: Find a good way to invalidate cache and send changed notification
            await OnDocumentsChangedAsync(ChangeType.Saved, EmptyList, options).AnyContext();
            if (IsCacheEnabled)
                await Cache.RemoveAsync(id).AnyContext();

            if (options.ShouldNotify())
                await PublishChangeTypeMessageAsync(ChangeType.Saved, id).AnyContext();
        }

        public virtual async Task PatchAsync(Ids ids, IPatchOperation operation, ICommandOptions options = null) {
            if (ids == null)
                throw new ArgumentNullException(nameof(ids));

            if (operation == null)
                throw new ArgumentNullException(nameof(operation));

            if (ids.Count == 0)
                return;

            options = ConfigureOptions(options);
            if (ids.Count == 1) {
                await PatchAsync(ids[0], operation, options).AnyContext();
                return;
            }

            if (operation is Models.JsonPatch) {
                await PatchAllAsync(NewQuery().Id(ids), operation, options).AnyContext();
                return;
            }

            var scriptOperation = operation as ScriptPatch;
            var partialOperation = operation as PartialPatch;
            if (scriptOperation == null && partialOperation == null)
                throw new ArgumentException("Unknown operation type", nameof(operation));

            var bulkResponse = await _client.BulkAsync(b => {
                b.Refresh(options.GetRefreshMode(DefaultConsistency));
                foreach (var id in ids) {
                    b.Pipeline(DefaultPipeline);

                    if (scriptOperation != null)
                        b.Update<T>(u => {
                            u.Id(id.Value)
                              .Index(ElasticIndex.GetIndex(id))
                              .Script(s => s.Source(scriptOperation.Script).Params(scriptOperation.Params))
                              .RetriesOnConflict(10);

                            if (id.Routing != null)
                                u.Routing(id.Routing);

                            return u;
                        });
                    else if (partialOperation != null)
                        b.Update<T, object>(u => {
                            u.Id(id.Value)
                                .Index(ElasticIndex.GetIndex(id))
                                .Doc(partialOperation.Document)
                                .RetriesOnConflict(10);

                            if (id.Routing != null)
                                u.Routing(id.Routing);

                            return u;
                        });
                }

                return b;
            }).AnyContext();

            // TODO: Is there a better way to handle failures?
            if (bulkResponse.IsValid) {
                _logger.LogTraceRequest(bulkResponse);
            } else {
                _logger.LogErrorRequest(bulkResponse, "Error bulk patching documents");
                throw new ApplicationException(bulkResponse.GetErrorMessage(), bulkResponse.OriginalException);
            }

            // TODO: Find a good way to invalidate cache and send changed notification
            await OnDocumentsChangedAsync(ChangeType.Saved, EmptyList, options).AnyContext();
            if (IsCacheEnabled)
                await Cache.RemoveAllAsync(ids.Select(id => id.Value)).AnyContext();

            if (options.ShouldNotify()) {
                var tasks = new List<Task>(ids.Count);
                foreach (var id in ids)
                    tasks.Add(PublishChangeTypeMessageAsync(ChangeType.Saved, id));

                await Task.WhenAll(tasks).AnyContext();
            }
        }

        public virtual Task<long> PatchAllAsync(RepositoryQueryDescriptor<T> query, IPatchOperation operation, CommandOptionsDescriptor<T> options = null) {
            return PatchAllAsync(query.Configure(), operation, options.Configure());
        }

        public virtual async Task<long> PatchAllAsync(IRepositoryQuery query, IPatchOperation operation, ICommandOptions options = null) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            if (operation == null)
                throw new ArgumentNullException(nameof(operation));

            options = ConfigureOptions(options);

            long affectedRecords = 0;
            if (operation is Models.JsonPatch jsonOperation) {
                var patcher = new JsonPatcher();
                affectedRecords += await BatchProcessAsAsync<JObject>(query, async results => {
                    var bulkResult = await _client.BulkAsync(b => {
                        b.Refresh(options.GetRefreshMode(DefaultConsistency));
                        foreach (var h in results.Hits) {
                            var target = h.Document as JToken;
                            patcher.Patch(ref target, jsonOperation.Patch);

                            b.Index<JObject>(i => i
                                .Document(target as JObject)
                                .Id(h.Id)
                                .Routing(h.Routing)
                                .Index(h.GetIndex())
                                .Pipeline(DefaultPipeline)
                                .IfPrimaryTerm(h.GetVersion().PrimaryTerm)
                                .IfSequenceNumber(h.GetVersion().SequenceNumber));
                        }

                        return b;
                    }).AnyContext();

                    if (bulkResult.IsValid) {
                        _logger.LogTraceRequest(bulkResult);
                    } else {
                        _logger.LogErrorRequest(bulkResult, "Error occurred while bulk updating");
                        return false;
                    }

                    var updatedIds = results.Hits.Select(h => h.Id).ToList();
                    if (IsCacheEnabled)
                        await Cache.RemoveAllAsync(updatedIds).AnyContext();

                    try {
                        options.GetUpdatedIdsCallback()?.Invoke(updatedIds);
                    } catch (Exception ex) {
                        _logger.LogError(ex, "Error calling updated ids callback.");
                    }

                    return true;
                }, options.Clone()).AnyContext();
            } else {
                var scriptOperation = operation as ScriptPatch;
                var partialOperation = operation as PartialPatch;
                if (scriptOperation == null && partialOperation == null)
                    throw new ArgumentException("Unknown operation type", nameof(operation));

                if (!IsCacheEnabled && scriptOperation != null) {
                    var request = new UpdateByQueryRequest(Indices.Index(String.Join(",", ElasticIndex.GetIndexesByQuery(query)))) {
                        Query = await ElasticIndex.QueryBuilder.BuildQueryAsync(query, options, new SearchDescriptor<T>()).AnyContext(),
                        Conflicts = Conflicts.Proceed,
                        Script = new InlineScript(scriptOperation.Script) { Params = scriptOperation.Params },
                        Pipeline = DefaultPipeline,
                        Version = HasVersion,
                        Refresh = options.GetRefreshMode(DefaultConsistency) != Refresh.False
                    };

                    var response = await _client.UpdateByQueryAsync(request).AnyContext();

                    if (response.IsValid) {
                        _logger.LogTraceRequest(response);
                    } else {
                        _logger.LogErrorRequest(response, "Error occurred while patching by query");
                        throw new ApplicationException(response.GetErrorMessage(), response.OriginalException);
                    }

                    // TODO: What do we want to do about failures and timeouts?
                    affectedRecords += response.Updated + response.Noops;
                    Debug.Assert(response.Total == affectedRecords, "Unable to update all documents");
                } else {
                    if (!query.GetIncludes().Contains(_idField.Value))
                        query.Include(_idField.Value);

                    affectedRecords += await BatchProcessAsync(query, async results => {
                        var bulkResult = await _client.BulkAsync(b => {
                            b.Pipeline(DefaultPipeline);
                            b.Refresh(options.GetRefreshMode(DefaultConsistency));

                            foreach (var h in results.Hits) {
                                if (scriptOperation != null)
                                    b.Update<T>(u => u
                                        .Id(h.Id)
                                        .Routing(h.Routing)
                                        .Index(h.GetIndex())
                                        .Script(s => s.Source(scriptOperation.Script).Params(scriptOperation.Params))
                                        .RetriesOnConflict(10));
                                else if (partialOperation != null)
                                    b.Update<T, object>(u => u.Id(h.Id)
                                        .Routing(h.Routing)
                                        .Index(h.GetIndex())
                                        .Doc(partialOperation.Document));
                            }

                            return b;
                        }).AnyContext();

                        if (bulkResult.IsValid) {
                            _logger.LogTraceRequest(bulkResult);
                        } else {
                            _logger.LogErrorRequest(bulkResult, "Error occurred while bulk updating");
                            return false;
                        }

                        var updatedIds = results.Hits.Select(h => h.Id).ToList();
                        if (IsCacheEnabled)
                            await Cache.RemoveAllAsync(updatedIds).AnyContext();

                        try {
                            options.GetUpdatedIdsCallback()?.Invoke(updatedIds);
                        } catch (Exception ex) {
                            _logger.LogError(ex, "Error calling updated ids callback.");
                        }

                        return true;
                    }, options).AnyContext();
                }
            }

            if (affectedRecords > 0) {
                // TODO: Find a good way to invalidate cache and send changed notification
                await OnDocumentsChangedAsync(ChangeType.Saved, EmptyList, options).AnyContext();
                await SendQueryNotificationsAsync(ChangeType.Saved, query, options).AnyContext();
            }

            return affectedRecords;
        }

        public virtual Task RemoveAsync(Id id, ICommandOptions options = null) {
            if (String.IsNullOrEmpty(id))
                throw new ArgumentNullException(nameof(id));

            return RemoveAsync((Ids)id, options);
        }

        public virtual async Task RemoveAsync(Ids ids, ICommandOptions options = null) {
            if (ids == null)
                throw new ArgumentNullException(nameof(ids));

            options = ConfigureOptions(options);
            if (IsCacheEnabled)
                options = options.ReadCache();

            // TODO: If not OriginalsEnabled then just delete by id
            // TODO: Delete by id using GetIndexById and id.Routing if its a child doc
            var documents = await GetByIdsAsync(ids, options).AnyContext();
            if (documents == null)
                return;

            await RemoveAsync(documents, options).AnyContext();
        }

        public virtual Task RemoveAsync(T document, ICommandOptions options = null) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            return RemoveAsync(new[] { document }, options);
        }

        public virtual async Task RemoveAsync(IEnumerable<T> documents, ICommandOptions options = null) {
            var docs = documents?.ToList();
            if (docs == null || docs.Any(d => d == null))
                throw new ArgumentNullException(nameof(documents));

            if (docs.Count == 0)
                return;

            if (ElasticIndex.HasMultipleIndexes) {
                foreach (var documentGroup in docs.GroupBy(ElasticIndex.GetIndex))
                    await ElasticIndex.EnsureIndexAsync(documentGroup.First()).AnyContext();
            }

            options = ConfigureOptions(options);
            await OnDocumentsRemovingAsync(docs, options).AnyContext();

            if (docs.Count == 1) {
                var document = docs.Single();
                var request = new DeleteRequest(ElasticIndex.GetIndex(document), document.Id) {
                    Refresh = options.GetRefreshMode(DefaultConsistency)
                };

                if (GetParentIdFunc != null)
                    request.Routing = GetParentIdFunc(document);

                var response = await _client.DeleteAsync(request).AnyContext();

                if (response.IsValid || response.ApiCall.HttpStatusCode == 404) {
                    _logger.LogTraceRequest(response);
                } else {
                    _logger.LogErrorRequest(response, "Error removing document {Index}/{Id}", ElasticIndex.GetIndex(document), document.Id);
                    throw new ApplicationException(response.GetErrorMessage(), response.OriginalException);
                }
            } else {
                var response = await _client.BulkAsync(bulk => {
                    bulk.Refresh(options.GetRefreshMode(DefaultConsistency));
                    foreach (var doc in docs)
                        bulk.Delete<T>(d => {
                            d.Id(doc.Id).Index(ElasticIndex.GetIndex(doc));

                            if (GetParentIdFunc != null)
                                d.Routing(GetParentIdFunc(doc));

                            return d;
                        });

                    return bulk;
                }).AnyContext();

                if (response.IsValid) {
                    _logger.LogTraceRequest(response);
                } else {
                    _logger.LogErrorRequest(response, "Error bulk removing documents");
                    throw new ApplicationException(response.GetErrorMessage(), response.OriginalException);
                }
            }

            await OnDocumentsRemovedAsync(docs, options).AnyContext();
        }

        public virtual async Task<long> RemoveAllAsync(ICommandOptions options = null) {
            if (IsCacheEnabled)
                await Cache.RemoveAllAsync().AnyContext();

            return await RemoveAllAsync(NewQuery(), options).AnyContext();
        }
        
        protected void AddPropertyRequiredForRemove(string field) {
            _propertiesRequiredForRemove.Add(new Lazy<Field>(() => field));
        }
        
        protected void AddPropertyRequiredForRemove(Lazy<string> field) {
            _propertiesRequiredForRemove.Add(new Lazy<Field>(() => field.Value));
        }
        
        protected void AddPropertyRequiredForRemove(Expression<Func<T, object>> objectPath) {
            _propertiesRequiredForRemove.Add(new Lazy<Field>(() => Infer.PropertyName(objectPath)));
        }
        
        protected void AddPropertyRequiredForRemove(params Expression<Func<T, object>>[] objectPaths) {
            _propertiesRequiredForRemove.AddRange(objectPaths.Select(o => new Lazy<Field>(() => Infer.PropertyName(o))));
        }

        public virtual Task<long> RemoveAllAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null) {
            return RemoveAllAsync(query.Configure(), options.Configure());
        }

        public virtual async Task<long> RemoveAllAsync(IRepositoryQuery query, ICommandOptions options = null) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            options = ConfigureOptions(options);
            if (IsCacheEnabled && options.ShouldUseCache(true)) {
                foreach (var field in _propertiesRequiredForRemove.Select(f => f.Value))
                    if (field != null && !query.GetIncludes().Contains(field))
                        query.Include(field);

                // TODO: What if you only want to send one notification?
                return await BatchProcessAsync(query, async results => {
                    await RemoveAsync(results.Documents, options).AnyContext();
                    return true;
                }, options.Clone()).AnyContext();
            }

            var response = await _client.DeleteByQueryAsync(new DeleteByQueryRequest(ElasticIndex.Name) {
                Refresh = options.GetRefreshMode(DefaultConsistency) != Refresh.False,
                Conflicts = Conflicts.Proceed,
                Query = await ElasticIndex.QueryBuilder.BuildQueryAsync(query, options, new SearchDescriptor<T>()).AnyContext()
            }).AnyContext();

            if (response.IsValid) {
                _logger.LogTraceRequest(response);
            } else {
                _logger.LogErrorRequest(response, "Error removing documents");
                throw new ApplicationException(response.GetErrorMessage(), response.OriginalException);
            }

            if (response.Deleted > 0) {
                await OnDocumentsRemovedAsync(EmptyList, options).AnyContext();
                await SendQueryNotificationsAsync(ChangeType.Removed, query, options).AnyContext();
            }

            Debug.Assert(response.Total == response.Deleted, "All records were not removed");
            return response.Deleted;
        }

        public virtual Task<long> BatchProcessAsync(RepositoryQueryDescriptor<T> query, Func<FindResults<T>, Task<bool>> processAsync, CommandOptionsDescriptor<T> options = null) {
            return BatchProcessAsAsync(query.Configure(), processAsync, options.Configure());
        }

        public virtual Task<long> BatchProcessAsync(IRepositoryQuery query, Func<FindResults<T>, Task<bool>> processAsync, ICommandOptions options = null) {
            return BatchProcessAsAsync(query, processAsync, options);
        }

        public virtual async Task<long> BatchProcessAsAsync<TResult>(IRepositoryQuery query, Func<FindResults<TResult>, Task<bool>> processAsync, ICommandOptions options = null)
            where TResult : class, new() {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            if (processAsync == null)
                throw new ArgumentNullException(nameof(processAsync));

            options = ConfigureOptions(options);
            options.SnapshotPaging();
            if (!options.HasPageLimit())
                options.PageLimit(500);
            if (!options.HasSnapshotLifetime())
                options.SnapshotPagingLifetime(TimeSpan.FromMinutes(5));

            long recordsProcessed = 0;
            var results = await FindAsAsync<TResult>(query, options).AnyContext();
            do {
                if (results.Hits.Count == 0)
                    break;

                // TODO: We need a generic way to do bulk operations and do exponential backoffs when we encounter on 429's (bulk queue is full). https://github.com/elastic/elasticsearch-net/pull/2162
                if (await processAsync(results).AnyContext()) {
                    recordsProcessed += results.Documents.Count;
                    continue;
                }

                _logger.LogTrace("Aborted batch processing.");
                break;
            } while (await results.NextPageAsync().AnyContext());

            _logger.LogTrace("{0} records processed", recordsProcessed);
            return recordsProcessed;
        }

        #region Events

        public AsyncEvent<DocumentsEventArgs<T>> DocumentsAdding { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

        private async Task OnDocumentsAddingAsync(IReadOnlyCollection<T> documents, ICommandOptions options) {
            if (HasDates)
                documents.OfType<IHaveDates>().SetDates();
            else if (HasCreatedDate)
                documents.OfType<IHaveCreatedDate>().SetCreatedDates();

            documents.EnsureIds(ElasticIndex.CreateDocumentId);

            if (DocumentsAdding != null)
                await DocumentsAdding.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this, options)).AnyContext();

            await OnDocumentsChangingAsync(ChangeType.Added, documents, options).AnyContext();
        }

        public AsyncEvent<DocumentsEventArgs<T>> DocumentsAdded { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

        private async Task OnDocumentsAddedAsync(IReadOnlyCollection<T> documents, ICommandOptions options) {
            if (DocumentsAdded != null)
                await DocumentsAdded.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this, options)).AnyContext();

            var modifiedDocs = documents.Select(d => new ModifiedDocument<T>(d, null)).ToList();
            await OnDocumentsChangedAsync(ChangeType.Added, modifiedDocs, options).AnyContext();
            await SendNotificationsAsync(ChangeType.Added, modifiedDocs, options).AnyContext();
        }

        public AsyncEvent<ModifiedDocumentsEventArgs<T>> DocumentsSaving { get; } = new AsyncEvent<ModifiedDocumentsEventArgs<T>>();

        private async Task OnDocumentsSavingAsync(IReadOnlyCollection<T> documents, IReadOnlyCollection<T> originalDocuments, ICommandOptions options) {
            if (documents.Count == 0)
                return;

            if (HasDates)
                documents.Cast<IHaveDates>().SetDates();

            documents.EnsureIds(ElasticIndex.CreateDocumentId);

            var modifiedDocs = originalDocuments.FullOuterJoin(
                documents, cf => cf.Id, cf => cf.Id,
                (original, modified, id) => new { Id = id, Original = original, Modified = modified }).Select(m => new ModifiedDocument<T>( m.Modified, m.Original)).ToList();

            if (DocumentsSaving != null)
                await DocumentsSaving.InvokeAsync(this, new ModifiedDocumentsEventArgs<T>(modifiedDocs, this, options)).AnyContext();

            await OnDocumentsChangingAsync(ChangeType.Saved, modifiedDocs, options).AnyContext();
        }

        public AsyncEvent<ModifiedDocumentsEventArgs<T>> DocumentsSaved { get; } = new AsyncEvent<ModifiedDocumentsEventArgs<T>>();

        private async Task OnDocumentsSavedAsync(IReadOnlyCollection<T> documents, IReadOnlyCollection<T> originalDocuments, ICommandOptions options) {
            var modifiedDocs = originalDocuments.FullOuterJoin(
                documents, cf => cf.Id, cf => cf.Id,
                (original, modified, id) => new { Id = id, Original = original, Modified = modified }).Select(m => new ModifiedDocument<T>(m.Modified, m.Original)).ToList();

            if (SupportsSoftDeletes && IsCacheEnabled) {
                string[] deletedIds = modifiedDocs.Where(d => ((ISupportSoftDeletes)d.Value).IsDeleted).Select(m => m.Value.Id).ToArray();
                if (deletedIds.Length > 0)
                    await Cache.SetAddAsync("deleted", deletedIds, TimeSpan.FromSeconds(30)).AnyContext();

                string[] undeletedIds = modifiedDocs.Where(d => ((ISupportSoftDeletes)d.Value).IsDeleted == false).Select(m => m.Value.Id).ToArray();
                if (undeletedIds.Length > 0)
                    await Cache.SetRemoveAsync("deleted", undeletedIds, TimeSpan.FromSeconds(30)).AnyContext();
            }

            if (DocumentsSaved != null)
                await DocumentsSaved.InvokeAsync(this, new ModifiedDocumentsEventArgs<T>(modifiedDocs, this, options)).AnyContext();

            await OnDocumentsChangedAsync(ChangeType.Saved, modifiedDocs, options).AnyContext();
            await SendNotificationsAsync(ChangeType.Saved, modifiedDocs, options).AnyContext();
        }

        public AsyncEvent<DocumentsEventArgs<T>> DocumentsRemoving { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

        private async Task OnDocumentsRemovingAsync(IReadOnlyCollection<T> documents, ICommandOptions options) {
            if (DocumentsRemoving != null)
                await DocumentsRemoving.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this, options)).AnyContext();

            await OnDocumentsChangingAsync(ChangeType.Removed, documents, options).AnyContext();
        }

        public AsyncEvent<DocumentsEventArgs<T>> DocumentsRemoved { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

        private async Task OnDocumentsRemovedAsync(IReadOnlyCollection<T> documents, ICommandOptions options) {
            if (DocumentsRemoved != null)
                await DocumentsRemoved.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this, options)).AnyContext();

            await OnDocumentsChangedAsync(ChangeType.Removed, documents, options).AnyContext();
            await SendNotificationsAsync(ChangeType.Removed, documents, options).AnyContext();
        }

        public AsyncEvent<DocumentsChangeEventArgs<T>> DocumentsChanging { get; } = new AsyncEvent<DocumentsChangeEventArgs<T>>();

        private Task OnDocumentsChangingAsync(ChangeType changeType, IReadOnlyCollection<T> documents, ICommandOptions options) {
            return OnDocumentsChangingAsync(changeType, documents.Select(d => new ModifiedDocument<T>(d, null)).ToList(), options);
        }

        private async Task OnDocumentsChangingAsync(ChangeType changeType, IReadOnlyCollection<ModifiedDocument<T>> documents, ICommandOptions options) {
            if (DocumentsChanging == null)
                return;

            await DocumentsChanging.InvokeAsync(this, new DocumentsChangeEventArgs<T>(changeType, documents, this, options)).AnyContext();
        }

        public AsyncEvent<DocumentsChangeEventArgs<T>> DocumentsChanged { get; } = new AsyncEvent<DocumentsChangeEventArgs<T>>();

        private Task OnDocumentsChangedAsync(ChangeType changeType, IReadOnlyCollection<T> documents, ICommandOptions options) {
            return OnDocumentsChangedAsync(changeType, documents.Select(d => new ModifiedDocument<T>(d, null)).ToList(), options);
        }

        private async Task OnDocumentsChangedAsync(ChangeType changeType, IReadOnlyCollection<ModifiedDocument<T>> documents, ICommandOptions options) {
            if (DocumentsChanged == null)
                return;

            if (changeType != ChangeType.Added)
                await InvalidateCacheAsync(documents, options).AnyContext();

            await DocumentsChanged.InvokeAsync(this, new DocumentsChangeEventArgs<T>(changeType, documents, this, options)).AnyContext();
        }

        #endregion

        private async Task IndexDocumentsAsync(IReadOnlyCollection<T> documents, bool isCreateOperation, ICommandOptions options) {
            if (ElasticIndex.HasMultipleIndexes) {
                foreach (var documentGroup in documents.GroupBy(ElasticIndex.GetIndex))
                    await ElasticIndex.EnsureIndexAsync(documentGroup.First()).AnyContext();
            }

            if (documents.Count == 1) {
                var document = documents.Single();
                var response = await _client.IndexAsync(document, i => {
                    i.OpType(isCreateOperation ? OpType.Create : OpType.Index);
                    i.Pipeline(DefaultPipeline);
                    i.Refresh(options.GetRefreshMode(DefaultConsistency));

                    if (GetParentIdFunc != null)
                        i.Routing(GetParentIdFunc(document));
                    //i.Routing(GetParentIdFunc != null ? GetParentIdFunc(document) : document.Id);

                    i.Index(ElasticIndex.GetIndex(document));

                    if (HasVersion && !isCreateOperation) {
                        var versionedDoc = (IVersioned)document;
                        i.IfPrimaryTerm(versionedDoc.GetVersion().PrimaryTerm);
                        i.IfSequenceNumber(versionedDoc.GetVersion().SequenceNumber);
                    }

                    return i;
                }).AnyContext();

                if (response.IsValid) {
                    _logger.LogTraceRequest(response);
                } else {
                    _logger.LogErrorRequest(response, $"Error {(isCreateOperation ? "adding" : "saving")} document");
                    if (isCreateOperation && response.ServerError?.Status == 409)
                        throw new DuplicateDocumentException(response.GetErrorMessage(), response.OriginalException);

                    throw new ApplicationException(response.GetErrorMessage(), response.OriginalException);
                }

                if (HasVersion) {
                    var versionDoc = (IVersioned)document;
                    versionDoc.Version = response.GetVersion();
                }
            } else {
                var bulkRequest = new BulkRequest();
                var list = documents.Select(d => {
                    var createOperation = new BulkCreateOperation<T>(d) { Pipeline = DefaultPipeline };
                    var indexOperation = new BulkIndexOperation<T>(d) { Pipeline = DefaultPipeline };
                    var baseOperation = isCreateOperation ? (IBulkOperation)createOperation : indexOperation;
                    
                    if (GetParentIdFunc != null)
                        baseOperation.Routing = GetParentIdFunc(d);
                    //baseOperation.Routing = GetParentIdFunc != null ? GetParentIdFunc(d) : d.Id;
                    baseOperation.Index = ElasticIndex.GetIndex(d);

                    if (HasVersion && !isCreateOperation) {
                        var versionedDoc = (IVersioned)d;
                        if (versionedDoc != null) {
                            indexOperation.IfSequenceNumber = versionedDoc.GetVersion().SequenceNumber;
                            indexOperation.IfPrimaryTerm = versionedDoc.GetVersion().PrimaryTerm;
                        }
                    }

                    return baseOperation;
                }).ToList();
                bulkRequest.Operations = list;
                bulkRequest.Refresh = options.GetRefreshMode(DefaultConsistency);

                var response = await _client.BulkAsync(bulkRequest).AnyContext();

                if (HasVersion) {
                    foreach (var hit in response.Items) {
                        if (!hit.IsValid)
                            continue;

                        var document = documents.FirstOrDefault(d => d.Id == hit.Id);
                        if (document == null)
                            continue;

                        var versionDoc = (IVersioned)document;
                        versionDoc.Version = hit.GetVersion();
                    }
                }

                var allErrors = response.ItemsWithErrors.ToList();
                if (allErrors.Count > 0) {
                    var retryableIds = allErrors.Where(e => e.Status == 429 || e.Status == 503).Select(e => e.Id).ToList();
                    if (retryableIds.Count > 0) {
                        var docs = documents.Where(d => retryableIds.Contains(d.Id)).ToList();
                        await IndexDocumentsAsync(docs, isCreateOperation, options).AnyContext();

                        // return as all recoverable items were retried.
                        if (allErrors.Count == retryableIds.Count)
                            return;
                    }
                }

                if (response.IsValid) {
                    _logger.LogTraceRequest(response);
                } else {
                    _logger.LogErrorRequest(response, $"Error {(isCreateOperation ? "adding" : "saving")} documents");
                    if (isCreateOperation && allErrors.Any(e => e.Status == 409))
                        throw new DuplicateDocumentException(response.GetErrorMessage(), response.OriginalException);

                    throw new ApplicationException(response.GetErrorMessage(), response.OriginalException);
                }
            }
            // 429 // 503
        }

        protected virtual async Task AddToCacheAsync(ICollection<T> documents, ICommandOptions options) {
            if (!IsCacheEnabled || Cache == null || !options.ShouldUseCache())
                return;

            foreach (var document in documents)
                await Cache.SetAsync(document.Id, document, options.GetExpiresIn()).AnyContext();
        }

        protected bool NotificationsEnabled { get; set; }
        protected bool OriginalsEnabled { get; set; }
        public bool BatchNotifications { get; set; }

        private Task SendNotificationsAsync(ChangeType changeType, ICommandOptions options) {
            return SendNotificationsAsync(changeType, EmptyList, options);
        }

        private Task SendNotificationsAsync(ChangeType changeType, IReadOnlyCollection<T> documents, ICommandOptions options) {
            return SendNotificationsAsync(changeType, documents.Select(d => new ModifiedDocument<T>(d, null)).ToList(), options);
        }

        protected virtual Task SendQueryNotificationsAsync(ChangeType changeType, IRepositoryQuery query, ICommandOptions options) {
            if (!NotificationsEnabled || !options.ShouldNotify())
                return Task.CompletedTask;

            var delay = TimeSpan.FromSeconds(1.5);
            var ids = query.GetIds();
            if (ids.Count > 0) {
                var tasks = new List<Task>(ids.Count);
                foreach (string id in ids) {
                    tasks.Add(PublishMessageAsync(new EntityChanged {
                        ChangeType = changeType,
                        Id = id,
                        Type = EntityTypeName
                    }, delay));
                }

                return Task.WhenAll(tasks);
            }

            return PublishMessageAsync(new EntityChanged {
                ChangeType = changeType,
                Type = EntityTypeName
            }, delay);
        }

        protected virtual Task SendNotificationsAsync(ChangeType changeType, IReadOnlyCollection<ModifiedDocument<T>> documents, ICommandOptions options) {
            if (!NotificationsEnabled || !options.ShouldNotify())
                return Task.CompletedTask;

            var delay = TimeSpan.FromSeconds(1.5);
            if (documents.Count == 0)
                return PublishChangeTypeMessageAsync(changeType, null, delay);

            var tasks = new List<Task>(documents.Count);
            if (BatchNotifications && documents.Count > 1) {
                // TODO: This needs to support batch notifications
                if (!SupportsSoftDeletes || changeType != ChangeType.Saved) {
                    foreach (var doc in documents.Select(d => d.Value))
                        tasks.Add(PublishChangeTypeMessageAsync(changeType, doc, delay));

                    return Task.WhenAll(tasks);
                }

                bool allDeleted = documents.All(d => d.Original != null && ((ISupportSoftDeletes)d.Original).IsDeleted == false && ((ISupportSoftDeletes)d.Value).IsDeleted);
                foreach (var doc in documents.Select(d => d.Value))
                    tasks.Add(PublishChangeTypeMessageAsync(allDeleted ? ChangeType.Removed : changeType, doc, delay));

                return Task.WhenAll(tasks);
            }

            if (!SupportsSoftDeletes) {
                foreach (var d in documents)
                    tasks.Add(PublishChangeTypeMessageAsync(changeType, d.Value, delay));

                return Task.WhenAll(tasks);
            }

            foreach (var d in documents) {
                var docChangeType = changeType;
                if (d.Original != null) {
                    var document = (ISupportSoftDeletes)d.Value;
                    var original = (ISupportSoftDeletes)d.Original;
                    if (original.IsDeleted == false && document.IsDeleted)
                        docChangeType = ChangeType.Removed;
                }

                tasks.Add(PublishChangeTypeMessageAsync(docChangeType, d.Value, delay));
            }

            return Task.WhenAll(tasks);
        }

        protected virtual Task PublishChangeTypeMessageAsync(ChangeType changeType, T document, TimeSpan delay) {
            return PublishChangeTypeMessageAsync(changeType, document, null, delay);
        }

        protected virtual Task PublishChangeTypeMessageAsync(ChangeType changeType, T document, IDictionary<string, object> data = null, TimeSpan? delay = null) {
            return PublishChangeTypeMessageAsync(changeType, document?.Id, data, delay);
        }

        protected virtual Task PublishChangeTypeMessageAsync(ChangeType changeType, string id, IDictionary<string, object> data = null, TimeSpan? delay = null) {
            if (!NotificationsEnabled)
                return Task.CompletedTask;

            return PublishMessageAsync(new EntityChanged {
                ChangeType = changeType,
                Id = id,
                Type = EntityTypeName,
                Data = new DataDictionary(data ?? new Dictionary<string, object>())
            }, delay);
        }

        protected virtual async Task PublishMessageAsync(EntityChanged message, TimeSpan? delay = null) {
            if (!NotificationsEnabled || _messagePublisher == null)
                return;

            if (BeforePublishEntityChanged != null) {
                var eventArgs = new BeforePublishEntityChangedEventArgs<T>(this, message);
                await BeforePublishEntityChanged.InvokeAsync(this, eventArgs).AnyContext();
                if (eventArgs.Cancel)
                    return;
            }

            await _messagePublisher.PublishAsync(message, delay).AnyContext();
        }

        public AsyncEvent<BeforePublishEntityChangedEventArgs<T>> BeforePublishEntityChanged { get; } = new AsyncEvent<BeforePublishEntityChangedEventArgs<T>>();
    }
}