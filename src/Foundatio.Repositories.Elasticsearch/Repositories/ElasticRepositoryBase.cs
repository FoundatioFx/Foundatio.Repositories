﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FluentValidation;
using Nest;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Logging;
using Foundatio.Messaging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Models;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.JsonPatch;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;
using Foundatio.Utility;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Elasticsearch {
    public abstract class ElasticRepositoryBase<T> : ElasticReadOnlyRepositoryBase<T>, IRepository<T> where T : class, IIdentity, new() {
        protected readonly IValidator<T> _validator;
        protected readonly IMessagePublisher _messagePublisher;

        protected ElasticRepositoryBase(IIndexType<T> indexType, IValidator<T> validator = null) : base(indexType) {
            _validator = validator;
            _messagePublisher = indexType.Configuration.MessageBus;
            NotificationsEnabled = _messagePublisher != null;

            if (HasCreatedDate) {
                var propertyName = _client.Infer.PropertyName(typeof(T).GetMember(nameof(IHaveCreatedDate.CreatedUtc)).Single());
                FieldsRequiredForRemove.Add(propertyName);
            }
        }
        
        public async Task<T> AddAsync(T document, bool addToCache = false, TimeSpan? expiresIn = null, bool sendNotification = true) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            await AddAsync(new[] { document }, addToCache, expiresIn, sendNotification).AnyContext();
            return document;
        }

        public async Task AddAsync(IEnumerable<T> documents, bool addToCache = false, TimeSpan? expiresIn = null, bool sendNotification = true) {
            var docs = documents?.ToList();
            if (docs == null || docs.Any(d => d == null))
                throw new ArgumentNullException(nameof(documents));

            if (docs.Count == 0)
                return;

            await OnDocumentsAddingAsync(docs).AnyContext();

            if (_validator != null)
                foreach (var doc in docs)
                    await _validator.ValidateAndThrowAsync(doc).AnyContext();

            await IndexDocumentsAsync(docs).AnyContext();

            if (addToCache)
                await AddToCacheAsync(docs, expiresIn).AnyContext();

            await OnDocumentsAddedAsync(docs, sendNotification).AnyContext();
        }

        public async Task<T> SaveAsync(T document, bool addToCache = false, TimeSpan? expiresIn = null, bool sendNotifications = true) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            await SaveAsync(new[] { document }, addToCache, expiresIn, sendNotifications).AnyContext();
            return document;
        }

        public async Task SaveAsync(IEnumerable<T> documents, bool addToCache = false, TimeSpan? expiresIn = null, bool sendNotifications = true) {
            var docs = documents?.ToList();
            if (docs == null || docs.Any(d => d == null))
                throw new ArgumentNullException(nameof(documents));

            if (docs.Count == 0)
                return;

            string[] ids = docs.Where(d => !String.IsNullOrEmpty(d.Id)).Select(d => d.Id).ToArray();
            if (ids.Length < docs.Count)
                throw new ApplicationException("Id must be set when calling Save.");

            var originalDocuments = ids.Length > 0 ? (await GetByIdsAsync(ids, useCache: true).AnyContext()) : EmptyList;
            // TODO: What should we do if original document count differs from document count?

            await OnDocumentsSavingAsync(docs, originalDocuments).AnyContext();

            if (_validator != null)
                foreach (var doc in docs)
                    await _validator.ValidateAndThrowAsync(doc).AnyContext();

            await IndexDocumentsAsync(docs).AnyContext();

            if (addToCache)
                await AddToCacheAsync(docs, expiresIn).AnyContext();

            await OnDocumentsSavedAsync(docs, originalDocuments, sendNotifications).AnyContext();
        }

        public async Task PatchAsync(string id, object update, bool sendNotification = true) {
            if (String.IsNullOrEmpty(id))
                throw new ArgumentNullException(nameof(id));

            if (update == null)
                throw new ArgumentNullException(nameof(update));

            string script = update as string;
            var patch = update as PatchDocument;

            if (script != null) {
                var response = await _client.UpdateAsync<T>(u => u
                    .Id(id)
                    .Index(GetIndexById(id))
                    .Type(ElasticType.Name)
                    .Script(script)
                    .RetryOnConflict(10)).AnyContext();

                _logger.Trace(() => response.GetRequest());

                if (!response.IsValid) {
                    string message = response.GetErrorMessage();
                    _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                    throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
                }
            } else if (patch != null) {
                var response = await _client.GetAsync<JObject>(u => u
                    .Id(id)
                    .Index(GetIndexById(id))
                    .Type(ElasticType.Name)).AnyContext();

                _logger.Trace(() => response.GetRequest());

                if (!response.IsValid) {
                    string message = response.GetErrorMessage();
                    _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                    throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
                }
                
                var target = response.Source as JToken;
                new JsonPatcher().Patch(ref target, patch);

                var updateResponse = await _client.Raw.IndexPutAsync(response.Index, response.Type, id, target.ToString()).AnyContext();
                _logger.Trace(() => updateResponse.GetRequest());

                if (!updateResponse.Success) {
                    string message = updateResponse.GetErrorMessage();
                    _logger.Error().Exception(updateResponse.OriginalException).Message(message).Property("request", updateResponse.GetRequest()).Write();
                    throw new ApplicationException(message, updateResponse.OriginalException);
                }
            } else {
                var response = await _client.UpdateAsync<T, object>(u => u
                    .Id(id)
                    .Index(GetIndexById(id))
                    .Type(ElasticType.Name)
                    .Doc(update)).AnyContext();

                _logger.Trace(() => response.GetRequest());

                if (!response.IsValid) {
                    string message = response.GetErrorMessage();
                    _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                    throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
                }
            }

            if (IsCacheEnabled)
                await Cache.RemoveAsync(id).AnyContext();

            if (sendNotification)
                await PublishChangeTypeMessageAsync(ChangeType.Saved, id).AnyContext();
        }

        public async Task PatchAsync(IEnumerable<string> ids, object update, bool sendNotifications = true) {
            if (ids == null)
                throw new ArgumentNullException(nameof(ids));

            if (update == null)
                throw new ArgumentNullException(nameof(update));

            var idList = ids.ToList();
            if (idList.Count == 0)
                return;

            if (idList.Count == 1) {
                await PatchAsync(idList[0], update, sendNotifications).AnyContext();
                return;
            }

            var patch = update as PatchDocument;
            if (patch != null) {
                await PatchAllAsync(NewQuery().WithIds(idList), update, sendNotifications).AnyContext();
                return;
            }

            var script = update as string;
            var bulkResponse = await _client.BulkAsync(b => {
                foreach (var id in idList) {
                    if (script != null)
                        b.Update<T>(u => u
                            .Id(id)
                            .Index(GetIndexById(id))
                            .Type(ElasticType.Name)
                            .Script(script)
                            .RetriesOnConflict(10));
                    else
                        b.Update<T, object>(u => u
                            .Id(id)
                            .Index(GetIndexById(id))
                            .Type(ElasticType.Name)
                            .Doc(update));
                }

                return b;
            }).AnyContext();
            _logger.Trace(() => bulkResponse.GetRequest());

            // TODO: Is there a better way to handle failures?
            if (!bulkResponse.IsValid) {
                string message = bulkResponse.GetErrorMessage();
                _logger.Error().Exception(bulkResponse.ConnectionStatus.OriginalException).Message(message).Property("request", bulkResponse.GetRequest()).Write();
                throw new ApplicationException(message, bulkResponse.ConnectionStatus.OriginalException);
            }

            // TODO: Find a better way to clear the cache.
            if (IsCacheEnabled)
                await Cache.RemoveAllAsync(idList).AnyContext();

            if (sendNotifications)
                foreach (var id in idList)
                    await PublishChangeTypeMessageAsync(ChangeType.Saved, id).AnyContext();
        }

        protected async Task<long> PatchAllAsync<TQuery>(TQuery query, object update, bool sendNotifications = true, Action<IEnumerable<string>> updatedIdsCallback = null) where TQuery : IPagableQuery, ISelectedFieldsQuery, IRepositoryQuery {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            if (update == null)
                throw new ArgumentNullException(nameof(update));

            long affectedRecords = 0;
            var patch = update as PatchDocument;
            if (patch != null) {
                var patcher = new JsonPatcher();
                affectedRecords += await BatchProcessAsAsync<TQuery, JObject>(query, async results => {
                    var bulkResult = await _client.BulkAsync(b => {
                        foreach (var h in results.Hits) {
                            var target = h.Document as JToken;
                            patcher.Patch(ref target, patch);

                            b.Index<JObject>(i => i
                                .Document(target as JObject)
                                .Id(h.Id)
                                .Index(h.GetIndex())
                                .Type(h.GetIndexType())
                                .Version(h.Version.HasValue ? h.Version.ToString() : null));
                        }

                        return b;
                    }).AnyContext();
                    _logger.Trace(() => bulkResult.GetRequest());

                    if (!bulkResult.IsValid) {
                        _logger.Error()
                            .Exception(bulkResult.ConnectionStatus.OriginalException)
                            .Message($"Error occurred while bulk updating: {bulkResult.GetErrorMessage()}")
                            .Property("Query", query)
                            .Property("Update", update)
                            .Write();

                        return false;
                    }

                    var updatedIds = results.Hits.Select(h => h.Id).ToList();
                    if (IsCacheEnabled)
                        await Cache.RemoveAllAsync(updatedIds).AnyContext();

                    try {
                        updatedIdsCallback?.Invoke(updatedIds);
                    } catch (Exception ex) {
                        _logger.Error(ex, "Error calling updated ids callback.");
                    }

                    return true;
                }).AnyContext();
            } else {
                if (!query.SelectedFields.Contains("id"))
                    query.SelectedFields.Add("id");

                string script = update as string;
                affectedRecords += await BatchProcessAsync(query, async results => {
                    var bulkResult = await _client.BulkAsync(b => {
                        foreach (var h in results.Hits) {
                            if (script != null)
                                b.Update<T>(u => u
                                    .Id(h.Id)
                                    .Index(h.GetIndex())
                                    .Type(h.GetIndexType())
                                    .Script(script)
                                    .RetriesOnConflict(10));
                            else
                                b.Update<T, object>(u => u.Id(h.Id)
                                    .Index(h.GetIndex())
                                    .Type(h.GetIndexType())
                                    .Doc(update));
                        }

                        return b;
                    }).AnyContext();
                    _logger.Trace(() => bulkResult.GetRequest());

                    if (!bulkResult.IsValid) {
                        _logger.Error()
                            .Exception(bulkResult.ConnectionStatus.OriginalException)
                            .Message($"Error occurred while bulk updating: {bulkResult.GetErrorMessage()}")
                            .Property("Query", query)
                            .Property("Update", update)
                            .Write();

                        return false;
                    }

                    var updatedIds = results.Hits.Select(h => h.Id).ToList();
                    if (IsCacheEnabled)
                        await Cache.RemoveAllAsync(updatedIds).AnyContext();

                    try {
                        updatedIdsCallback?.Invoke(updatedIds);
                    } catch (Exception ex) {
                        _logger.Error(ex, "Error calling updated ids callback.");
                    }

                    return true;
                }).AnyContext();
            }

            if (affectedRecords > 0 && sendNotifications)
                await SendQueryNotificationsAsync(ChangeType.Saved, query).AnyContext();

            return affectedRecords;
        }

        public async Task RemoveAsync(string id, bool sendNotification = true) {
            if (String.IsNullOrEmpty(id))
                throw new ArgumentNullException(nameof(id));

            var document = await GetByIdAsync(id, true).AnyContext();
            if (document == null)
                return;

            await RemoveAsync(new[] { document }, sendNotification).AnyContext();
        }

        public Task RemoveAsync(T document, bool sendNotification = true) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            return RemoveAsync(new[] { document }, sendNotification);
        }

        public async Task RemoveAsync(IEnumerable<T> documents, bool sendNotification = true) {
            var docs = documents?.ToList();
            if (docs == null || docs.Any(d => d == null))
                throw new ArgumentNullException(nameof(documents));

            if (docs.Count == 0)
                return;

            if (HasMultipleIndexes) {
                foreach (var documentGroup in docs.GroupBy(TimeSeriesType.GetDocumentIndex))
                    await TimeSeriesType.EnsureIndexAsync(documentGroup.First()).AnyContext();
            }

            await OnDocumentsRemovingAsync(docs).AnyContext();

            // TODO: support Parent and child docs.
            if (docs.Count == 1) {
                var document = docs.First();
                var response = await _client.DeleteAsync(document, descriptor => descriptor.Index(GetDocumentIndexFunc?.Invoke(document)).Type(ElasticType.Name)).AnyContext();
                _logger.Trace(() => response.GetRequest());

                if (!response.IsValid) {
                    string message = response.GetErrorMessage();
                    _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                    throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
                }
            } else {
                var documentsByIndex = docs.GroupBy(d => GetDocumentIndexFunc?.Invoke(d));
                var response = await _client.BulkAsync(bulk => {
                    foreach (var group in documentsByIndex)
                        bulk.DeleteMany<T>(group.Select(g => g.Id), (b, id) => b.Index(group.Key).Type(ElasticType.Name));

                    return bulk;
                }).AnyContext();
                _logger.Trace(() => response.GetRequest());

                if (!response.IsValid) {
                    string message = response.GetErrorMessage();
                    _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                    throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
                }
            }
            
            await OnDocumentsRemovedAsync(docs, sendNotification).AnyContext();
        }

        public async Task<long> RemoveAllAsync(bool sendNotification = true) {
            if (IsCacheEnabled)
                await Cache.RemoveAllAsync().AnyContext();

            return await RemoveAllAsync(NewQuery(), sendNotification).AnyContext();
        }

        protected List<string> FieldsRequiredForRemove { get; } = new List<string>();

        protected async Task<long> RemoveAllAsync<TQuery>(TQuery query, bool sendNotifications = true) where TQuery: IPagableQuery, ISelectedFieldsQuery, IRepositoryQuery {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            if (IsCacheEnabled) {
                foreach (var field in FieldsRequiredForRemove.Union(new[] { "id" }))
                    if (!query.SelectedFields.Contains(field))
                        query.SelectedFields.Add(field);

                // TODO: What if you only want to send one notification?
                return await BatchProcessAsync(query, async results => {
                    await RemoveAsync(results.Documents, sendNotifications).AnyContext();
                    return true;
                }).AnyContext();
            }

            // Delete by query if no caching options.
            long affectedRecords = await CountAsync(query).AnyContext();
            if (affectedRecords == 0)
                return 0;

            var response = await _client.DeleteByQueryAsync(new DeleteByQueryRequest {
                Query = await ElasticType.QueryBuilder.BuildQueryAsync(query, GetQueryOptions(), new SearchDescriptor<T>()).AnyContext(),
                Indices =  new List<IndexNameMarker> { ElasticType.Index.Name },
                Types = new List<TypeNameMarker> { ElasticType.Name }
            }).AnyContext();
            _logger.Trace(() => response.GetRequest());

            if (!response.IsValid) {
                string message = response.GetErrorMessage();
                _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
            }

            if (sendNotifications)
                await SendQueryNotificationsAsync(ChangeType.Removed, query).AnyContext();

            return affectedRecords;
        }

        protected Task<long> BatchProcessAsync<TQuery>(TQuery query, Func<FindResults<T>, Task<bool>> processAsync) where TQuery : IPagableQuery, ISelectedFieldsQuery, IRepositoryQuery {
            return BatchProcessAsAsync(query, processAsync);
        }

        protected async Task<long> BatchProcessAsAsync<TQuery, TResult>(TQuery query, Func<FindResults<TResult>, Task<bool>> processAsync) where TQuery : IPagableQuery, ISelectedFieldsQuery, IRepositoryQuery where TResult : class, new() {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            if (processAsync == null)
                throw new ArgumentNullException(nameof(processAsync));

            var elasticPagingOptions = query.Options as ElasticPagingOptions;
            if (query.Options == null || elasticPagingOptions == null) {
                elasticPagingOptions = ElasticPagingOptions.FromOptions(query.Options);
                query.Options = elasticPagingOptions;
            }

            elasticPagingOptions.UseSnapshotPaging = true;
            if (!elasticPagingOptions.SnapshotLifetime.HasValue)
                elasticPagingOptions.SnapshotLifetime = TimeSpan.FromMinutes(5);

            long recordsProcessed = 0;
            var results = await FindAsAsync<TResult>(query).AnyContext();
            do {
                if (results.Hits.Count == 0)
                    break;

                // TODO: We need a generic way to do bulk operations and do exponential backoffs when we encounter on 429's (bulk queue is full). https://github.com/elastic/elasticsearch-net/pull/2162
                if (await processAsync(results).AnyContext()) {
                    recordsProcessed += results.Documents.Count;
                    continue;
                }

                _logger.Trace("Aborted batch processing.");
                break;
            } while (await results.NextPageAsync().AnyContext());

            _logger.Trace("{0} records processed", recordsProcessed);
            return recordsProcessed;
        }

        #region Events

        public AsyncEvent<DocumentsEventArgs<T>> DocumentsAdding { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

        private async Task OnDocumentsAddingAsync(IReadOnlyCollection<T> documents) {
            if (HasDates)
                documents.OfType<IHaveDates>().SetDates();
            else if (HasCreatedDate)
                documents.OfType<IHaveCreatedDate>().SetCreatedDates();
            
            documents.EnsureIds(ElasticType.CreateDocumentId);
            foreach (var doc in documents.OfType<IVersioned>())
                doc.Version = 0;

            if (DocumentsAdding != null)
                await DocumentsAdding.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this)).AnyContext();

            await OnDocumentsChangingAsync(ChangeType.Added, documents).AnyContext();
        }

        public AsyncEvent<DocumentsEventArgs<T>> DocumentsAdded { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

        private async Task OnDocumentsAddedAsync(IReadOnlyCollection<T> documents, bool sendNotifications) {
            if (DocumentsAdded != null)
                await DocumentsAdded.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this)).AnyContext();

            var modifiedDocs = documents.Select(d => new ModifiedDocument<T>(d, null)).ToList();
            await OnDocumentsChangedAsync(ChangeType.Added, modifiedDocs).AnyContext();

            if (sendNotifications)
                await SendNotificationsAsync(ChangeType.Added, modifiedDocs).AnyContext();
        }

        public AsyncEvent<ModifiedDocumentsEventArgs<T>> DocumentsSaving { get; } = new AsyncEvent<ModifiedDocumentsEventArgs<T>>();

        private async Task OnDocumentsSavingAsync(IReadOnlyCollection<T> documents, IReadOnlyCollection<T> originalDocuments) {
            if (HasDates)
                documents.Cast<IHaveDates>().SetDates();

            var modifiedDocs = originalDocuments.FullOuterJoin(
                documents, cf => cf.Id, cf => cf.Id,
                (original, modified, id) => new { Id = id, Original = original, Modified = modified }).Select(m => new ModifiedDocument<T>( m.Modified, m.Original)).ToList();
            
            var savingDocs = modifiedDocs.Where(m => m.Original != null).ToList();
            if (savingDocs.Count > 0)
                await InvalidateCacheAsync(savingDocs).AnyContext();

            // if we couldn't find an original document, then it must be new.
            var addingDocs = modifiedDocs.Where(m => m.Original == null).Select(m => m.Value).ToList();
            if (addingDocs.Count > 0) {
                await InvalidateCacheAsync(addingDocs).AnyContext();
                await OnDocumentsAddingAsync(addingDocs).AnyContext();
            }

            if (savingDocs.Count == 0)
                return;

            if (DocumentsSaving != null)
                await DocumentsSaving.InvokeAsync(this, new ModifiedDocumentsEventArgs<T>(modifiedDocs, this)).AnyContext();

            await OnDocumentsChangingAsync(ChangeType.Saved, modifiedDocs).AnyContext();
        }

        public AsyncEvent<ModifiedDocumentsEventArgs<T>> DocumentsSaved { get; } = new AsyncEvent<ModifiedDocumentsEventArgs<T>>();

        private async Task OnDocumentsSavedAsync(IReadOnlyCollection<T> documents, IReadOnlyCollection<T> originalDocuments, bool sendNotifications) {
            var modifiedDocs = originalDocuments.FullOuterJoin(
                documents, cf => cf.Id, cf => cf.Id,
                (original, modified, id) => new { Id = id, Original = original, Modified = modified }).Select(m => new ModifiedDocument<T>(m.Modified, m.Original)).ToList();
            
            // if we couldn't find an original document, then it must be new.
            var addedDocs = modifiedDocs.Where(m => m.Original == null).Select(m => m.Value).ToList();
            if (addedDocs.Count > 0)
                await OnDocumentsAddedAsync(addedDocs, sendNotifications).AnyContext();

            var savedDocs = modifiedDocs.Where(m => m.Original != null).ToList();
            if (savedDocs.Count == 0)
                return;

            if (SupportsSoftDeletes && IsCacheEnabled) {
                var deletedIds = modifiedDocs.Where(d => ((ISupportSoftDeletes)d.Original).IsDeleted == false && ((ISupportSoftDeletes)d.Value).IsDeleted).Select(m => m.Value.Id).ToArray();
                if (deletedIds.Length > 0)
                    await Cache.SetAddAsync("deleted", deletedIds, TimeSpan.FromSeconds(30)).AnyContext();

                var undeletedIds = modifiedDocs.Where(d => ((ISupportSoftDeletes)d.Original).IsDeleted && ((ISupportSoftDeletes)d.Value).IsDeleted == false).Select(m => m.Value.Id).ToArray();
                if (undeletedIds.Length > 0)
                    await Cache.SetRemoveAsync("deleted", undeletedIds, TimeSpan.FromSeconds(30)).AnyContext();
            }

            if (DocumentsSaved != null)
                await DocumentsSaved.InvokeAsync(this, new ModifiedDocumentsEventArgs<T>(modifiedDocs, this)).AnyContext();

            await OnDocumentsChangedAsync(ChangeType.Saved, savedDocs).AnyContext();

            if (sendNotifications)
                await SendNotificationsAsync(ChangeType.Saved, savedDocs).AnyContext();
        }

        public AsyncEvent<DocumentsEventArgs<T>> DocumentsRemoving { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

        private async Task OnDocumentsRemovingAsync(IReadOnlyCollection<T> documents) {
            await InvalidateCacheAsync(documents).AnyContext();

            if (DocumentsRemoving != null)
                await DocumentsRemoving.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this)).AnyContext();

            await OnDocumentsChangingAsync(ChangeType.Removed, documents).AnyContext();
        }

        public AsyncEvent<DocumentsEventArgs<T>> DocumentsRemoved { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

        private async Task OnDocumentsRemovedAsync(IReadOnlyCollection<T> documents, bool sendNotifications) {
            if (DocumentsRemoved != null)
                await DocumentsRemoved.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this)).AnyContext();

            await OnDocumentsChangedAsync(ChangeType.Removed, documents).AnyContext();

            if (sendNotifications)
                await SendNotificationsAsync(ChangeType.Removed, documents).AnyContext();
        }

        public AsyncEvent<DocumentsChangeEventArgs<T>> DocumentsChanging { get; } = new AsyncEvent<DocumentsChangeEventArgs<T>>();

        private Task OnDocumentsChangingAsync(ChangeType changeType, IReadOnlyCollection<T> documents) {
            return OnDocumentsChangingAsync(changeType, documents.Select(d => new ModifiedDocument<T>(d, null)).ToList());
        }

        private async Task OnDocumentsChangingAsync(ChangeType changeType, IReadOnlyCollection<ModifiedDocument<T>> documents) {
            if (DocumentsChanging == null)
                return;

            await DocumentsChanging.InvokeAsync(this, new DocumentsChangeEventArgs<T>(changeType, documents, this)).AnyContext();
        }

        public AsyncEvent<DocumentsChangeEventArgs<T>> DocumentsChanged { get; } = new AsyncEvent<DocumentsChangeEventArgs<T>>();

        private Task OnDocumentsChangedAsync(ChangeType changeType, IReadOnlyCollection<T> documents) {
            return OnDocumentsChangedAsync(changeType, documents.Select(d => new ModifiedDocument<T>(d, null)).ToList());
        }

        private async Task OnDocumentsChangedAsync(ChangeType changeType, IReadOnlyCollection<ModifiedDocument<T>> documents) {
            if (DocumentsChanged == null)
                return;

            await DocumentsChanged.InvokeAsync(this, new DocumentsChangeEventArgs<T>(changeType, documents, this)).AnyContext();
        }

        #endregion

        private async Task IndexDocumentsAsync(IReadOnlyCollection<T> documents) {
            if (HasMultipleIndexes) {
                foreach (var documentGroup in documents.GroupBy(TimeSeriesType.GetDocumentIndex))
                    await TimeSeriesType.EnsureIndexAsync(documentGroup.First()).AnyContext();
            }

            if (documents.Count == 1) {
                var document = documents.Single();
                var response = await _client.IndexAsync(document, i => {
                    i.Type(ElasticType.Name);

                    if (GetParentIdFunc != null)
                        i.Parent(GetParentIdFunc(document));

                    if (GetDocumentIndexFunc != null)
                        i.Index(GetDocumentIndexFunc(document));

                    if (HasVersion) {
                        var versionDoc = (IVersioned)document;
                        i.Version(versionDoc.Version);
                    }

                    return i;
                }).AnyContext();

                _logger.Trace(() => response.GetRequest());
                if (!response.IsValid) {
                    string message = response.GetErrorMessage();
                    _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                    throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
                }

                if (HasVersion) {
                    var versionDoc = (IVersioned)document;
                    versionDoc.Version = response.Version != null ? Int64.Parse(response.Version) : -1;
                }
            } else {
                var response = await _client.IndexManyAsync(documents, GetParentIdFunc, GetDocumentIndexFunc, ElasticType.Name).AnyContext();

                _logger.Trace(() => response.GetRequest());
                if (HasVersion) {
                    foreach (var hit in response.Items) {
                        var document = documents.FirstOrDefault(d => d.Id == hit.Id);
                        if (document == null)
                            continue;

                        var versionDoc = (IVersioned)document;
                        if (hit.Version != null)
                            versionDoc.Version = Int64.Parse(hit.Version);
                    }
                }

                if (!response.IsValid) {
                    string message = response.GetErrorMessage();
                    _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                    throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
                }
            }
        }

        protected virtual async Task AddToCacheAsync(ICollection<T> documents, TimeSpan? expiresIn = null) {
            if (!IsCacheEnabled || Cache == null)
                return;

            foreach (var document in documents)
                await Cache.SetAsync(document.Id, document, expiresIn ?? TimeSpan.FromSeconds(ElasticType.DefaultCacheExpirationSeconds)).AnyContext();
        }

        protected bool NotificationsEnabled { get; set; }

        public bool BatchNotifications { get; set; }

        private Task SendNotificationsAsync(ChangeType changeType) {
            return SendNotificationsAsync(changeType, EmptyList);
        }

        private Task SendNotificationsAsync(ChangeType changeType, IReadOnlyCollection<T> documents) {
            return SendNotificationsAsync(changeType, documents.Select(d => new ModifiedDocument<T>(d, null)).ToList());
        }

        protected virtual async Task SendQueryNotificationsAsync(ChangeType changeType, object query) {
            if (!NotificationsEnabled)
                return;

            var delay = TimeSpan.FromSeconds(1.5);

            var idsQuery = query as IIdentityQuery;
            if (idsQuery != null && idsQuery.Ids.Count > 0) {
                foreach (var id in idsQuery.Ids) {
                    await PublishMessageAsync(new EntityChanged {
                        ChangeType = changeType,
                        Id = id,
                        Type = EntityTypeName
                    }, delay).AnyContext();
                }

                return;
            }

            await PublishMessageAsync(new EntityChanged {
                ChangeType = changeType,
                Type = EntityTypeName
            }, delay).AnyContext();
        }

        protected virtual async Task SendNotificationsAsync(ChangeType changeType, IReadOnlyCollection<ModifiedDocument<T>> documents) {
            if (!NotificationsEnabled)
                return;

            var delay = TimeSpan.FromSeconds(1.5);

            if (documents.Count == 0) {
                await PublishChangeTypeMessageAsync(changeType, null, delay).AnyContext();
            } else if (BatchNotifications && documents.Count > 1) {
                // TODO: This needs to support batch notifications
                if (!SupportsSoftDeletes || changeType != ChangeType.Saved) {
                    foreach (var doc in documents.Select(d => d.Value)) {
                        await PublishChangeTypeMessageAsync(changeType, doc, delay).AnyContext();
                    }

                    return;
                }
                var allDeleted = documents.All(d => d.Original != null && ((ISupportSoftDeletes)d.Original).IsDeleted == false && ((ISupportSoftDeletes)d.Value).IsDeleted);
                foreach (var doc in documents.Select(d => d.Value)) {
                    await PublishChangeTypeMessageAsync(allDeleted ? ChangeType.Removed : changeType, doc, delay).AnyContext();
                }
            } else {
                if (!SupportsSoftDeletes) {
                    foreach (var d in documents)
                        await PublishChangeTypeMessageAsync(changeType, d.Value, delay).AnyContext();

                    return;
                }

                foreach (var d in documents) {
                    var docChangeType = changeType;
                    if (d.Original != null) {
                        var document = (ISupportSoftDeletes)d.Value;
                        var original = (ISupportSoftDeletes)d.Original;
                        if (original.IsDeleted == false && document.IsDeleted)
                            docChangeType = ChangeType.Removed;
                    }

                    await PublishChangeTypeMessageAsync(docChangeType, d.Value, delay).AnyContext();
                }
            }
        }

        protected Task PublishChangeTypeMessageAsync(ChangeType changeType, T document, TimeSpan delay) {
            return PublishChangeTypeMessageAsync(changeType, document, null, delay);
        }

        protected virtual Task PublishChangeTypeMessageAsync(ChangeType changeType, T document, IDictionary<string, object> data = null, TimeSpan? delay = null) {
            return PublishChangeTypeMessageAsync(changeType, document?.Id, data, delay);
        }

        protected virtual Task PublishChangeTypeMessageAsync(ChangeType changeType, string id, IDictionary<string, object> data = null, TimeSpan? delay = null) {
            return PublishMessageAsync(new EntityChanged {
                ChangeType = changeType,
                Id = id,
                Type = EntityTypeName,
                Data = new DataDictionary(data ?? new Dictionary<string, object>())
            }, delay);
        }

        protected Task PublishMessageAsync<TMessageType>(TMessageType message, TimeSpan? delay = null) where TMessageType : class {
            if (_messagePublisher == null)
                return Task.CompletedTask;

            return _messagePublisher.PublishAsync(message, delay);
        }
    }
}