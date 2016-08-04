using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FluentValidation;
using Nest;
using Elasticsearch.Net;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Logging;
using Foundatio.Messaging;
using Foundatio.Repositories.Elasticsearch.Queries;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;
using Foundatio.Utility;

namespace Foundatio.Repositories.Elasticsearch {
    public abstract class ElasticRepositoryBase<T> : ElasticReadOnlyRepositoryBase<T>, IRepository<T> where T : class, IIdentity, new() {
        protected readonly IValidator<T> _validator;
        protected readonly IMessagePublisher _messagePublisher;

        protected ElasticRepositoryBase(IElasticClient client) : this(client, null, null, null, null) { }

        protected ElasticRepositoryBase(IElasticClient client, IValidator<T> validator, ICacheClient cache, IMessagePublisher messagePublisher, ILogger logger) : base(client, cache, logger) {
            _validator = validator;
            _messagePublisher = messagePublisher;
            NotificationsEnabled = _messagePublisher != null;

            if (HasCreatedDate)
               FieldsRequiredForRemove.Add("createdUtc"); // TODO: What do we do if this field is serialized differently?
        }
        
        public async Task<T> AddAsync(T document, bool addToCache = false, TimeSpan? expiresIn = null, bool sendNotification = true) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            await AddAsync(new[] { document }, addToCache, expiresIn, sendNotification).AnyContext();
            return document;
        }

        public async Task AddAsync(ICollection<T> documents, bool addToCache = false, TimeSpan? expiresIn = null, bool sendNotification = true) {
            if (documents == null || documents.Any(d => d == null))
                throw new ArgumentNullException(nameof(documents));

            if (documents.Count == 0)
                return;

            await OnDocumentsAddingAsync(documents).AnyContext();

            if (_validator != null)
                foreach (var doc in documents)
                    await _validator.ValidateAndThrowAsync(doc).AnyContext();

            await IndexDocuments(documents);

            if (addToCache)
                await AddToCacheAsync(documents, expiresIn).AnyContext();

            await OnDocumentsAddedAsync(documents, sendNotification).AnyContext();
        }

        public async Task<T> SaveAsync(T document, bool addToCache = false, TimeSpan? expiresIn = null, bool sendNotifications = true) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            await SaveAsync(new[] { document }, addToCache, expiresIn, sendNotifications).AnyContext();
            return document;
        }

        public async Task SaveAsync(ICollection<T> documents, bool addToCache = false, TimeSpan? expiresIn = null, bool sendNotifications = true) {
            if (documents == null || documents.Any(d => d == null))
                throw new ArgumentNullException(nameof(documents));

            if (documents.Count == 0)
                return;
            
            string[] ids = documents.Where(d => !String.IsNullOrEmpty(d.Id)).Select(d => d.Id).ToArray();
            if (ids.Length < documents.Count)
                throw new ApplicationException("Id must be set when calling Save.");

            var originalDocuments = ids.Length > 0 ? (await GetByIdsAsync(ids).AnyContext()).Documents : new List<T>();
            // TODO: What should we do if original document count differs from document count?

            await OnDocumentsSavingAsync(documents, originalDocuments).AnyContext();

            if (_validator != null)
                foreach (var doc in documents)
                    await _validator.ValidateAndThrowAsync(doc).AnyContext();

            await IndexDocuments(documents).AnyContext();

            if (addToCache)
                await AddToCacheAsync(documents, expiresIn).AnyContext();

            await OnDocumentsSavedAsync(documents, originalDocuments, sendNotifications).AnyContext();
        }

        // TODO: Refactor to a single method that does a bulk operation and then RemoveAll and UpdateAll use that
        protected async Task<long> UpdateAllAsync(object query, object update, bool sendNotifications = true) {
            long recordsAffected = 0;

            var searchDescriptor = CreateSearchDescriptor(query)
                .Source(s => s.Include(f => f.Id))
                .SearchType(SearchType.Scan)
                .Scroll("4s")
                .Size(ElasticType.BulkBatchSize);

            var scanResults = await _client.SearchAsync<T>(searchDescriptor).AnyContext();

            // Check to see if no scroll id was returned. This will occur when the index doesn't exist.
            if (!scanResults.IsValid || String.IsNullOrEmpty(scanResults.ScrollId)) {
                _logger.Error().Exception(scanResults.ConnectionStatus.OriginalException).Message(scanResults.GetErrorMessage()).Property("request", scanResults.GetRequest()).WriteIf(!scanResults.IsValid);
                return 0;
            }

            var results = await _client.ScrollAsync<T>("4s", scanResults.ScrollId).AnyContext();
            _logger.Trace(() => results.GetRequest());
            while (results.Hits.Any()) {
                var bulkResult = await _client.BulkAsync(b => {
                    string script = update as string;
                    foreach (var h in results.Hits) {
                        if (script != null)
                            b.Update<T>(u => u.Id(h.Id).Index(h.Index).Version(h.Version).Script(script));
                        else
                            b.Update<T, object>(u => u.Id(h.Id).Index(h.Index).Version(h.Version).Doc(update));
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

                    return 0;
                }

                if (IsCacheEnabled)
                    foreach (var d in results.Hits)
                        await Cache.RemoveAsync(d.Id).AnyContext();

                recordsAffected += results.Documents.Count();
                results = await _client.ScrollAsync<T>("4s", results.ScrollId).AnyContext();
                _logger.Trace(() => results.GetRequest());
            }

            if (recordsAffected <= 0)
                return 0;

            if (sendNotifications)
                await SendNotificationsAsync(ChangeType.Saved).AnyContext();

            return recordsAffected;
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

        public async Task RemoveAsync(ICollection<T> documents, bool sendNotification = true) {
            if (documents == null || documents.Any(d => d == null))
                throw new ArgumentNullException(nameof(documents));

            if (documents.Count == 0)
                return;

            await OnDocumentsRemovingAsync(documents).AnyContext();

            // TODO: support Parent and child docs.
            if (documents.Count == 1) {
                var document = documents.First();
                var response = await _client.DeleteAsync(document, descriptor => descriptor.Index(GetDocumentIndexFunc?.Invoke(document))).AnyContext();
                _logger.Trace(() => response.GetRequest());

                if (!response.IsValid) {
                    string message = response.GetErrorMessage();
                    _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                    throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
                }
            } else {
                var documentsByIndex = documents.GroupBy(d => GetDocumentIndexFunc?.Invoke(d));
                var response = await _client.BulkAsync(bulk => {
                    foreach (var group in documentsByIndex)
                        bulk.DeleteMany<T>(group.Select(g => g.Id), (b, id) => b.Index(group.Key));

                    return bulk;
                }).AnyContext();
                _logger.Trace(() => response.GetRequest());

                if (!response.IsValid) {
                    string message = response.GetErrorMessage();
                    _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                    throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
                }
            }

            await OnDocumentsRemovedAsync(documents, sendNotification).AnyContext();
        }

        public async Task RemoveAllAsync() {
            if (IsCacheEnabled)
                await Cache.RemoveAllAsync().AnyContext();

            await RemoveAllAsync(new Query(), false).AnyContext();
        }

        protected List<string> FieldsRequiredForRemove { get; } = new List<string>();

        protected async Task<long> RemoveAllAsync<TQuery>(TQuery query, bool sendNotifications = true) where TQuery: IPagableQuery, ISelectedFieldsQuery {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            long recordsAffected = 0;

            query.UseSnapshotPaging = true;
            foreach (var field in FieldsRequiredForRemove.Union(new[] { "id" }))
                if (!query.SelectedFields.Contains(field))
                    query.SelectedFields.Add(field);

            var results = await FindAsAsync<T>(query).AnyContext();
            do {
                recordsAffected += results.Documents.Count;
                await RemoveAsync(results.Documents, sendNotifications).AnyContext();
            } while (await results.NextPageAsync().AnyContext());

            _logger.Trace("{0} records removed", recordsAffected);
            return recordsAffected;
        }

        #region Events

        public AsyncEvent<DocumentsEventArgs<T>> DocumentsAdding { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

        private async Task OnDocumentsAddingAsync(ICollection<T> documents) {
            if (HasDates)
                documents.OfType<IHaveDates>().SetDates();
            else if (HasCreatedDate)
                documents.OfType<IHaveCreatedDate>().SetCreatedDates();
            
            documents.EnsureIds(ElasticType.CreateDocumentId);

            if (HasVersion) {
                foreach (var document in documents.OfType<IVersioned>())
                    document.Version = 0;
            }

            if (DocumentsAdding != null)
                await DocumentsAdding.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this)).AnyContext();

            await OnDocumentsChangingAsync(ChangeType.Added, documents).AnyContext();
        }

        public AsyncEvent<DocumentsEventArgs<T>> DocumentsAdded { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

        private async Task OnDocumentsAddedAsync(ICollection<T> documents, bool sendNotifications) {
            if (DocumentsAdded != null)
                await DocumentsAdded.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this)).AnyContext();

            var modifiedDocs = documents.Select(d => new ModifiedDocument<T>(d, null)).ToList();
            await OnDocumentsChangedAsync(ChangeType.Added, modifiedDocs).AnyContext();

            if (sendNotifications)
                await SendNotificationsAsync(ChangeType.Added, modifiedDocs).AnyContext();
        }

        public AsyncEvent<ModifiedDocumentsEventArgs<T>> DocumentsSaving { get; } = new AsyncEvent<ModifiedDocumentsEventArgs<T>>();

        private async Task OnDocumentsSavingAsync(ICollection<T> documents, ICollection<T> originalDocuments) {
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

        private async Task OnDocumentsSavedAsync(ICollection<T> documents, ICollection<T> originalDocuments, bool sendNotifications) {
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

            if (DocumentsSaved != null)
                await DocumentsSaved.InvokeAsync(this, new ModifiedDocumentsEventArgs<T>(modifiedDocs, this)).AnyContext();

            await OnDocumentsChangedAsync(ChangeType.Saved, savedDocs).AnyContext();

            if (sendNotifications)
                await SendNotificationsAsync(ChangeType.Saved, savedDocs).AnyContext();
        }

        public AsyncEvent<DocumentsEventArgs<T>> DocumentsRemoving { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

        private async Task OnDocumentsRemovingAsync(ICollection<T> documents) {
            await InvalidateCacheAsync(documents).AnyContext();

            if (DocumentsRemoving != null)
                await DocumentsRemoving.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this)).AnyContext();

            await OnDocumentsChangingAsync(ChangeType.Removed, documents).AnyContext();
        }

        public AsyncEvent<DocumentsEventArgs<T>> DocumentsRemoved { get; } = new AsyncEvent<DocumentsEventArgs<T>>();

        private async Task OnDocumentsRemovedAsync(ICollection<T> documents, bool sendNotifications) {
            if (DocumentsRemoved != null)
                await DocumentsRemoved.InvokeAsync(this, new DocumentsEventArgs<T>(documents, this)).AnyContext();

            await OnDocumentsChangedAsync(ChangeType.Removed, documents).AnyContext();

            if (sendNotifications)
                await SendNotificationsAsync(ChangeType.Removed, documents).AnyContext();
        }

        public AsyncEvent<DocumentsChangeEventArgs<T>> DocumentsChanging { get; } = new AsyncEvent<DocumentsChangeEventArgs<T>>();

        private Task OnDocumentsChangingAsync(ChangeType changeType, ICollection<T> documents) {
            return OnDocumentsChangingAsync(changeType, documents.Select(d => new ModifiedDocument<T>(d, null)).ToList());
        }

        private async Task OnDocumentsChangingAsync(ChangeType changeType, ICollection<ModifiedDocument<T>> documents) {
            if (DocumentsChanging == null)
                return;

            await DocumentsChanging.InvokeAsync(this, new DocumentsChangeEventArgs<T>(changeType, documents, this)).AnyContext();
        }

        public AsyncEvent<DocumentsChangeEventArgs<T>> DocumentsChanged { get; } = new AsyncEvent<DocumentsChangeEventArgs<T>>();

        private Task OnDocumentsChangedAsync(ChangeType changeType, ICollection<T> documents) {
            return OnDocumentsChangedAsync(changeType, documents.Select(d => new ModifiedDocument<T>(d, null)).ToList());
        }

        private async Task OnDocumentsChangedAsync(ChangeType changeType, ICollection<ModifiedDocument<T>> documents) {
            if (DocumentsChanged == null)
                return;

            await DocumentsChanged.InvokeAsync(this, new DocumentsChangeEventArgs<T>(changeType, documents, this)).AnyContext();
        }

        #endregion

        private async Task IndexDocuments(ICollection<T> documents) {
            if (HasMultipleIndexes) {
                foreach (var document in documents)
                    TimeSeriesType.EnsureIndex(document);
            }

            IResponse response;
            if (documents.Count == 1) {
                var document = documents.Single();
                response = await _client.IndexAsync(document, i => {
                    if (GetParentIdFunc != null)
                        i.Parent(GetParentIdFunc(document));

                    if (GetDocumentIndexFunc != null)
                        i.Index(GetDocumentIndexFunc(document));

                    if (HasVersion) {
                        var versionDoc = (IVersioned)document;
                        i.Version(versionDoc.Version);
                        versionDoc.Version++;
                    }

                    return i;
                }).AnyContext();
            } else {
                response = await _client.IndexManyAsync(documents, GetParentIdFunc, GetDocumentIndexFunc).AnyContext();
            }

            _logger.Trace(() => response.GetRequest());
            
            if (!response.IsValid) {
                string message = response.GetErrorMessage();
                _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();

                var bulkResponse = response as IBulkResponse;
                if (bulkResponse != null) {
                    if (HasVersion) {
                        var idsWithErrors = bulkResponse.ItemsWithErrors.Where(i => !i.IsValid).Select(i => i.Id).ToList();
                        foreach (var document in documents.Where(d => idsWithErrors.Contains(d.Id)))
                            ((IVersioned)document).Version--;
                    }
                    
                    throw new ApplicationException(message, bulkResponse.ConnectionStatus.OriginalException);
                }

                if (HasVersion) {
                    foreach (var document in documents)
                        ((IVersioned)document).Version--;
                }
                
                throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
            }
        }

        protected virtual async Task AddToCacheAsync(ICollection<T> documents, TimeSpan? expiresIn = null) {
            if (!IsCacheEnabled || Cache == null)
                return;

            foreach (var document in documents)
                await Cache.SetAsync(document.Id, document, expiresIn ?? TimeSpan.FromSeconds(RepositoryConstants.DEFAULT_CACHE_EXPIRATION_SECONDS)).AnyContext();
        }

        protected bool NotificationsEnabled { get; set; }

        public bool BatchNotifications { get; set; }

        private Task SendNotificationsAsync(ChangeType changeType) {
            return SendNotificationsAsync(changeType, new List<T>());
        }

        private Task SendNotificationsAsync(ChangeType changeType, ICollection<T> documents) {
            return SendNotificationsAsync(changeType, documents.Select(d => new ModifiedDocument<T>(d, null)).ToList());
        }

        protected virtual async Task SendNotificationsAsync(ChangeType changeType, ICollection<ModifiedDocument<T>> documents) {
            if (!NotificationsEnabled)
                return;

            var delay = TimeSpan.FromSeconds(1.5);

            if (!documents.Any()) {
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
            return PublishMessageAsync(new EntityChanged {
                ChangeType = changeType,
                Id = document?.Id,
                Type = ElasticType.Name,
                Data = new DataDictionary(data ?? new Dictionary<string, object>())
            }, delay);
        }
        
        protected async Task PublishMessageAsync<TMessageType>(TMessageType message, TimeSpan? delay = null) where TMessageType : class {
            if (_messagePublisher == null)
                return;

            await _messagePublisher.PublishAsync(message, delay).AnyContext();
        }
    }
}