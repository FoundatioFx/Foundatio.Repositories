using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Utility;

namespace Foundatio.Repositories {
    public interface IRepository<T> : IReadOnlyRepository<T> where T : class, IIdentity, new() {
        Task<T> AddAsync(T document, bool addToCache = false, TimeSpan? expiresIn = null, bool sendNotification = true);
        Task AddAsync(IEnumerable<T> documents, bool addToCache = false, TimeSpan? expiresIn = null, bool sendNotification = true);
        Task<T> SaveAsync(T document, bool addToCache = false, TimeSpan? expiresIn = null, bool sendNotification = true);
        Task SaveAsync(IEnumerable<T> documents, bool addToCache = false, TimeSpan? expiresIn = null, bool sendNotification = true);
        Task PatchAsync(Id id, object update, bool sendNotification = true);
        Task PatchAsync(Ids ids, object update, bool sendNotification = true);
        Task RemoveAsync(Id id, bool sendNotification = true);
        Task RemoveAsync(T document, bool sendNotification = true);
        Task RemoveAsync(IEnumerable<T> documents, bool sendNotification = true);
        Task<long> RemoveAllAsync(bool sendNotification = true);

        AsyncEvent<DocumentsEventArgs<T>> DocumentsAdding { get; }
        AsyncEvent<DocumentsEventArgs<T>> DocumentsAdded { get; }
        AsyncEvent<ModifiedDocumentsEventArgs<T>> DocumentsSaving { get; }
        AsyncEvent<ModifiedDocumentsEventArgs<T>> DocumentsSaved { get; }
        AsyncEvent<DocumentsEventArgs<T>> DocumentsRemoving { get; }
        AsyncEvent<DocumentsEventArgs<T>> DocumentsRemoved { get; }
        AsyncEvent<DocumentsChangeEventArgs<T>> DocumentsChanging { get; }
        AsyncEvent<DocumentsChangeEventArgs<T>> DocumentsChanged { get; }
    }

    public static class RepositoryExtensions {
        public static Task PatchAsync<T>(this IRepository<T> repository, IEnumerable<string> ids, object update, bool sendNotification = true) where T : class, IIdentity, new() {
            return repository.PatchAsync(new Ids(ids), update, sendNotification);
        }
    }
}