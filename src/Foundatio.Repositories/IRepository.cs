using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Utility;

namespace Foundatio.Repositories {
    public interface IRepository<T> : IReadOnlyRepository<T> where T : class, IIdentity, new() {
        Task<T> AddAsync(T document, ICommandOptions options = null);
        Task AddAsync(IEnumerable<T> documents, ICommandOptions options = null);
        Task<T> SaveAsync(T document, ICommandOptions options = null);
        Task SaveAsync(IEnumerable<T> documents, ICommandOptions options = null);
        Task PatchAsync(Id id, object update, ICommandOptions options = null);
        Task PatchAsync(Ids ids, object update, ICommandOptions options = null);
        Task RemoveAsync(Id id, ICommandOptions options = null);
        Task RemoveAsync(Ids ids, ICommandOptions options = null);
        Task RemoveAsync(T document, ICommandOptions options = null);
        Task RemoveAsync(IEnumerable<T> documents, ICommandOptions options = null);
        Task<long> RemoveAllAsync(ICommandOptions options = null);

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
        public static Task PatchAsync<T>(this IRepository<T> repository, IEnumerable<string> ids, object update, ICommandOptions options = null) where T : class, IIdentity, new() {
            return repository.PatchAsync(new Ids(ids), update, options);
        }

        public static Task RemoveAsync<T>(this IRepository<T> repository, Ids ids, ICommandOptions options = null) where T : class, IIdentity, new() {
            return repository.RemoveAsync(new Ids(ids), options);
        }
    }
}