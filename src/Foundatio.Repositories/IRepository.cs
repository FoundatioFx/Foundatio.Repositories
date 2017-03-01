using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Utility;

namespace Foundatio.Repositories {
    public interface IRepository<T> : IReadOnlyRepository<T> where T : class, IIdentity, new() {
        Task<T> AddAsync(T document, ICommandOptions<T> options = null);
        Task AddAsync(IEnumerable<T> documents, ICommandOptions<T> options = null);
        Task<T> SaveAsync(T document, ICommandOptions<T> options = null);
        Task SaveAsync(IEnumerable<T> documents, ICommandOptions<T> options = null);
        Task PatchAsync(Id id, object update, ICommandOptions<T> options = null);
        Task PatchAsync(Ids ids, object update, ICommandOptions<T> options = null);
        Task RemoveAsync(Id id, ICommandOptions<T> options = null);
        Task RemoveAsync(Ids ids, ICommandOptions<T> options = null);
        Task RemoveAsync(T document, ICommandOptions<T> options = null);
        Task RemoveAsync(IEnumerable<T> documents, ICommandOptions<T> options = null);
        Task<long> RemoveAllAsync(ICommandOptions<T> options = null);

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
        public static Task<T> AddAsync<T>(this IRepository<T> repository, T document, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.AddAsync(document, options.Configure());
        }

        public static Task AddAsync<T>(this IRepository<T> repository, IEnumerable<T> documents, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.AddAsync(documents, options.Configure());
        }

        public static Task<T> SaveAsync<T>(this IRepository<T> repository, T document, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.SaveAsync(document, options.Configure());
        }

        public static Task SaveAsync<T>(this IRepository<T> repository, IEnumerable<T> documents, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.SaveAsync(documents, options.Configure());
        }

        public static Task PatchAsync<T>(this IRepository<T> repository, Id id, object update, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.PatchAsync(id, update, options.Configure());
        }

        public static Task PatchAsync<T>(this IRepository<T> repository, Ids ids, object update, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.PatchAsync(ids, update, options.Configure());
        }

        public static Task RemoveAsync<T>(this IRepository<T> repository, Id id, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.RemoveAsync(id, options.Configure());
        }

        public static Task PatchAsync<T>(this IRepository<T> repository, IEnumerable<string> ids, object update, ICommandOptions<T> options = null) where T : class, IIdentity, new() {
            return repository.PatchAsync(new Ids(ids), update, options);
        }

        public static Task RemoveAsync<T>(this IRepository<T> repository, Ids ids, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.RemoveAsync(ids, options.Configure());
        }

        public static Task RemoveAsync<T>(this IRepository<T> repository, IEnumerable<string> ids, ICommandOptions<T> options = null) where T : class, IIdentity, new() {
            return repository.RemoveAsync(new Ids(ids), options);
        }

        public static Task RemoveAsync<T>(this IRepository<T> repository, T document, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.RemoveAsync(document, options.Configure());
        }

        public static Task RemoveAsync<T>(this IRepository<T> repository, IEnumerable<T> documents, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.RemoveAsync(documents, options.Configure());
        }

        public static Task<long> RemoveAllAsync<T>(this IRepository<T> repository, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.RemoveAllAsync(options.Configure());
        }
    }
}