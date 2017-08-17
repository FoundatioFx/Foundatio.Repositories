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
        Task PatchAsync(Id id, IPatchOperation operation, ICommandOptions options = null);
        Task PatchAsync(Ids ids, IPatchOperation operation, ICommandOptions options = null);
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

        public static Task PatchAsync<T>(this IRepository<T> repository, Id id, IPatchOperation operation, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.PatchAsync(id, operation, options.Configure());
        }

        public static Task PatchAsync<T>(this IRepository<T> repository, Ids ids, IPatchOperation operation, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.PatchAsync(ids, operation, options.Configure());
        }

        public static Task RemoveAsync<T>(this IRepository<T> repository, Id id, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.RemoveAsync(id, options.Configure());
        }

        public static Task RemoveAsync<T>(this IRepository<T> repository, Ids ids, CommandOptionsDescriptor<T> options = null) where T : class, IIdentity, new() {
            return repository.RemoveAsync(ids, options.Configure());
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