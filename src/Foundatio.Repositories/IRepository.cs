using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Utility;

namespace Foundatio.Repositories {
    public interface IRepository<T> : IReadOnlyRepository<T> where T : class, IIdentity, new() {
        Task<T> AddAsync(T document, CommandOptionsDescriptor<T> options = null);
        Task AddAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options = null);
        Task<T> SaveAsync(T document, CommandOptionsDescriptor<T> options = null);
        Task SaveAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options = null);
        Task PatchAsync(Id id, IPatchOperation operation, CommandOptionsDescriptor<T> options = null);
        Task PatchAsync(Ids ids, IPatchOperation operation, CommandOptionsDescriptor<T> options = null);
        Task RemoveAsync(Id id, CommandOptionsDescriptor<T> options = null);
        Task RemoveAsync(Ids ids, CommandOptionsDescriptor<T> options = null);
        Task RemoveAsync(T document, CommandOptionsDescriptor<T> options = null);
        Task RemoveAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options = null);
        Task<long> RemoveAllAsync(CommandOptionsDescriptor<T> options = null);

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
        public static Task<T> AddAsync<T>(this IRepository<T> repository, T document, ICommandOptions options) where T : class, IIdentity, new() {
            return repository.AddAsync(document, o => options.As<T>());
        }

        public static Task AddAsync<T>(this IRepository<T> repository, IEnumerable<T> documents, ICommandOptions options) where T : class, IIdentity, new() {
            return repository.AddAsync(documents, o => options.As<T>());
        }

        public static Task<T> SaveAsync<T>(this IRepository<T> repository, T document, ICommandOptions options) where T : class, IIdentity, new() {
            return repository.SaveAsync(document, o => options.As<T>());
        }

        public static Task SaveAsync<T>(this IRepository<T> repository, IEnumerable<T> documents, ICommandOptions options) where T : class, IIdentity, new() {
            return repository.SaveAsync(documents, o => options.As<T>());
        }

        public static Task PatchAsync<T>(this IRepository<T> repository, Id id, IPatchOperation operation, ICommandOptions options) where T : class, IIdentity, new() {
            return repository.PatchAsync(id, operation, o => options.As<T>());
        }

        public static Task PatchAsync<T>(this IRepository<T> repository, Ids ids, IPatchOperation operation, ICommandOptions options) where T : class, IIdentity, new() {
            return repository.PatchAsync(ids, operation, o => options.As<T>());
        }

        public static Task RemoveAsync<T>(this IRepository<T> repository, Id id, ICommandOptions options) where T : class, IIdentity, new() {
            return repository.RemoveAsync(id, o => options.As<T>());
        }

        public static Task RemoveAsync<T>(this IRepository<T> repository, Ids ids, ICommandOptions options) where T : class, IIdentity, new() {
            return repository.RemoveAsync(ids, o => options.As<T>());
        }

        public static Task RemoveAsync<T>(this IRepository<T> repository, T document, ICommandOptions options) where T : class, IIdentity, new() {
            return repository.RemoveAsync(document, o => options.As<T>());
        }

        public static Task RemoveAsync<T>(this IRepository<T> repository, IEnumerable<T> documents, ICommandOptions options) where T : class, IIdentity, new() {
            return repository.RemoveAsync(documents, o => options.As<T>());
        }

        public static Task<long> RemoveAllAsync<T>(this IRepository<T> repository, ICommandOptions options) where T : class, IIdentity, new() {
            return repository.RemoveAllAsync(o => options.As<T>());
        }
    }}