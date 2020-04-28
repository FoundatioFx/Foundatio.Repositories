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
        
        public Task<T> AddAsync(T document, CommandOptionsDescriptor<T> options) {
            return AddAsync(document, options.Configure());
        }

        public Task AddAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options) {
            return AddAsync(documents, options.Configure());
        }

        public Task<T> SaveAsync(T document, CommandOptionsDescriptor<T> options) {
            return SaveAsync(document, options.Configure());
        }

        public Task SaveAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options) {
            return SaveAsync(documents, options.Configure());
        }

        public Task PatchAsync(Id id, IPatchOperation operation, CommandOptionsDescriptor<T> options) {
            return PatchAsync(id, operation, options.Configure());
        }

        public Task PatchAsync(Ids ids, IPatchOperation operation, CommandOptionsDescriptor<T> options) {
            return PatchAsync(ids, operation, options.Configure());
        }

        public Task RemoveAsync(Id id, CommandOptionsDescriptor<T> options) {
            return RemoveAsync(id, options.Configure());
        }

        public Task RemoveAsync(Ids ids, CommandOptionsDescriptor<T> options) {
            return RemoveAsync(ids, options.Configure());
        }

        public Task RemoveAsync(T document, CommandOptionsDescriptor<T> options) {
            return RemoveAsync(document, options.Configure());
        }

        public Task RemoveAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options) {
            return RemoveAsync(documents, options.Configure());
        }

        public Task<long> RemoveAllAsync(CommandOptionsDescriptor<T> options) {
            return RemoveAllAsync(options.Configure());
        }
    }
}