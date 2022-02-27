using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Utility;

namespace Foundatio.Repositories;

public interface IRepository<T> : IReadOnlyRepository<T> where T: class, IIdentity, new() {
    Task<T> AddAsync(T document, CommandOptionsDescriptor<T> options);
    Task<T> AddAsync(T document, ICommandOptions options = null);

    Task AddAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options);
    Task AddAsync(IEnumerable<T> documents, ICommandOptions options = null);

    Task<T> SaveAsync(T document, CommandOptionsDescriptor<T> options);
    Task<T> SaveAsync(T document, ICommandOptions options = null);

    Task SaveAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options);
    Task SaveAsync(IEnumerable<T> documents, ICommandOptions options = null);

    Task PatchAsync(Id id, IPatchOperation operation, CommandOptionsDescriptor<T> options);
    Task PatchAsync(Id id, IPatchOperation operation, ICommandOptions options = null);

    Task PatchAsync(Ids ids, IPatchOperation operation, CommandOptionsDescriptor<T> options);
    Task PatchAsync(Ids ids, IPatchOperation operation, ICommandOptions options = null);

    Task RemoveAsync(Id id, CommandOptionsDescriptor<T> options);
    Task RemoveAsync(Id id, ICommandOptions options = null);

    Task RemoveAsync(Ids ids, CommandOptionsDescriptor<T> options);
    Task RemoveAsync(Ids ids, ICommandOptions options = null);

    Task RemoveAsync(T document, CommandOptionsDescriptor<T> options);
    Task RemoveAsync(T document, ICommandOptions options = null);

    Task RemoveAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options);
    Task RemoveAsync(IEnumerable<T> documents, ICommandOptions options = null);

    Task<long> RemoveAllAsync(CommandOptionsDescriptor<T> options);
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
