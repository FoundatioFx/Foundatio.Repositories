using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Utility;

namespace Foundatio.Repositories;

/// <summary>
/// Provides full CRUD (Create, Read, Update, Delete) operations for documents of type <typeparamref name="T"/>.
/// </summary>
/// <typeparam name="T">The document type, which must implement <see cref="IIdentity"/>.</typeparam>
/// <remarks>
/// <para>
/// <see cref="RemoveAsync(Id, ICommandOptions)"/> performs a hard delete, permanently removing the document.
/// To soft delete a document, set <see cref="ISupportSoftDeletes.IsDeleted"/> to <c>true</c> and call
/// <see cref="SaveAsync(T, ICommandOptions)"/>. When a document's <c>IsDeleted</c> changes from <c>false</c>
/// to <c>true</c>, the repository publishes a <see cref="ChangeType.Removed"/> change notification.
/// </para>
/// </remarks>
public interface IRepository<T> : IReadOnlyRepository<T> where T : class, IIdentity, new()
{
    /// <summary>
    /// Adds a new document to the repository.
    /// </summary>
    /// <param name="document">The document to add.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    /// <returns>The added document with any generated fields populated (e.g., Id).</returns>
    Task<T> AddAsync(T document, CommandOptionsDescriptor<T> options);

    /// <inheritdoc cref="AddAsync(T, CommandOptionsDescriptor{T})"/>
    Task<T> AddAsync(T document, ICommandOptions options = null);

    /// <summary>
    /// Adds multiple documents to the repository.
    /// </summary>
    /// <param name="documents">The documents to add.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    Task AddAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options);

    /// <inheritdoc cref="AddAsync(IEnumerable{T}, CommandOptionsDescriptor{T})"/>
    Task AddAsync(IEnumerable<T> documents, ICommandOptions options = null);

    /// <summary>
    /// Saves changes to an existing document, or adds it if it doesn't exist.
    /// </summary>
    /// <param name="document">The document to save.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    /// <returns>The saved document.</returns>
    Task<T> SaveAsync(T document, CommandOptionsDescriptor<T> options);

    /// <inheritdoc cref="SaveAsync(T, CommandOptionsDescriptor{T})"/>
    Task<T> SaveAsync(T document, ICommandOptions options = null);

    /// <summary>
    /// Saves changes to multiple existing documents.
    /// </summary>
    /// <param name="documents">The documents to save.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    Task SaveAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options);

    /// <inheritdoc cref="SaveAsync(IEnumerable{T}, CommandOptionsDescriptor{T})"/>
    Task SaveAsync(IEnumerable<T> documents, ICommandOptions options = null);

    /// <summary>
    /// Applies a patch operation to a document.
    /// </summary>
    /// <param name="id">The identifier of the document to patch.</param>
    /// <param name="operation">The patch operation to apply (e.g., <see cref="PartialPatch"/>, <see cref="JsonPatch"/>, <see cref="ScriptPatch"/>).</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    Task PatchAsync(Id id, IPatchOperation operation, CommandOptionsDescriptor<T> options);

    /// <inheritdoc cref="PatchAsync(Id, IPatchOperation, CommandOptionsDescriptor{T})"/>
    Task PatchAsync(Id id, IPatchOperation operation, ICommandOptions options = null);

    /// <summary>
    /// Applies a patch operation to multiple documents.
    /// </summary>
    /// <param name="ids">The identifiers of the documents to patch.</param>
    /// <param name="operation">The patch operation to apply.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    Task PatchAsync(Ids ids, IPatchOperation operation, CommandOptionsDescriptor<T> options);

    /// <inheritdoc cref="PatchAsync(Ids, IPatchOperation, CommandOptionsDescriptor{T})"/>
    Task PatchAsync(Ids ids, IPatchOperation operation, ICommandOptions options = null);

    /// <summary>
    /// Permanently removes a document from the repository (hard delete).
    /// </summary>
    /// <param name="id">The identifier of the document to remove.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    Task RemoveAsync(Id id, CommandOptionsDescriptor<T> options);

    /// <inheritdoc cref="RemoveAsync(Id, CommandOptionsDescriptor{T})"/>
    Task RemoveAsync(Id id, ICommandOptions options = null);

    /// <summary>
    /// Permanently removes multiple documents from the repository (hard delete).
    /// </summary>
    /// <param name="ids">The identifiers of the documents to remove.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    Task RemoveAsync(Ids ids, CommandOptionsDescriptor<T> options);

    /// <inheritdoc cref="RemoveAsync(Ids, CommandOptionsDescriptor{T})"/>
    Task RemoveAsync(Ids ids, ICommandOptions options = null);

    /// <summary>
    /// Permanently removes a document from the repository (hard delete).
    /// </summary>
    /// <param name="document">The document to remove.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    Task RemoveAsync(T document, CommandOptionsDescriptor<T> options);

    /// <inheritdoc cref="RemoveAsync(T, CommandOptionsDescriptor{T})"/>
    Task RemoveAsync(T document, ICommandOptions options = null);

    /// <summary>
    /// Permanently removes multiple documents from the repository (hard delete).
    /// </summary>
    /// <param name="documents">The documents to remove.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    Task RemoveAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options);

    /// <inheritdoc cref="RemoveAsync(IEnumerable{T}, CommandOptionsDescriptor{T})"/>
    Task RemoveAsync(IEnumerable<T> documents, ICommandOptions options = null);

    /// <summary>
    /// Permanently removes all documents from the repository (hard delete).
    /// </summary>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    /// <returns>The number of documents removed.</returns>
    Task<long> RemoveAllAsync(CommandOptionsDescriptor<T> options);

    /// <inheritdoc cref="RemoveAllAsync(CommandOptionsDescriptor{T})"/>
    Task<long> RemoveAllAsync(ICommandOptions options = null);

    /// <summary>
    /// Event raised before documents are added to the repository.
    /// </summary>
    AsyncEvent<DocumentsEventArgs<T>> DocumentsAdding { get; }

    /// <summary>
    /// Event raised after documents have been added to the repository.
    /// </summary>
    AsyncEvent<DocumentsEventArgs<T>> DocumentsAdded { get; }

    /// <summary>
    /// Event raised before documents are saved to the repository.
    /// </summary>
    AsyncEvent<ModifiedDocumentsEventArgs<T>> DocumentsSaving { get; }

    /// <summary>
    /// Event raised after documents have been saved to the repository.
    /// </summary>
    AsyncEvent<ModifiedDocumentsEventArgs<T>> DocumentsSaved { get; }

    /// <summary>
    /// Event raised before documents are removed from the repository.
    /// </summary>
    AsyncEvent<DocumentsEventArgs<T>> DocumentsRemoving { get; }

    /// <summary>
    /// Event raised after documents have been removed from the repository.
    /// </summary>
    AsyncEvent<DocumentsEventArgs<T>> DocumentsRemoved { get; }

    /// <summary>
    /// Event raised before any document change (add, save, or remove) is persisted.
    /// </summary>
    AsyncEvent<DocumentsChangeEventArgs<T>> DocumentsChanging { get; }

    /// <summary>
    /// Event raised after any document change (add, save, or remove) has been persisted.
    /// </summary>
    AsyncEvent<DocumentsChangeEventArgs<T>> DocumentsChanged { get; }
}
