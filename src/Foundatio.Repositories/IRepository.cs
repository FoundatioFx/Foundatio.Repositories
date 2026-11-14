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
    /// <exception cref="Exceptions.DuplicateDocumentException">Thrown when a document with the same ID already exists.</exception>
    /// <exception cref="Exceptions.DocumentException">Thrown when the Elasticsearch request fails.</exception>
    Task<T> AddAsync(T document, CommandOptionsDescriptor<T>? options);

    /// <inheritdoc cref="AddAsync(T, CommandOptionsDescriptor{T})"/>
    Task<T> AddAsync(T document, ICommandOptions? options = null);

    /// <summary>
    /// Adds multiple documents to the repository.
    /// </summary>
    /// <param name="documents">The documents to add.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    /// <exception cref="Exceptions.DuplicateDocumentException">Thrown when any document ID already exists. Successfully added documents are still cached and notified.</exception>
    /// <exception cref="Exceptions.DocumentException">Thrown when the Elasticsearch request fails.</exception>
    Task AddAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T>? options);

    /// <inheritdoc cref="AddAsync(IEnumerable{T}, CommandOptionsDescriptor{T})"/>
    Task AddAsync(IEnumerable<T> documents, ICommandOptions? options = null);

    /// <summary>
    /// Saves changes to an existing document, or adds it if it doesn't exist.
    /// </summary>
    /// <param name="document">The document to save.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    /// <returns>The saved document.</returns>
    /// <remarks>An <see cref="IVersioned"/> document with a version uses optimistic concurrency. A versioned document
    /// with no version is created and fails if its ID already exists. <c>SkipVersionCheck()</c> allows an unconditional overwrite.
    /// <c>IfVersion</c> is supported only by single-document patches and is rejected here.</remarks>
    /// <exception cref="Exceptions.DuplicateDocumentException">Thrown when a versioned document with no version has an ID that already exists.</exception>
    /// <exception cref="Exceptions.VersionConflictDocumentException">Thrown when the document version conflicts with the stored version (optimistic concurrency).</exception>
    /// <exception cref="Exceptions.DocumentException">Thrown when the Elasticsearch request fails.</exception>
    Task<T> SaveAsync(T document, CommandOptionsDescriptor<T>? options);

    /// <inheritdoc cref="SaveAsync(T, CommandOptionsDescriptor{T})"/>
    Task<T> SaveAsync(T document, ICommandOptions? options = null);

    /// <summary>
    /// Saves changes to multiple existing documents.
    /// </summary>
    /// <param name="documents">The documents to save.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    /// <remarks>Each <see cref="IVersioned"/> document checks its version, or is created if it has no version, unless
    /// <c>SkipVersionCheck()</c> is set. Successful documents are cached and notified before a failure is thrown.
    /// <c>IfVersion</c> is rejected.</remarks>
    /// <exception cref="Exceptions.DuplicateDocumentException">Thrown when a versioned document with no version has an ID that already exists.</exception>
    /// <exception cref="Exceptions.VersionConflictDocumentException">Thrown when any document has a version conflict. Successfully saved documents are still cached and notified.</exception>
    /// <exception cref="Exceptions.DocumentException">Thrown when the Elasticsearch request fails.</exception>
    Task SaveAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T>? options);

    /// <inheritdoc cref="SaveAsync(IEnumerable{T}, CommandOptionsDescriptor{T})"/>
    Task SaveAsync(IEnumerable<T> documents, ICommandOptions? options = null);

    /// <summary>
    /// Applies a patch operation to a document.
    /// </summary>
    /// <param name="id">The identifier of the document to patch.</param>
    /// <param name="operation">The patch operation to apply (e.g., <see cref="PartialPatch"/>, <see cref="JsonPatch"/>, <see cref="ScriptPatch"/>).</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    /// <returns>
    /// <c>true</c> if the document was modified; <c>false</c> if the operation was a no-op.
    /// <list type="bullet">
    /// <item><see cref="PartialPatch"/>: Elasticsearch's automatic <c>detect_noop</c> determines the result, except with a default ingest pipeline, where the document is reindexed. Automatic date tracking injects <c>UpdatedUtc</c>, which typically prevents noop detection.</item>
    /// <item><see cref="ScriptPatch"/>: Signal a no-op explicitly with <c>ctx.op = 'noop'</c>.</item>
    /// <item><see cref="ActionPatch{T}"/>: Returns <c>false</c> when all <see cref="ActionPatch{T}.Actions"/> return <c>false</c>, or when there are no actions. The <c>Action{T}</c> overload always assumes modification.</item>
    /// <item><see cref="JsonPatch"/>: Always returns <c>true</c> (uses get-modify-reindex). Empty operations return <c>false</c>.</item>
    /// </list>
    /// </returns>
    /// <exception cref="Exceptions.DocumentNotFoundException">Thrown when the document does not exist.</exception>
    /// <exception cref="Exceptions.VersionConflictDocumentException">Thrown when the document version conflicts (HTTP 409).</exception>
    /// <exception cref="Exceptions.DocumentException">Thrown when the Elasticsearch request fails.</exception>
    /// <remarks>
    /// <para><b>Concurrency:</b> Client-side patches on <see cref="IVersioned"/> documents re-read and re-apply on
    /// conflicts unless <c>SkipVersionCheck()</c> is set. <c>IfVersion(version)</c> instead requires that exact version
    /// and never re-reads after a conflict. It also works for nonversioned documents, but cannot be combined with
    /// <c>SkipVersionCheck()</c> or used with a script patch and a default ingest pipeline.</para>
    /// <para><b>Cache invalidation:</b> Client-side patches (<see cref="ActionPatch{T}"/>, <see cref="JsonPatch"/> and
    /// <see cref="PartialPatch"/> with a default ingest pipeline) pass the patched document to the document-based
    /// invalidation hook. Server-side patches provide no document values and invalidate by ID.</para>
    /// </remarks>
    Task<bool> PatchAsync(Id id, IPatchOperation operation, CommandOptionsDescriptor<T>? options);

    /// <inheritdoc cref="PatchAsync(Id, IPatchOperation, CommandOptionsDescriptor{T})"/>
    Task<bool> PatchAsync(Id id, IPatchOperation operation, ICommandOptions? options = null);

    /// <summary>
    /// Applies a patch operation to multiple documents.
    /// </summary>
    /// <param name="ids">The identifiers of the documents to patch.</param>
    /// <param name="operation">The patch operation to apply.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    /// <returns>The number of documents actually modified (excludes no-ops).</returns>
    /// <remarks>
    /// <para><b>Cache invalidation:</b> Client-side patches (<see cref="ActionPatch{T}"/>, <see cref="JsonPatch"/> and
    /// <see cref="PartialPatch"/> with a default ingest pipeline) use document-based invalidation. Server-side patches
    /// provide no document values and invalidate by ID.</para>
    /// <para>For inputs containing multiple IDs, JSON and action patches use search and skip IDs excluded by the initial
    /// query or soft-delete filters. A selected document that disappears before a conflict re-read is reported as
    /// <see cref="Exceptions.DocumentNotFoundException"/>. Other patch paths report missing documents as well.
    /// Successful documents are invalidated and notified before a failure is thrown.
    /// <c>IfVersion</c> is rejected, even for one ID.</para>
    /// </remarks>
    Task<long> PatchAsync(Ids ids, IPatchOperation operation, CommandOptionsDescriptor<T>? options);

    /// <inheritdoc cref="PatchAsync(Ids, IPatchOperation, CommandOptionsDescriptor{T})"/>
    Task<long> PatchAsync(Ids ids, IPatchOperation operation, ICommandOptions? options = null);

    /// <summary>
    /// Permanently removes a document from the repository (hard delete).
    /// </summary>
    /// <param name="id">The identifier of the document to remove.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    Task RemoveAsync(Id id, CommandOptionsDescriptor<T>? options);

    /// <inheritdoc cref="RemoveAsync(Id, CommandOptionsDescriptor{T})"/>
    Task RemoveAsync(Id id, ICommandOptions? options = null);

    /// <summary>
    /// Permanently removes multiple documents from the repository (hard delete).
    /// </summary>
    /// <param name="ids">The identifiers of the documents to remove.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    Task RemoveAsync(Ids ids, CommandOptionsDescriptor<T>? options);

    /// <inheritdoc cref="RemoveAsync(Ids, CommandOptionsDescriptor{T})"/>
    Task RemoveAsync(Ids ids, ICommandOptions? options = null);

    /// <summary>
    /// Permanently removes a document from the repository (hard delete).
    /// </summary>
    /// <param name="document">The document to remove.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    /// <remarks>An <see cref="IVersioned"/> document checks its version unless <c>SkipVersionCheck()</c> is set.
    /// A missing version throws <see cref="Exceptions.DocumentException"/> before deletion. A document that is already
    /// gone is evicted from the cache without removal events or notifications. <c>IfVersion</c> is rejected.</remarks>
    /// <exception cref="Exceptions.VersionConflictDocumentException">Thrown when the document has changed since it was read.</exception>
    Task RemoveAsync(T document, CommandOptionsDescriptor<T>? options);

    /// <inheritdoc cref="RemoveAsync(T, CommandOptionsDescriptor{T})"/>
    Task RemoveAsync(T document, ICommandOptions? options = null);

    /// <summary>
    /// Permanently removes multiple documents from the repository (hard delete).
    /// </summary>
    /// <param name="documents">The documents to remove.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    /// <remarks>Each <see cref="IVersioned"/> document checks its version unless <c>SkipVersionCheck()</c> is set.
    /// A missing version throws <see cref="Exceptions.DocumentException"/> before any deletion. Documents actually
    /// removed are notified before a failure is thrown; already-missing documents are only evicted from the cache.
    /// <c>IfVersion</c> is rejected.</remarks>
    /// <exception cref="Exceptions.VersionConflictDocumentException">Thrown when any document has changed since it was read.</exception>
    Task RemoveAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T>? options);

    /// <inheritdoc cref="RemoveAsync(IEnumerable{T}, CommandOptionsDescriptor{T})"/>
    Task RemoveAsync(IEnumerable<T> documents, ICommandOptions? options = null);

    /// <summary>
    /// Permanently removes all documents from the repository (hard delete).
    /// </summary>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    /// <returns>The number deleted by delete-by-query, or the number processed by the batch path.</returns>
    /// <remarks>When caching or remove event handlers require batches, the count includes documents that were found
    /// but concurrently deleted before their remove request. Those missing documents raise no removal notifications.
    /// A failed batch stops further processing after its successful removals are notified.</remarks>
    Task<long> RemoveAllAsync(CommandOptionsDescriptor<T>? options);

    /// <inheritdoc cref="RemoveAllAsync(CommandOptionsDescriptor{T})"/>
    Task<long> RemoveAllAsync(ICommandOptions? options = null);

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
