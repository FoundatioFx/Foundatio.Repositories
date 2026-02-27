using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Utility;

namespace Foundatio.Repositories;

/// <summary>
/// Provides read-only data access operations for documents of type <typeparamref name="T"/>.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
/// <remarks>
/// When <typeparamref name="T"/> implements <see cref="ISupportSoftDeletes"/>, read operations automatically
/// filter out documents where <see cref="ISupportSoftDeletes.IsDeleted"/> is <c>true</c>.
/// Use <c>options.IncludeSoftDeletes()</c> to include soft-deleted documents in results.
/// </remarks>
public interface IReadOnlyRepository<T> where T : class, new()
{
    /// <summary>
    /// Retrieves a document by its unique identifier.
    /// </summary>
    /// <param name="id">The document identifier.</param>
    /// <param name="options">Options to control caching, soft delete filtering, and other behaviors.</param>
    Task<T> GetByIdAsync(Id id, CommandOptionsDescriptor<T> options);

    /// <inheritdoc cref="GetByIdAsync(Id, CommandOptionsDescriptor{T})"/>
    Task<T> GetByIdAsync(Id id, ICommandOptions options = null);

    /// <summary>
    /// Retrieves multiple documents by their unique identifiers.
    /// </summary>
    /// <param name="ids">The document identifiers.</param>
    /// <param name="options">Options to control caching, soft delete filtering, and other behaviors.</param>
    Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, CommandOptionsDescriptor<T> options);

    /// <inheritdoc cref="GetByIdsAsync(Ids, CommandOptionsDescriptor{T})"/>
    Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, ICommandOptions options = null);

    /// <summary>
    /// Retrieves all documents in the repository.
    /// </summary>
    /// <param name="options">Options to control paging, caching, soft delete filtering, and other behaviors.</param>
    Task<FindResults<T>> GetAllAsync(CommandOptionsDescriptor<T> options);

    /// <inheritdoc cref="GetAllAsync(CommandOptionsDescriptor{T})"/>
    Task<FindResults<T>> GetAllAsync(ICommandOptions options = null);

    /// <summary>
    /// Checks whether a document with the specified identifier exists.
    /// </summary>
    /// <param name="id">The document identifier.</param>
    /// <param name="options">Options to control caching, soft delete filtering, and other behaviors.</param>
    Task<bool> ExistsAsync(Id id, CommandOptionsDescriptor<T> options);

    /// <inheritdoc cref="ExistsAsync(Id, CommandOptionsDescriptor{T})"/>
    Task<bool> ExistsAsync(Id id, ICommandOptions options = null);

    /// <summary>
    /// Gets the total count of documents in the repository.
    /// </summary>
    /// <param name="options">Options to control caching, soft delete filtering, and other behaviors.</param>
    Task<CountResult> CountAsync(CommandOptionsDescriptor<T> options);

    /// <inheritdoc cref="CountAsync(CommandOptionsDescriptor{T})"/>
    Task<CountResult> CountAsync(ICommandOptions options = null);

    /// <summary>
    /// Invalidates cached data for the specified document.
    /// </summary>
    /// <param name="document">The document whose cache entries should be invalidated.</param>
    Task InvalidateCacheAsync(T document);

    /// <summary>
    /// Invalidates cached data for the specified documents.
    /// </summary>
    /// <param name="documents">The documents whose cache entries should be invalidated.</param>
    Task InvalidateCacheAsync(IEnumerable<T> documents);

    /// <summary>
    /// Invalidates a specific cache entry by key.
    /// </summary>
    /// <param name="cacheKey">The cache key to invalidate.</param>
    Task InvalidateCacheAsync(string cacheKey);

    /// <summary>
    /// Invalidates multiple cache entries by key.
    /// </summary>
    /// <param name="cacheKeys">The cache keys to invalidate.</param>
    Task InvalidateCacheAsync(IEnumerable<string> cacheKeys);

    /// <summary>
    /// Event raised before a query is executed, allowing modification of query parameters.
    /// </summary>
    AsyncEvent<BeforeQueryEventArgs<T>> BeforeQuery { get; }
}
