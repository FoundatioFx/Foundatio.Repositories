using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories;

/// <summary>
/// Extends <see cref="IReadOnlyRepository{T}"/> with query-based search capabilities.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
/// <remarks>
/// When <typeparamref name="T"/> implements <see cref="ISupportSoftDeletes"/>, queries automatically
/// filter out documents where <see cref="ISupportSoftDeletes.IsDeleted"/> is <c>true</c>.
/// Use <c>options.IncludeSoftDeletes()</c> to include soft-deleted documents in results.
/// </remarks>
public interface ISearchableReadOnlyRepository<T> : IReadOnlyRepository<T> where T : class, new()
{
    /// <summary>
    /// Removes the cached results of an async query.
    /// </summary>
    /// <param name="queryId">The identifier of the query to remove.</param>
    Task RemoveQueryAsync(string queryId);

    /// <summary>
    /// Finds documents matching the specified query.
    /// </summary>
    /// <param name="query">An object containing filter criteria used to enforce tenancy or other system-level filters.</param>
    /// <param name="options">Options to control paging, caching, soft delete filtering, and other behaviors.</param>
    Task<FindResults<T>> FindAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null);

    /// <inheritdoc cref="FindAsync(RepositoryQueryDescriptor{T}, CommandOptionsDescriptor{T})"/>
    Task<FindResults<T>> FindAsync(IRepositoryQuery query, ICommandOptions options = null);

    /// <summary>
    /// Finds documents matching the specified query and maps results to a different type.
    /// </summary>
    /// <typeparam name="TResult">The type to map results to.</typeparam>
    /// <param name="query">An object containing filter criteria used to enforce tenancy or other system-level filters.</param>
    /// <param name="options">Options to control paging, caching, soft delete filtering, and other behaviors.</param>
    Task<FindResults<TResult>> FindAsAsync<TResult>(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null) where TResult : class, new();

    /// <inheritdoc cref="FindAsAsync{TResult}(RepositoryQueryDescriptor{T}, CommandOptionsDescriptor{T})"/>
    Task<FindResults<TResult>> FindAsAsync<TResult>(IRepositoryQuery query, ICommandOptions options = null) where TResult : class, new();

    /// <summary>
    /// Finds a single document matching the specified query.
    /// </summary>
    /// <param name="query">An object containing filter criteria used to enforce tenancy or other system-level filters.</param>
    /// <param name="options">Options to control caching, soft delete filtering, and other behaviors.</param>
    Task<FindHit<T>> FindOneAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null);

    /// <inheritdoc cref="FindOneAsync(RepositoryQueryDescriptor{T}, CommandOptionsDescriptor{T})"/>
    Task<FindHit<T>> FindOneAsync(IRepositoryQuery query, ICommandOptions options = null);

    /// <summary>
    /// Gets a document count and optional aggregation data for documents matching the query.
    /// </summary>
    /// <param name="query">An object containing filter criteria used to enforce tenancy or other system-level filters.</param>
    /// <param name="options">Options to control caching, soft delete filtering, and other behaviors.</param>
    Task<CountResult> CountAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null);

    /// <inheritdoc cref="CountAsync(RepositoryQueryDescriptor{T}, CommandOptionsDescriptor{T})"/>
    Task<CountResult> CountAsync(IRepositoryQuery query, ICommandOptions options = null);

    /// <summary>
    /// Checks whether any document exists that matches the specified query.
    /// </summary>
    /// <param name="query">An object containing filter criteria used to enforce tenancy or other system-level filters.</param>
    /// <param name="options">Options to control caching, soft delete filtering, and other behaviors.</param>
    Task<bool> ExistsAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null);

    /// <inheritdoc cref="ExistsAsync(RepositoryQueryDescriptor{T}, CommandOptionsDescriptor{T})"/>
    Task<bool> ExistsAsync(IRepositoryQuery query, ICommandOptions options = null);

    /// <summary>
    /// Finds documents using search criteria.
    /// </summary>
    /// <param name="systemFilter">An object containing filter criteria used to enforce tenancy or other system-level filters.</param>
    /// <param name="filter">Used to filter the documents (defaults to AND and does not score).</param>
    /// <param name="criteria">Search criteria to find documents and score the results within any given filters (defaults to OR).</param>
    /// <param name="sort">How to sort the results. Must be null if you want the results ordered by score.</param>
    /// <param name="aggregations">Aggregation expression used to return aggregated data within any given filters.</param>
    /// <param name="options">Options to control paging, caching, soft delete filtering, and other behaviors.</param>
    [Obsolete("Use FindAsync")]
    Task<FindResults<T>> SearchAsync(ISystemFilter systemFilter, string filter = null, string criteria = null, string sort = null, string aggregations = null, ICommandOptions options = null);

    /// <summary>
    /// Gets a document count and optional aggregation data using search criteria.
    /// </summary>
    /// <param name="systemFilter">An object containing filter criteria used to enforce tenancy or other system-level filters.</param>
    /// <param name="filter">Used to filter the documents (defaults to AND and does not score).</param>
    /// <param name="aggregations">Aggregation expression used to return aggregated data within any given filters.</param>
    /// <param name="options">Options to control caching, soft delete filtering, and other behaviors.</param>
    [Obsolete("Use CountAsync")]
    Task<CountResult> CountBySearchAsync(ISystemFilter systemFilter, string filter = null, string aggregations = null, ICommandOptions options = null);
}
