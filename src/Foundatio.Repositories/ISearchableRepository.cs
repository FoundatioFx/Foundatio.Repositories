using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories;

/// <summary>
/// Extends <see cref="IRepository{T}"/> and <see cref="ISearchableReadOnlyRepository{T}"/> with query-based mutation operations.
/// </summary>
/// <typeparam name="T">The document type, which must implement <see cref="IIdentity"/>.</typeparam>
/// <remarks>
/// <para>
/// <see cref="RemoveAllAsync(IRepositoryQuery, ICommandOptions)"/> performs hard deletes, permanently removing documents.
/// To soft delete documents, use <see cref="PatchAllAsync(IRepositoryQuery, IPatchOperation, ICommandOptions)"/> to set
/// <see cref="ISupportSoftDeletes.IsDeleted"/> to <c>true</c>.
/// </para>
/// </remarks>
public interface ISearchableRepository<T> : IRepository<T>, ISearchableReadOnlyRepository<T> where T : class, IIdentity, new()
{
    /// <summary>
    /// Applies a patch operation to all documents matching the query.
    /// </summary>
    /// <param name="query">An object containing filter criteria used to enforce tenancy or other system-level filters.</param>
    /// <param name="operation">The patch operation to apply (e.g., <see cref="PartialPatch"/>, <see cref="JsonPatch"/>, <see cref="ScriptPatch"/>).</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    /// <returns>The number of documents patched.</returns>
    Task<long> PatchAllAsync(RepositoryQueryDescriptor<T> query, IPatchOperation operation, CommandOptionsDescriptor<T> options = null);

    /// <inheritdoc cref="PatchAllAsync(RepositoryQueryDescriptor{T}, IPatchOperation, CommandOptionsDescriptor{T})"/>
    Task<long> PatchAllAsync(IRepositoryQuery query, IPatchOperation operation, ICommandOptions options = null);

    /// <summary>
    /// Permanently removes all documents matching the query (hard delete).
    /// </summary>
    /// <param name="query">An object containing filter criteria used to enforce tenancy or other system-level filters.</param>
    /// <param name="options">Options to control caching, notifications, and other behaviors.</param>
    /// <returns>The number of documents removed.</returns>
    Task<long> RemoveAllAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null);

    /// <inheritdoc cref="RemoveAllAsync(RepositoryQueryDescriptor{T}, CommandOptionsDescriptor{T})"/>
    Task<long> RemoveAllAsync(IRepositoryQuery query, ICommandOptions options = null);

    /// <summary>
    /// Processes all documents matching the query in batches.
    /// </summary>
    /// <param name="query">An object containing filter criteria used to enforce tenancy or other system-level filters.</param>
    /// <param name="processFunc">A function that processes each batch and returns <c>true</c> to continue or <c>false</c> to stop.</param>
    /// <param name="options">Options to control paging, caching, and other behaviors.</param>
    /// <returns>The total number of documents processed.</returns>
    Task<long> BatchProcessAsync(RepositoryQueryDescriptor<T> query, Func<FindResults<T>, Task<bool>> processFunc, CommandOptionsDescriptor<T> options = null);

    /// <inheritdoc cref="BatchProcessAsync(RepositoryQueryDescriptor{T}, Func{FindResults{T}, Task{bool}}, CommandOptionsDescriptor{T})"/>
    Task<long> BatchProcessAsync(IRepositoryQuery query, Func<FindResults<T>, Task<bool>> processFunc, ICommandOptions options = null);

    /// <summary>
    /// Processes all documents matching the query in batches, mapping results to a different type.
    /// </summary>
    /// <typeparam name="TResult">The type to map results to.</typeparam>
    /// <param name="query">An object containing filter criteria used to enforce tenancy or other system-level filters.</param>
    /// <param name="processFunc">A function that processes each batch and returns <c>true</c> to continue or <c>false</c> to stop.</param>
    /// <param name="options">Options to control paging, caching, and other behaviors.</param>
    /// <returns>The total number of documents processed.</returns>
    Task<long> BatchProcessAsAsync<TResult>(RepositoryQueryDescriptor<T> query, Func<FindResults<TResult>, Task<bool>> processFunc, CommandOptionsDescriptor<T> options = null) where TResult : class, new();

    /// <inheritdoc cref="BatchProcessAsAsync{TResult}(RepositoryQueryDescriptor{T}, Func{FindResults{TResult}, Task{bool}}, CommandOptionsDescriptor{T})"/>
    Task<long> BatchProcessAsAsync<TResult>(IRepositoryQuery query, Func<FindResults<TResult>, Task<bool>> processFunc, ICommandOptions options = null) where TResult : class, new();
}
