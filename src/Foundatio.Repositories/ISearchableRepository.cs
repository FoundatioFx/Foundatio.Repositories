using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories;

public interface ISearchableRepository<T> : IRepository<T>, ISearchableReadOnlyRepository<T> where T : class, IIdentity {
    /// <summary>
    /// Patch all documents that match the query using the specified patch operation.
    /// </summary>
    /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
    /// <param name="operation"></param>
    /// <param name="options">Command options used to control things like paging, caching, etc</param>
    /// <returns></returns>
    Task<long> PatchAllAsync(RepositoryQueryDescriptor<T> query, IPatchOperation operation, CommandOptionsDescriptor<T> options = null);

    /// <summary>
    /// Patch all documents that match the query using the specified patch operation.
    /// </summary>
    /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
    /// <param name="operation"></param>
    /// <param name="options">Command options used to control things like paging, caching, etc</param>
    /// <returns></returns>
    Task<long> PatchAllAsync(IRepositoryQuery query, IPatchOperation operation, ICommandOptions options = null);

    /// <summary>
    /// Remove all documents that match the query.
    /// </summary>
    /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
    /// <param name="options">Command options used to control things like paging, caching, etc</param>
    /// <returns></returns>
    Task<long> RemoveAllAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null);

    /// <summary>
    /// Remove all documents that match the query.
    /// </summary>
    /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
    /// <param name="options">Command options used to control things like paging, caching, etc</param>
    /// <returns></returns>
    Task<long> RemoveAllAsync(IRepositoryQuery query, ICommandOptions options = null);

    /// <summary>
    /// Batch process all documents that match the query using the specified process function.
    /// </summary>
    /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
    /// <param name="processFunc">The function used to process each batch of documents.</param>
    /// <param name="options">Command options used to control things like paging, caching, etc</param>
    /// <returns></returns>
    Task<long> BatchProcessAsync(RepositoryQueryDescriptor<T> query, Func<FindResults<T>, Task<bool>> processFunc, CommandOptionsDescriptor<T> options = null);

    /// <summary>
    /// Batch process all documents that match the query using the specified process function.
    /// </summary>
    /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
    /// <param name="processFunc">The function used to process each batch of documents.</param>
    /// <param name="options">Command options used to control things like paging, caching, etc</param>
    /// <returns></returns>
    Task<long> BatchProcessAsync(IRepositoryQuery query, Func<FindResults<T>, Task<bool>> processFunc, ICommandOptions options = null);

    /// <summary>
    /// Batch process all documents that match the query using the specified process function.
    /// </summary>
    /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
    /// <param name="processFunc">The function used to process each batch of documents.</param>
    /// <param name="options">Command options used to control things like paging, caching, etc</param>
    /// <returns></returns>
    Task<long> BatchProcessAsAsync<TResult>(RepositoryQueryDescriptor<T> query, Func<FindResults<TResult>, Task<bool>> processFunc, CommandOptionsDescriptor<T> options = null) where TResult : class;

    /// <summary>
    /// Batch process all documents that match the query using the specified process function.
    /// </summary>
    /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
    /// <param name="processFunc">The function used to process each batch of documents.</param>
    /// <param name="options">Command options used to control things like paging, caching, etc</param>
    /// <returns></returns>
    Task<long> BatchProcessAsAsync<TResult>(IRepositoryQuery query, Func<FindResults<TResult>, Task<bool>> processFunc, ICommandOptions options = null) where TResult : class;
}
