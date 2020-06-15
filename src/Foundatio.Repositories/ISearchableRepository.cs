using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public interface ISearchableRepository<T> : IRepository<T>, ISearchableReadOnlyRepository<T> where T : class, IIdentity, new() {
        /// <summary>
        /// Patch all documents that match the query using the specified patch operation.
        /// </summary>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="operation"></param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        /// <returns></returns>
        Task<long> PatchAllAsync(RepositoryQueryDescriptor<T> query, IPatchOperation operation, CommandOptionsDescriptor<T> options = null);

        /// <summary>
        /// Remove all documents that match the query.
        /// </summary>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        /// <returns></returns>
        Task<long> RemoveAllAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null);

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
        Task<long> BatchProcessAsAsync<TResult>(RepositoryQueryDescriptor<T> query, Func<FindResults<TResult>, Task<bool>> processFunc, CommandOptionsDescriptor<T> options = null) where TResult : class, new();
    }

    public static class SearchableRepositoryExtensions {
        /// <summary>
        /// Patch all documents that match the query using the specified patch operation.
        /// </summary>
        /// <param name="repository"></param>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="operation"></param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        /// <returns></returns>
        public static Task<long> PatchByQueryAsync<T>(this ISearchableRepository<T> repository, RepositoryQueryDescriptor<T> query, IPatchOperation operation, ICommandOptions options) where T : class, IIdentity, new() {
            return repository.PatchAllAsync(query, operation, o => options.As<T>());
        }

        /// <summary>
        /// Remove all documents that match the query.
        /// </summary>
        /// <param name="repository"></param>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        /// <returns></returns>
        public static Task<long> RemoveAllAsync<T>(this ISearchableRepository<T> repository, RepositoryQueryDescriptor<T> query, ICommandOptions options) where T : class, IIdentity, new() {
            return repository.RemoveAllAsync(query, o => options.As<T>());
        }

        /// <summary>
        /// Batch process all documents that match the query using the specified process function.
        /// </summary>
        /// <param name="repository"></param>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="processFunc">The function used to process each batch of documents.</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        /// <returns></returns>
        public static Task<long> BatchProcessAsync<T>(this ISearchableRepository<T> repository, RepositoryQueryDescriptor<T> query, Func<FindResults<T>, Task<bool>> processFunc, ICommandOptions options) where T : class, IIdentity, new() {
            return repository.BatchProcessAsync(query, processFunc, o => options.As<T>());
        }

        /// <summary>
        /// Batch process all documents that match the query using the specified process function.
        /// </summary>
        /// <param name="repository"></param>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="processFunc">The function used to process each batch of documents.</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        /// <returns></returns>
        public static Task<long> BatchProcessAsAsync<T, TResult>(this ISearchableRepository<T> repository, RepositoryQueryDescriptor<T> query, Func<FindResults<TResult>, Task<bool>> processFunc, ICommandOptions options) where TResult : class, new() where T : class, IIdentity, new() {
            return repository.BatchProcessAsAsync(query, processFunc, o => options.As<T>());
        }
    }
}