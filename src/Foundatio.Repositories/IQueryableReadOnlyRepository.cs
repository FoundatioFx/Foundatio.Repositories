using System.Threading.Tasks;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories {
    public interface IQueryableReadOnlyRepository<T> : IReadOnlyRepository<T> where T : class, new() {
        /// <summary>
        /// Find documents using a query
        /// </summary>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        Task<QueryResults<T>> QueryAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null);

        /// <summary>
        /// Find documents using a query and map the result to the specified model type
        /// </summary>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        Task<QueryResults<TResult>> QueryAsAsync<TResult>(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null) where TResult : class, new();

        /// <summary>
        /// Gets a document count and optional aggregation data using a query
        /// </summary>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        Task<CountResult> CountByQueryAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null);

        /// <summary>
        /// Find a single document using a query
        /// </summary>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        Task<FindHit<T>> FindOneAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null);

        /// <summary>
        /// Checks to see if any document exists that matches the specified query
        /// </summary>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        Task<bool> ExistsAsync(RepositoryQueryDescriptor<T> query);
    }
}