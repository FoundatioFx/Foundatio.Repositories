using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public interface ISearchableReadOnlyRepository<T> : IReadOnlyRepository<T> where T : class, new() {
        /// <summary>
        /// Find documents using a query
        /// </summary>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        Task<FindResults<T>> FindAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null);

        /// <summary>
        /// Find documents using search criteria
        /// </summary>
        /// <param name="systemFilter">A object containing filter criteria used to enforce any tennancy or other system level filters</param>
        /// <param name="filter">Used to filter the documents (defaults to AND and does not score)</param>
        /// <param name="criteria">Search criteria to find documents and score the results within any given filters (defaults to OR)</param>
        /// <param name="sort">How to sort the results. Must be null if you want the results ordered by score</param>
        /// <param name="aggregations">Aggregation expression used to return aggregated data within any given filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        /// <returns></returns>
        Task<FindResults<T>> SearchAsync(ISystemFilter systemFilter, string filter = null, string criteria = null, string sort = null, string aggregations = null, CommandOptionsDescriptor<T> options = null);

        /// <summary>
        /// Find documents using a query and map the result to the specified model type
        /// </summary>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        Task<FindResults<TResult>> FindAsAsync<TResult>(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null) where TResult : class, new();

        /// <summary>
        /// Gets a document count and optional aggregation data using a query
        /// </summary>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        Task<CountResult> CountAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null);
        
        /// <summary>
        /// Gets a document count and optional aggregation data using search criteria
        /// </summary>
        /// <param name="systemFilter">A object containing filter criteria used to enforce any tennancy or other system level filters</param>
        /// <param name="filter">Used to filter the documents (defaults to AND and does not score)</param>
        /// <param name="aggregations">Aggregation expression used to return aggregated data within any given filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        /// <returns></returns>
        Task<CountResult> CountBySearchAsync(ISystemFilter systemFilter, string filter = null, string aggregations = null, CommandOptionsDescriptor<T> options = null);

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

    public static class SearchableReadOnlyRepositoryExtensions {
        /// <summary>
        /// Find documents using a query
        /// </summary>
        /// <param name="repository"></param>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        public static Task<FindResults<T>> FindAsync<T>(this ISearchableReadOnlyRepository<T> repository, RepositoryQueryDescriptor<T> query, ICommandOptions options) where T : class, new() {
            return repository.FindAsync(query, o => options.As<T>());
        }

        /// <summary>
        /// Find documents using search criteria
        /// </summary>
        /// <param name="repository"></param>
        /// <param name="systemFilter">A object containing filter criteria used to enforce any tennancy or other system level filters</param>
        /// <param name="filter">Used to filter the documents (defaults to AND and does not score)</param>
        /// <param name="criteria">Search criteria to find documents and score the results within any given filters (defaults to OR)</param>
        /// <param name="sort">How to sort the results. Must be null if you want the results ordered by score</param>
        /// <param name="aggregations">Aggregation expression used to return aggregated data within any given filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        /// <returns></returns>
        public static Task<FindResults<T>> SearchAsync<T>(this ISearchableReadOnlyRepository<T> repository, ISystemFilter systemFilter, string filter = null, string criteria = null, string sort = null, string aggregations = null, ICommandOptions options = null) where T : class, new() {
            return repository.SearchAsync(systemFilter, filter, criteria, sort, aggregations, o => options.As<T>());
        }

        /// <summary>
        /// Find documents using a query and map the result to the specified model type
        /// </summary>
        /// <param name="repository"></param>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        public static Task<FindResults<TResult>> FindAsAsync<T, TResult>(this ISearchableReadOnlyRepository<T> repository, RepositoryQueryDescriptor<T> query, ICommandOptions options) where TResult : class, new() where T : class, new() {
            return repository.FindAsAsync<TResult>(query, o => options.As<T>());
        }

        /// <summary>
        /// Gets a document count and optional aggregation data using a query
        /// </summary>
        /// <param name="repository"></param>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        public static Task<CountResult> CountAsync<T>(this ISearchableReadOnlyRepository<T> repository, RepositoryQueryDescriptor<T> query, ICommandOptions options) where T : class, new() {
            return repository.CountAsync(query, o => options.As<T>());
        }

        /// <summary>
        /// Gets a document count and optional aggregation data using search criteria
        /// </summary>
        /// <param name="repository"></param>
        /// <param name="systemFilter">A object containing filter criteria used to enforce any tennancy or other system level filters</param>
        /// <param name="filter">Used to filter the documents (defaults to AND and does not score)</param>
        /// <param name="aggregations">Aggregation expression used to return aggregated data within any given filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        /// <returns></returns>
        public static Task<CountResult> CountBySearchAsync<T>(this ISearchableReadOnlyRepository<T> repository, ISystemFilter systemFilter, string filter = null, string aggregations = null, ICommandOptions options = null) where T : class, new() {
            return repository.CountBySearchAsync(systemFilter, filter, aggregations, o => options.As<T>());
        }

        /// <summary>
        /// Find a single document using a query
        /// </summary>
        /// <param name="repository"></param>
        /// <param name="query">A object containing filter criteria used to enforce any tenancy or other system level filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        public static Task<FindHit<T>> FindOneAsync<T>(this ISearchableReadOnlyRepository<T> repository, RepositoryQueryDescriptor<T> query, ICommandOptions options) where T : class, new() {
            return repository.FindOneAsync(query, o => options.As<T>());
        }
    }
}