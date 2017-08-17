using System.Threading.Tasks;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories {
    public interface ISearchableReadOnlyRepository<T> : IReadOnlyRepository<T> where T : class, new() {
        /// <summary>
        /// Gets a document count and optional aggregation data using search criteria
        /// </summary>
        /// <param name="systemFilter">A object containing filter criteria used to enforce any tennancy or other system level filters</param>
        /// <param name="filter">Used to filter the documents (defaults to AND and does not score)</param>
        /// <param name="aggregations">Aggregation expression used to return aggregated data within any given filters</param>
        /// <param name="options">Command options used to control things like paging, caching, etc</param>
        /// <returns></returns>
        Task<CountResult> CountBySearchAsync(ISystemFilter systemFilter, string filter = null, string aggregations = null, ICommandOptions options = null);

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
        Task<FindResults<T>> SearchAsync(ISystemFilter systemFilter, string filter = null, string criteria = null, string sort = null, string aggregations = null, ICommandOptions options = null);
    }

    public static class SearchableReadOnlyRepositoryExtensions {
        public static Task<CountResult> CountBySearchAsync<T>(this ISearchableReadOnlyRepository<T> repository, ISystemFilter systemFilter, string filter = null, string aggregations = null, CommandOptionsDescriptor<T> options = null) where T : class, new() {
            return repository.CountBySearchAsync(systemFilter, filter, aggregations, options.Configure());
        }

        public static Task<FindResults<T>> SearchAsync<T>(this ISearchableReadOnlyRepository<T> repository, ISystemFilter systemFilter, string filter = null, string criteria = null, string sort = null, string aggregations = null, CommandOptionsDescriptor<T> options = null) where T : class, new() {
            return repository.SearchAsync(systemFilter, filter, criteria, sort, aggregations, options.Configure());
        }
    }

    public interface ISystemFilter {
        IRepositoryQuery GetQuery();
    }
}