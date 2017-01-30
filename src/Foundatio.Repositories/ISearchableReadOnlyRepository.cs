using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories {
    public interface ISearchableReadOnlyRepository<T> : IReadOnlyRepository<T> where T : class, new() {
        Task<CountResult> CountBySearchAsync(IRepositoryQuery systemFilter, string filter = null, string aggregations = null, ICommandOptions options = null);

        /// <summary>
        /// Find documents using search criteria
        /// </summary>
        /// <param name="systemFilter">A query object used to enforce any tennancy or other system level filters</param>
        /// <param name="filter">Used to filter the documents that will be searched (defaults to AND and does not score)</param>
        /// <param name="criteria">Search criteria to find documents and score the results</param>
        /// <param name="sort">How to sort the results. Must be null if you want the results ordered by score (defaults to OR and scores the matches)</param>
        /// <param name="aggregations"></param>
        /// <param name="options">Command options using to control things like paging, caching, etc</param>
        /// <returns></returns>
        Task<FindResults<T>> SearchAsync(IRepositoryQuery systemFilter, string filter = null, string criteria = null, string sort = null, string aggregations = null, ICommandOptions options = null);
    }
}