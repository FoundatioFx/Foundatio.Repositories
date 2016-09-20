using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories {
    public interface ISearchableReadOnlyRepository<T> : IReadOnlyRepository<T> where T : class, new() {
        Task<CountResult> CountBySearchAsync(IRepositoryQuery systemFilter, string filter = null, AggregationOptions aggregations = null);

        /// <summary>
        /// Find documents using search criteria
        /// </summary>
        /// <param name="systemFilter">A query object used to enforce any tennancy or other system level filters</param>
        /// <param name="filter">Used to filter the documents that will be searched</param>
        /// <param name="criteria">Search criteria to find documents and score the results</param>
        /// <param name="sorting">How to sort the results. Must be null if you want the results ordered by score</param>
        /// <param name="paging">Paging options like page size and page number</param>
        /// <param name="aggregations"></param>
        /// <returns></returns>
        Task<FindResults<T>> SearchAsync(IRepositoryQuery systemFilter, string filter = null, string criteria = null, SortingOptions sorting = null, PagingOptions paging = null, AggregationOptions aggregations = null);
        Task<IReadOnlyCollection<AggregationResult>> GetAggregationsAsync(IRepositoryQuery systemFilter, AggregationOptions aggregations, string filter = null);
    }
}