using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories {
    public interface ISearchableRepository<T> : IReadOnlyRepository<T> where T : class, new() {
        Task<CountResult> CountAsync(object systemFilter, string userFilter = null, string query = null, AggregationOptions aggregations = null);
        Task<FindResults<T>> SearchAsync(object systemFilter, string userFilter = null, string query = null, SortingOptions sorting = null, PagingOptions paging = null, AggregationOptions aggregations = null);
        Task<ICollection<AggregationResult>> GetAggregationsAsync(object systemFilter, AggregationOptions aggregations, string userFilter = null, string query = null);
    }
}