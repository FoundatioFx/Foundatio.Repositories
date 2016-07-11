using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories {
    public interface ISearchableRepository<T> : IReadOnlyRepository<T> where T : class, new() {
        Task<FindResults<T>> SearchAsync(string systemFilter, string userFilter = null, string query = null, SortingOptions sorting = null, PagingOptions paging = null, AggregationOptions aggregations = null);
        Task<ICollection<AggregationResult>> GetAggregationsAsync(string systemFilter, AggregationOptions aggregations, string userFilter = null, string query = null);
    }
}