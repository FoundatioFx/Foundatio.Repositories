using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories {
    public interface ISearchableReadOnlyRepository<T> : IReadOnlyRepository<T> where T : class, new() {
        Task<CountResult> CountBySearchAsync(IRepositoryQuery systemFilter, string userFilter = null, string query = null, AggregationOptions aggregations = null);
        Task<IFindResults<T>> SearchAsync(IRepositoryQuery systemFilter, string userFilter = null, string query = null, SortingOptions sorting = null, PagingOptions paging = null, AggregationOptions aggregations = null);
        Task<IReadOnlyCollection<AggregationResult>> GetAggregationsAsync(IRepositoryQuery systemFilter, AggregationOptions aggregations, string userFilter = null, string query = null);
    }
}