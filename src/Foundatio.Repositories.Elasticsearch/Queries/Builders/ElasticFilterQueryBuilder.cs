using System.Collections.Generic;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories
{
    public static class ElasticFilterQueryExtensions
    {
        internal const string ElasticFiltersKey = "@ElasticFilters";

        public static T ElasticFilter<T>(this T query, Query filter) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(ElasticFiltersKey, filter);
        }
    }
}

namespace Foundatio.Repositories.Options
{
    public static class ReadElasticFilterQueryExtensions
    {
        public static ICollection<Query> GetElasticFilters(this IRepositoryQuery query)
        {
            return query.SafeGetCollection<Query>(ElasticFilterQueryExtensions.ElasticFiltersKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders
{
    public class ElasticFilterQueryBuilder : IElasticQueryBuilder
    {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            var elasticFilters = ctx.Source.GetElasticFilters();
            if (elasticFilters.Count == 0)
                return Task.CompletedTask;

            foreach (var filter in elasticFilters)
                ctx.Filter &= filter;

            return Task.CompletedTask;
        }
    }
}
