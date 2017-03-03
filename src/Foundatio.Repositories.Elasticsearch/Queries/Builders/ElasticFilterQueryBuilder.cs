using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories {
    public static class ElasticFilterQueryExtensions {
        internal const string ElasticFiltersKey = "@ElasticFilters";

        public static T ElasticFilter<T>(this T query, QueryContainer filter) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(ElasticFiltersKey, filter);
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadElasticFilterQueryExtensions {
        public static ICollection<QueryContainer> GetElasticFilters(this IRepositoryQuery query) {
            return query.SafeGetCollection<QueryContainer>(ElasticFilterQueryExtensions.ElasticFiltersKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class ElasticFilterQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var elasticFilters = ctx.Source.GetElasticFilters();
            if (elasticFilters.Count == 0)
                return Task.CompletedTask;

            foreach (var filter in elasticFilters)
                ctx.Filter &= filter;

            return Task.CompletedTask;
        }
    }
}