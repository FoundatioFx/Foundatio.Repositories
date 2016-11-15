using System;
using System.Threading.Tasks;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IElasticFilterQuery {
        FilterContainer ElasticFilter { get; set; }
    }

    public static class ElasticFilterQueryExtensions {
        public static T WithElasticFilter<T>(this T query, FilterContainer filter) where T : IElasticFilterQuery {
            query.ElasticFilter = filter;
            return query;
        }
    }

    public class ElasticFilterQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var elasticQuery = ctx.GetSourceAs<IElasticFilterQuery>();
            if (elasticQuery?.ElasticFilter == null)
                return Task.CompletedTask;

            ctx.Filter &= elasticQuery.ElasticFilter;

            return Task.CompletedTask;
        }
    }
}