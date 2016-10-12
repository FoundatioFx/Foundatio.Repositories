using System;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IElasticFilterQuery {
        QueryContainer ElasticFilter { get; set; }
    }

    public static class ElasticFilterQueryExtensions {
        public static T WithElasticFilter<T>(this T query, QueryContainer filter) where T : IElasticFilterQuery {
            query.ElasticFilter = filter;
            return query;
        }
    }

    public class ElasticFilterQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var elasticQuery = ctx.GetSourceAs<IElasticFilterQuery>();
            if (elasticQuery?.ElasticFilter == null)
                return;

            ctx.Filter &= elasticQuery.ElasticFilter;
        }
    }
}