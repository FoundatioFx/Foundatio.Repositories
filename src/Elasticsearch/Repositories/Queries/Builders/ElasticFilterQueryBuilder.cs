using System;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class ElasticFilterQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var elasticQuery = ctx.GetQueryAs<IElasticFilterQuery>();
            if (elasticQuery?.ElasticFilter == null)
                return;

            ctx.Filter &= elasticQuery.ElasticFilter;
        }
    }
}