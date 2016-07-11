using System;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class ChildQueryBuilder : IElasticQueryBuilder {
        private readonly ElasticQueryBuilder _queryBuilder;

        public ChildQueryBuilder(ElasticQueryBuilder queryBuilder) {
            _queryBuilder = queryBuilder;
        }

        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var childQuery = ctx.GetQueryAs<IChildQuery>();
            if (childQuery?.ChildQuery == null)
                return;
            
            ctx.Filter &= new HasChildFilter {
                Query = _queryBuilder.BuildQuery<T>(childQuery.ChildQuery, ctx.Options),
                Type = childQuery.ChildQuery.Type
            };
        }
    }
}