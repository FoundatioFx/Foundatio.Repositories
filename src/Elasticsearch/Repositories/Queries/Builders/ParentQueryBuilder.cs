using System;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class ParentQueryBuilder : IElasticQueryBuilder {
        private readonly ElasticQueryBuilder _queryBuilder;

        public ParentQueryBuilder(ElasticQueryBuilder queryBuilder) {
            _queryBuilder = queryBuilder;
        }

        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var parentQuery = ctx.GetQueryAs<IParentQuery>();
            if (parentQuery?.ParentQuery == null)
                return;
            
            ctx.Filter &= new HasParentFilter {
                Query = _queryBuilder.BuildQuery<T>(parentQuery.ParentQuery, ctx.Options),
                Type = parentQuery.ParentQuery.Type
            };
        }
    }
}