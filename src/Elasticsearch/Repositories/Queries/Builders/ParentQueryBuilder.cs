using System;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class ParentQueryBuilder : ElasticQueryBuilderBase {
        private readonly ElasticQueryBuilder _queryBuilder;

        public ParentQueryBuilder(ElasticQueryBuilder queryBuilder) {
            _queryBuilder = queryBuilder;
        }

        public override void BuildFilter<T>(object query, object options, ref FilterContainer container) {
            var parentQuery = query as IParentQuery;
            if (parentQuery?.ParentQuery == null)
                return;
            
            container &= new HasParentFilter {
                Query = _queryBuilder.CreateQuery<T>(parentQuery.ParentQuery, options),
                Type = parentQuery.ParentQuery.Type
            };
        }
    }
}