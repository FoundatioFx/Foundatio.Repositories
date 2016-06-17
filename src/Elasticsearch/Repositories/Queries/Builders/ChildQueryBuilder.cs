using System;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class ChildQueryBuilder : QueryBuilderBase {
        private readonly ElasticQueryBuilder _queryBuilder;

        public ChildQueryBuilder(ElasticQueryBuilder queryBuilder) {
            _queryBuilder = queryBuilder;
        }

        public override void BuildFilter<T>(object query, object options, ref FilterContainer container) {
            var childQuery = query as IChildQuery;
            if (childQuery?.ChildQuery == null)
                return;
            
            container &= new HasChildFilter {
                Query = _queryBuilder.CreateQuery<T>(childQuery.ChildQuery, options),
                Type = childQuery.ChildQuery.Type
            };
        }
    }
}