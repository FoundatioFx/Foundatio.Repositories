using System;
using Foundatio.Elasticsearch.Repositories.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Builders {
    public class AgeQueryBuilder : QueryBuilderBase {
        public override void BuildFilter<T>(object query, object options, ref QueryContainer container) {
            var ageQuery = query as IAgeQuery;
            if (ageQuery?.Ages == null || ageQuery.Ages.Count <= 0)
                return;

            container &= Query<T>.Terms(t => t.Field("age").Terms(ageQuery.Ages.ToArray()));
        }
    }
}