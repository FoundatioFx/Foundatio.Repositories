using System;
using System.Linq;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Builders {
    public class AgeQueryBuilder : QueryBuilderBase {
        public override void BuildFilter<T>(object query, object options, ref FilterContainer container) {
            var ageQuery = query as IAgeQuery;
            if (ageQuery?.Ages == null || ageQuery.Ages.Count <= 0)
                return;

            if (ageQuery.Ages.Count == 1)
                container &= Filter<T>.Term("age", ageQuery.Ages.First());
            else
                container &= Filter<T>.Terms("age", ageQuery.Ages.Select(a => a.ToString()));
        }
    }
}