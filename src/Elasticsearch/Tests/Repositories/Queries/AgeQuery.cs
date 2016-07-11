using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Queries {
    public interface IAgeQuery {
        List<int> Ages { get; set; }
        // TODO: Support age range query
    }

    public static class AgeQueryExtensions {
        public static T WithAge<T>(this T query, int age) where T : IAgeQuery {
            query.Ages.Add(age);
            return query;
        }

        public static T WithAgeRange<T>(this T query, int minAge, int maxAge) where T : IAgeQuery {
            query.Ages.AddRange(Enumerable.Range(minAge, maxAge - minAge + 1));
            return query;
        }
    }

    public class AgeQueryBuilder : ElasticQueryBuilderBase {
        public override void BuildFilter<T>(object query, object options, ref FilterContainer container) {
            var ageQuery = query as IAgeQuery;
            if (ageQuery?.Ages == null || ageQuery.Ages.Count <= 0)
                return;

            if (ageQuery.Ages.Count == 1)
                container &= Filter<T>.Term(EmployeeType.Fields.Age, ageQuery.Ages.First());
            else
                container &= Filter<T>.Terms(EmployeeType.Fields.Age, ageQuery.Ages.Select(a => a.ToString()));
        }
    }
}