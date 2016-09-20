using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Extensions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Queries {
    public interface IAgeQuery {
        ICollection<int> Ages { get; set; }
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

    public class AgeQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var ageQuery = ctx.GetSourceAs<IAgeQuery>();
            if (ageQuery?.Ages == null || ageQuery.Ages.Count <= 0)
                return;

            if (ageQuery.Ages.Count == 1)
                ctx.Filter &= Filter<T>.Term(EmployeeType.Fields.Age, ageQuery.Ages.First());
            else
                ctx.Filter &= Filter<T>.Terms(EmployeeType.Fields.Age, ageQuery.Ages.Select(a => a.ToString()));
        }
    }
}