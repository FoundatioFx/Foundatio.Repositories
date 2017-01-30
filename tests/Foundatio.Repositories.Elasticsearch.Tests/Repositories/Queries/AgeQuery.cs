using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries {
    public interface IAgeQuery : IRepositoryQuery {
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
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var ageQuery = ctx.GetSourceAs<IAgeQuery>();
            if (ageQuery?.Ages == null || ageQuery.Ages.Count <= 0)
                return Task.CompletedTask;

            if (ageQuery.Ages.Count == 1)
                ctx.Filter &= Query<Employee>.Term(f => f.Age, ageQuery.Ages.First());
            else
                ctx.Filter &= Query<Employee>.Terms(d => d.Field(f => f.Age).Terms(ageQuery.Ages));

            return Task.CompletedTask;
        }
    }
}