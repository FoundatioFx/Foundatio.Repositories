using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories
{
    public static class AgeQueryExtensions
    {
        internal const string AgesKey = "@Ages";

        public static T Age<T>(this T query, int age) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(AgesKey, age);
        }

        public static T AgeRange<T>(this T query, int minAge, int maxAge) where T : IRepositoryQuery
        {
            foreach (int age in Enumerable.Range(minAge, maxAge - minAge + 1))
                query.AddCollectionOptionValue(AgesKey, age);

            return query;
        }
    }
}

namespace Foundatio.Repositories.Options
{
    public static class ReadAgeQueryExtensions
    {
        public static ICollection<int> GetAges(this IRepositoryQuery query)
        {
            return query.SafeGetCollection<int>(AgeQueryExtensions.AgesKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries
{
    public class AgeQueryBuilder : IElasticQueryBuilder
    {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            var ages = ctx.Source.GetAges();
            if (ages.Count <= 0)
                return Task.CompletedTask;

            if (ages.Count == 1)
                ctx.Filter &= new TermQuery { Field = Infer.Field<Employee>(f => f.Age), Value = ages.First() };
            else
                ctx.Filter &= new TermsQuery { Field = Infer.Field<Employee>(f => f.Age), Terms = new TermsQueryField(ages.Select(a => FieldValue.Long(a)).ToArray()) };

            return Task.CompletedTask;
        }
    }
}
