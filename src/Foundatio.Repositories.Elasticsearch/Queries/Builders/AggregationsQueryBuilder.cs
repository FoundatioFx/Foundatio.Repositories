using System;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories
{
    public static class AggregationQueryExtensions
    {
        internal const string AggregationsKey = "@AggregationsExpressionKey";

        public static T AggregationsExpression<T>(this T options, string? aggregations) where T : IRepositoryQuery
        {
            if (aggregations is null)
            {
                options.Values?.Remove(AggregationsKey);
                return options;
            }

            return options.BuildOption(AggregationsKey, aggregations);
        }
    }
}

namespace Foundatio.Repositories.Options
{
    public static class ReadAggregationQueryExtensions
    {
        public static string? GetAggregationsExpression(this IRepositoryQuery query)
        {
            return query.SafeGetOption<string?>(AggregationQueryExtensions.AggregationsKey, null);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders
{
    public class AggregationsQueryBuilder : IElasticQueryBuilder
    {
        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            var elasticIndex = ctx.Options.GetElasticIndex();
            if (elasticIndex?.QueryParser == null)
                return;

            string? aggregations = ctx.Source.GetAggregationsExpression();
            if (String.IsNullOrEmpty(aggregations))
                return;

            var result = await elasticIndex.QueryParser.BuildAggregationsAsync(aggregations, ctx).AnyContext();
            if (result is not null)
                ctx.Search.Aggregations(result);
        }
    }
}
