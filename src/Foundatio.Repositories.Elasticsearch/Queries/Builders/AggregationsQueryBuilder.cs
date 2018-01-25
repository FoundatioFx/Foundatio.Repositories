using System;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class AggregationQueryExtensions {
        internal const string AggregationsKey = "@AggregationsExpressionKey";

        public static T AggregationsExpression<T>(this T options, string aggregations) where T : IRepositoryQuery {
            return options.BuildOption(AggregationsKey, aggregations);
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadAggregationQueryExtensions {
        public static string GetAggregationsExpression(this IRepositoryQuery query) {
            return query.SafeGetOption<string>(AggregationQueryExtensions.AggregationsKey, null);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class AggregationsQueryBuilder : IElasticQueryBuilder {
        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var elasticOptions = ctx.Options.GetElasticTypeSettings();
            if (elasticOptions?.IndexType?.QueryParser == null)
                return;

            string aggregations = ctx.Source.GetAggregationsExpression();
            if (String.IsNullOrEmpty(aggregations))
                return;

            var result = await elasticOptions.IndexType.QueryParser.BuildAggregationsAsync(aggregations, ctx).AnyContext();
            ctx.Search.Aggregations(result);
        }
    }
}