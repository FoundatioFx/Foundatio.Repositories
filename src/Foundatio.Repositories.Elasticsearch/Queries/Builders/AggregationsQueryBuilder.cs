using System;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Options;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IAggregationQuery : IRepositoryQuery {
        string Aggregations { get; set; }
    }

    public class AggregationsQueryBuilder : IElasticQueryBuilder {
        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var elasticOptions = ctx.GetOptionsAs<IElasticCommandOptions>();
            if (elasticOptions?.IndexType?.QueryParser == null)
                return;

            var aggregationQuery = ctx.GetSourceAs<IAggregationQuery>();
            if (String.IsNullOrEmpty(aggregationQuery?.Aggregations))
                return;

            var result = await elasticOptions.IndexType.QueryParser.BuildAggregationsAsync(aggregationQuery.Aggregations, ctx).AnyContext();
            ctx.Search.Aggregations(result);
        }
    }

    public static class AggregationQueryExtensions {
        public static T WithAggregations<T>(this T query, string aggregations) where T : IAggregationQuery {
            query.Aggregations = aggregations;

            return query;
        }
    }
}