using System;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IAggregationQuery {
        string Aggregations { get; set; }
    }

    public class AggregationsQueryBuilder : IElasticQueryBuilder {
        private readonly ElasticQueryParser _parser;

        public AggregationsQueryBuilder(ElasticQueryParser parser = null) {
            _parser = parser ?? new ElasticQueryParser();
        }

        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var aggregationQuery = ctx.GetSourceAs<IAggregationQuery>();
            if (String.IsNullOrEmpty(aggregationQuery?.Aggregations))
                return;

            var result = await _parser.BuildAggregationsAsync(aggregationQuery.Aggregations, ctx).AnyContext();
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