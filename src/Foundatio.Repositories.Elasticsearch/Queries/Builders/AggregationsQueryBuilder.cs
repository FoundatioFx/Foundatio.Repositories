using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IAggregationQuery {
        string Aggregations { get; set; }
    }

    public class AggregationsQueryBuilder : IElasticQueryBuilder {
        private readonly ElasticQueryParser _parser;
        private readonly GetReferencedFieldsQueryVisitor _referencedFieldsVisitor = new GetReferencedFieldsQueryVisitor();
        private readonly AssignAggregationTypeVisitor _aggregationTypeVisitorVisitor = new AssignAggregationTypeVisitor();
        private readonly LuceneQueryParser _luceneQueryParser = new LuceneQueryParser();

        public AggregationsQueryBuilder(ElasticQueryParser parser = null) {
            _parser = parser ?? new ElasticQueryParser();
        }

        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var aggregationQuery = ctx.GetSourceAs<IAggregationQuery>();
            if (String.IsNullOrEmpty(aggregationQuery?.Aggregations))
                return;

            var opt = ctx.GetOptionsAs<IElasticQueryOptions>();
            if (opt?.AllowedAggregationFields?.Count > 0 && !(await GetAggregationFieldsAsync(aggregationQuery.Aggregations).AnyContext()).All(f => opt.AllowedAggregationFields.Contains(f)))
                throw new InvalidOperationException("All aggregation fields must be allowed.");

            var result = await _parser.BuildAggregationsAsync(aggregationQuery.Aggregations, ctx).AnyContext();
            ctx.Search.Aggregations(result);
        }

        private async Task<ISet<string>> GetAggregationFieldsAsync(string aggregations) {
            var result = _luceneQueryParser.Parse(aggregations);
            var aggResult = await _aggregationTypeVisitorVisitor.AcceptAsync(result, null).AnyContext();
            return await _referencedFieldsVisitor.AcceptAsync(aggResult, null).AnyContext();
        }
    }

    public static class AggregationQueryExtensions {
        public static T WithAggregations<T>(this T query, string aggregations) where T : IAggregationQuery {
            query.Aggregations = aggregations;

            return query;
        }
    }
}