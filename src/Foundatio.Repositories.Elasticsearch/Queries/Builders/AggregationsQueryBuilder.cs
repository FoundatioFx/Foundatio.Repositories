using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Queries.Options;

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

        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var aggregationQuery = ctx.GetSourceAs<IAggregationQuery>();
            if (String.IsNullOrEmpty(aggregationQuery?.Aggregations))
                return;

            var opt = ctx.GetOptionsAs<IElasticQueryOptions>();
            if (opt?.AllowedAggregationFields?.Count > 0 && !GetAggregationFields(aggregationQuery.Aggregations).All(f => opt.AllowedAggregationFields.Contains(f)))
                throw new InvalidOperationException("All aggregation fields must be allowed.");

            var result = _parser.BuildAggregations(aggregationQuery.Aggregations, ctx);
            ctx.Search.Aggregations(result);
        }

        private ISet<string> GetAggregationFields(string aggregations) {
            var result = _luceneQueryParser.Parse(aggregations);
            var aggResult = _aggregationTypeVisitorVisitor.Accept(result, null);
            return _referencedFieldsVisitor.Accept(aggResult, null);
        }
    }

    public static class AggregationQueryExtensions {
        public static T WithAggregations<T>(this T query, string aggregations) where T : IAggregationQuery {
            query.Aggregations = aggregations;

            return query;
        }
    }
}