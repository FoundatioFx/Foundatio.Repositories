using System;
using System.Linq;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class AggregationsQueryBuilder : ElasticQueryBuilderBase {
        public override void BuildSearch<T>(object query, object options, ref SearchDescriptor<T> descriptor) {
            var aggregationQuery = query as IAggregationQuery;
            if (aggregationQuery?.AggregationFields == null || aggregationQuery.AggregationFields.Count <= 0)
                return;

            var opt = options as IQueryOptions;
            if (opt?.AllowedFacetFields?.Length > 0 && !aggregationQuery.AggregationFields.All(f => opt.AllowedFacetFields.Contains(f.Field)))
                throw new InvalidOperationException("All facet fields must be allowed.");

            descriptor.Aggregations(agg => GetAggregationDescriptor<T>(aggregationQuery));
        }

        private AggregationDescriptor<T> GetAggregationDescriptor<T>(object query) where T : class {
            var aggregationQuery = query as IAggregationQuery;
            if (aggregationQuery == null || aggregationQuery.AggregationFields.Count == 0)
                return null;

            var descriptor = new AggregationDescriptor<T>();
            foreach (var t in aggregationQuery.AggregationFields)
                descriptor = descriptor.Terms(t.Field, s => s.Field(t.Field).Size(t.Size ?? 100));

            return descriptor;
        }
    }
}