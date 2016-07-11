using System;
using System.Linq;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class AggregationsQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var aggregationQuery = ctx.GetQueryAs<IAggregationQuery>();
            if (aggregationQuery?.AggregationFields == null || aggregationQuery.AggregationFields.Count <= 0)
                return;

            var opt = ctx.GetOptionsAs<IQueryOptions>();
            if (opt?.AllowedAggregationFields?.Length > 0 && !aggregationQuery.AggregationFields.All(f => opt.AllowedAggregationFields.Contains(f.Field)))
                throw new InvalidOperationException("All facet fields must be allowed.");

            ctx.Search.Aggregations(agg => GetAggregationDescriptor<T>(aggregationQuery));
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