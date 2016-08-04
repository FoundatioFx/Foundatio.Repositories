using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IAggregationQuery {
        List<AggregationField> AggregationFields { get; }
    }

    public class AggregationsQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var aggregationQuery = ctx.GetSourceAs<IAggregationQuery>();
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

    public static class AggregationQueryExtensions {
        public static T WithAggregation<T>(this T query, string field, int? maxTerms = null) where T : IAggregationQuery {
            if (!String.IsNullOrEmpty(field))
                query.AggregationFields?.Add(new AggregationField { Field = field, Size = maxTerms });

            return query;
        }

        public static T WithAggregation<T>(this T query, params string[] fields) where T : IAggregationQuery {
            if (fields.Length > 0)
                query.AggregationFields?.AddRange(fields.Select(f => new AggregationField { Field = f }));
            return query;
        }

        public static T WithAggregation<T>(this T query, int maxTerms, params string[] fields) where T : IAggregationQuery {
            if (fields.Length > 0)
                query.AggregationFields?.AddRange(fields.Select(f => new AggregationField { Field = f, Size = maxTerms }));
            return query;
        }

        public static T WithAggregation<T>(this T query, AggregationOptions aggregations) where T : IAggregationQuery {
            if (aggregations != null)
                query.AggregationFields?.AddRange(aggregations.Fields);
            return query;
        }
    }
}