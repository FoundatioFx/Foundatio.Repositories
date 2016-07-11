using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries {
    public interface IAggregationQuery {
        List<AggregationField> AggregationFields { get; }
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
