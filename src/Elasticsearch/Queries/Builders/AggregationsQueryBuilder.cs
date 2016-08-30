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

            var opt = ctx.GetOptionsAs<IElasticQueryOptions>();
            if (opt?.AllowedAggregationFields?.Length > 0 && !FlattenedFields(aggregationQuery.AggregationFields).All(f => opt.AllowedAggregationFields.Contains(f.Field)))
                throw new InvalidOperationException("All aggregation fields must be allowed.");

            ctx.Search.Aggregations(agg => GetAggregationDescriptor<T>(aggregationQuery.AggregationFields));
        }

        private AggregationDescriptor<T> GetAggregationDescriptor<T>(IEnumerable<AggregationField> fields) where T : class {
            var descriptor = new AggregationDescriptor<T>();
            foreach (var t in fields)
                descriptor = descriptor.Terms(t.Field, s =>
                {
                    s = s.Field(t.Field).Size(t.Size ?? 100);
                    if (t.Nested?.Fields.Count > 0)
                        s = s.Aggregations(a => GetAggregationDescriptor<T>(t.Nested.Fields));
                    return s;
                });

            return descriptor;
        }

        private static IEnumerable<AggregationField> FlattenedFields(IEnumerable<AggregationField> source)
        {
            foreach (var field in source)
            {
                yield return field;
                if (field.Nested != null)
                {
                    foreach (var nested in FlattenedFields(field.Nested.Fields))
                        yield return nested;
                }
            }
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