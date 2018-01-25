using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Utility;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Elasticsearch.Extensions {
    public static class ElasticIndexExtensions {
        public static FindResults<T> ToFindResults<T>(this Nest.ISearchResponse<T> response, int? limit = null) where T : class, new() {
            var docs = response.Hits.Take(limit ?? Int32.MaxValue).ToFindHits().ToList();
            var data = response.ScrollId != null ? new DataDictionary { { ElasticDataKeys.ScrollId, response.ScrollId } } : null;
            return new FindResults<T>(docs, response.Total, response.ToAggregations(), null, data);
        }

        public static IEnumerable<FindHit<T>> ToFindHits<T>(this IEnumerable<Nest.IHit<T>> hits) where T : class {
            return hits.Select(h => h.ToFindHit());
        }

        public static FindHit<T> ToFindHit<T>(this Nest.IGetResponse<T> hit) where T : class {
            var versionedDoc = hit.Source as IVersioned;
            if (versionedDoc != null)
                versionedDoc.Version = hit.Version;

            var data = new DataDictionary { { ElasticDataKeys.Index, hit.Index }, { ElasticDataKeys.IndexType, hit.Type } };
            return new FindHit<T>(hit.Id, hit.Source, 0, versionedDoc?.Version ?? null, hit.Routing, data);
        }

        public static FindHit<T> ToFindHit<T>(this Nest.IHit<T> hit) where T : class {
            var versionedDoc = hit.Source as IVersioned;
            if (versionedDoc != null && hit.Version.HasValue)
                versionedDoc.Version = hit.Version.Value;

            var data = new DataDictionary { { ElasticDataKeys.Index, hit.Index }, { ElasticDataKeys.IndexType, hit.Type } };
            return new FindHit<T>(hit.Id, hit.Source, hit.Score.GetValueOrDefault(), versionedDoc?.Version ?? null, hit.Routing, data);
        }

        public static FindHit<T> ToFindHit<T>(this Nest.IMultiGetHit<T> hit) where T : class {
            var versionedDoc = hit.Source as IVersioned;
            if (versionedDoc != null)
                versionedDoc.Version = hit.Version;

            var data = new DataDictionary { { ElasticDataKeys.Index, hit.Index }, { ElasticDataKeys.IndexType, hit.Type } };
            return new FindHit<T>(hit.Id, hit.Source, 0, versionedDoc?.Version ?? null, hit.Routing, data);
        }

        private static readonly long _epochTicks = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero).Ticks;
        public static IAggregate ToAggregate(this Nest.IAggregate aggregate) {
            if (aggregate is Nest.ValueAggregate valueAggregate) {
                if (valueAggregate.Meta != null && valueAggregate.Meta.TryGetValue("@field_type", out var value)) {
                    string type = value.ToString();
                    if (type == "date" && valueAggregate.Value.HasValue) {
                        return new ValueAggregate<DateTime> {
                            Value = GetDate(valueAggregate),
                            Data = valueAggregate.Meta.ToData<ValueAggregate<DateTime>>()
                        };
                    }
                }

                return new ValueAggregate { Value = valueAggregate.Value, Data = valueAggregate.Meta.ToData<ValueAggregate>() };
            }

            if (aggregate is Nest.ScriptedMetricAggregate scriptedAggregate)
                return new ObjectValueAggregate {
                    Value = scriptedAggregate.Value<object>(),
                    Data = scriptedAggregate.Meta.ToData<ObjectValueAggregate>()
                };

            if (aggregate is Nest.StatsAggregate statsAggregate)
                return new StatsAggregate {
                    Count = statsAggregate.Count,
                    Min = statsAggregate.Min,
                    Max = statsAggregate.Max,
                    Average = statsAggregate.Average,
                    Sum = statsAggregate.Sum,
                    Data = statsAggregate.Meta.ToData<StatsAggregate>()
                };

            if (aggregate is Nest.TopHitsAggregate topHitsAggregate)
                return new TopHitsAggregate(topHitsAggregate.Documents<JToken>().Select(topHit => new LazyDocument(topHit.Value<JToken>())).ToList()) {
                    Total = topHitsAggregate.Total,
                    MaxScore = topHitsAggregate.MaxScore,
                    Data = topHitsAggregate.Meta.ToData<TopHitsAggregate>()
                };

            if (aggregate is Nest.ExtendedStatsAggregate extendedStatsAggregate)
                return new ExtendedStatsAggregate {
                    Count = extendedStatsAggregate.Count,
                    Min = extendedStatsAggregate.Min,
                    Max = extendedStatsAggregate.Max,
                    Average = extendedStatsAggregate.Average,
                    Sum = extendedStatsAggregate.Sum,
                    StdDeviation = extendedStatsAggregate.StdDeviation,
                    StdDeviationBounds = new StandardDeviationBounds {
                        Lower = extendedStatsAggregate.StdDeviationBounds.Lower,
                        Upper = extendedStatsAggregate.StdDeviationBounds.Upper
                    },
                    SumOfSquares = extendedStatsAggregate.SumOfSquares,
                    Variance = extendedStatsAggregate.Variance,
                    Data = extendedStatsAggregate.Meta.ToData<ExtendedStatsAggregate>()
                };

            if (aggregate is Nest.PercentilesAggregate percentilesAggregate)
                return new PercentilesAggregate(percentilesAggregate.Items.Select(i => new PercentileItem { Percentile = i.Percentile, Value = i.Value })) {
                    Data = percentilesAggregate.Meta.ToData<PercentilesAggregate>()
                };

            if (aggregate is Nest.SingleBucketAggregate singleBucketAggregate)
                return new SingleBucketAggregate(singleBucketAggregate.Aggregations.ToAggregations()) {
                    Data = singleBucketAggregate.Meta.ToData<SingleBucketAggregate>(),
                    Total = singleBucketAggregate.DocCount
                };            

            if (aggregate is Nest.BucketAggregate bucketAggregation) {
                var data = new Dictionary<string, object>((IDictionary<string, object>)bucketAggregation.Meta ?? new Dictionary<string, object>());
                if (bucketAggregation.DocCountErrorUpperBound.GetValueOrDefault() > 0)
                    data.Add(nameof(bucketAggregation.DocCountErrorUpperBound), bucketAggregation.DocCountErrorUpperBound);
                if (bucketAggregation.SumOtherDocCount.GetValueOrDefault() > 0)
                    data.Add(nameof(bucketAggregation.SumOtherDocCount), bucketAggregation.SumOtherDocCount);

                return new BucketAggregate {
                    Items = bucketAggregation.Items.Select(i => i.ToBucket(data)).ToList(),
                    Data = new ReadOnlyDictionary<string, object>(data).ToData<BucketAggregate>(),
                    Total = bucketAggregation.DocCount
                };
            }

            return null;
        }

        private static DateTime GetDate(Nest.ValueAggregate valueAggregate) {
            if (valueAggregate?.Value == null)
                throw new ArgumentNullException(nameof(valueAggregate));

            var kind = DateTimeKind.Utc;
            long ticks = _epochTicks + ((long)valueAggregate.Value * TimeSpan.TicksPerMillisecond);

            if (valueAggregate.Meta.TryGetValue("@timezone", out var value) && value != null) {
                kind = DateTimeKind.Unspecified;
                ticks -= TimeUnit.Parse(value.ToString()).Ticks;
            }

            return new DateTime(ticks, kind);
        }

        public static IBucket ToBucket(this Nest.IBucket bucket, IDictionary<string, object> parentData = null) {
            if (bucket is Nest.DateHistogramBucket dateHistogramBucket) {
                var kind = parentData != null && parentData.ContainsKey("@timezone") ? DateTimeKind.Unspecified : DateTimeKind.Utc;
                var date = new DateTime(_epochTicks + ((long)dateHistogramBucket.Key * TimeSpan.TicksPerMillisecond), kind);
                return new DateHistogramBucket(date, dateHistogramBucket.Aggregations.ToAggregations()) {
                    Total = dateHistogramBucket.DocCount,
                    Key = dateHistogramBucket.Key,
                    KeyAsString = date.ToString("O"),
                    Data = new Dictionary<string, object> { { "@type", "datehistogram" } }
                };
            }

            if (bucket is Nest.RangeBucket rangeBucket)
                return new RangeBucket(rangeBucket.Aggregations.ToAggregations()) {
                    Total = rangeBucket.DocCount,
                    Key = rangeBucket.Key,
                    From = rangeBucket.From,
                    FromAsString = rangeBucket.FromAsString,
                    To = rangeBucket.To,
                    ToAsString = rangeBucket.ToAsString,
                    Data = new Dictionary<string, object> {{ "@type", "range" }}
                };

            if (bucket is Nest.KeyedBucket<string> stringKeyedBucket)
                return new KeyedBucket<string>(stringKeyedBucket.Aggregations.ToAggregations()) {
                    Total = stringKeyedBucket.DocCount,
                    Key = stringKeyedBucket.Key,
                    KeyAsString = stringKeyedBucket.KeyAsString,
                    Data = new Dictionary<string, object> { { "@type", "string" } }
                };

            if (bucket is Nest.KeyedBucket<double> doubleKeyedBucket)
                return new KeyedBucket<double>(doubleKeyedBucket.Aggregations.ToAggregations()) {
                    Total = doubleKeyedBucket.DocCount,
                    Key = doubleKeyedBucket.Key,
                    KeyAsString = doubleKeyedBucket.KeyAsString,
                    Data = new Dictionary<string, object> { { "@type", "double" } }
                };

            if (bucket is Nest.KeyedBucket<object> objectKeyedBucket)
                return new KeyedBucket<object>(objectKeyedBucket.Aggregations.ToAggregations()) {
                    Total = objectKeyedBucket.DocCount,
                    Key = objectKeyedBucket.Key,
                    KeyAsString = objectKeyedBucket.KeyAsString,
                    Data = new Dictionary<string, object> { { "@type", "object" } }
                };

            return null;
        }

        public static IDictionary<string, IAggregate> ToAggregations(this IReadOnlyDictionary<string, Nest.IAggregate> aggregations) {
            return aggregations?.ToDictionary(a => a.Key, a => a.Value.ToAggregate());
        }

        public static IDictionary<string, IAggregate> ToAggregations<T>(this Nest.ISearchResponse<T> res) where T : class {
            return res.Aggregations.ToAggregations();
        }

        public static Nest.PropertiesDescriptor<T> SetupDefaults<T>(this Nest.PropertiesDescriptor<T> pd) where T : class {
            bool hasIdentity = typeof(IIdentity).IsAssignableFrom(typeof(T));
            bool hasDates = typeof(IHaveDates).IsAssignableFrom(typeof(T));
            bool hasCreatedDate = typeof(IHaveCreatedDate).IsAssignableFrom(typeof(T));
            bool supportsSoftDeletes = typeof(ISupportSoftDeletes).IsAssignableFrom(typeof(T));

            if (hasIdentity)
                pd.Keyword(p => p.Name(d => ((IIdentity)d).Id));

            if (supportsSoftDeletes)
                pd.Boolean(p => p.Name(d => ((ISupportSoftDeletes)d).IsDeleted).Alias("deleted"));

            if (hasCreatedDate)
                pd.Date(p => p.Name(d => ((IHaveCreatedDate)d).CreatedUtc).Alias("created"));

            if (hasDates)
                pd.Date(p => p.Name(d => ((IHaveDates)d).UpdatedUtc).Alias("updated"));

            return pd;
        }

        public static Nest.TextPropertyDescriptor<T> AddKeywordField<T>(this Nest.TextPropertyDescriptor<T> descriptor) where T : class {
            return descriptor.Fields(f => f.Keyword(s => s.Name("keyword").IgnoreAbove(256)));
        }
    }
}