using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Utility;

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
            return new FindHit<T>(hit.Id, hit.Source, 0, versionedDoc?.Version ?? null, data);
        }

        public static FindHit<T> ToFindHit<T>(this Nest.IHit<T> hit) where T : class {
            var versionedDoc = hit.Source as IVersioned;
            if (versionedDoc != null && hit.Version.HasValue)
                versionedDoc.Version = hit.Version.Value;

            var data = new DataDictionary { { ElasticDataKeys.Index, hit.Index }, { ElasticDataKeys.IndexType, hit.Type } };
            return new FindHit<T>(hit.Id, hit.Source, hit.Score.GetValueOrDefault(), versionedDoc?.Version ?? null, data);
        }

        public static FindHit<T> ToFindHit<T>(this Nest.IMultiGetHit<T> hit) where T : class {
            var versionedDoc = hit.Source as IVersioned;
            if (versionedDoc != null)
                versionedDoc.Version = hit.Version;

            var data = new DataDictionary { { ElasticDataKeys.Index, hit.Index }, { ElasticDataKeys.IndexType, hit.Type } };
            return new FindHit<T>(hit.Id, hit.Source, 0, versionedDoc?.Version ?? null, data);
        }

        private static readonly long _epochTicks = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero).Ticks;
        public static IAggregate ToAggregate(this Nest.IAggregate aggregate) {
            var valueAggregate = aggregate as Nest.ValueAggregate;
            if (valueAggregate != null) {
                object value;
                if (valueAggregate.Meta != null && valueAggregate.Meta.TryGetValue("@type", out value)) {
                    string type = value.ToString();
                    if (type == "date" && valueAggregate.Value.HasValue) {
                        return new ValueAggregate<DateTime> {
                            Value = GetDate(valueAggregate),
                            Data = valueAggregate.Meta.ToData()
                        };
                    }
                }

                return new ValueAggregate { Value = valueAggregate.Value, Data = valueAggregate.Meta.ToData() };
            }

            var scriptedAggregate = aggregate as Nest.ScriptedMetricAggregate;
            if (scriptedAggregate != null)
                return new ObjectValueAggregate {
                    Value = scriptedAggregate.Value<object>(),
                    Data = scriptedAggregate.Meta.ToData()
                };

            var statsAggregate = aggregate as Nest.StatsAggregate;
            if (statsAggregate != null)
                return new StatsAggregate {
                    Count = statsAggregate.Count,
                    Min = statsAggregate.Min,
                    Max = statsAggregate.Max,
                    Average = statsAggregate.Average,
                    Sum = statsAggregate.Sum,
                    Data = statsAggregate.Meta.ToData()
                };

            var extendedStatsAggregate = aggregate as Nest.ExtendedStatsAggregate;
            if (extendedStatsAggregate != null)
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
                    Data = extendedStatsAggregate.Meta.ToData()
                };

            var percentilesAggregate = aggregate as Nest.PercentilesAggregate;
            if (percentilesAggregate != null)
                return new PercentilesAggregate(percentilesAggregate.Items.Select(i => new PercentileItem { Percentile = i.Percentile, Value = i.Value } )) {
                    Data = percentilesAggregate.Meta.ToData()
                };

            var singleBucketAggregate = aggregate as Nest.SingleBucketAggregate;
            if (singleBucketAggregate != null)
                return new SingleBucketAggregate {
                    Data = singleBucketAggregate.Meta.ToData(),
                    Total = singleBucketAggregate.DocCount
                };

            var bucketAggregation = aggregate as Nest.BucketAggregate;
            if (bucketAggregation != null) {
                var data = new Dictionary<string, object>((IDictionary<string, object>)bucketAggregation.Meta ?? new Dictionary<string, object>());
                if (bucketAggregation.DocCountErrorUpperBound.GetValueOrDefault() > 0)
                    data.Add(nameof(bucketAggregation.DocCountErrorUpperBound), bucketAggregation.DocCountErrorUpperBound);
                if (bucketAggregation.SumOtherDocCount.GetValueOrDefault() > 0)
                    data.Add(nameof(bucketAggregation.SumOtherDocCount), bucketAggregation.SumOtherDocCount);

                return new BucketAggregate {
                    Items = bucketAggregation.Items.Select(i => i.ToBucket(data)).ToList(),
                    Data = new ReadOnlyDictionary<string, object>(data).ToData(),
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

            object value;
            if (valueAggregate.Meta.TryGetValue("@offset", out value) && value != null) {
                kind = DateTimeKind.Unspecified;
                ticks -= TimeUnit.Parse(value.ToString()).Ticks;
            }

            return new DateTime(ticks, kind);
        }

        public static IBucket ToBucket(this Nest.IBucket bucket, IDictionary<string, object> parentData = null) {
            var dateHistogramBucket = bucket as Nest.DateHistogramBucket;
            if (dateHistogramBucket != null) {
                var kind = parentData != null && parentData.ContainsKey("@offset") ? DateTimeKind.Unspecified : DateTimeKind.Utc;
                var date = new DateTime(_epochTicks + ((long)dateHistogramBucket.Key * TimeSpan.TicksPerMillisecond), kind);
                return new DateHistogramBucket(date, dateHistogramBucket.Aggregations.ToAggregations()) {
                    Total = dateHistogramBucket.DocCount,
                    Key = dateHistogramBucket.Key,
                    KeyAsString = date.ToString("O")
                };
            }

            var stringKeyedBucket = bucket as Nest.KeyedBucket<string>;
            if (stringKeyedBucket != null)
                return new KeyedBucket<string>(stringKeyedBucket.Aggregations.ToAggregations()) {
                    Total = stringKeyedBucket.DocCount,
                    Key = stringKeyedBucket.Key,
                    KeyAsString = stringKeyedBucket.KeyAsString
                };

            var doubleKeyedBucket = bucket as Nest.KeyedBucket<double>;
            if (doubleKeyedBucket != null)
                return new KeyedBucket<double>(doubleKeyedBucket.Aggregations.ToAggregations()) {
                    Total = doubleKeyedBucket.DocCount,
                    Key = doubleKeyedBucket.Key,
                    KeyAsString = doubleKeyedBucket.KeyAsString
                };

            var objectKeyedBucket = bucket as Nest.KeyedBucket<object>;
            if (objectKeyedBucket != null)
                return new KeyedBucket<object>(objectKeyedBucket.Aggregations.ToAggregations()) {
                    Total = objectKeyedBucket.DocCount,
                    Key = objectKeyedBucket.Key,
                    KeyAsString = objectKeyedBucket.KeyAsString
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
            var hasIdentity = typeof(IIdentity).IsAssignableFrom(typeof(T));
            var hasDates = typeof(IHaveDates).IsAssignableFrom(typeof(T));
            var hasCreatedDate = typeof(IHaveCreatedDate).IsAssignableFrom(typeof(T));
            var supportsSoftDeletes = typeof(ISupportSoftDeletes).IsAssignableFrom(typeof(T));

            if (hasIdentity)
                pd.Keyword(p => p.Name(d => (d as IIdentity).Id));

            if (supportsSoftDeletes)
                pd.Boolean(p => p.Name(d => (d as ISupportSoftDeletes).IsDeleted));

            if (hasCreatedDate)
                pd.Date(p => p.Name(d => (d as IHaveCreatedDate).CreatedUtc));

            if (hasDates)
                pd.Date(p => p.Name(d => (d as IHaveDates).UpdatedUtc));

            return pd;
        }
    }
}