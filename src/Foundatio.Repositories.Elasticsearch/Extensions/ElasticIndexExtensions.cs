using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;
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

        public static FindHit<T> ToFindHit<T>(this Nest.GetResponse<T> hit) where T : class {
            var versionedDoc = hit.Source as IVersioned;
            if (versionedDoc != null)
                versionedDoc.Version = hit.GetElasticVersion();

            var data = new DataDictionary { { ElasticDataKeys.Index, hit.Index } };
            return new FindHit<T>(hit.Id, hit.Source, 0, versionedDoc?.Version ?? null, hit.Routing, data);
        }

        public static ElasticDocumentVersion GetElasticVersion<T>(this Nest.GetResponse<T> hit) where T : class {
            if (!hit.PrimaryTerm.HasValue || !hit.SequenceNumber.HasValue)
                return ElasticDocumentVersion.Empty;
            
            return new ElasticDocumentVersion(hit.PrimaryTerm.Value, hit.SequenceNumber.Value);
        }

        public static ElasticDocumentVersion GetElasticVersion<T>(this Nest.IHit<T> hit) where T : class {
            if (!hit.PrimaryTerm.HasValue || !hit.SequenceNumber.HasValue)
                return ElasticDocumentVersion.Empty;
            
            return new ElasticDocumentVersion(hit.PrimaryTerm.Value, hit.SequenceNumber.Value);
        }

        public static ElasticDocumentVersion GetElasticVersion<T>(this FindHit<T> hit) where T : class {
            if (hit == null || String.IsNullOrEmpty(hit.Version))
                return ElasticDocumentVersion.Empty;
            
            return hit.Version;
        }

        public static ElasticDocumentVersion GetElasticVersion(this Nest.IndexResponse hit) {
            if (hit.PrimaryTerm == 0 && hit.SequenceNumber == 0)
                return ElasticDocumentVersion.Empty;
            
            return new ElasticDocumentVersion(hit.PrimaryTerm, hit.SequenceNumber);
        }

        public static ElasticDocumentVersion GetElasticVersion<T>(this Nest.IMultiGetHit<T> hit) where T : class {
            if (!hit.PrimaryTerm.HasValue || !hit.SequenceNumber.HasValue)
                return ElasticDocumentVersion.Empty;
            
            return new ElasticDocumentVersion(hit.PrimaryTerm.Value, hit.SequenceNumber.Value);
        }

        public static ElasticDocumentVersion GetElasticVersion(this Nest.BulkResponseItemBase hit) {
            if (hit.PrimaryTerm == 0 && hit.SequenceNumber == 0)
                return ElasticDocumentVersion.Empty;
            
            return new ElasticDocumentVersion(hit.PrimaryTerm, hit.SequenceNumber);
        }

        public static ElasticDocumentVersion GetElasticVersion(this IVersioned versioned) {
            if (versioned == null || String.IsNullOrEmpty(versioned.Version))
                return ElasticDocumentVersion.Empty;
            
            return versioned.Version;
        }

        public static FindHit<T> ToFindHit<T>(this Nest.IHit<T> hit) where T : class {
            var versionedDoc = hit.Source as IVersioned;
            if (versionedDoc != null && hit.PrimaryTerm.HasValue)
                versionedDoc.Version = hit.GetElasticVersion();

            var data = new DataDictionary {
                { ElasticDataKeys.Index, hit.Index },
                { ElasticDataKeys.Sorts, hit.Sorts }
            };
            return new FindHit<T>(hit.Id, hit.Source, hit.Score.GetValueOrDefault(), versionedDoc?.Version ?? null, hit.Routing, data);
        }

        public static FindHit<T> ToFindHit<T>(this Nest.IMultiGetHit<T> hit) where T : class {
            var versionedDoc = hit.Source as IVersioned;
            if (versionedDoc != null)
                versionedDoc.Version = hit.GetElasticVersion();

            var data = new DataDictionary { { ElasticDataKeys.Index, hit.Index } };
            return new FindHit<T>(hit.Id, hit.Source, 0, versionedDoc?.Version ?? null, hit.Routing, data);
        }

        private static readonly long _epochTicks = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero).Ticks;

        private static readonly Lazy<Func<Nest.TopHitsAggregate, IList<Nest.LazyDocument>>> _getHits =
            new Lazy<Func<Nest.TopHitsAggregate, IList<Nest.LazyDocument>>>(() => {
                var hitsField = typeof(Nest.TopHitsAggregate).GetField("_hits", BindingFlags.GetField | BindingFlags.NonPublic | BindingFlags.Instance);
                return agg => hitsField?.GetValue(agg) as IList<Nest.LazyDocument>;
            });
        
        public static IAggregate ToAggregate(this Nest.IAggregate aggregate) {
            if (aggregate is Nest.ValueAggregate valueAggregate) {
                if (valueAggregate.Meta != null && valueAggregate.Meta.TryGetValue("@field_type", out var value)) {
                    string type = value.ToString();
                    if (type == "date" && valueAggregate.Value.HasValue) {
                        return new ValueAggregate<DateTime> {
                            Value = GetDate(valueAggregate),
                            Data = valueAggregate.Meta.ToReadOnlyData<ValueAggregate<DateTime>>()
                        };
                    }
                }

                return new ValueAggregate { Value = valueAggregate.Value, Data = valueAggregate.Meta.ToReadOnlyData<ValueAggregate>() };
            }

            if (aggregate is Nest.ScriptedMetricAggregate scriptedAggregate)
                return new ObjectValueAggregate {
                    Value = scriptedAggregate.Value<object>(),
                    Data = scriptedAggregate.Meta.ToReadOnlyData<ObjectValueAggregate>()
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
                    Data = extendedStatsAggregate.Meta.ToReadOnlyData<ExtendedStatsAggregate>()
                };
            
            if (aggregate is Nest.StatsAggregate statsAggregate)
                return new StatsAggregate {
                    Count = statsAggregate.Count,
                    Min = statsAggregate.Min,
                    Max = statsAggregate.Max,
                    Average = statsAggregate.Average,
                    Sum = statsAggregate.Sum,
                    Data = statsAggregate.Meta.ToReadOnlyData<StatsAggregate>()
                };

            if (aggregate is Nest.TopHitsAggregate topHitsAggregate) {
                var hits = _getHits.Value(topHitsAggregate);
                var docs = hits?.Select(h => new ElasticLazyDocument(h)).Cast<ILazyDocument>().ToList();
                
                return new TopHitsAggregate(docs) {
                    Total = topHitsAggregate.Total.Value,
                    MaxScore = topHitsAggregate.MaxScore,
                    Data = topHitsAggregate.Meta.ToReadOnlyData<TopHitsAggregate>()
                };
            }

            if (aggregate is Nest.PercentilesAggregate percentilesAggregate)
                return new PercentilesAggregate(percentilesAggregate.Items.Select(i => new PercentileItem { Percentile = i.Percentile, Value = i.Value })) {
                    Data = percentilesAggregate.Meta.ToReadOnlyData<PercentilesAggregate>()
                };

            if (aggregate is Nest.SingleBucketAggregate singleBucketAggregate)
                return new SingleBucketAggregate(singleBucketAggregate.ToAggregations()) {
                    Data = singleBucketAggregate.Meta.ToReadOnlyData<SingleBucketAggregate>(),
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
                    Data = new ReadOnlyDictionary<string, object>(data).ToReadOnlyData<BucketAggregate>(),
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
                ticks -= Exceptionless.DateTimeExtensions.TimeUnit.Parse(value.ToString()).Ticks;
            }

            return GetDate(ticks, kind);
        }
        
        private static DateTime GetDate(long ticks, DateTimeKind kind) {
            if (ticks <= DateTime.MinValue.Ticks)
                return DateTime.MinValue;
            
            if (ticks >= DateTime.MaxValue.Ticks)
                return DateTime.MaxValue;
            
            return new DateTime(ticks, kind);
        }

        public static IBucket ToBucket(this Nest.IBucket bucket, IDictionary<string, object> parentData = null) {
            if (bucket is Nest.DateHistogramBucket dateHistogramBucket) {
                bool hasTimezone = parentData != null && parentData.ContainsKey("@timezone");
                var kind = hasTimezone ? DateTimeKind.Unspecified : DateTimeKind.Utc;
                var ticks = _epochTicks + ((long)dateHistogramBucket.Key * TimeSpan.TicksPerMillisecond);
                var date = GetDate(ticks, kind);
                var data = new Dictionary<string, object> { { "@type", "datehistogram" } };
                
                if (hasTimezone)
                    data.Add("@timezone", parentData["@timezone"]);
                        
                return new DateHistogramBucket(date, dateHistogramBucket.ToAggregations()) {
                    Total = dateHistogramBucket.DocCount,
                    Key = dateHistogramBucket.Key,
                    KeyAsString = date.ToString("O"),
                    Data = data
                };
            }

            if (bucket is Nest.RangeBucket rangeBucket)
                return new RangeBucket(rangeBucket.ToAggregations()) {
                    Total = rangeBucket.DocCount,
                    Key = rangeBucket.Key,
                    From = rangeBucket.From,
                    FromAsString = rangeBucket.FromAsString,
                    To = rangeBucket.To,
                    ToAsString = rangeBucket.ToAsString,
                    Data = new Dictionary<string, object> { { "@type", "range" } }
                };

            if (bucket is Nest.KeyedBucket<string> stringKeyedBucket)
                return new KeyedBucket<string>(stringKeyedBucket.ToAggregations()) {
                    Total = stringKeyedBucket.DocCount,
                    Key = stringKeyedBucket.Key,
                    KeyAsString = stringKeyedBucket.KeyAsString,
                    Data = new Dictionary<string, object> { { "@type", "string" } }
                };

            if (bucket is Nest.KeyedBucket<double> doubleKeyedBucket)
                return new KeyedBucket<double>(doubleKeyedBucket.ToAggregations()) {
                    Total = doubleKeyedBucket.DocCount,
                    Key = doubleKeyedBucket.Key,
                    KeyAsString = doubleKeyedBucket.KeyAsString,
                    Data = new Dictionary<string, object> { { "@type", "double" } }
                };

            if (bucket is Nest.KeyedBucket<object> objectKeyedBucket)
                return new KeyedBucket<object>(objectKeyedBucket.ToAggregations()) {
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
                pd.Boolean(p => p.Name(d => ((ISupportSoftDeletes)d).IsDeleted)).FieldAlias(a => a.Path(p => ((ISupportSoftDeletes)p).IsDeleted).Name("deleted"));

            if (hasCreatedDate)
                pd.Date(p => p.Name(d => ((IHaveCreatedDate)d).CreatedUtc)).FieldAlias(a => a.Path(p => ((IHaveCreatedDate)p).CreatedUtc).Name("created"));

            if (hasDates)
                pd.Date(p => p.Name(d => ((IHaveDates)d).UpdatedUtc)).FieldAlias(a => a.Path(p => ((IHaveDates)p).UpdatedUtc).Name("updated"));;

            return pd;
        }
    }
}