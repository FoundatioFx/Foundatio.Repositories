using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Repositories.Models;
using Nest;
using Foundatio.Utility;

namespace Foundatio.Repositories.Elasticsearch.Extensions {
    public static class ElasticIndexExtensions {
        public static FindResults<T> ToFindResults<T>(this ISearchResponse<T> response, int? limit = null) where T : class, new() {
            var docs = response.Hits.Take(limit ?? Int32.MaxValue).ToFindHits().ToList();
            var data = response.ScrollId != null ? new DataDictionary { { ElasticDataKeys.ScrollId, response.ScrollId } } : null;
            return new FindResults<T>(docs, response.Total, response.ToAggregationResult(), null, data);
        }

        public static IEnumerable<FindHit<T>> ToFindHits<T>(this IEnumerable<IHit<T>> hits) where T : class {
            return hits.Select(h => h.ToFindHit());
        }

        public static FindHit<T> ToFindHit<T>(this IGetResponse<T> hit) where T : class {
            var versionedDoc = hit.Source as IVersioned;
            if (versionedDoc != null)
                versionedDoc.Version = hit.Version;

            var data = new DataDictionary { { ElasticDataKeys.Index, hit.Index }, { ElasticDataKeys.IndexType, hit.Type } };
            return new FindHit<T>(hit.Id, hit.Source, 0, versionedDoc?.Version ?? null, data);
        }

        public static FindHit<T> ToFindHit<T>(this IHit<T> hit) where T : class {
            var versionedDoc = hit.Source as IVersioned;
            if (versionedDoc != null && hit.Version.HasValue)
                versionedDoc.Version = hit.Version.Value;

            var data = new DataDictionary { { ElasticDataKeys.Index, hit.Index }, { ElasticDataKeys.IndexType, hit.Type } };
            return new FindHit<T>(hit.Id, hit.Source, hit.Score.GetValueOrDefault(), versionedDoc?.Version ?? null, data);
        }

        public static FindHit<T> ToFindHit<T>(this IMultiGetHit<T> hit) where T : class {
            var versionedDoc = hit.Source as IVersioned;
            if (versionedDoc != null)
                versionedDoc.Version = hit.Version;

            var data = new DataDictionary { { ElasticDataKeys.Index, hit.Index }, { ElasticDataKeys.IndexType, hit.Type } };
            return new FindHit<T>(hit.Id, hit.Source, 0, versionedDoc?.Version ?? null, data);
        }

        public static IDictionary<string, AggregationResult> ToAggregationResult(this IReadOnlyDictionary<string, IAggregate> aggregations) {
            var results = new Dictionary<string, AggregationResult>();
            if (aggregations == null || aggregations.Count == 0)
                return null;

            foreach (var key in aggregations.Keys) {
                var aggValue = aggregations[key];

                var metricAggValue = aggValue as ValueAggregate;
                if (metricAggValue != null) {
                    results.Add(key, new AggregationResult { Value = metricAggValue.Value });
                    continue;
                }

                var percentilesAggregate = aggValue as PercentilesAggregate;
                if (percentilesAggregate != null) {
                    var result = new AggregationResult {
                        Data = new Dictionary<string, object>(percentilesAggregate.Items.Count)
                    };

                    foreach (var percentileItem in percentilesAggregate.Items)
                        result.Data.Add(percentileItem.Percentile.ToString(), percentileItem.Value);

                    results.Add(key, result);
                    continue;
                }

                var singleBucketAggregate = aggValue as SingleBucketAggregate;
                if (singleBucketAggregate != null) {
                    results.Add(key, new AggregationResult {
                        Value = singleBucketAggregate.DocCount,
                        Aggregations = singleBucketAggregate.Aggregations.ToAggregationResult()
                    });
                    continue;
                }

                var bucketValue = aggValue as BucketAggregate;
                if (bucketValue != null) {
                    var result = new AggregationResult {
                        Buckets = new List<BucketResult>()
                    };

                    foreach (var keyItem in bucketValue.Items.OfType<KeyedBucket>()) {
                        var bucketResult = new BucketResult {
                            Key = keyItem.Key,
                            KeyAsString = keyItem.KeyAsString,
                            Total = keyItem.DocCount,
                            Aggregations = keyItem.Aggregations.ToAggregationResult()
                        };

                        result.Buckets.Add(bucketResult);
                    }

                    foreach (var keyItem in bucketValue.Items.OfType<HistogramBucket>()) {
                        var bucketResult = new BucketResult {
                            Key = keyItem.Key.ToString(),
                            KeyAsString = keyItem.KeyAsString,
                            Total = keyItem.DocCount,
                            Aggregations = keyItem.Aggregations.ToAggregationResult()
                        };

                        result.Buckets.Add(bucketResult);
                    }

                    results.Add(key, result);
                }
            }

            return results;
        }

        public static IDictionary<string, AggregationResult> ToAggregationResult<T>(this ISearchResponse<T> res) where T : class {
            return res.Aggregations.ToAggregationResult();
        }

        public static PropertiesDescriptor<T> SetupDefaults<T>(this PropertiesDescriptor<T> pd) where T : class {
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