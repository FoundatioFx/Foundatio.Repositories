using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Nest;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Repositories;

namespace Foundatio.Repositories.Elasticsearch.Extensions {
    public static class ElasticIndexExtensions {
        public static ElasticFindResults<T> ToFindResults<T>(this ISearchResponse<T> res, int? limit = null) where T : class, new() {
            var docs = res.Hits.ToFindHits().Take(limit ?? Int32.MaxValue).ToList();
            docs.SetVersions();

            var result = new ElasticFindResults<T>(docs, res.Total, res.ToAggregationResult(), res.ScrollId) {
                HasMore = limit.HasValue && res.Documents.Count() > limit.Value
            };

            return result;
        }

        public static void SetVersions<T>(this IEnumerable<IFindHit<T>> hits) where T : class {
            foreach (var hit in hits)
                hit.SetVersion();
        }

        public static void SetVersion<T>(this IFindHit<T> hit) where T : class {
            var versionedDoc = hit.Document as IVersioned;
            if (versionedDoc != null && hit.Version.HasValue)
                versionedDoc.Version = hit.Version.Value;
        }

        public static void SetVersion<T>(this IGetResponse<T> hit) where T : class {
            var versionedDoc = hit.Source as IVersioned;
            if (versionedDoc != null && hit.Version != null)
                versionedDoc.Version = Int64.Parse(hit.Version);
        }

        public static IEnumerable<ElasticFindHit<T>> ToFindHits<T>(this IEnumerable<IHit<T>> hits) where T : class {
            return hits.Select(h => h.ToFindHit());
        }

        public static ElasticFindHit<T> ToFindHit<T>(this IGetResponse<T> hit) where T : class {
            return new ElasticFindHit<T> {
                Document = hit.Source,
                Id = hit.Id,
                Index = hit.Index,
                Type = hit.Type,
                Version = hit.Version != null ? Int64.Parse(hit.Version) : (long?)null
            };
        }

        public static ElasticFindHit<T> ToFindHit<T>(this IHit<T> hit) where T : class {
            return new ElasticFindHit<T> {
                Document = hit.Source,
                Id = hit.Id,
                Index = hit.Index,
                Type = hit.Type,
                Score = hit.Score,
                Version = hit.Version != null ? Int64.Parse(hit.Version) : (long?)null
            };
        }

        public static ElasticFindHit<T> ToFindHit<T>(this IMultiGetHit<T> response) where T : class {
            var versionedDoc = response.Source as IVersioned;
            if (versionedDoc != null && response.Version != null)
                versionedDoc.Version = Int64.Parse(response.Version);

            return new ElasticFindHit<T> {
                Document = response.Source,
                Id = response.Id,
                Index = response.Index,
                Type = response.Type,
                Version = versionedDoc?.Version ?? null
            };
        }

        private static AggregationResult ToAggregationResult(this Bucket bucket, string field) {
            return new AggregationResult {
                Field = field,
                Terms = new AggregationDictionary<AggregationResult>(bucket.Items.OfType<KeyItem>().ToDictionary(t => t.Key, t => {
                    var termRes = new AggregationResult<AggregationResult> {
                        Total = t.DocCount
                    };

                    if (t.Aggregations?.Count > 0) {
                        termRes.Aggregations = new List<AggregationResult>();
                        foreach (var key in t.Aggregations.Keys) {
                            var nestedBucket = t.Aggregations[key] as Bucket;
                            if (nestedBucket == null)
                                continue;

                            termRes.Aggregations.Add(nestedBucket.ToAggregationResult(key));
                        }
                    }

                    return termRes;
                })),
            };
        }

        public static IReadOnlyCollection<AggregationResult> ToAggregationResult<T>(this ISearchResponse<T> res) where T : class {
            var result = new List<AggregationResult>();
            if (res.Aggregations == null || res.Aggregations.Count == 0)
                return result;

            foreach (var key in res.Aggregations.Keys) {
                var bucket = res.Aggregations[key] as Bucket;

                if (bucket == null)
                    continue;

                result.Add(bucket.ToAggregationResult(key));
            }

            return result;
        }
        
        public static Task<IBulkResponse> IndexManyAsync<T>(this IElasticClient client, IEnumerable<T> objects, Func<T, string> getParent, Func<T, string> getIndex = null, string type = null) where T : class {
            if (objects == null)
                throw new ArgumentNullException(nameof(objects));

            if (getParent == null && getIndex == null)
                return client.IndexManyAsync(objects, null, type);

            var indexBulkRequest = CreateIndexBulkRequest(objects, getIndex, type, getParent);
            return client.BulkAsync(indexBulkRequest);
        }

        private static BulkRequest CreateIndexBulkRequest<T>(IEnumerable<T> objects, Func<T, string> getIndex, string type, Func<T, string> getParent) where T : class {
            var bulkRequest = new BulkRequest();
            TypeNameMarker typeNameMarker = type;
            bulkRequest.Type = typeNameMarker;
            var list = objects.Select(o => {
                var doc = new BulkIndexOperation<T>(o);
                if (getParent != null)
                    doc.Parent = getParent(o);

                if (getIndex != null)
                    doc.Index = getIndex(o);

                var versionedDoc = o as IVersioned;
                if (versionedDoc != null)
                    doc.Version = versionedDoc.Version.ToString();

                return doc;
            }).Cast<IBulkOperation>().ToList();
            bulkRequest.Operations = list;

            return bulkRequest;
        }

        public static PropertiesDescriptor<T> SetupDefaults<T>(this PropertiesDescriptor<T> pd) where T : class {
            var hasIdentity = typeof(IIdentity).IsAssignableFrom(typeof(T));
            var hasDates = typeof(IHaveDates).IsAssignableFrom(typeof(T));
            var hasCreatedDate = typeof(IHaveCreatedDate).IsAssignableFrom(typeof(T));
            var supportsSoftDeletes = typeof(ISupportSoftDeletes).IsAssignableFrom(typeof(T));

            if (hasIdentity)
                pd.String(p => p.Name(d => (d as IIdentity).Id).IndexName("id").Index(FieldIndexOption.NotAnalyzed));

            if (supportsSoftDeletes)
                pd.Boolean(p => p.Name(d => (d as ISupportSoftDeletes).IsDeleted).IndexName(SoftDeletesQueryBuilder.Fields.Deleted));

            if (hasCreatedDate)
                pd.Date(p => p.Name(d => (d as IHaveCreatedDate).CreatedUtc).IndexName("created"));

            if (hasDates)
                pd.Date(p => p.Name(d => (d as IHaveDates).UpdatedUtc).IndexName("updated"));

            return pd;
        }
    }
}