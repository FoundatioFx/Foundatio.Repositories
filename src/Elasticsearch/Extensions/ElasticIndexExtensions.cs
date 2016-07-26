using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Extensions {
    public static class ElasticIndexExtensions {
        public static FindResults<T> ToFindResults<T>(this ISearchResponse<T> res, bool isVersioned, int? limit = null) where T : class, new() {
            var result = new FindResults<T> {
                Total = res.Total,
                Aggregations = res.ToAggregationResult(),
                ScrollId = res.ScrollId,
                HasMore = limit.HasValue && res.Documents.Count() > limit.Value
            };

            result.Documents.AddRange(res.Hits.Take(limit ?? Int32.MaxValue).Select(h => h.ToDocument(isVersioned)));

            return result;
        }

        public static T ToDocument<T>(this IHit<T> hit, bool isVersioned) where T : class {
            if (!isVersioned)
                return hit.Source;

            var doc = hit.Source;
            var versionedDoc = doc as IVersioned;
            if (versionedDoc != null)
                versionedDoc.Version = hit.Version != null ? Int64.Parse(hit.Version) : versionedDoc.Version;

            return doc;
        }

        public static T ToDocument<T>(this IGetResponse<T> hit, bool isVersioned) where T : class {
            if (!isVersioned)
                return hit.Source;

            var doc = hit.Source;
            var versionedDoc = doc as IVersioned;
            if (versionedDoc != null)
                versionedDoc.Version = hit.Version != null ? Int64.Parse(hit.Version) : versionedDoc.Version;

            return doc;
        }

        public static T ToDocument<T>(this IMultiGetHit<T> hit, bool isVersioned) where T : class {
            if (!isVersioned)
                return hit.Source;

            var doc = hit.Source;
            var versionedDoc = doc as IVersioned;
            if (versionedDoc != null)
                versionedDoc.Version = hit.Version != null ? Int64.Parse(hit.Version) : versionedDoc.Version;

            return doc;
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

        public static ICollection<AggregationResult> ToAggregationResult<T>(this ISearchResponse<T> res) where T : class {
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

        public static IBulkResponse IndexMany<T>(this IElasticClient client, IEnumerable<T> objects, Func<T, string> getParent, Func<T, string> getIndex = null, string type = null) where T : class {
            if (getParent == null)
                return client.IndexMany(objects, null, type);

            var indexBulkRequest = CreateIndexBulkRequest(objects, getIndex, type, getParent);
            return client.Bulk((IBulkRequest)indexBulkRequest);
        }

        public static Task<IBulkResponse> IndexManyAsync<T>(this IElasticClient client, IEnumerable<T> objects, Func<T, string> getParent, Func<T, string> getIndex = null, string type = null) where T : class {
            if (getParent == null && getIndex == null)
                return client.IndexManyAsync(objects, null, type);

            var indexBulkRequest = CreateIndexBulkRequest(objects, getIndex, type, getParent);
            return client.BulkAsync(indexBulkRequest);
        }

        private static BulkRequest CreateIndexBulkRequest<T>(IEnumerable<T> objects, Func<T, string> getIndex, string type, Func<T, string> getParent) where T : class {
            if (objects == null)
                throw new ArgumentNullException(nameof(objects));

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
                if (versionedDoc != null) {
                    doc.Version = versionedDoc.Version.ToString();
                    versionedDoc.Version++;
                }

                return doc;
            }).Cast<IBulkOperation>().ToList();
            bulkRequest.Operations = list;

            return bulkRequest;
        }

        public static ObjectMappingDescriptor<TParent, TChild> RootPath<TParent, TChild>(this ObjectMappingDescriptor<TParent, TChild> t) where TParent : class where TChild : class {
            return t.Path("just_name");
        }
    }
}