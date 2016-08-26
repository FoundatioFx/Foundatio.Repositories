using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Repositories {
    public class ElasticFindResults<T> : FindResults<T>, IElasticFindResults<T> where T : class {
        public ElasticFindResults(IEnumerable<IFindHit<T>> hits = null, long total = 0, IEnumerable<AggregationResult> aggregationResults = null, string scrollId = null, Func<IFindResults<T>, Task<IFindResults<T>>> getNextPage = null)
            : base(hits, total, aggregationResults, getNextPage) {
            ScrollId = scrollId;
        }

        public string ScrollId { get; protected set; }

        public override async Task<bool> NextPageAsync() {
            if (!HasMore) {
                Aggregations = EmptyAggregations;
                Hits = EmptyFindHits;
                Documents = EmptyDocuments;
                ScrollId = null;

                return false;
            }

            if (((IGetNextPage<T>)this).GetNextPageFunc == null) {
                Page = -1;
                Aggregations = EmptyAggregations;
                Hits = EmptyFindHits;
                Documents = EmptyDocuments;
                ScrollId = null;

                return false;
            }

            var results = await ((IGetNextPage<T>)this).GetNextPageFunc(this).AnyContext() as IElasticFindResults<T>;
            if (results == null || results.Hits.Count == 0) {
                Aggregations = EmptyAggregations;
                Hits = EmptyFindHits;
                Documents = EmptyDocuments;
                HasMore = false;
                ScrollId = null;

                return false;
            }

            Aggregations = results.Aggregations;
            Documents = results.Documents;
            Hits = results.Hits;
            Page = results.Page;
            Total = results.Total;
            HasMore = results.HasMore;
            ScrollId = results.ScrollId;

            return true;
        }
    }

    public class ElasticFindHit<T> : FindResult<T>, IElasticFindHit<T> {
        public string Index { get; set; }
        public string Type { get; set; }
    }
}
