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

        public string ScrollId { get; set; }

        public override async Task<bool> NextPageAsync() {
            Aggregations = new List<AggregationResult>();
            Documents = new List<T>();

            if (((IGetNextPage<T>)this).GetNextPageFunc == null) {
                Page = -1;
                return false;
            }

            var results = await ((IGetNextPage<T>)this).GetNextPageFunc(this).AnyContext() as IElasticFindResults<T>;
            Aggregations = results.Aggregations;
            Hits = results.Hits;
            Documents = results.Documents;
            Page = results.Page;
            Total = results.Total;
            ScrollId = results.ScrollId;

            return Documents.Count > 0;
        }
    }

    public class ElasticFindHit<T> : FindResult<T>, IElasticFindHit<T> {
        public string Index { get; set; }
        public string Type { get; set; }
    }
}
