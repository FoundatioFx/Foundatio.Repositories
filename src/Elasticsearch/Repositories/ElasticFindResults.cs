using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Repositories {
    public class ElasticFindResults<T> : FindResults<T>, IElasticFindResults<T> where T : class {
        public ElasticFindResults(IEnumerable<IFindHit<T>> hits = null, long total = 0, IEnumerable<AggregationResult> aggregationResults = null, string scrollId = null, Func<IFindResults<T>, Task<IFindResults<T>>> getNextPage = null)
            : base(hits, total, aggregationResults, getNextPage) {
            ScrollId = scrollId;
        }

        public string ScrollId { get; set; }
    }

    public class ElasticFindHit<T> : FindResult<T>, IElasticFindHit<T> {
        public string Index { get; set; }
        public string Type { get; set; }
    }
}
