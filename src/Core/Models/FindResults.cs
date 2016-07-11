using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Extensions;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Models {
    public class FindResults<T> : CountResult, IGetNextPage<T> where T : class, new() {
        public FindResults(ICollection<T> documents = null, long total = 0, ICollection<AggregationResult> facetResults = null, string scrollId = null, Func<FindResults<T>, Task<FindResults<T>>> getNextPage = null) {
            Documents = documents ?? new List<T>();
            Aggregations = facetResults ?? new List<AggregationResult>();
            ScrollId = scrollId;
            ((IGetNextPage<T>)this).GetNextPageFunc = getNextPage;
            Total = total;
        }

        public ICollection<T> Documents { get; set; }

        public string ScrollId { get; set; }
        public int Page { get; set; } = 1;
        public bool HasMore { get; set; }
        Func<FindResults<T>, Task<FindResults<T>>> IGetNextPage<T>.GetNextPageFunc { get; set; }

        public async Task<bool> NextPageAsync() {
            Documents.Clear();

            if (((IGetNextPage<T>)this).GetNextPageFunc == null) {
                Page = -1;
                return false;
            }

            var results = await ((IGetNextPage<T>)this).GetNextPageFunc(this).AnyContext();
            Documents.AddRange(results.Documents);

            return Documents.Count > 0;
        }
    }

    public interface IGetNextPage<T> where T : class, new() {
        Func<FindResults<T>, Task<FindResults<T>>> GetNextPageFunc { get; set; }
    }

    public class CountResult {
        public CountResult(long total = 0, ICollection<AggregationResult> aggregations = null) {
            Aggregations = aggregations ?? new List<AggregationResult>();
            Total = total;
        }

        public long Total { get; set; }
        public ICollection<AggregationResult> Aggregations { get; set; }

        public static implicit operator long(CountResult result) {
            return result.Total;
        }

        public static implicit operator int(CountResult result) {
            return (int)result.Total;
        }
    }
}
