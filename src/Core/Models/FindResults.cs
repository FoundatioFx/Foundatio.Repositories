using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Models {
    public class FindResults<T> : CountResult, IGetNextPage<T>, IFindResults<T> where T : class {
        public FindResults(IEnumerable<IFindHit<T>> hits = null, long total = 0, IEnumerable<AggregationResult> aggregationResults = null, Func<IFindResults<T>, Task<IFindResults<T>>> getNextPage = null) {
            Hits = new List<IFindHit<T>>(hits ?? new IFindHit<T>[] {});
            Documents = hits?.Select(r => r.Document).ToList() ?? new List<T>();
            Aggregations = new List<AggregationResult>(aggregationResults ?? new AggregationResult[] { });
            ((IGetNextPage<T>)this).GetNextPageFunc = getNextPage;
            Total = total;
        }

        public IReadOnlyCollection<T> Documents { get; protected set; }
        public IReadOnlyCollection<IFindHit<T>> Hits { get; }
        public int Page { get; set; } = 1;
        public bool HasMore { get; set; }
        Func<IFindResults<T>, Task<IFindResults<T>>> IGetNextPage<T>.GetNextPageFunc { get; set; }

        public virtual async Task<bool> NextPageAsync() {
            if (!HasMore)
                return false;

            Aggregations = new List<AggregationResult>();
            Documents = new List<T>();
            
            if (((IGetNextPage<T>)this).GetNextPageFunc == null) {
                HasMore = false;
                Page = -1;
                return false;
            }

            var results = await ((IGetNextPage<T>)this).GetNextPageFunc(this).AnyContext();
            Aggregations = results.Aggregations;
            Documents = results.Documents;
            HasMore = results.HasMore;
            Page = results.Page;
            Total = results.Total;
            return Documents.Count > 0;
        }
    }

    public interface IGetNextPage<T> where T : class {
        Func<IFindResults<T>, Task<IFindResults<T>>> GetNextPageFunc { get; set; }
    }

    public class CountResult {
        public CountResult(long total = 0, IEnumerable<AggregationResult> aggregations = null) {
            Aggregations = new List<AggregationResult>(aggregations ?? new AggregationResult[] {});
            Total = total;
        }

        public long Total { get; set; }
        public IReadOnlyCollection<AggregationResult> Aggregations { get; set; }

        public static implicit operator long(CountResult result) {
            return result.Total;
        }

        public static implicit operator int(CountResult result) {
            return (int)result.Total;
        }
    }

    public class FindResult<T> {
        public T Document { get; set; }
        public double Score { get; set; }
        public long? Version { get; set; }
        public string Id { get; set; }
    }
}

