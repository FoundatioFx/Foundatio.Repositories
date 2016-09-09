using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Models {
    public class FindResults<T> : CountResult, IGetNextPage<T>, IFindResults<T> where T : class {
        protected static readonly IReadOnlyCollection<IFindHit<T>> EmptyFindHits = new List<IFindHit<T>>(0).AsReadOnly();
        protected static readonly IReadOnlyCollection<T> EmptyDocuments = new List<T>(0).AsReadOnly();

        public FindResults(IEnumerable<IFindHit<T>> hits = null, long total = 0, IEnumerable<AggregationResult> aggregationResults = null, Func<IFindResults<T>, Task<IFindResults<T>>> getNextPage = null) {
            Hits = new List<IFindHit<T>>(hits ?? new IFindHit<T>[] {});
            Documents = hits?.Select(r => r.Document).ToList() ?? new List<T>();
            Aggregations = new List<AggregationResult>(aggregationResults ?? new AggregationResult[] { });
            ((IGetNextPage<T>)this).GetNextPageFunc = getNextPage;
            Total = total;
        }

        public IReadOnlyCollection<T> Documents { get; protected set; }
        public IReadOnlyCollection<IFindHit<T>> Hits { get; protected set; }
        public int Page { get; set; } = 1;
        public bool HasMore { get; set; }
        Func<IFindResults<T>, Task<IFindResults<T>>> IGetNextPage<T>.GetNextPageFunc { get; set; }

        public virtual async Task<bool> NextPageAsync() {
            if (!HasMore) {
                Aggregations = EmptyAggregations;
                Hits = EmptyFindHits;
                Documents = EmptyDocuments;

                return false;
            }

            if (((IGetNextPage<T>)this).GetNextPageFunc == null) {
                Page = -1;
                Aggregations = EmptyAggregations;
                Hits = EmptyFindHits;
                Documents = EmptyDocuments;

                return false;
            }

            var results = await ((IGetNextPage<T>)this).GetNextPageFunc(this).AnyContext();
            if (results == null || results.Hits.Count == 0) {
                Aggregations = EmptyAggregations;
                Hits = EmptyFindHits;
                Documents = EmptyDocuments;
                HasMore = false;

                return false;
            }

            Aggregations = results.Aggregations;
            Documents = results.Documents;
            Hits = results.Hits;
            Page = results.Page;
            Total = results.Total;
            HasMore = results.HasMore;

            return true;
        }
    }

    public interface IGetNextPage<T> where T : class {
        Func<IFindResults<T>, Task<IFindResults<T>>> GetNextPageFunc { get; set; }
    }

    public class CountResult {
        protected static readonly IReadOnlyCollection<AggregationResult> EmptyAggregations = new List<AggregationResult>(0).AsReadOnly();

        public CountResult(long total = 0, IEnumerable<AggregationResult> aggregations = null) {
            Aggregations = aggregations == null ? EmptyAggregations : new List<AggregationResult>(aggregations);
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

