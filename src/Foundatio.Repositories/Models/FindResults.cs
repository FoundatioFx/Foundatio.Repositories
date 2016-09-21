using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;

namespace Foundatio.Repositories.Models {
    public class FindResults<T> : CountResult, IGetNextPage<T> where T : class {
        protected static readonly IReadOnlyCollection<FindHit<T>> EmptyFindHits = new List<FindHit<T>>(0).AsReadOnly();
        protected static readonly IReadOnlyCollection<T> EmptyDocuments = new List<T>(0).AsReadOnly();

        public FindResults(IEnumerable<FindHit<T>> hits = null, long total = 0, IEnumerable<AggregationResult> aggregations = null, Func<FindResults<T>, Task<FindResults<T>>> getNextPage = null, DataDictionary data = null)
            : base(total, aggregations, data) {
            ((IGetNextPage<T>)this).GetNextPageFunc = getNextPage;
            if (hits != null) {
                Hits = new List<FindHit<T>>(hits).AsReadOnly();
                Documents = Hits.Select(r => r.Document).ToList().AsReadOnly();
            }
        }

        [IgnoreDataMember]
        public IReadOnlyCollection<T> Documents { get; protected set; } = EmptyDocuments;
        public IReadOnlyCollection<FindHit<T>> Hits { get; protected set; } = EmptyFindHits;
        public int Page { get; set; } = 1;
        public bool HasMore { get; set; }
        Func<FindResults<T>, Task<FindResults<T>>> IGetNextPage<T>.GetNextPageFunc { get; set; }

        public virtual async Task<bool> NextPageAsync() {
            if (!HasMore) {
                Aggregations = EmptyAggregations;
                Hits = EmptyFindHits;
                Documents = EmptyDocuments;
                Data = EmptyData;

                return false;
            }

            if (((IGetNextPage<T>)this).GetNextPageFunc == null) {
                Page = -1;
                Aggregations = EmptyAggregations;
                Hits = EmptyFindHits;
                Documents = EmptyDocuments;
                Data = EmptyData;

                return false;
            }

            var results = await ((IGetNextPage<T>)this).GetNextPageFunc(this).AnyContext();
            if (results == null || results.Hits.Count == 0) {
                Aggregations = EmptyAggregations;
                Hits = EmptyFindHits;
                Documents = EmptyDocuments;
                HasMore = false;
                Data = EmptyData;

                return false;
            }

            Aggregations = results.Aggregations;
            Documents = results.Documents;
            Hits = results.Hits;
            Page = results.Page;
            Total = results.Total;
            HasMore = results.HasMore;
            Data = results.Data;

            return true;
        }
    }

    public interface IGetNextPage<T> where T : class {
        Func<FindResults<T>, Task<FindResults<T>>> GetNextPageFunc { get; set; }
    }

    public class CountResult {
        protected static readonly IReadOnlyCollection<AggregationResult> EmptyAggregations = new List<AggregationResult>(0).AsReadOnly();
        internal static readonly IReadOnlyDictionary<string, object> EmptyData = new ReadOnlyDictionary<string, object>(new Dictionary<string, object>());

        public CountResult(long total = 0, IEnumerable<AggregationResult> aggregations = null, IDictionary<string, object> data = null) {
            Aggregations = aggregations == null ? EmptyAggregations : new List<AggregationResult>(aggregations);
            Total = total;
            Data = data != null ? new ReadOnlyDictionary<string, object>(data) : EmptyData;
        }

        public long Total { get; protected set; }
        public IReadOnlyCollection<AggregationResult> Aggregations { get; protected set; }
        public IReadOnlyDictionary<string, object> Data { get; protected set; }

        public static implicit operator long(CountResult result) {
            return result.Total;
        }

        public static implicit operator int(CountResult result) {
            return (int)result.Total;
        }
    }

    public class FindHit<T> {
        public static readonly FindHit<T> Empty = new FindHit<T>(null, default(T), 0);

        public FindHit(string id, T document, double score, long? version = null, IDictionary<string, object> data = null) {
            Id = id;
            Document = document;
            Score = score;
            Version = version;
            Data = data != null ? new ReadOnlyDictionary<string, object>(data) : CountResult.EmptyData;
        }

        public T Document { get; }
        public double Score { get; }
        public long? Version { get; }
        public string Id { get; }
        public IReadOnlyDictionary<string, object> Data { get; }
    }
}

