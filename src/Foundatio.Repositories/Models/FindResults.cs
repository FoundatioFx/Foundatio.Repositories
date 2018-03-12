using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;
using Newtonsoft.Json;

namespace Foundatio.Repositories.Models {
    public class FindResults<T> : CountResult, IGetNextPage<T> where T : class {
        public FindResults(IEnumerable<FindHit<T>> hits = null, long total = 0, IDictionary<string, IAggregate> aggregations = null, Func<FindResults<T>, Task<FindResults<T>>> getNextPage = null, DataDictionary data = null)
            : base(total, aggregations, data) {
            ((IGetNextPage<T>)this).GetNextPageFunc = getNextPage;
            if (hits != null) {
                Hits = new List<FindHit<T>>(hits).AsReadOnly();
                Documents = Hits.Where(r => r.Document != null).Select(r => r.Document).ToList().AsReadOnly();
            }
        }

        [IgnoreDataMember]
        public IReadOnlyCollection<T> Documents { get; protected set; } = EmptyReadOnly<T>.Collection;
        public IReadOnlyCollection<FindHit<T>> Hits { get; protected set; } = EmptyReadOnly<FindHit<T>>.Collection;
        public int Page { get; set; } = 1;
        public bool HasMore { get; set; }
        Func<FindResults<T>, Task<FindResults<T>>> IGetNextPage<T>.GetNextPageFunc { get; set; }

        public virtual async Task<bool> NextPageAsync() {
            if (!HasMore) {
                Aggregations = EmptyReadOnly<string, IAggregate>.Dictionary;
                Hits = EmptyReadOnly<FindHit<T>>.Collection;
                Documents = EmptyReadOnly<T>.Collection;
                Data = EmptyReadOnly<string, object>.Dictionary;

                return false;
            }

            if (((IGetNextPage<T>)this).GetNextPageFunc == null) {
                Page = -1;
                Aggregations = EmptyReadOnly<string, IAggregate>.Dictionary;
                Hits = EmptyReadOnly<FindHit<T>>.Collection;
                Documents = EmptyReadOnly<T>.Collection;
                Data = EmptyReadOnly<string, object>.Dictionary;

                return false;
            }

            var results = await ((IGetNextPage<T>)this).GetNextPageFunc(this).AnyContext();
            if (results == null || results.Hits.Count == 0) {
                Aggregations = EmptyReadOnly<string, IAggregate>.Dictionary;
                Hits = EmptyReadOnly<FindHit<T>>.Collection;
                Documents = EmptyReadOnly<T>.Collection;
                HasMore = false;
                Data = EmptyReadOnly<string, object>.Dictionary;

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

    public class CountResult : IHaveData {
        public static readonly CountResult Empty = new CountResult();
        private AggregationsHelper _agg;

        public CountResult(long total = 0, IDictionary<string, IAggregate> aggregations = null, IDictionary<string, object> data = null) {
            Aggregations = aggregations == null ? EmptyReadOnly<string, IAggregate>.Dictionary : new Dictionary<string, IAggregate>(aggregations);
            Total = total;
            Data = data == null ? EmptyReadOnly<string, object>.Dictionary : new ReadOnlyDictionary<string, object>(data);
        }

        public long Total { get; protected set; }
        public IReadOnlyDictionary<string, IAggregate> Aggregations { get; protected set; }
        public IReadOnlyDictionary<string, object> Data { get; protected set; }

        [JsonIgnore]
        public AggregationsHelper Aggs => _agg ?? (_agg = new AggregationsHelper(Aggregations));

        public static implicit operator long(CountResult result) {
            return result.Total;
        }

        public static implicit operator int(CountResult result) {
            return (int)result.Total;
        }
    }

    public class FindHit<T> : IHaveData {
        public static readonly FindHit<T> Empty = new FindHit<T>(null, default(T), 0);

        public FindHit(string id, T document, double score, long? version = null, string routing = null, IDictionary<string, object> data = null) {
            Id = id;
            Document = document;
            Score = score;
            Version = version;
            Data = data != null ? new ReadOnlyDictionary<string, object>(data) : EmptyReadOnly<string, object>.Dictionary;
        }

        public T Document { get; }
        public double Score { get; }
        public long? Version { get; }
        public string Id { get; }
        public string Routing { get; }
        public IReadOnlyDictionary<string, object> Data { get; }
    }
}

