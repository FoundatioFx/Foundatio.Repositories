using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;

namespace Foundatio.Repositories.Models;

[DebuggerDisplay("Total: {Total} Documents: {Documents.Count} Hits: {Hits.Count} Aggs: {Aggregations.Count} Page: {Page} HasMore: {HasMore}")]
public class FindResults<T> : CountResult, IFindResults<T> where T : class
{
    public static new readonly FindResults<T> Empty = new();

    [JsonConstructor]
    public FindResults()
    {
    }

    [Newtonsoft.Json.JsonConstructor]
    public FindResults(IEnumerable<FindHit<T>> hits = null, long total = 0, IReadOnlyDictionary<string, IAggregate> aggregations = null, Func<FindResults<T>, Task<FindResults<T>>> getNextPageFunc = null, IDictionary<string, object> data = null)
        : base(total, aggregations, data)
    {
        ((IFindResults<T>)this).GetNextPageFunc = getNextPageFunc;
        if (hits is not null)
            Hits = new List<FindHit<T>>(hits).AsReadOnly();
    }

    [IgnoreDataMember]
    [JsonIgnore]
    public IReadOnlyCollection<T> Documents { get; private set; } = EmptyReadOnly<T>.Collection;

    [JsonInclude]
    public IReadOnlyCollection<FindHit<T>> Hits
    {
        get;
        protected set
        {
            if (value is { Count: > 0 })
            {
                field = value;
                Documents = field.Where(r => r.Document is not null).Select(r => r.Document).ToList().AsReadOnly();
            }
            else
            {
                field = EmptyReadOnly<FindHit<T>>.Collection;
                Documents = EmptyReadOnly<T>.Collection;
            }
        }
    } = EmptyReadOnly<FindHit<T>>.Collection;

    [JsonInclude]
    [Newtonsoft.Json.JsonProperty]
    public int Page { get; protected set; } = 1;
    [JsonInclude]
    [Newtonsoft.Json.JsonProperty]
    public bool HasMore { get; protected set; }

    int IFindResults<T>.Page
    {
        get => Page;
        set { Page = value; }
    }

    bool IFindResults<T>.HasMore
    {
        get => HasMore;
        set { HasMore = value; }
    }

    Func<FindResults<T>, Task<FindResults<T>>> IFindResults<T>.GetNextPageFunc { get; set; }

    void IFindResults<T>.Reverse()
    {
        Hits = Hits.Reverse().ToList().AsReadOnly();
    }

    public virtual async Task<bool> NextPageAsync()
    {
        if (!HasMore)
        {
            Aggregations = EmptyReadOnly<string, IAggregate>.Dictionary;
            Hits = EmptyReadOnly<FindHit<T>>.Collection;
            Data = new Dictionary<string, object>();

            return false;
        }

        if (((IFindResults<T>)this).GetNextPageFunc == null)
        {
            Page = -1;
            Aggregations = EmptyReadOnly<string, IAggregate>.Dictionary;
            Hits = EmptyReadOnly<FindHit<T>>.Collection;
            Data = new Dictionary<string, object>();

            return false;
        }

        var results = await ((IFindResults<T>)this).GetNextPageFunc(this).AnyContext();
        if (results == null || results.Hits.Count == 0)
        {
            Aggregations = EmptyReadOnly<string, IAggregate>.Dictionary;
            Hits = EmptyReadOnly<FindHit<T>>.Collection;
            HasMore = false;
            Data = new Dictionary<string, object>();

            return false;
        }

        Aggregations = results.Aggregations;
        Hits = results.Hits;
        Page = results.Page;
        Total = results.Total;
        HasMore = results.HasMore;
        Data = results.Data;

        return true;
    }
}

public interface IFindResults<T> where T : class
{
    int Page { get; set; }
    bool HasMore { get; set; }
    Func<FindResults<T>, Task<FindResults<T>>> GetNextPageFunc { get; set; }
    void Reverse();
}

public class CountResult : IHaveData
{
    public static readonly CountResult Empty = new();

    [JsonConstructor]
    [Newtonsoft.Json.JsonConstructor]
    public CountResult(long total = 0, IReadOnlyDictionary<string, IAggregate> aggregations = null, IDictionary<string, object> data = null)
    {
        Aggregations = aggregations ?? EmptyReadOnly<string, IAggregate>.Dictionary;
        Total = total;
        Data = data ?? new Dictionary<string, object>();
    }

    [JsonInclude]
    public long Total { get; protected set; }
    [JsonInclude]
    public IReadOnlyDictionary<string, IAggregate> Aggregations { get; protected set; }
    [JsonInclude]
    public IDictionary<string, object> Data { get; protected set; }

    [IgnoreDataMember]
    [JsonIgnore]
    public AggregationsHelper Aggs => field ??= new AggregationsHelper(Aggregations);

    public static implicit operator long(CountResult result)
    {
        return result?.Total ?? 0;
    }

    public static implicit operator int(CountResult result)
    {
        return (int)(result?.Total ?? 0);
    }

    public override string ToString()
    {
        return Total.ToString();
    }
}

public class FindHit<T> : IHaveData
{
    public static readonly FindHit<T> Empty = new(null, default, 0);

    [JsonConstructor]
    [Newtonsoft.Json.JsonConstructor]
    public FindHit(string id, T document, double score, string version = null, string routing = null, IDictionary<string, object> data = null)
    {
        Id = id;
        Document = document;
        Score = score;
        Version = version;
        Routing = routing;
        Data = new DataDictionary(data);
    }

    public T Document { get; }
    public double Score { get; }
    public string Version { get; }
    public string Id { get; }
    public string Routing { get; }
    public IDictionary<string, object> Data { get; }
}

