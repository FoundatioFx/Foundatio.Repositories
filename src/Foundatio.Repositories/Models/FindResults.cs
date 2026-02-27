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

/// <summary>
/// Contains the results of a find operation, including documents, pagination, and aggregations.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
[DebuggerDisplay("Total: {Total} Documents: {Documents.Count} Hits: {Hits.Count} Aggs: {Aggregations.Count} Page: {Page} HasMore: {HasMore}")]
public class FindResults<T> : CountResult, IFindResults<T> where T : class
{
    /// <summary>
    /// An empty result set.
    /// </summary>
    public static new readonly FindResults<T> Empty = new();

    /// <summary>
    /// Initializes a new empty instance of the <see cref="FindResults{T}"/> class.
    /// </summary>
    [JsonConstructor]
    public FindResults()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="FindResults{T}"/> class.
    /// </summary>
    /// <param name="hits">The search hits.</param>
    /// <param name="total">The total number of matching documents.</param>
    /// <param name="aggregations">The aggregation results.</param>
    /// <param name="getNextPageFunc">A function to retrieve the next page of results.</param>
    /// <param name="data">Additional metadata.</param>
    [Newtonsoft.Json.JsonConstructor]
    public FindResults(IEnumerable<FindHit<T>> hits = null, long total = 0, IReadOnlyDictionary<string, IAggregate> aggregations = null, Func<FindResults<T>, Task<FindResults<T>>> getNextPageFunc = null, IDictionary<string, object> data = null)
        : base(total, aggregations, data)
    {
        ((IFindResults<T>)this).GetNextPageFunc = getNextPageFunc;
        if (hits is not null)
            Hits = new List<FindHit<T>>(hits).AsReadOnly();
    }

    /// <summary>
    /// Gets the documents from the search hits (convenience property).
    /// </summary>
    [IgnoreDataMember]
    [JsonIgnore]
    public IReadOnlyCollection<T> Documents { get; private set; } = EmptyReadOnly<T>.Collection;

    /// <summary>
    /// Gets the search hits, which include documents along with metadata like score and version.
    /// </summary>
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

    /// <summary>
    /// Gets the current page number (1-based).
    /// </summary>
    [JsonInclude]
    [Newtonsoft.Json.JsonProperty]
    public int Page { get; protected set; } = 1;

    /// <summary>
    /// Gets whether there are more pages of results available.
    /// </summary>
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

    /// <summary>
    /// Advances to the next page of results.
    /// </summary>
    /// <returns><c>true</c> if there was a next page; <c>false</c> if there are no more results.</returns>
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

/// <summary>
/// Provides pagination support for find results.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
public interface IFindResults<T> where T : class
{
    /// <summary>
    /// Gets or sets the current page number (1-based).
    /// </summary>
    int Page { get; set; }

    /// <summary>
    /// Gets or sets whether there are more pages of results available.
    /// </summary>
    bool HasMore { get; set; }

    /// <summary>
    /// Gets or sets the function used to retrieve the next page of results.
    /// </summary>
    Func<FindResults<T>, Task<FindResults<T>>> GetNextPageFunc { get; set; }

    /// <summary>
    /// Reverses the order of the results.
    /// </summary>
    void Reverse();
}

/// <summary>
/// Contains the result of a count operation, including the total count and optional aggregations.
/// </summary>
public class CountResult : IHaveData
{
    /// <summary>
    /// An empty count result.
    /// </summary>
    public static readonly CountResult Empty = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="CountResult"/> class.
    /// </summary>
    /// <param name="total">The total document count.</param>
    /// <param name="aggregations">The aggregation results.</param>
    /// <param name="data">Additional metadata.</param>
    [JsonConstructor]
    [Newtonsoft.Json.JsonConstructor]
    public CountResult(long total = 0, IReadOnlyDictionary<string, IAggregate> aggregations = null, IDictionary<string, object> data = null)
    {
        Aggregations = aggregations ?? EmptyReadOnly<string, IAggregate>.Dictionary;
        Total = total;
        Data = data ?? new Dictionary<string, object>();
    }

    /// <summary>
    /// Gets the total number of documents matching the query.
    /// </summary>
    [JsonInclude]
    public long Total { get; protected set; }

    /// <summary>
    /// Gets the aggregation results.
    /// </summary>
    [JsonInclude]
    public IReadOnlyDictionary<string, IAggregate> Aggregations { get; protected set; }

    /// <summary>
    /// Gets additional metadata associated with the result.
    /// </summary>
    [JsonInclude]
    public IDictionary<string, object> Data { get; protected set; }

    /// <summary>
    /// Gets a helper for accessing typed aggregation results.
    /// </summary>
    [IgnoreDataMember]
    [JsonIgnore]
    public AggregationsHelper Aggs => field ??= new AggregationsHelper(Aggregations);

    /// <summary>
    /// Implicitly converts a <see cref="CountResult"/> to a <see cref="long"/>.
    /// </summary>
    public static implicit operator long(CountResult result)
    {
        return result?.Total ?? 0;
    }

    /// <summary>
    /// Implicitly converts a <see cref="CountResult"/> to an <see cref="int"/>.
    /// </summary>
    public static implicit operator int(CountResult result)
    {
        return (int)(result?.Total ?? 0);
    }

    /// <inheritdoc/>
    public override string ToString()
    {
        return Total.ToString();
    }
}

/// <summary>
/// Represents a single search hit, containing the document and associated metadata.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
public class FindHit<T> : IHaveData
{
    /// <summary>
    /// An empty search hit.
    /// </summary>
    public static readonly FindHit<T> Empty = new(null, default, 0);

    /// <summary>
    /// Initializes a new instance of the <see cref="FindHit{T}"/> class.
    /// </summary>
    /// <param name="id">The document identifier.</param>
    /// <param name="document">The document.</param>
    /// <param name="score">The relevance score.</param>
    /// <param name="version">The document version.</param>
    /// <param name="routing">The routing value.</param>
    /// <param name="data">Additional metadata.</param>
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

    /// <summary>
    /// Gets the document.
    /// </summary>
    public T Document { get; }

    /// <summary>
    /// Gets the relevance score for this hit.
    /// </summary>
    public double Score { get; }

    /// <summary>
    /// Gets the document version.
    /// </summary>
    public string Version { get; }

    /// <summary>
    /// Gets the document identifier.
    /// </summary>
    public string Id { get; }

    /// <summary>
    /// Gets the routing value used for this document.
    /// </summary>
    public string Routing { get; }

    /// <summary>
    /// Gets additional metadata associated with this hit.
    /// </summary>
    public IDictionary<string, object> Data { get; }
}

