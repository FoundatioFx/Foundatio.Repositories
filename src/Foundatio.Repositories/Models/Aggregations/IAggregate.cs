using System.Collections.Generic;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Models;

/// <summary>
/// Represents an aggregation result from a search query.
/// </summary>
/// <remarks>
/// Aggregations provide summarized data about the documents matching a query, such as
/// counts, averages, date histograms, and term frequencies.
/// </remarks>
[Newtonsoft.Json.JsonConverter(typeof(AggregationsNewtonsoftJsonConverter))]
[System.Text.Json.Serialization.JsonConverter(typeof(AggregationsSystemTextJsonConverter))]
public interface IAggregate
{
    /// <summary>
    /// Gets or sets the aggregation data.
    /// </summary>
    IReadOnlyDictionary<string, object> Data { get; set; }
}
