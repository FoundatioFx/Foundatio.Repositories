using System.Collections.Generic;
using Foundatio.Repositories.Utility;
using Newtonsoft.Json;

namespace Foundatio.Repositories.Models;

[JsonConverter(typeof(AggregationsJsonConverter))]
[System.Text.Json.Serialization.JsonConverter(typeof(AggregationsSystemTextJsonConverter))]
public interface IAggregate {
    IReadOnlyDictionary<string, object> Data { get; set; }
}