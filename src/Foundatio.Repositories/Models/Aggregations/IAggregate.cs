using System.Collections.Generic;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Models;

[Newtonsoft.Json.JsonConverter(typeof(AggregationsNewtonsoftJsonConverter))]
[System.Text.Json.Serialization.JsonConverter(typeof(AggregationsSystemTextJsonConverter))]
public interface IAggregate
{
    IReadOnlyDictionary<string, object> Data { get; set; }
}
