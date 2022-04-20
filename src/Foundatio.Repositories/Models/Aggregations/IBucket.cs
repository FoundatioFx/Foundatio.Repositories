using System.Collections.Generic;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Models;

[Newtonsoft.Json.JsonConverter(typeof(BucketsNewtonsoftJsonConverter))]
[System.Text.Json.Serialization.JsonConverter(typeof(BucketsSystemTextJsonConverter))]
public interface IBucket {
    IReadOnlyDictionary<string, object> Data { get; set; }
}