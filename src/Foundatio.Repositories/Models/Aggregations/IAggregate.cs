using System.Collections.Generic;
using Foundatio.Repositories.Utility;
using Newtonsoft.Json;

namespace Foundatio.Repositories.Models {
    [JsonConverter(typeof(AggregationsJsonConverter))]
    public interface IAggregate {
        IReadOnlyDictionary<string, object> Data { get; set; }
    }
}
