using System.Collections.Generic;
using Foundatio.Repositories.Utility;
using Newtonsoft.Json;

namespace Foundatio.Repositories.Models {
    [JsonConverter(typeof(BucketsJsonConverter))]
    public interface IBucket {
        IReadOnlyDictionary<string, object> Data { get; set; }
    }
}
