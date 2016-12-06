using System.Collections.Generic;

namespace Foundatio.Repositories.Models {
    public class MetricAggregateBase : IAggregate {
        public IReadOnlyDictionary<string, object> Data { get; set; }
    }
}
