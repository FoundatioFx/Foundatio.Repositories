using System.Collections.Generic;

namespace Foundatio.Repositories.Models {
    public class AggregationsHelper {
        public IReadOnlyDictionary<string, IAggregate> Aggregations { get; protected internal set; } = EmptyReadOnly<string, IAggregate>.Dictionary;

        public AggregationsHelper() { }

        protected AggregationsHelper(IDictionary<string, IAggregate> aggregations) {
            Aggregations = aggregations != null ?
                new Dictionary<string, IAggregate>(aggregations)
                : EmptyReadOnly<string, IAggregate>.Dictionary;
        }

        public AggregationsHelper(IReadOnlyDictionary<string, IAggregate> aggregations) {
            Aggregations = aggregations ?? EmptyReadOnly<string, IAggregate>.Dictionary;
        }
    }
}
