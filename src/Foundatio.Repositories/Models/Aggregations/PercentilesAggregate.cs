using System.Collections.Generic;
using System.Diagnostics;

namespace Foundatio.Repositories.Models {
    [DebuggerDisplay("Percentile: {Percentile} Value: {Value}")]
    public class PercentileItem {
        public double Percentile { get; set; }
        public double Value { get; set; }
    }

    public class PercentilesAggregate : MetricAggregateBase {
        public PercentilesAggregate() {}

        public PercentilesAggregate(IEnumerable<PercentileItem> items) {
            Items = new List<PercentileItem>(items).AsReadOnly();
        }

        public IReadOnlyCollection<PercentileItem> Items { get; internal set; }
    }
}
