using System.Diagnostics;

namespace Foundatio.Repositories.Models {
    [DebuggerDisplay("Value: {Value}")]
    public class ValueAggregate : MetricAggregateBase {
        public double? Value { get; set; }
    }

    [DebuggerDisplay("Value: {Value}")]
    public class ValueAggregate<T> : MetricAggregateBase {
        public T Value { get; set; }
    }
}
