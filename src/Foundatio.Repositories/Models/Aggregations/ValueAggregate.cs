namespace Foundatio.Repositories.Models {
    public class ValueAggregate : MetricAggregateBase {
        public double? Value { get; set; }
    }

    public class ValueAggregate<T> : MetricAggregateBase {
        public T Value { get; set; }
    }
}
