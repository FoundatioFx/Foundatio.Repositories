using System.Diagnostics;

namespace Foundatio.Repositories.Models {
    [DebuggerDisplay("Count: {Count} Min: {Min} Max: {Max} Average: {Average} Sum: {Sum}")]
    public class StatsAggregate : MetricAggregateBase {
        public long Count { get; set; }
        public double? Min { get; set; }
        public double? Max { get; set; }
        public double? Average { get; set; }
        public double? Sum { get; set; }
    }
}
