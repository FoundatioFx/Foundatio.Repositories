using System.Diagnostics;

namespace Foundatio.Repositories.Models {
    [DebuggerDisplay("Count: {Count} Min: {Min} Max: {Max} Average: {Average} Sum: {Sum} SumOfSquares: {SumOfSquares} Variance: {Variance} StdDeviation: {StdDeviation}")]
    public class ExtendedStatsAggregate : StatsAggregate {
        public double? SumOfSquares { get; set; }
        public double? Variance { get; set; }
        public double? StdDeviation { get; set; }
        public StandardDeviationBounds StdDeviationBounds { get; set; }
    }
}
