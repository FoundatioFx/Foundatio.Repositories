namespace Foundatio.Repositories.Models {
    public class ExtendedStatsAggregate : StatsAggregate {
        public double? SumOfSquares { get; set; }
        public double? Variance { get; set; }
        public double? StdDeviation { get; set; }
        public StandardDeviationBounds StdDeviationBounds { get; set; }
    }
}
