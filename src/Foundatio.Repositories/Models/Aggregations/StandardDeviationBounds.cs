using System.Diagnostics;

namespace Foundatio.Repositories.Models {
    [DebuggerDisplay("Lower: {Lower} Upper: {Upper}")]

    public class StandardDeviationBounds {
        public double? Upper { get; set; }
        public double? Lower { get; set; }
    }
}
