using System.Collections.Generic;
using System.Diagnostics;

namespace Foundatio.Repositories.Models {
    [DebuggerDisplay("Total: {Total}")]
    public class SingleBucketAggregate : BucketAggregateBase {
        public SingleBucketAggregate() { }
        public SingleBucketAggregate(IDictionary<string, IAggregate> aggregations) : base(aggregations) { }

        public long Total { get; set; }
    }
}
