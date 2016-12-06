using System.Collections.Generic;

namespace Foundatio.Repositories.Models {
    public class SingleBucketAggregate : BucketAggregateBase {
        public SingleBucketAggregate() { }
        public SingleBucketAggregate(IDictionary<string, IAggregate> aggregations) : base(aggregations) { }

        public long DocCount { get; set; }
    }
}
