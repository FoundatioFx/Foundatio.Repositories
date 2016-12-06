using System.Collections.Generic;

namespace Foundatio.Repositories.Models {
    public class MultiBucketAggregate<TBucket> : BucketAggregateBase
        where TBucket : IBucket {
        public MultiBucketAggregate() { }
        public MultiBucketAggregate(IDictionary<string, IAggregate> aggregations) : base(aggregations) { }

        public IReadOnlyCollection<TBucket> Buckets { get; set; } = EmptyReadOnly<TBucket>.Collection;
    }
}
