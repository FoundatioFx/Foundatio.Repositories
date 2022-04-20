using System.Collections.Generic;

namespace Foundatio.Repositories.Models;

public abstract class BucketAggregateBase : AggregationsHelper, IAggregate {
    protected BucketAggregateBase() { }
    protected BucketAggregateBase(IReadOnlyDictionary<string, IAggregate> aggregations) : base(aggregations) { }

    public IReadOnlyDictionary<string, object> Data { get; set; }
}
