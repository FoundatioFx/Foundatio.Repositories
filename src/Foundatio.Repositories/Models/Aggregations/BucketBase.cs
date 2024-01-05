using System.Collections.Generic;

namespace Foundatio.Repositories.Models;

public abstract class BucketBase : AggregationsHelper, IBucket
{
    protected BucketBase() { }
    protected BucketBase(IReadOnlyDictionary<string, IAggregate> aggregations) : base(aggregations) { }

    public IReadOnlyDictionary<string, object> Data { get; set; }
}
