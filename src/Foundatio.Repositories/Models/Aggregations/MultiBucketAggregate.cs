using System.Collections.Generic;

namespace Foundatio.Repositories.Models;

public class MultiBucketAggregate<TBucket> : BucketAggregateBase
    where TBucket : IBucket
{
    public MultiBucketAggregate() { }

    [System.Text.Json.Serialization.JsonConstructor]
    public MultiBucketAggregate(IReadOnlyDictionary<string, IAggregate> aggregations) : base(aggregations) { }

    public IReadOnlyCollection<TBucket> Buckets { get; set; } = EmptyReadOnly<TBucket>.Collection;
}
