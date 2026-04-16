using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Foundatio.Repositories.Models;

public class MultiBucketAggregate<TBucket> : BucketAggregateBase
    where TBucket : IBucket
{
    public MultiBucketAggregate() { }

    public MultiBucketAggregate(IReadOnlyDictionary<string, IAggregate>? aggregations) : base(aggregations) { }

    [DisallowNull]
    public IReadOnlyCollection<TBucket> Buckets { get => field; set => field = value ?? EmptyReadOnly<TBucket>.Collection; } = EmptyReadOnly<TBucket>.Collection;
}
