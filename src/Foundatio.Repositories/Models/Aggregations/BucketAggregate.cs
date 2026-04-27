using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Foundatio.Repositories.Models;

public class BucketAggregate : IAggregate
{
    [DisallowNull]
    public IReadOnlyCollection<IBucket> Items { get => field; set => field = value ?? EmptyReadOnly<IBucket>.Collection; } = EmptyReadOnly<IBucket>.Collection;
    public IReadOnlyDictionary<string, object>? Data { get; set; } = EmptyReadOnly<string, object>.Dictionary;
    public long Total { get; set; }
}
