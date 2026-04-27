using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace Foundatio.Repositories.Models;

[DebuggerDisplay("Percentile: {Percentile} Value: {Value}")]
public class PercentileItem
{
    public double Percentile { get; set; }
    public double? Value { get; set; }
}

public class PercentilesAggregate : MetricAggregateBase
{
    public PercentilesAggregate() { }

    public PercentilesAggregate(IEnumerable<PercentileItem>? items)
    {
        if (items is not null)
            Items = new List<PercentileItem>(items).AsReadOnly();
    }

    [DisallowNull]
    public IReadOnlyCollection<PercentileItem> Items { get => field; internal set => field = value ?? []; } = [];
}
