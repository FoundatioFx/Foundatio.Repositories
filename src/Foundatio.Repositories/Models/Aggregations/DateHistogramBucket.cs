using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Foundatio.Repositories.Models;

[DebuggerDisplay("Date: {Date}: Ticks: {Key}")]
public class DateHistogramBucket : KeyedBucket<double>
{
    public DateHistogramBucket()
    {
    }

    [System.Text.Json.Serialization.JsonConstructor]
    public DateHistogramBucket(DateTime date, IReadOnlyDictionary<string, IAggregate> aggregations) : base(aggregations)
    {
        Date = date;
    }

    public DateTime Date { get; }
}
