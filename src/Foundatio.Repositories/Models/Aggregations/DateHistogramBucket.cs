using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Foundatio.Repositories.Models {
    [DebuggerDisplay("Date: {Date}: Ticks: {Key}")]
    public class DateHistogramBucket : KeyedBucket<double> {
        public DateHistogramBucket() { }

        public DateHistogramBucket(DateTime date, IDictionary<string, IAggregate> aggregations) : base(aggregations) {
            Date = date;
        }

        public DateTime Date { get; }
    }
}
