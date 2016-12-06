using System;
using System.Collections.Generic;

namespace Foundatio.Repositories.Models {
    public class DateHistogramBucket : KeyedBucket<double> {
        public DateHistogramBucket() { }
        public DateHistogramBucket(IDictionary<string, IAggregate> aggregations) : base(aggregations) { }

        public DateTime Date => DateTime.SpecifyKind(new DateTime(1970, 1, 1).AddMilliseconds(0 + Key), DateTimeKind.Unspecified);
    }
}
