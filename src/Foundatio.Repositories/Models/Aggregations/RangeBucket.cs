using System.Collections.Generic;
using System.Diagnostics;

namespace Foundatio.Repositories.Models {
    [DebuggerDisplay("Key: {Key} From: {FromAsString} To: {ToAsString} Total: {Total}")]
    public class RangeBucket : BucketBase {
        public RangeBucket() { }
        public RangeBucket(IDictionary<string, IAggregate> aggregations) : base(aggregations) { }

        public string Key { get; set; }
        public double? From { get; set; }
        public string FromAsString { get; set; }
        public double? To { get; set; }
        public string ToAsString { get; set; }
        public long Total { get; set; }
    }
}