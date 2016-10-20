using System;
using System.Collections.Generic;

namespace Foundatio.Repositories.Models {
    public class AggregationResult {
        public double? Value { get; set; }
        public long? Total { get; set; }
        public IDictionary<string, object> Data { get; set; }
        public ICollection<BucketResult> Buckets { get; set; }
        public IDictionary<string, AggregationResult> Aggregations { get; set; }
    }

    public class BucketResult {
        public string Key { get; set; }
        public string KeyAsString { get; set; }
        public double? Value { get; set; }
        public long? Total { get; set; }
        public IDictionary<string, object> Data { get; set; }
        public IDictionary<string, AggregationResult> Aggregations { get; set; }
    }
}
