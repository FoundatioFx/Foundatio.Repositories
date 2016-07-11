using System;
using System.Collections.Generic;
using Foundatio.Extensions;

namespace Foundatio.Repositories.Models {
    public class AggregationResult {
        public AggregationResult() {
            Terms = new AggregationDictionary<AggregationResult>();
        }

        public string Field { get; set; }
        public AggregationDictionary<AggregationResult> Terms { get; set; }
    }

    public class AggregationDictionary<T> : Dictionary<string, AggregationResult<T>> where T : class {
        public AggregationDictionary() : base() { }

        public AggregationDictionary(IDictionary<string, AggregationResult<T>> items) {
            this.AddRange(items);
        }
    }

    public class AggregationResult<T> where T : class {
        public long Total { get; set; }
        public List<T> Aggregations { get; set; }
    }
}
