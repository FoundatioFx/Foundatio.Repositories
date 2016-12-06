using System.Collections.Generic;

namespace Foundatio.Repositories.Models {
    public class KeyedBucket<T> : BucketBase {
        public KeyedBucket() { }
        public KeyedBucket(IDictionary<string, IAggregate> aggregations) : base(aggregations) { }

        public T Key { get; set; }
        public string KeyAsString { get; set; }
        public long? DocCount { get; set; }
    }
}
