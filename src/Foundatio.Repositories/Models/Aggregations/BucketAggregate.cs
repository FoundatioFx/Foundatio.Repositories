using System.Collections.Generic;

namespace Foundatio.Repositories.Models {
    public class BucketAggregate : IAggregate {
        public IReadOnlyCollection<IBucket> Items { get; set; } = EmptyReadOnly<IBucket>.Collection;
        public long? DocCountErrorUpperBound { get; set; }
        public long? SumOtherDocCount { get; set; }
        public IReadOnlyDictionary<string, object> Data { get; set; } = EmptyReadOnly<string, object>.Dictionary;
        public long DocCount { get; set; }
    }
}
