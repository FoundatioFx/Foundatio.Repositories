using System.Collections.Generic;

namespace Foundatio.Repositories.Models {
    public class BucketAggregate : IAggregate {
        public IReadOnlyCollection<IBucket> Items { get; set; } = EmptyReadOnly<IBucket>.Collection;
        public IReadOnlyDictionary<string, object> Data { get; set; } = EmptyReadOnly<string, object>.Dictionary;
        public long Total { get; set; }
    }
}
