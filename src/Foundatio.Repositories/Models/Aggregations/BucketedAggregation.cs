using System.Collections.Generic;

namespace Foundatio.Repositories.Models {
    public class BucketedAggregation<T> {
        public IReadOnlyCollection<T> Buckets { get; set; } = EmptyReadOnly<T>.Collection;
    }
}
