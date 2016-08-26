using System.Collections.Generic;
using System.Threading.Tasks;

namespace Foundatio.Repositories.Models {
    public interface IFindResults<out T> where T : class {
        long Total { get; }
        IReadOnlyCollection<IFindHit<T>> Hits { get; }
        IReadOnlyCollection<T> Documents { get; }
        IReadOnlyCollection<AggregationResult> Aggregations { get; }
        int Page { get; }
        bool HasMore { get; }
        Task<bool> NextPageAsync();
    }

    public interface IFindHit<out T> {
        T Document { get; }
        double Score { get; }
        long? Version { get; }
        string Id { get; }
    }
}