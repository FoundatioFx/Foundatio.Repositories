using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IIndex {
        string Name { get; }
        IReadOnlyCollection<IIndexType> IndexTypes { get; }
        void Configure();
        void Delete();
        Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null);
    }

    public interface IMaintainableIndex {
        void Maintain();
    }

    public interface ITimeSeriesIndex : IIndex, IMaintainableIndex {
        void EnsureIndex(DateTime utcDate);
        string GetIndex(DateTime utcDate);
        string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd);
    }
}
