using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IIndex : IDisposable {
        string Name { get; }
        IElasticConfiguration Configuration { get; }
        IReadOnlyCollection<IIndexType> IndexTypes { get; }
        Task ConfigureAsync();
        Task DeleteAsync();
        Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null);
        void ConfigureSettings(ConnectionSettings settings);
    }

    public interface IMaintainableIndex {
        Task MaintainAsync(bool includeOptionalTasks = true);
    }

    public interface ITimeSeriesIndex : IIndex, IMaintainableIndex {
        Task EnsureIndexAsync(DateTime utcDate);
        string GetIndex(DateTime utcDate);
        string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd);
    }
}
