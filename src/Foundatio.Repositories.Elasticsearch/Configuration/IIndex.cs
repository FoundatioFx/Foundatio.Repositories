using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IIndex : IDisposable {
        string Name { get; }
        bool HasMultipleIndexes { get; }
        IElasticQueryBuilder QueryBuilder { get; }
        ElasticQueryParser QueryParser { get; }
        IElasticConfiguration Configuration { get; }
        void ConfigureSettings(ConnectionSettings settings);
        Task ConfigureAsync();
        Task EnsureIndexAsync(object target);
        Task MaintainAsync(bool includeOptionalTasks = true);
        Task DeleteAsync();
        Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null);
        string CreateDocumentId(object document);
        string[] GetIndexesByQuery(IRepositoryQuery query);
        string GetIndex(object target);
    }
}
