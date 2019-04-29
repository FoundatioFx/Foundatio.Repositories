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
        Type Type { get; }
        ElasticQueryParser QueryParser { get; }
        Consistency DefaultConsistency { get; }
        int BulkBatchSize { get; set; }
        IElasticQueryBuilder QueryBuilder { get; }
        string GetFieldName(Field field);
        string GetPropertyName(PropertyName property);
        IElasticConfiguration Configuration { get; }
        Task ConfigureAsync();
        Task DeleteAsync();
        Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null);
        IPromise<IAliases> ConfigureIndexAliases(AliasesDescriptor aliases);
        void ConfigureSettings(ConnectionSettings settings);
    }

    public interface IIndex<T>: IIndex where T : class {
        /// <summary>
        /// Creates a new document id. If a date can be resolved, it will be taken into account when creating a new id.
        /// </summary>
        string CreateDocumentId(T document);
        /// <summary>
        /// Used for sorting
        /// </summary>
        string GetFieldName(Expression<Func<T, object>> objectPath);
        /// <summary>
        /// Used for everything not sorting
        /// </summary>
        string GetPropertyName(Expression<Func<T, object>> objectPath);
        ITypeMapping ConfigureIndexMapping(TypeMappingDescriptor<T> mappings);
    }

    public interface IHavePipelinedIndexType {
        string Pipeline { get; }
    }

    public interface IMaintainableIndex {
        Task MaintainAsync(bool includeOptionalTasks = true);
    }

    public interface IIndexNameStrategy {
        bool HasMultipleIndexes { get; }
        IIndex Index { get; }
        string[] GetIndexesByQuery(IRepositoryQuery query);
        string GetIndex(object doc);
    }
}
