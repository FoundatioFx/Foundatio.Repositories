using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

public interface IIndex : IDisposable
{
    string Name { get; }
    bool HasMultipleIndexes { get; }

    /// <summary>
    /// Whether the model's <c>id</c> field is guaranteed to be mapped and sortable in the
    /// underlying Elasticsearch index(es), so query builders can safely add it as a default
    /// sort or search_after tiebreaker.
    /// </summary>
    /// <remarks>
    /// Defaults to <c>true</c> for indexes whose mapping is created and owned by this library.
    /// Override to <c>false</c> for indexes whose mapping is managed externally (e.g. by
    /// Logstash, ILM, or another system), where the code-declared <c>id</c> mapping cannot be
    /// trusted to reflect the real server-side mapping: <see cref="ElasticMappingResolver"/>
    /// always merges the code mapping into its resolved field list, so a missing server-side
    /// <c>id</c> field cannot be reliably detected at query time. When this is <c>false</c>,
    /// query builders skip the automatic <c>id</c> tiebreaker; supply an explicit, verified sort
    /// field of your own for deterministic paging.
    /// </remarks>
    bool HasSortableIdField { get; }

    IElasticQueryBuilder QueryBuilder { get; }
    ElasticMappingResolver MappingResolver { get; }
    ElasticQueryParser QueryParser { get; }
    IElasticConfiguration Configuration { get; }
    IDictionary<string, ICustomFieldType> CustomFieldTypes { get; }

    void ConfigureSettings(ElasticsearchClientSettings settings);
    Task ConfigureAsync();
    Task EnsureIndexAsync(object? target);
    Task MaintainAsync(bool includeOptionalTasks = true);
    Task DeleteAsync();
    Task ReindexAsync(Func<int, string?, Task>? progressCallbackAsync = null);
    string CreateDocumentId(object document);
    string[] GetIndexesByQuery(IRepositoryQuery query);
    string GetIndex(object target);
}

public interface IIndex<T> : IIndex where T : class
{
    void ConfigureIndexMapping(TypeMappingDescriptor<T> map);
    Inferrer Infer { get; }
    string InferField(Expression<Func<T, object?>> objectPath);
    string InferPropertyName(Expression<Func<T, object?>> objectPath);
}
