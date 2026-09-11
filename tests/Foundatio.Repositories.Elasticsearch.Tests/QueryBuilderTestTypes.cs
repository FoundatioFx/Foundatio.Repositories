using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;

namespace Foundatio.Repositories.Elasticsearch.Tests;

/// <summary>
/// A minimal <see cref="IIndex"/> test double that exposes a caller-supplied mapping resolver and
/// intentionally relies on the default <see cref="IIndex.HasSortableIdField"/> implementation.
/// Only the members exercised by
/// <see cref="DefaultSortQueryBuilder"/> and <see cref="SearchAfterQueryBuilder"/> are implemented;
/// everything else throws because these tests never touch a real Elasticsearch index.
/// </summary>
internal class FakeIndex : IIndex
{
    public required ElasticMappingResolver MappingResolver { get; init; }

    public string Name => "fake";
    public bool HasMultipleIndexes => false;
    public IElasticQueryBuilder QueryBuilder => throw new NotSupportedException();
    public ElasticQueryParser QueryParser => throw new NotSupportedException();
    public IElasticConfiguration Configuration => throw new NotSupportedException();
    public IDictionary<string, ICustomFieldType> CustomFieldTypes => throw new NotSupportedException();

    public void ConfigureSettings(ElasticsearchClientSettings settings) => throw new NotSupportedException();
    public Task ConfigureAsync() => throw new NotSupportedException();
    public Task EnsureIndexAsync(object? target) => throw new NotSupportedException();
    public Task MaintainAsync(bool includeOptionalTasks = true) => throw new NotSupportedException();
    public Task DeleteAsync() => throw new NotSupportedException();
    public Task ReindexAsync(Func<int, string?, Task>? progressCallbackAsync = null) => throw new NotSupportedException();
    public string CreateDocumentId(object document) => throw new NotSupportedException();
    public string[] GetIndexesByQuery(IRepositoryQuery query) => throw new NotSupportedException();
    public string GetIndex(object target) => throw new NotSupportedException();
    public void Dispose() { }
}

internal sealed class UnsortableIdFakeIndex : FakeIndex, IIndex
{
    bool IIndex.HasSortableIdField => false;
}

/// <summary>
/// A model with no <see cref="Foundatio.Repositories.Models.IIdentity"/> concept at all -- e.g. a
/// read-only projection or report. <see cref="ISearchableReadOnlyRepository{T}"/> only requires
/// <c>class, new()</c>, so this is a legitimate shape for <see cref="DefaultSortQueryBuilder"/> and
/// <see cref="SearchAfterQueryBuilder"/> to encounter, and neither should attempt an id sort for it.
/// </summary>
internal sealed class NonIdentityDocument
{
    public string Name { get; set; } = null!;
}
