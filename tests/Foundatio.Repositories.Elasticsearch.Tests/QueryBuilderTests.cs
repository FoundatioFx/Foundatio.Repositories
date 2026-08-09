using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Options;
using Foundatio.Xunit;
using Xunit;

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

public sealed class SearchAfterQueryExtensionsTests
{
    [Fact]
    public void SearchAfter_WithAllNullValues_RemovesExistingCursor()
    {
        var options = new CommandOptions<Employee>().SearchAfter("existing");

        options.SearchAfter(new object[] { null!, null! });

        Assert.False(options.HasSearchAfter());
    }

    [Fact]
    public void SearchAfter_WithMixedNullValues_PreservesCursor()
    {
        var options = new CommandOptions<Employee>();

        options.SearchAfter(new object[] { "value", null! });

        Assert.Equal(new object?[] { "value", null }, options.GetSearchAfter());
    }

    [Fact]
    public void SearchBefore_WithAllNullValues_RemovesExistingCursor()
    {
        var options = new CommandOptions<Employee>().SearchBefore("existing");

        options.SearchBefore(new object[] { null!, null! });

        Assert.False(options.HasSearchBefore());
    }

    [Fact]
    public void SearchBefore_WithMixedNullValues_PreservesCursor()
    {
        var options = new CommandOptions<Employee>();

        options.SearchBefore(new object[] { "value", null! });

        Assert.Equal(new object?[] { "value", null }, options.GetSearchBefore());
    }
}

public sealed class DefaultSortQueryBuilderTests : TestWithLoggingBase
{
    public DefaultSortQueryBuilderTests(ITestOutputHelper output) : base(output)
    {
    }

    // Mirrors EmployeeIndex's SetupDefaults() mapping: `id` is a plain keyword field, so the
    // resolved sort field name is the same as the resolved field name ("id").
    private static ElasticMappingResolver CreateKeywordIdResolver()
    {
        var inferrer = new Inferrer(new ElasticsearchClientSettings(new Uri("http://localhost:9200")));
        var codeMapping = new TypeMapping { Properties = new Properties { { "id", new KeywordProperty() } } };
        return new ElasticMappingResolver(codeMapping, inferrer, () => null);
    }

    // A text id with a keyword sort sub-field resolves "id" to "id.sort" for sorting.
    private static ElasticMappingResolver CreateTextSortableIdResolver()
    {
        var inferrer = new Inferrer(new ElasticsearchClientSettings(new Uri("http://localhost:9200")));
        var sortFields = new Properties { { "sort", new KeywordProperty() } };
        var codeMapping = new TypeMapping { Properties = new Properties { { "id", new TextProperty { Fields = sortFields } } } };
        return new ElasticMappingResolver(codeMapping, inferrer, () => null);
    }

    private static QueryBuilderContext<Employee> CreateContext(ElasticMappingResolver resolver, bool hasSortableIdField = true)
    {
        var query = new RepositoryQuery<Employee>();
        IIndex index = hasSortableIdField
            ? new FakeIndex { MappingResolver = resolver }
            : new UnsortableIdFakeIndex { MappingResolver = resolver };
        var options = new CommandOptions<Employee>().ElasticIndex(index);
        return new QueryBuilderContext<Employee>(query, options);
    }

    [Fact]
    public void HasSortableIdField_WhenImplementationOmitsMember_DefaultsToTrue()
    {
        IIndex index = new FakeIndex { MappingResolver = CreateKeywordIdResolver() };

        Assert.True(index.HasSortableIdField);
    }

    [Fact]
    public async Task BuildAsync_WithExistingSorts_AppendsIdSortAsTiebreaker()
    {
        // Arrange
        var queryBuilder = new DefaultSortQueryBuilder();
        var ctx = CreateContext(CreateKeywordIdResolver());
        ctx.Data[SortQueryBuilder.SortFieldsKey] = new List<SortOptions> { new FieldSort { Field = "name" } };

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        var sortFields = Assert.IsType<List<SortOptions>>(ctx.Data[SortQueryBuilder.SortFieldsKey]);
        Assert.Equal(2, sortFields.Count);
        Assert.Equal("name", sortFields[0].Field!.Field.Name);
        Assert.Equal("id", sortFields[1].Field!.Field.Name);
    }

    [Fact]
    public async Task BuildAsync_WithExplicitIdSort_DoesNotAddDuplicateIdSort()
    {
        // Arrange
        var queryBuilder = new DefaultSortQueryBuilder();
        var ctx = CreateContext(CreateKeywordIdResolver());
        ctx.Data[SortQueryBuilder.SortFieldsKey] = new List<SortOptions> { new FieldSort { Field = "id" } };

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        var sortFields = Assert.IsType<List<SortOptions>>(ctx.Data[SortQueryBuilder.SortFieldsKey]);
        Assert.Single(sortFields);
    }

    [Fact]
    public async Task BuildAsync_WithExplicitIdSortOnTextMappedField_DoesNotAddDuplicateIdSort()
    {
        // Arrange: exercise the same resolved sort data that the default pipeline produces.
        var queryBuilder = new DefaultSortQueryBuilder();
        var resolver = CreateTextSortableIdResolver();
        var query = new RepositoryQuery<Employee>().SortAscending(e => e.Id);
        var options = new CommandOptions<Employee>().ElasticIndex(new FakeIndex { MappingResolver = resolver });
        var ctx = new QueryBuilderContext<Employee>(query, options);

        // Act
        await new SortQueryBuilder().BuildAsync(ctx);
        await queryBuilder.BuildAsync(ctx);

        // Assert
        var sortFields = Assert.IsType<List<SortOptions>>(ctx.Data[SortQueryBuilder.SortFieldsKey]);
        var sort = Assert.Single(sortFields);
        Assert.Equal("id.sort", sort.Field!.Field.Name);
    }

    [Fact]
    public async Task BuildAsync_WithHasSortableIdFieldFalse_DoesNotAddIdSort()
    {
        // Arrange: an externally-managed index opts out of the id tiebreaker because the
        // code-declared mapping cannot be trusted to reflect the real server-side mapping.
        var queryBuilder = new DefaultSortQueryBuilder();
        var ctx = CreateContext(CreateKeywordIdResolver(), hasSortableIdField: false);

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        Assert.False(ctx.Data.ContainsKey(SortQueryBuilder.SortFieldsKey));
    }

    [Fact]
    public async Task BuildAsync_WithNoExistingSorts_AddsIdSortAsDefault()
    {
        // Arrange
        var queryBuilder = new DefaultSortQueryBuilder();
        var ctx = CreateContext(CreateKeywordIdResolver());

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        var sortFields = Assert.IsType<List<SortOptions>>(ctx.Data[SortQueryBuilder.SortFieldsKey]);
        Assert.Single(sortFields);
        Assert.Equal("id", sortFields[0].Field!.Field.Name);
    }

    [Fact]
    public async Task BuildAsync_WithTextMappedId_AddsSortSafeIdFieldAsDefault()
    {
        var queryBuilder = new DefaultSortQueryBuilder();
        var ctx = CreateContext(CreateTextSortableIdResolver());

        await queryBuilder.BuildAsync(ctx);

        var sortFields = Assert.IsType<List<SortOptions>>(ctx.Data[SortQueryBuilder.SortFieldsKey]);
        var sort = Assert.Single(sortFields);
        Assert.Equal("id.sort", sort.Field!.Field.Name);
    }

    [Fact]
    public async Task BuildAsync_WithModelNotImplementingIIdentity_DoesNotAddIdSort()
    {
        // Arrange: models with no Id property at all (e.g. read-only projections/reports) must
        // never get an id sort appended. Without this guard, GetResolvedField falls back to the
        // literal, unresolved "Id" field name instead of null, so the tiebreaker would still be
        // added on a field that doesn't exist on the model or the index.
        var queryBuilder = new DefaultSortQueryBuilder();
        var query = new RepositoryQuery<NonIdentityDocument>();
        var options = new CommandOptions<NonIdentityDocument>().ElasticIndex(new FakeIndex { MappingResolver = CreateKeywordIdResolver() });
        var ctx = new QueryBuilderContext<NonIdentityDocument>(query, options);

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        Assert.False(ctx.Data.ContainsKey(SortQueryBuilder.SortFieldsKey));
    }
}

public sealed class RuntimeFieldsQueryBuilderTests : TestWithLoggingBase
{
    public RuntimeFieldsQueryBuilderTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task BuildAsync_WithRuntimeFields_TransfersFieldsToContext()
    {
        // Arrange
        var queryBuilder = new AddRuntimeFieldsToContextQueryBuilder();
        var query = new RepositoryQuery<Employee>()
            .RuntimeField("field_one", ElasticRuntimeFieldType.Keyword)
            .RuntimeField(new ElasticRuntimeField { Name = "field_two", FieldType = ElasticRuntimeFieldType.Long, Script = "emit(doc['age'].value)" });
        var ctx = new QueryBuilderContext<Employee>(query, new CommandOptions<Employee>());
        var ctxElastic = (IElasticQueryVisitorContext)ctx;

        Assert.Empty(ctxElastic.RuntimeFields);

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        Assert.Equal(2, ctxElastic.RuntimeFields.Count);
        Assert.Equal("field_one", ctxElastic.RuntimeFields.ElementAt(0).Name);
        Assert.Equal(ElasticRuntimeFieldType.Keyword, ctxElastic.RuntimeFields.ElementAt(0).FieldType);
        Assert.Equal("field_two", ctxElastic.RuntimeFields.ElementAt(1).Name);
        Assert.Equal(ElasticRuntimeFieldType.Long, ctxElastic.RuntimeFields.ElementAt(1).FieldType);
        Assert.Equal("emit(doc['age'].value)", ctxElastic.RuntimeFields.ElementAt(1).Script);
    }

    [Fact]
    public async Task BuildAsync_WithContextFields_ConsumesFields()
    {
        // Arrange
        var queryBuilder = new RuntimeFieldsQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var ctx = new QueryBuilderContext<Employee>(query, new CommandOptions<Employee>());
        var ctxElastic = (IElasticQueryVisitorContext)ctx;
        ctxElastic.RuntimeFields.Add(new ElasticRuntimeField { Name = "field_one", FieldType = ElasticRuntimeFieldType.Keyword });
        ctxElastic.RuntimeFields.Add(new ElasticRuntimeField { Name = "field_two", FieldType = ElasticRuntimeFieldType.Long, Script = "emit(doc['age'].value)" });

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        Assert.Equal(2, ctxElastic.RuntimeFields.Count);
    }

    [Fact]
    public async Task BuildAsync_WithEmptyFields_DoesNotMutateSearch()
    {
        // Arrange
        var queryBuilder = new RuntimeFieldsQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var ctx = new QueryBuilderContext<Employee>(query, new CommandOptions<Employee>());
        var ctxElastic = (IElasticQueryVisitorContext)ctx;

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        Assert.Empty(ctxElastic.RuntimeFields);
    }
}

public sealed class SearchAfterQueryBuilderTests : TestWithLoggingBase
{
    public SearchAfterQueryBuilderTests(ITestOutputHelper output) : base(output)
    {
    }

    // Mirrors EmployeeIndex's SetupDefaults() mapping: `id` is a plain keyword field, so the
    // resolved sort field name is the same as the resolved field name ("id").
    private static ElasticMappingResolver CreateKeywordIdResolver()
    {
        var inferrer = new Inferrer(new ElasticsearchClientSettings(new Uri("http://localhost:9200")));
        var codeMapping = new TypeMapping { Properties = new Properties { { "id", new KeywordProperty() } } };
        return new ElasticMappingResolver(codeMapping, inferrer, () => null);
    }

    // A text id with a keyword sort sub-field resolves "id" to "id.sort" for sorting.
    private static ElasticMappingResolver CreateTextSortableIdResolver()
    {
        var inferrer = new Inferrer(new ElasticsearchClientSettings(new Uri("http://localhost:9200")));
        var sortFields = new Properties { { "sort", new KeywordProperty() } };
        var codeMapping = new TypeMapping { Properties = new Properties { { "id", new TextProperty { Fields = sortFields } } } };
        return new ElasticMappingResolver(codeMapping, inferrer, () => null);
    }

    private static QueryBuilderContext<Employee> CreateContext(ElasticMappingResolver resolver, bool hasSortableIdField = true, bool useSearchAfterPaging = true)
    {
        var query = new RepositoryQuery<Employee>();
        IIndex index = hasSortableIdField
            ? new FakeIndex { MappingResolver = resolver }
            : new UnsortableIdFakeIndex { MappingResolver = resolver };
        var options = new CommandOptions<Employee>().ElasticIndex(index);
        if (useSearchAfterPaging)
            options.SearchAfterPaging();

        return new QueryBuilderContext<Employee>(query, options);
    }

    private static List<SortOptions> GetAppliedSort(QueryBuilderContext<Employee> ctx)
    {
        return ((SearchRequest)ctx.Search).Sort!.ToList();
    }

    [Fact]
    public async Task BuildAsync_WithoutSearchAfterPaging_DoesNotAddIdTiebreaker()
    {
        // Arrange
        var queryBuilder = new SearchAfterQueryBuilder();
        var ctx = CreateContext(CreateKeywordIdResolver(), useSearchAfterPaging: false);
        ctx.Data[SortQueryBuilder.SortFieldsKey] = new List<SortOptions> { new FieldSort { Field = "name" } };

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        var sortFields = GetAppliedSort(ctx);
        Assert.Single(sortFields);
        Assert.Equal("name", sortFields[0].Field!.Field.Name);
    }

    [Fact]
    public async Task BuildAsync_WithSearchAfterPagingAndExistingSorts_AppendsIdSortAsTiebreaker()
    {
        // Arrange
        var queryBuilder = new SearchAfterQueryBuilder();
        var ctx = CreateContext(CreateKeywordIdResolver());
        ctx.Data[SortQueryBuilder.SortFieldsKey] = new List<SortOptions> { new FieldSort { Field = "name" } };

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        var sortFields = GetAppliedSort(ctx);
        Assert.Equal(2, sortFields.Count);
        Assert.Equal("name", sortFields[0].Field!.Field.Name);
        Assert.Equal("id", sortFields[1].Field!.Field.Name);
    }

    [Fact]
    public async Task BuildAsync_WithSearchAfterPagingAndExplicitIdSort_DoesNotAddDuplicateIdSort()
    {
        // Arrange
        var queryBuilder = new SearchAfterQueryBuilder();
        var ctx = CreateContext(CreateKeywordIdResolver());
        ctx.Data[SortQueryBuilder.SortFieldsKey] = new List<SortOptions> { new FieldSort { Field = "id" } };

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        Assert.Single(GetAppliedSort(ctx));
    }

    [Fact]
    public async Task BuildAsync_WithSearchAfterPagingAndExplicitIdSortOnTextMappedField_DoesNotAddDuplicateIdSort()
    {
        // Arrange: exercise the same resolved sort data that the default pipeline produces.
        var queryBuilder = new SearchAfterQueryBuilder();
        var resolver = CreateTextSortableIdResolver();
        var query = new RepositoryQuery<Employee>().SortAscending(e => e.Id);
        var options = new CommandOptions<Employee>()
            .ElasticIndex(new FakeIndex { MappingResolver = resolver })
            .SearchAfterPaging();
        var ctx = new QueryBuilderContext<Employee>(query, options);

        // Act
        await new SortQueryBuilder().BuildAsync(ctx);
        await queryBuilder.BuildAsync(ctx);

        // Assert
        var sort = Assert.Single(GetAppliedSort(ctx));
        Assert.Equal("id.sort", sort.Field!.Field.Name);
    }

    [Fact]
    public async Task BuildAsync_WithSearchAfterPagingAndHasSortableIdFieldFalse_DoesNotAddIdSort()
    {
        // Arrange: an externally-managed index opts out of the id tiebreaker because the
        // code-declared mapping cannot be trusted to reflect the real server-side mapping. The
        // caller-supplied sort is used as-is; no id tiebreaker (and never a bare "_id") is added.
        var queryBuilder = new SearchAfterQueryBuilder();
        var ctx = CreateContext(CreateKeywordIdResolver(), hasSortableIdField: false);
        ctx.Data[SortQueryBuilder.SortFieldsKey] = new List<SortOptions> { new FieldSort { Field = "name" } };

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        var sortFields = GetAppliedSort(ctx);
        Assert.Single(sortFields);
        Assert.Equal("name", sortFields[0].Field!.Field.Name);
        Assert.DoesNotContain(sortFields, s => s.Field?.Field.Name == "_id");
    }

    [Fact]
    public async Task BuildAsync_WithSearchAfterPagingAndNoExistingSorts_AddsIdSortAsTiebreaker()
    {
        // Arrange
        var queryBuilder = new SearchAfterQueryBuilder();
        var ctx = CreateContext(CreateKeywordIdResolver());

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        var sortFields = GetAppliedSort(ctx);
        Assert.Single(sortFields);
        Assert.Equal("id", sortFields[0].Field!.Field.Name);
    }

    [Fact]
    public async Task BuildAsync_WithSearchAfterPagingAndTextMappedId_AddsSortSafeIdFieldAsTiebreaker()
    {
        var queryBuilder = new SearchAfterQueryBuilder();
        var ctx = CreateContext(CreateTextSortableIdResolver());

        await queryBuilder.BuildAsync(ctx);

        var sort = Assert.Single(GetAppliedSort(ctx));
        Assert.Equal("id.sort", sort.Field!.Field.Name);
    }

    [Fact]
    public async Task BuildAsync_WithLiveSearchAfterPagingAndNoAvailableSort_ThrowsQueryValidationException()
    {
        var queryBuilder = new SearchAfterQueryBuilder();
        var ctx = CreateContext(CreateKeywordIdResolver(), hasSortableIdField: false);

        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => queryBuilder.BuildAsync(ctx));

        Assert.Contains("requires at least one sortable field", exception.Message);
    }

    [Fact]
    public async Task BuildAsync_WithPointInTimeSearchAfterPagingAndNoAvailableSort_AddsShardDocSort()
    {
        var queryBuilder = new SearchAfterQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var options = new CommandOptions<Employee>()
            .ElasticIndex(new UnsortableIdFakeIndex { MappingResolver = CreateKeywordIdResolver() })
            .SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        var ctx = new QueryBuilderContext<Employee>(query, options);

        await queryBuilder.BuildAsync(ctx);

        var sort = Assert.Single(((SearchRequest)ctx.Search).Sort!);
        Assert.Equal("_shard_doc", sort.Field!.Field.Name);
    }

    [Fact]
    public async Task BuildAsync_WithPointInTimeSearchBeforePagingAndNoSortableId_ReversesCallerAndShardDocSorts()
    {
        var queryBuilder = new SearchAfterQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var options = new CommandOptions<Employee>()
            .ElasticIndex(new UnsortableIdFakeIndex { MappingResolver = CreateKeywordIdResolver() })
            .SearchAfterPaging(SearchAfterPagingMode.PointInTime)
            .SearchBefore(42);
        var ctx = new QueryBuilderContext<Employee>(query, options);
        ctx.Data[SortQueryBuilder.SortFieldsKey] = new List<SortOptions> { new FieldSort { Field = "name", Order = SortOrder.Asc } };

        await queryBuilder.BuildAsync(ctx);

        var sorts = ((SearchRequest)ctx.Search).Sort!.ToList();
        Assert.Equal(2, sorts.Count);
        Assert.Equal("name", sorts[0].Field!.Field.Name);
        Assert.Equal(SortOrder.Desc, sorts[0].Field!.Order);
        Assert.Equal("_shard_doc", sorts[1].Field!.Field.Name);
        Assert.Equal(SortOrder.Desc, sorts[1].Field!.Order);
    }

    [Fact]
    public async Task BuildAsync_WithPointInTimeSearchAfterPagingAndExplicitShardDoc_DoesNotAddDuplicate()
    {
        var queryBuilder = new SearchAfterQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var options = new CommandOptions<Employee>()
            .ElasticIndex(new UnsortableIdFakeIndex { MappingResolver = CreateKeywordIdResolver() })
            .SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        var ctx = new QueryBuilderContext<Employee>(query, options);
        ctx.Data[SortQueryBuilder.SortFieldsKey] = new List<SortOptions> { new FieldSort { Field = "_shard_doc" } };

        await queryBuilder.BuildAsync(ctx);

        var sort = Assert.Single(((SearchRequest)ctx.Search).Sort!);
        Assert.Equal("_shard_doc", sort.Field!.Field.Name);
    }

    [Fact]
    public async Task BuildAsync_WithPointInTimeSearchBeforePagingAndNoAvailableSort_ReversesShardDocSort()
    {
        var queryBuilder = new SearchAfterQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var options = new CommandOptions<Employee>()
            .ElasticIndex(new UnsortableIdFakeIndex { MappingResolver = CreateKeywordIdResolver() })
            .SearchAfterPaging(SearchAfterPagingMode.PointInTime)
            .SearchBefore(42);
        var ctx = new QueryBuilderContext<Employee>(query, options);

        await queryBuilder.BuildAsync(ctx);

        var sort = Assert.Single(((SearchRequest)ctx.Search).Sort!);
        Assert.Equal("_shard_doc", sort.Field!.Field.Name);
        Assert.Equal(SortOrder.Desc, sort.Field.Order);
    }

    [Fact]
    public async Task BuildAsync_WithLiveSearchAfterPagingAndNonIdentityModelWithoutSort_ThrowsQueryValidationException()
    {
        var queryBuilder = new SearchAfterQueryBuilder();
        var query = new RepositoryQuery<NonIdentityDocument>();
        var options = new CommandOptions<NonIdentityDocument>()
            .ElasticIndex(new FakeIndex { MappingResolver = CreateKeywordIdResolver() })
            .SearchAfterPaging();
        var ctx = new QueryBuilderContext<NonIdentityDocument>(query, options);

        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => queryBuilder.BuildAsync(ctx));

        Assert.Contains("requires at least one sortable field", exception.Message);
    }

    [Fact]
    public async Task BuildAsync_WithSearchAfterPagingAndModelNotImplementingIIdentity_DoesNotAddIdTiebreaker()
    {
        // Arrange: models with no Id property at all (e.g. read-only projections/reports) must
        // never get an id tiebreaker appended. Without this guard, GetResolvedField falls back to
        // the literal, unresolved "Id" field name instead of null, so the tiebreaker would still
        // be added on a field that doesn't exist on the model or the index. Callers of such models
        // must supply their own unique, sortable field(s) to keep the search_after cursor stable.
        var queryBuilder = new SearchAfterQueryBuilder();
        var query = new RepositoryQuery<NonIdentityDocument>();
        var options = new CommandOptions<NonIdentityDocument>().ElasticIndex(new FakeIndex { MappingResolver = CreateKeywordIdResolver() });
        options.SearchAfterPaging();
        var ctx = new QueryBuilderContext<NonIdentityDocument>(query, options);
        ctx.Data[SortQueryBuilder.SortFieldsKey] = new List<SortOptions> { new FieldSort { Field = "name" } };

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        var sortFields = ((SearchRequest)ctx.Search).Sort!.ToList();
        Assert.Single(sortFields);
        Assert.Equal("name", sortFields[0].Field!.Field.Name);
    }
}
