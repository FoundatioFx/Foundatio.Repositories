using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Options;
using Foundatio.Xunit;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

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
    public async Task BuildAsync_WithBothCursorDirections_ThrowsQueryValidationException()
    {
        // Arrange
        var ctx = CreateContext(CreateKeywordIdResolver());
        ctx.Options.Values.Set(SearchAfterQueryExtensions.SearchAfterKey, new object?[] { "after" });
        ctx.Options.Values.Set(SearchAfterQueryExtensions.SearchBeforeKey, new object?[] { "before" });

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => new SearchAfterQueryBuilder().BuildAsync(ctx));

        // Assert
        Assert.Contains("cannot both be set", exception.Message);
    }

    [Fact]
    public async Task BuildAsync_WithCursorCountDifferentFromFinalSortCount_ThrowsQueryValidationException()
    {
        // Arrange
        var ctx = CreateContext(CreateKeywordIdResolver());
        ctx.Data[SortQueryBuilder.SortFieldsKey] = new List<SortOptions> { new FieldSort { Field = "name" } };
        ctx.Options.SearchAfter("name-only");

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => new SearchAfterQueryBuilder().BuildAsync(ctx));

        // Assert
        Assert.Contains("final sort contains 2", exception.Message);
    }

    [Fact]
    public async Task BuildAsync_WithDefaultSortTiebreakerAndSearchBefore_ReversesCompleteTuple()
    {
        // Arrange: mirrors the real pipeline order -- SortQueryBuilder translates caller sorts,
        // DefaultSortQueryBuilder (lower priority) appends the id tiebreaker, and
        // SearchAfterQueryBuilder (highest priority) reverses the accumulated tuple for backward
        // paging. If any builder ran out of order, the tiebreaker would not be reversed.
        var resolver = CreateKeywordIdResolver();
        var query = new RepositoryQuery<Employee>().SortAscending(e => e.Name);
        var options = new CommandOptions<Employee>()
            .ElasticIndex(new FakeIndex { MappingResolver = resolver })
            .SearchAfterPaging()
            .SearchBefore("name-cursor", "id-cursor");
        var ctx = new QueryBuilderContext<Employee>(query, options);

        // Act
        await new SortQueryBuilder().BuildAsync(ctx);
        await new DefaultSortQueryBuilder().BuildAsync(ctx);
        await new SearchAfterQueryBuilder().BuildAsync(ctx);

        // Assert
        var sortFields = GetAppliedSort(ctx);
        Assert.Equal(2, sortFields.Count);
        Assert.Equal("name", sortFields[0].Field!.Field.Name);
        Assert.Equal(SortOrder.Desc, sortFields[0].Field!.Order);
        Assert.Equal("id", sortFields[1].Field!.Field.Name);
        Assert.Equal(SortOrder.Desc, sortFields[1].Field!.Order);
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
    public async Task BuildAsync_WithPointInTimeSearchAfterPagingAndSortableId_AppendsShardDocSort()
    {
        var queryBuilder = new SearchAfterQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var options = new CommandOptions<Employee>()
            .ElasticIndex(new FakeIndex { MappingResolver = CreateKeywordIdResolver() })
            .SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        var ctx = new QueryBuilderContext<Employee>(query, options);

        await queryBuilder.BuildAsync(ctx);

        var sorts = GetAppliedSort(ctx);
        Assert.Equal(2, sorts.Count);
        Assert.Equal("id", sorts[0].Field!.Field.Name);
        Assert.Equal("_shard_doc", sorts[1].Field!.Field.Name);
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
    public async Task BuildAsync_WithPointInTimeSearchBeforePagingAndNoSortableId_ReversesCallerAndShardDocSorts()
    {
        var queryBuilder = new SearchAfterQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var options = new CommandOptions<Employee>()
            .ElasticIndex(new UnsortableIdFakeIndex { MappingResolver = CreateKeywordIdResolver() })
            .SearchAfterPaging(SearchAfterPagingMode.PointInTime)
            .SearchBefore(42, 7);
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
    public async Task BuildAsync_WithPointInTimeSearchBeforePagingAndSortableId_ReversesIdAndShardDocSorts()
    {
        var queryBuilder = new SearchAfterQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var options = new CommandOptions<Employee>()
            .ElasticIndex(new FakeIndex { MappingResolver = CreateKeywordIdResolver() })
            .SearchAfterPaging(SearchAfterPagingMode.PointInTime)
            .SearchBefore(42, 7);
        var ctx = new QueryBuilderContext<Employee>(query, options);

        await queryBuilder.BuildAsync(ctx);

        var sorts = GetAppliedSort(ctx);
        Assert.Equal(2, sorts.Count);
        Assert.Equal("id", sorts[0].Field!.Field.Name);
        Assert.Equal(SortOrder.Desc, sorts[0].Field!.Order);
        Assert.Equal("_shard_doc", sorts[1].Field!.Field.Name);
        Assert.Equal(SortOrder.Desc, sorts[1].Field!.Order);
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
}
