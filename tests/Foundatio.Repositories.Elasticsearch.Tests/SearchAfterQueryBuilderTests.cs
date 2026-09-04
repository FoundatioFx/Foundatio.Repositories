using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Transport.Extensions;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.Extensions;
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

    [Theory]
    [InlineData("field")]
    [InlineData("score")]
    [InlineData("doc")]
    [InlineData("geo")]
    [InlineData("script")]
    public async Task BuildAsync_WithRepeatedBackwardQuery_PreservesCallerSortAndSettings(string variant)
    {
        var nested = new NestedSortValue { Path = "nested", MaxChildren = 3 };
        SortOptions original = variant switch
        {
            "field" => new FieldSort("date")
            {
                Format = "strict_date_optional_time_nanos",
                Missing = "_last",
                Mode = SortMode.Max,
                Nested = nested,
                NumericType = FieldSortNumericType.DateNanos,
                Order = SortOrder.Desc,
                UnmappedType = FieldType.Date
            },
            "score" => new SortOptions { Score = new ScoreSort { Order = SortOrder.Desc } },
            "doc" => new SortOptions { Doc = new ScoreSort { Order = SortOrder.Desc } },
            "geo" => new GeoDistanceSort("location", new List<GeoLocation> { GeoLocation.Text("40,-70") })
            {
                DistanceType = GeoDistanceType.Plane,
                IgnoreUnmapped = true,
                Mode = SortMode.Max,
                Nested = nested,
                Order = SortOrder.Desc,
                Unit = DistanceUnit.Miles
            },
            _ => new ScriptSort(new Script { Source = "return 1", Lang = "painless" })
            {
                Mode = SortMode.Max,
                Nested = nested,
                Order = SortOrder.Desc,
                Type = ScriptSortType.Number
            }
        };
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new Uri("http://localhost:9200")));
        string callerJson = client.RequestResponseSerializer.SerializeToString(original);
        var resolver = CreateKeywordIdResolver();
        var copy = resolver.ResolveFieldSort(original)!;
        Assert.Equal(callerJson, client.RequestResponseSerializer.SerializeToString(copy));
        var query = new RepositoryQuery<Employee>().SortAscending("unused");
        query.GetSorts().Clear();
        query.GetSorts().Add(original);
        var options = new CommandOptions<Employee>().ElasticIndex(new UnsortableIdFakeIndex { MappingResolver = resolver })
            .SearchAfterPaging().SearchBefore(1);

        async Task<string> BuildAsync()
        {
            var context = new QueryBuilderContext<Employee>(query, options);
            await new SortQueryBuilder().BuildAsync(context);
            await new SearchAfterQueryBuilder().BuildAsync(context);
            return client.RequestResponseSerializer.SerializeToString((SearchRequest)context.Search);
        }

        string firstRequest = await BuildAsync();
        string secondRequest = await BuildAsync();

        Assert.Equal(firstRequest, secondRequest);
        Assert.Contains("\"order\":\"asc\"", firstRequest);
        Assert.Equal(callerJson, client.RequestResponseSerializer.SerializeToString(original));
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
        // Arrange
        var queryBuilder = new SearchAfterQueryBuilder();
        var ctx = CreateContext(CreateKeywordIdResolver(), hasSortableIdField: false);

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => queryBuilder.BuildAsync(ctx));

        // Assert
        Assert.Contains("requires at least one sortable field", exception.Message);
    }

    [Fact]
    public async Task BuildAsync_WithLiveSearchAfterPagingAndNonIdentityModelWithoutSort_ThrowsQueryValidationException()
    {
        // Arrange
        var queryBuilder = new SearchAfterQueryBuilder();
        var query = new RepositoryQuery<NonIdentityDocument>();
        var options = new CommandOptions<NonIdentityDocument>()
            .ElasticIndex(new FakeIndex { MappingResolver = CreateKeywordIdResolver() })
            .SearchAfterPaging();
        var ctx = new QueryBuilderContext<NonIdentityDocument>(query, options);

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => queryBuilder.BuildAsync(ctx));

        // Assert
        Assert.Contains("requires at least one sortable field", exception.Message);
    }

    [Fact]
    public async Task BuildAsync_WithPointInTimeSearchAfterPagingAndExplicitShardDoc_DoesNotAddDuplicate()
    {
        // Arrange
        var queryBuilder = new SearchAfterQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var options = new CommandOptions<Employee>()
            .ElasticIndex(new UnsortableIdFakeIndex { MappingResolver = CreateKeywordIdResolver() })
            .SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        var ctx = new QueryBuilderContext<Employee>(query, options);
        ctx.Data[SortQueryBuilder.SortFieldsKey] = new List<SortOptions> { new FieldSort { Field = "_shard_doc" } };

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        var sort = Assert.Single(((SearchRequest)ctx.Search).Sort!);
        Assert.Equal("_shard_doc", sort.Field!.Field.Name);
    }

    [Fact]
    public async Task BuildAsync_WithPointInTimeSearchAfterPagingAndNoAvailableSort_AddsShardDocSort()
    {
        // Arrange
        var queryBuilder = new SearchAfterQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var options = new CommandOptions<Employee>()
            .ElasticIndex(new UnsortableIdFakeIndex { MappingResolver = CreateKeywordIdResolver() })
            .SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        var ctx = new QueryBuilderContext<Employee>(query, options);

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        var sort = Assert.Single(((SearchRequest)ctx.Search).Sort!);
        Assert.Equal("_shard_doc", sort.Field!.Field.Name);
    }

    [Fact]
    public async Task BuildAsync_WithPointInTimeSearchAfterPagingAndSortableId_AppendsShardDocSort()
    {
        // Arrange
        var queryBuilder = new SearchAfterQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var options = new CommandOptions<Employee>()
            .ElasticIndex(new FakeIndex { MappingResolver = CreateKeywordIdResolver() })
            .SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        var ctx = new QueryBuilderContext<Employee>(query, options);

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        var sorts = GetAppliedSort(ctx);
        Assert.Equal(2, sorts.Count);
        Assert.Equal("id", sorts[0].Field!.Field.Name);
        Assert.Equal("_shard_doc", sorts[1].Field!.Field.Name);
    }

    [Fact]
    public async Task BuildAsync_WithPointInTimeSearchBeforePagingAndNoAvailableSort_ReversesShardDocSort()
    {
        // Arrange
        var queryBuilder = new SearchAfterQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var options = new CommandOptions<Employee>()
            .ElasticIndex(new UnsortableIdFakeIndex { MappingResolver = CreateKeywordIdResolver() })
            .SearchAfterPaging(SearchAfterPagingMode.PointInTime)
            .SearchBefore(42);
        var ctx = new QueryBuilderContext<Employee>(query, options);

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        var sort = Assert.Single(((SearchRequest)ctx.Search).Sort!);
        Assert.Equal("_shard_doc", sort.Field!.Field.Name);
        Assert.Equal(SortOrder.Desc, sort.Field.Order);
    }

    [Fact]
    public async Task BuildAsync_WithPointInTimeSearchBeforePagingAndNoSortableId_ReversesCallerAndShardDocSorts()
    {
        // Arrange
        var queryBuilder = new SearchAfterQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var options = new CommandOptions<Employee>()
            .ElasticIndex(new UnsortableIdFakeIndex { MappingResolver = CreateKeywordIdResolver() })
            .SearchAfterPaging(SearchAfterPagingMode.PointInTime)
            .SearchBefore(42, 7);
        var ctx = new QueryBuilderContext<Employee>(query, options);
        ctx.Data[SortQueryBuilder.SortFieldsKey] = new List<SortOptions> { new FieldSort { Field = "name", Order = SortOrder.Asc } };

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
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
        // Arrange
        var queryBuilder = new SearchAfterQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var options = new CommandOptions<Employee>()
            .ElasticIndex(new FakeIndex { MappingResolver = CreateKeywordIdResolver() })
            .SearchAfterPaging(SearchAfterPagingMode.PointInTime)
            .SearchBefore(42, 7);
        var ctx = new QueryBuilderContext<Employee>(query, options);

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
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
        // Arrange
        var queryBuilder = new SearchAfterQueryBuilder();
        var ctx = CreateContext(CreateTextSortableIdResolver());

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
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
