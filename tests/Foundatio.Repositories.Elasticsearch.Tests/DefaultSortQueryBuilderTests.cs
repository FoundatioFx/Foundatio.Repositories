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
using Foundatio.Repositories.Options;
using Foundatio.Xunit;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

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
        // Arrange
        var queryBuilder = new DefaultSortQueryBuilder();
        var ctx = CreateContext(CreateTextSortableIdResolver());

        // Act
        await queryBuilder.BuildAsync(ctx);

        // Assert
        var sortFields = Assert.IsType<List<SortOptions>>(ctx.Data[SortQueryBuilder.SortFieldsKey]);
        var sort = Assert.Single(sortFields);
        Assert.Equal("id.sort", sort.Field!.Field.Name);
    }

    [Fact]
    public void ElasticQueryBuilder_DefaultRegistrations_AreInExecutionOrder()
    {
        // Arrange / Act
        Type[] builderTypes = new ElasticQueryBuilder().GetRegistrations().Select(registration => registration.Builder.GetType()).ToArray();

        // Assert
        Assert.Equal(new[]
        {
            typeof(AddRuntimeFieldsToContextQueryBuilder),
            typeof(PageableQueryBuilder),
            typeof(FieldIncludesQueryBuilder),
            typeof(SortQueryBuilder),
            typeof(AggregationsQueryBuilder),
            typeof(ParentQueryBuilder),
            typeof(ChildQueryBuilder),
            typeof(IdentityQueryBuilder),
            typeof(SoftDeletesQueryBuilder),
            typeof(DateRangeQueryBuilder),
            typeof(ExpressionQueryBuilder),
            typeof(ElasticFilterQueryBuilder),
            typeof(FieldConditionsQueryBuilder),
            typeof(DefaultSortQueryBuilder),
            typeof(RuntimeFieldsQueryBuilder),
            typeof(SearchAfterQueryBuilder)
        }, builderTypes);
    }

    [Fact]
    public void HasSortableIdField_WhenImplementationOmitsMember_DefaultsToTrue()
    {
        // Arrange / Act
        IIndex index = new FakeIndex { MappingResolver = CreateKeywordIdResolver() };

        // Assert
        Assert.True(index.HasSortableIdField);
    }
}
