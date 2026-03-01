using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Xunit;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class RuntimeFieldsQueryBuilderTests : TestWithLoggingBase
{
    public RuntimeFieldsQueryBuilderTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task AddToContext_TransfersQueryFieldsToContext()
    {
        var queryBuilder = new AddRuntimeFieldsToContextQueryBuilder();
        var query = new RepositoryQuery<Employee>()
            .RuntimeField("field_one", ElasticRuntimeFieldType.Keyword)
            .RuntimeField(new ElasticRuntimeField { Name = "field_two", FieldType = ElasticRuntimeFieldType.Long, Script = "emit(doc['age'].value)" });
        var ctx = new QueryBuilderContext<Employee>(query, new CommandOptions<Employee>());
        var ctxElastic = (IElasticQueryVisitorContext)ctx;

        Assert.Empty(ctxElastic.RuntimeFields);

        await queryBuilder.BuildAsync(ctx);

        Assert.Equal(2, ctxElastic.RuntimeFields.Count);
        Assert.Equal("field_one", ctxElastic.RuntimeFields.ElementAt(0).Name);
        Assert.Equal(ElasticRuntimeFieldType.Keyword, ctxElastic.RuntimeFields.ElementAt(0).FieldType);
        Assert.Equal("field_two", ctxElastic.RuntimeFields.ElementAt(1).Name);
        Assert.Equal(ElasticRuntimeFieldType.Long, ctxElastic.RuntimeFields.ElementAt(1).FieldType);
        Assert.Equal("emit(doc['age'].value)", ctxElastic.RuntimeFields.ElementAt(1).Script);
    }

    [Fact]
    public async Task Build_WithFields_ConsumesContextFields()
    {
        var queryBuilder = new RuntimeFieldsQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var ctx = new QueryBuilderContext<Employee>(query, new CommandOptions<Employee>());
        var ctxElastic = (IElasticQueryVisitorContext)ctx;
        ctxElastic.RuntimeFields.Add(new ElasticRuntimeField { Name = "field_one", FieldType = ElasticRuntimeFieldType.Keyword });
        ctxElastic.RuntimeFields.Add(new ElasticRuntimeField { Name = "field_two", FieldType = ElasticRuntimeFieldType.Long, Script = "emit(doc['age'].value)" });

        await queryBuilder.BuildAsync(ctx);

        Assert.Equal(2, ctxElastic.RuntimeFields.Count);
    }

    [Fact]
    public async Task Build_EmptyFields_DoesNotMutateSearch()
    {
        var queryBuilder = new RuntimeFieldsQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var ctx = new QueryBuilderContext<Employee>(query, new CommandOptions<Employee>());
        var ctxElastic = (IElasticQueryVisitorContext)ctx;

        await queryBuilder.BuildAsync(ctx);

        Assert.Empty(ctxElastic.RuntimeFields);
    }
}
