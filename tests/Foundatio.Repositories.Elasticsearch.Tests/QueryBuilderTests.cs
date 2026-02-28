using System.Linq;
using System.Threading.Tasks;
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
    public async Task BuildAsync_MultipleFields()
    {
        var queryBuilder = new RuntimeFieldsQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        string runtimeField1 = "One", runtimeField2 = "Two";
        var ctx = new QueryBuilderContext<Employee>(query, new CommandOptions<Employee>());
        var ctxElastic = ctx as IElasticQueryVisitorContext;
        ctxElastic.RuntimeFields.Add(new Parsers.ElasticRuntimeField() { Name = runtimeField1 });
        ctxElastic.RuntimeFields.Add(new Parsers.ElasticRuntimeField() { Name = runtimeField2 });

        await queryBuilder.BuildAsync(ctx);

        Assert.Equal(2, ctxElastic.RuntimeFields.Count);
        Assert.Equal(runtimeField1, ctxElastic.RuntimeFields.ElementAt(0).Name);
        Assert.Equal(runtimeField2, ctxElastic.RuntimeFields.ElementAt(1).Name);
    }

    [Fact]
    public async Task BuildAsync_EmptyFields_DoesNotMutateSearch()
    {
        var queryBuilder = new RuntimeFieldsQueryBuilder();
        var query = new RepositoryQuery<Employee>();
        var ctx = new QueryBuilderContext<Employee>(query, new CommandOptions<Employee>());
        var ctxElastic = ctx as IElasticQueryVisitorContext;

        await queryBuilder.BuildAsync(ctx);

        Assert.Empty(ctxElastic.RuntimeFields);
    }
}
