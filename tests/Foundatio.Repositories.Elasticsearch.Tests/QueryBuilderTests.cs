using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Xunit;
using Nest;
using Xunit;
using Xunit.Abstractions;

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

        ISearchRequest request = ctx.Search;
        Assert.Equal(2, request.RuntimeFields.Count);
        Assert.Equal(runtimeField1, request.RuntimeFields.First().Key);
        Assert.Equal(runtimeField2, request.RuntimeFields.Last().Key);
    }
}
