using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
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

        // Verify runtime fields were added to the context
        Assert.Equal(2, ctxElastic.RuntimeFields.Count);
        Assert.Equal(runtimeField1, ctxElastic.RuntimeFields.First().Name);
        Assert.Equal(runtimeField2, ctxElastic.RuntimeFields.Last().Name);
    }
}
