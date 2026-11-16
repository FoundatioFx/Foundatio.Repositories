using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Elastic.Transport.Extensions;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Utility;
using Foundatio.Repositories.Options;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public class FieldIncludeParserTests
{
    [Theory]
    [InlineData("property1", "property1", "property1")]
    [InlineData("property1.test", "property1.test", "property1.test")]
    [InlineData("property1 test", "property1 test", "property1 test")]
    [InlineData("property1 test     ,   property2  test    ", "property1 test,property2  test", "property1 test,property2  test")]
    [InlineData("StringProperty2,Object1(NestedObject1),Object1(NestedObject1(StringProperty1))", "StringProperty2,Object1(NestedObject1(StringProperty1))", "StringProperty2,Object1.NestedObject1.StringProperty1")]
    [InlineData("Results(id,applicant,program(name,id,properties),application_id,cas_id,application_status,cas_date_submitted),NextPageToken,Results(applicant(givenName))", "Results(id,applicant(givenName),program(name,id,properties),application_id,cas_id,application_status,cas_date_submitted),NextPageToken", "Results.id,Results.applicant.givenName,Results.program.name,Results.program.id,Results.program.properties,Results.application_id,Results.cas_id,Results.application_status,Results.cas_date_submitted,NextPageToken")]
    [InlineData("meTa(sTuFf)  ,  CreaTedUtc", "meTa(sTuFf),CreaTedUtc", "meTa.sTuFf,CreaTedUtc")]
    [InlineData("", "", "")]
    [InlineData("(id,name)", "id,name", "id,name")]
    public void CanParse(string expression, string mask, string fields)
    {
        var result = FieldIncludeParser.Parse(expression);
        Assert.Equal(mask, result.ToString());
        Assert.Equal(fields, String.Join(',', result.ToFieldPaths()));
    }

    [Theory]
    [InlineData(")", "unexpected")]
    [InlineData("blah)", "unexpected")]
    [InlineData("(", "missing")]
    [InlineData("((", "missing")]
    [InlineData("blah(", "missing")]
    [InlineData("blah((", "missing")]
    [InlineData("blah(()", "missing")]
    public void CanHandleInvalid(string expression, string message)
    {
        var result = FieldIncludeParser.Parse(expression);
        Assert.False(result.IsValid);

        if (!String.IsNullOrEmpty(message))
            Assert.Contains(message, result.ValidationMessage, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("is_*", "is_deleted", "Command", false)]
    [InlineData("*", "is_deleted", "Query", true)]
    [InlineData("*deleted", "is_deleted", "CommandMask", true)]
    [InlineData("metadata", "metadata.flags.is_deleted", "QueryMask", false)]
    [InlineData("metadata.*", "metadata.flags.is_deleted", "Command", true)]
    [InlineData("*flags", "metadata.flags.is_deleted", "Query", true)]
    [InlineData("metadata.flags", "metadata.flags.is_deleted", "CommandMask", false)]
    [InlineData("metadata(flags(is_*))", "metadata.flags.is_deleted", "QueryMask", true)]
    public async Task FieldIncludes_WithExclusionCoveringRequiredField_RejectsTheProjection(
        string exclusion, string requiredField, string origin, bool includeFields)
    {
        using var configuration = new TestConfiguration();
        using var index = new TestIndex(configuration);
        var query = new RepositoryQuery<object>();
        var options = new CommandOptions().ElasticIndex(index).RequiredFields([requiredField]);
        if (includeFields)
            options.Include("id");
        switch (origin)
        {
            case "Command":
                options.Exclude(exclusion);
                break;
            case "CommandMask":
                options.ExcludeMask(exclusion);
                break;
            case "Query":
                query.Exclude(exclusion);
                break;
            case "QueryMask":
                query.ExcludeMask(exclusion);
                break;
        }
        var context = new QueryBuilderContext<object>(query, options);

        var exception = await Assert.ThrowsAsync<ArgumentException>(() => new FieldIncludesQueryBuilder().BuildAsync(context));

        Assert.Contains(requiredField, exception.Message, StringComparison.Ordinal);
        Assert.Null(((SearchRequest)context.Search).Source);
    }

    [Theory]
    [InlineData("is_deleted", "is_deleted", false)]
    [InlineData("detail*", "is_deleted", true)]
    [InlineData("IS_*", "is_deleted", true)]
    [InlineData("is?deleted", "is_deleted", true)]
    [InlineData("met", "metadata.flags.is_deleted", true)]
    [InlineData("metadata.other", "metadata.flags.is_deleted", true)]
    [InlineData("metadata.fl?gs", "metadata.flags.is_deleted", true)]
    public async Task FieldIncludes_WithNonconflictingOrExactRequiredExclude_PreservesNativeFilters(
        string exclusion, string requiredField, bool exclusionRetained)
    {
        using var configuration = new TestConfiguration();
        using var index = new TestIndex(configuration);
        var options = new CommandOptions().ElasticIndex(index).RequiredFields([requiredField]).Include("id");
        var query = new RepositoryQuery<object>().Exclude(exclusion);
        var context = new QueryBuilderContext<object>(query, options);

        await new FieldIncludesQueryBuilder().BuildAsync(context);

        using var json = JsonDocument.Parse(configuration.Client.ElasticsearchClientSettings.RequestResponseSerializer.SerializeToString((SearchRequest)context.Search));
        var source = json.RootElement.GetProperty("_source");
        Assert.Equal(new[] { "id", requiredField }.Distinct().OrderBy(field => field, StringComparer.Ordinal),
            ReadFields(source.GetProperty("includes")).OrderBy(field => field, StringComparer.Ordinal));
        var excludes = source.TryGetProperty("excludes", out var serializedExcludes) ? ReadFields(serializedExcludes) : [];
        Assert.Equal(exclusionRetained ? new[] { exclusion } : [], excludes);
    }

    [Fact]
    public async Task FieldIncludes_WithoutRequiredFields_PreservesOverlappingNativeWildcard()
    {
        using var configuration = new TestConfiguration();
        using var index = new TestIndex(configuration);
        var query = new RepositoryQuery<object>().Include("is_deleted").Exclude("is_*");
        var context = new QueryBuilderContext<object>(query, new CommandOptions().ElasticIndex(index));

        await new FieldIncludesQueryBuilder().BuildAsync(context);

        using var json = JsonDocument.Parse(configuration.Client.ElasticsearchClientSettings.RequestResponseSerializer.SerializeToString((SearchRequest)context.Search));
        var source = json.RootElement.GetProperty("_source");
        Assert.Equal(new[] { "is_deleted" }, ReadFields(source.GetProperty("includes")));
        Assert.Equal(new[] { "is_*" }, ReadFields(source.GetProperty("excludes")));
    }

    private static string[] ReadFields(JsonElement fields) => fields.ValueKind is JsonValueKind.String
        ? [fields.GetString()!]
        : fields.EnumerateArray().Select(field => field.GetString()!).ToArray();

    private sealed class TestConfiguration : ElasticConfiguration
    {
        protected override IRequestInvoker CreateRequestInvoker() => new InMemoryRequestInvoker();
    }

    private sealed class TestIndex(TestConfiguration configuration) : Index<object>(configuration, "field-include-tests")
    {
        protected override ElasticMappingResolver CreateMappingResolver() =>
            ElasticMappingResolver.Create<object>(ConfigureIndexMapping, Configuration.Client.Infer, () => null);
    }
}
