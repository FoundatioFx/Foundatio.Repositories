using System;
using Foundatio.Repositories.Elasticsearch.Utility;
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
}
