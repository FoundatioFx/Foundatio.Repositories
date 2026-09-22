using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public class ResolverExtensionsTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetResolvedFields_WithFieldSort_PreservesAllSettings(bool asynchronous)
    {
        using var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"));
        using var resolver = ElasticMappingResolver.CreateWithAsyncLoader(_ => Task.FromResult<TypeMapping?>(new TypeMapping
        {
            Properties = new Properties
            {
                { "events", new NestedProperty { Properties = new Properties { { "occurredAt", new DateProperty() } } } }
            }
        }), new Inferrer(settings));
        var field = new FieldSort
        {
            Field = "events.occurredAt",
            Format = "strict_date_optional_time_nanos",
            Missing = "_last",
            Mode = SortMode.Max,
            Nested = new NestedSortValue { Path = "events" },
            NumericType = FieldSortNumericType.DateNanos,
            Order = SortOrder.Desc,
            UnmappedType = FieldType.Date
        };
        var score = new SortOptions { Score = new ScoreSort { Order = SortOrder.Desc } };
        var input = new List<SortOptions> { field, score, null! };

        var resolved = asynchronous
            ? await resolver.GetResolvedFieldsAsync(input, TestContext.Current.CancellationToken)
            : resolver.GetResolvedFields(input);

        Assert.Collection(resolved,
            sort =>
            {
                var actual = Assert.IsType<FieldSort>(sort.Field);
                Assert.NotSame(field, actual);
                Assert.Equal("events.occurredAt", actual.Field.Name);
                Assert.Equal(field.Format, actual.Format);
                Assert.Equal(field.Missing, actual.Missing);
                Assert.Equal(field.Mode, actual.Mode);
                Assert.Same(field.Nested, actual.Nested);
                Assert.Equal(field.NumericType, actual.NumericType);
                Assert.Equal(field.Order, actual.Order);
                Assert.Equal(field.UnmappedType, actual.UnmappedType);
            },
            sort => Assert.Same(score, sort));
        Assert.Equal(3, input.Count);
        Assert.Same(field, input[0].Field);
        Assert.Equal("events.occurredAt", field.Field.Name);
        Assert.Equal("strict_date_optional_time_nanos", field.Format);
    }
}
