using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests.Extensions;

public sealed class FindHitExtensionsTests
{
    [Theory]
    [InlineData(null, SortMode.Min)]
    [InlineData(SortOrder.Asc, SortMode.Min)]
    [InlineData(SortOrder.Desc, SortMode.Max)]
    public void ReverseOrder_WithDistanceOrScriptSort_PreservesSelectedValue(SortOrder? order, SortMode expected)
    {
        // Arrange
        var distance = new SortOptions { GeoDistance = new GeoDistanceSort { Field = "location", Location = [GeoLocation.Text("0,0")], Order = order } };
        var script = new SortOptions { Script = new ScriptSort { Script = new Script { Source = "1" }, Order = order } };

        // Act
        distance.ReverseOrder();
        script.ReverseOrder();

        // Assert
        Assert.Equal(expected, distance.GeoDistance!.Mode);
        Assert.Equal(expected, script.Script!.Mode);
        Assert.Equal(order is SortOrder.Desc ? SortOrder.Asc : SortOrder.Desc, distance.GeoDistance.Order);
        Assert.Equal(distance.GeoDistance.Order, script.Script.Order);
    }

    [Theory]
    [InlineData(null, "_first")]
    [InlineData("_last", "_first")]
    [InlineData("_first", "_last")]
    [InlineData("replacement", "replacement")]
    public void ReverseOrder_WithFieldMissingValues_ReversesPlacement(string? missing, string expected)
    {
        // Arrange
        SortOptions sort = new FieldSort { Field = "name", Order = SortOrder.Asc };
        if (missing is not null)
            sort.Field!.Missing = missing;

        // Act
        sort.ReverseOrder();

        // Assert
        Assert.Equal(SortOrder.Desc, sort.Field!.Order);
        Assert.Equal(expected, sort.Field.Missing);
    }

    [Theory]
    [InlineData(null, SortMode.Min)]
    [InlineData(SortOrder.Asc, SortMode.Min)]
    [InlineData(SortOrder.Desc, SortMode.Max)]
    public void ReverseOrder_WithImplicitFieldMode_PreservesSelectedValue(SortOrder? order, SortMode expected)
    {
        // Arrange
        SortOptions sort = new FieldSort { Field = "values", Order = order };

        // Act
        sort.ReverseOrder();

        // Assert
        Assert.Equal(expected, sort.Field!.Mode);
        Assert.Equal(order is SortOrder.Desc ? SortOrder.Asc : SortOrder.Desc, sort.Field.Order);
    }

    [Fact]
    public void ReverseOrder_WithNumericMissingValue_PreservesReplacement()
    {
        // Arrange
        SortOptions sort = new FieldSort { Field = "values", Missing = 42L, Mode = SortMode.Avg };

        // Act
        sort.ReverseOrder();

        // Assert
        Assert.Equal(42L, sort.Field!.Missing);
        Assert.Equal(SortMode.Avg, sort.Field.Mode);
    }

    [Fact]
    public void ReverseOrder_WithRepeatedReversal_RestoresOriginalSortSemantics()
    {
        // Arrange
        SortOptions sort = new FieldSort { Field = "values", Order = SortOrder.Asc };

        // Act
        sort.ReverseOrder();
        sort.ReverseOrder();

        // Assert
        Assert.Equal(SortOrder.Asc, sort.Field!.Order);
        Assert.Equal(SortMode.Min, sort.Field.Mode);
        Assert.Equal("_last", sort.Field.Missing);
    }

    [Theory]
    [InlineData(null, SortOrder.Asc)]
    [InlineData(SortOrder.Asc, SortOrder.Desc)]
    [InlineData(SortOrder.Desc, SortOrder.Asc)]
    public void ReverseOrder_WithScoreSort_UsesDescendingDefault(SortOrder? order, SortOrder expected)
    {
        // Arrange
        var sort = new SortOptions { Score = new ScoreSort { Order = order } };

        // Act
        sort.ReverseOrder();

        // Assert
        Assert.Equal(expected, sort.Score!.Order);
    }

    [Theory]
    [InlineData("_doc", SortOrder.Desc)]
    [InlineData("_score", SortOrder.Asc)]
    [InlineData("_shard_doc", SortOrder.Desc)]
    public void ReverseOrder_WithSpecialField_DoesNotAddFieldOnlyOptions(string field, SortOrder expected)
    {
        // Arrange
        SortOptions sort = new FieldSort { Field = field };

        // Act
        sort.ReverseOrder();

        // Assert
        Assert.Equal(expected, sort.Field!.Order);
        Assert.Null(sort.Field.Missing);
        Assert.Null(sort.Field.Mode);
    }
}
