using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Serializer;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class SearchAfterQueryExtensionsTests
{
    private static readonly ITextSerializer Serializer = new SystemTextJsonSerializer();

    private static CommandOptions<Employee> CreateActivePagingSession()
    {
        var options = new CommandOptions<Employee>()
            .SearchAfterPaging(SearchAfterPagingMode.PointInTime)
            .RepositoryOwnedPointInTimeId("pit-id");
        options.Values.Set(SearchAfterQueryExtensions.SearchAfterKey, new object?[] { "after" });
        options.Values.Set(SearchAfterQueryExtensions.SearchBeforeKey, new object?[] { "before" });
        options.Values.Set(SearchAfterQueryExtensions.UnstableSortWarnedKey, true);
        return options;
    }

    private static void AssertPagingSessionCleared(CommandOptions<Employee> options)
    {
        Assert.False(options.ShouldUseSearchAfterPaging());
        Assert.Equal(SearchAfterPagingMode.Live, options.GetSearchAfterPagingMode());
        Assert.False(options.HasSearchAfter());
        Assert.False(options.HasSearchBefore());
        Assert.False(options.HasPointInTimeId());
        Assert.False(options.IsRepoOwnedPointInTime());
        Assert.False(options.SafeGetOption<bool>(SearchAfterQueryExtensions.UnstableSortWarnedKey, false));
    }

    private static string EncodeToken(object?[] values)
    {
        string json = Serializer.SerializeToString(values);
        return Convert.ToBase64String(Encoding.UTF8.GetBytes(json))
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }

    [Fact]
    public void CursorArrayReturnTypes_RemainSourceCompatible()
    {
        // Arrange
        var afterOptions = new CommandOptions<Employee>().SearchAfter("after");
        var beforeOptions = new CommandOptions<Employee>().SearchBefore("before");
        var hit = new FindHit<Employee>(null, null, 0);
        hit.Data[ElasticDataKeys.Sorts] = new object?[] { null };

        // Act
        object[]? after = afterOptions.GetSearchAfter();
        object[]? before = beforeOptions.GetSearchBefore();
        object[]? decoded = FindHitExtensions.DecodeSortToken(EncodeToken([null]), Serializer);
        object[]? sorts = hit.GetSorts();

        // Assert
        Assert.Equal(new object[] { "after" }, after);
        Assert.Equal(new object[] { "before" }, before);
        Assert.Equal(new object?[] { null }, decoded);
        Assert.Equal(new object?[] { null }, sorts);
    }

    [Fact]
    public async Task PageableQueryBuilder_WithDisabledPaging_IgnoresStaleCursor()
    {
        // Arrange
        var options = new CommandOptions<Employee>().SearchAfterPaging(false);
        options.Values.Set(SearchAfterQueryExtensions.SearchAfterKey, new object?[] { "stale" });
        var context = new QueryBuilderContext<Employee>(new RepositoryQuery<Employee>(), options);

        // Act
        await new PageableQueryBuilder().BuildAsync(context);
        SearchRequest request = context.Search;

        // Assert
        Assert.Null(request.SearchAfter);
    }

    [Fact]
    public void PointInTimeId_WhenReplacingRepositoryOwnedId_MarksReplacementAsCallerOwned()
    {
        // Arrange
        var options = CreateActivePagingSession();

        // Act
        options.PointInTimeId("caller-owned-pit-id");

        // Assert
        Assert.Equal("caller-owned-pit-id", options.GetPointInTimeId());
        Assert.False(options.IsRepoOwnedPointInTime());
    }

    [Fact]
    public void SearchAfterPagingMode_Disabled_ClearsPagingSession()
    {
        // Arrange
        var options = CreateActivePagingSession();

        // Act
        options.SearchAfterPaging(SearchAfterPagingMode.PointInTime, false);

        // Assert
        AssertPagingSessionCleared(options);
    }

    [Fact]
    public void SearchAfterPagingMode_WhenModeChangesToLive_ClearsPagingSessionState()
    {
        // Arrange
        var options = CreateActivePagingSession();

        // Act
        options.SearchAfterPaging(SearchAfterPagingMode.Live);

        // Assert
        Assert.True(options.ShouldUseSearchAfterPaging());
        Assert.Equal(SearchAfterPagingMode.Live, options.GetSearchAfterPagingMode());
        Assert.False(options.HasSearchAfter());
        Assert.False(options.HasSearchBefore());
        Assert.False(options.HasPointInTimeId());
        Assert.False(options.IsRepoOwnedPointInTime());
        Assert.False(options.SafeGetOption<bool>(SearchAfterQueryExtensions.UnstableSortWarnedKey, false));
    }

    [Fact]
    public void SearchAfterPagingMode_WhenModeChangesToPointInTime_ClearsPagingSessionState()
    {
        // Arrange
        var options = new CommandOptions<Employee>()
            .SearchAfterPaging(SearchAfterPagingMode.Live)
            .SearchAfter("after")
            .RepositoryOwnedPointInTimeId("stale-pit-id");
        options.Values.Set(SearchAfterQueryExtensions.UnstableSortWarnedKey, true);

        // Act
        options.SearchAfterPaging(SearchAfterPagingMode.PointInTime);

        // Assert
        Assert.True(options.ShouldUseSearchAfterPaging());
        Assert.Equal(SearchAfterPagingMode.PointInTime, options.GetSearchAfterPagingMode());
        Assert.False(options.HasSearchAfter());
        Assert.False(options.HasPointInTimeId());
        Assert.False(options.IsRepoOwnedPointInTime());
        Assert.False(options.SafeGetOption<bool>(SearchAfterQueryExtensions.UnstableSortWarnedKey, false));
    }

    [Fact]
    public void SearchAfterPagingMode_WhenModeIsUnchanged_PreservesPagingSessionState()
    {
        // Arrange
        var options = CreateActivePagingSession();

        // Act
        options.SearchAfterPaging(SearchAfterPagingMode.PointInTime);

        // Assert
        Assert.True(options.HasSearchAfter());
        Assert.True(options.HasSearchBefore());
        Assert.True(options.HasPointInTimeId());
        Assert.True(options.IsRepoOwnedPointInTime());
        Assert.True(options.SafeGetOption<bool>(SearchAfterQueryExtensions.UnstableSortWarnedKey, false));
    }

    [Fact]
    public void SearchAfterPaging_Disabled_ClearsPagingSession()
    {
        // Arrange
        var options = CreateActivePagingSession();

        // Act
        options.SearchAfterPaging(false);

        // Assert
        AssertPagingSessionCleared(options);
    }

    [Fact]
    public void SearchAfterPaging_WhenReenabledAfterReset_StartsCleanSession()
    {
        // Arrange
        var options = CreateActivePagingSession();
        options.SearchAfterPaging(false);

        // Act
        options.SearchAfterPaging(SearchAfterPagingMode.PointInTime);

        // Assert
        Assert.True(options.ShouldUseSearchAfterPaging());
        Assert.Equal(SearchAfterPagingMode.PointInTime, options.GetSearchAfterPagingMode());
        Assert.False(options.HasSearchAfter());
        Assert.False(options.HasSearchBefore());
        Assert.False(options.HasPointInTimeId());
        Assert.False(options.IsRepoOwnedPointInTime());
    }

    [Fact]
    public void SearchAfterToken_WithAllNullValues_SetsCursorUnlikeRawPath()
    {
        // Arrange
        string token = EncodeToken([null]);

        // Act
        var options = new CommandOptions<Employee>().SearchAfter("a").SearchAfterToken(token, Serializer);

        // Assert
        Assert.True(options.HasSearchAfter());
        Assert.Equal(new object?[] { null }, options.GetSearchAfter());
    }

    [Fact]
    public void SearchAfterToken_WithEmptyToken_ClearsBothCursors()
    {
        // Arrange
        var options = new CommandOptions<Employee>().SearchBefore("before");

        // Act
        options.SearchAfterToken(String.Empty, Serializer);

        // Assert
        Assert.False(options.HasSearchAfter());
        Assert.False(options.HasSearchBefore());
    }

    [Fact]
    public void SearchAfterToken_WithExistingBeforeCursor_SelectsAfterDirection()
    {
        // Arrange / Act
        var options = new CommandOptions<Employee>().SearchBefore("before").SearchAfterToken(EncodeToken(["after"]), Serializer);

        // Assert
        Assert.True(options.HasSearchAfter());
        Assert.False(options.HasSearchBefore());
    }

    [Fact]
    public void SearchAfterToken_WithMalformedToken_Throws()
    {
        // Arrange
        var options = new CommandOptions<Employee>();

        // Act / Assert
        Assert.Throws<JsonException>(() => options.SearchAfterToken("not-a-token", Serializer));
    }

    [Fact]
    public void SearchAfterToken_WithNullSerializer_ThrowsArgumentNullException()
    {
        // Arrange
        var options = new CommandOptions<Employee>();

        // Act / Assert
        Assert.Throws<ArgumentNullException>(() => options.SearchAfterToken("token", null!));
    }

    [Fact]
    public void SearchAfterToken_WithNullToken_ClearsBothCursors()
    {
        // Arrange / Act
        var options = new CommandOptions<Employee>().SearchBefore("before").SearchAfterToken(null, Serializer);

        // Assert
        Assert.False(options.HasSearchAfter());
        Assert.False(options.HasSearchBefore());
    }

    [Fact]
    public void SearchAfter_WithAllNullValues_ClearsCursor()
    {
        // Arrange
        var options = new CommandOptions<Employee>().SearchAfter("existing");

        // Act
        options.SearchAfter(new object?[] { null, null });

        // Assert
        Assert.False(options.HasSearchAfter());
    }

    [Fact]
    public void SearchAfter_WithEmptyArray_ClearsCursor()
    {
        // Arrange / Act
        var options = new CommandOptions<Employee>().SearchAfter("a").SearchAfter();

        // Assert
        Assert.False(options.HasSearchAfter());
    }

    [Fact]
    public void SearchAfter_WithExistingBeforeCursor_SelectsAfterDirection()
    {
        // Arrange / Act
        var options = new CommandOptions<Employee>().SearchBefore("before").SearchAfter("after");

        // Assert
        Assert.True(options.HasSearchAfter());
        Assert.False(options.HasSearchBefore());
    }

    [Fact]
    public void SearchAfter_WithMixedNullValues_PreservesCursor()
    {
        // Arrange
        var options = new CommandOptions<Employee>();

        // Act
        options.SearchAfter(new object?[] { "value", null });

        // Assert
        Assert.Equal(new object?[] { "value", null }, options.GetSearchAfter());
    }

    [Fact]
    public void SearchAfter_WithNullArrayReference_ClearsCursor()
    {
        // Arrange / Act
        var options = new CommandOptions<Employee>().SearchAfter("a").SearchAfter(null);

        // Assert
        Assert.False(options.HasSearchAfter());
    }

    [Fact]
    public void SearchAfter_WithValues_EnablesPagingAndSetsCursor()
    {
        // Arrange / Act
        var options = new CommandOptions<Employee>().SearchAfter("a", "b");

        // Assert
        Assert.True(options.ShouldUseSearchAfterPaging());
        Assert.True(options.HasSearchAfter());
        Assert.Equal(new object?[] { "a", "b" }, options.GetSearchAfter());
    }

    [Fact]
    public void SearchBeforeToken_WithExistingAfterCursor_SelectsBeforeDirection()
    {
        // Arrange / Act
        var options = new CommandOptions<Employee>().SearchAfter("after").SearchBeforeToken(EncodeToken(["before"]), Serializer);

        // Assert
        Assert.False(options.HasSearchAfter());
        Assert.True(options.HasSearchBefore());
    }

    [Fact]
    public void SearchBeforeToken_WithNullSerializer_ThrowsArgumentNullException()
    {
        // Arrange
        var options = new CommandOptions<Employee>();

        // Act / Assert
        Assert.Throws<ArgumentNullException>(() => options.SearchBeforeToken("token", null!));
    }

    [Fact]
    public void SearchBefore_WithAllNullValues_ClearsCursor()
    {
        // Arrange
        var options = new CommandOptions<Employee>().SearchBefore("existing");

        // Act
        options.SearchBefore(new object?[] { null, null });

        // Assert
        Assert.False(options.HasSearchBefore());
    }

    [Fact]
    public void SearchBefore_WithExistingAfterCursor_SelectsBeforeDirection()
    {
        // Arrange / Act
        var options = new CommandOptions<Employee>().SearchAfter("after").SearchBefore("before");

        // Assert
        Assert.False(options.HasSearchAfter());
        Assert.True(options.HasSearchBefore());
    }

    [Fact]
    public void SearchBefore_WithMixedNullValues_PreservesCursor()
    {
        // Arrange
        var options = new CommandOptions<Employee>();

        // Act
        options.SearchBefore(new object?[] { "value", null });

        // Assert
        Assert.Equal(new object?[] { "value", null }, options.GetSearchBefore());
    }

    [Fact]
    public void SearchBefore_WithNullArrayReference_ClearsCursor()
    {
        // Arrange / Act
        var options = new CommandOptions<Employee>().SearchBefore("a").SearchBefore(null);

        // Assert
        Assert.False(options.HasSearchBefore());
    }

    [Fact]
    public void SearchBefore_WithValues_EnablesPagingAndSetsCursor()
    {
        // Arrange / Act
        var options = new CommandOptions<Employee>().SearchBefore("a", "b");

        // Assert
        Assert.True(options.ShouldUseSearchAfterPaging());
        Assert.True(options.HasSearchBefore());
        Assert.Equal(new object?[] { "a", "b" }, options.GetSearchBefore());
    }
}
