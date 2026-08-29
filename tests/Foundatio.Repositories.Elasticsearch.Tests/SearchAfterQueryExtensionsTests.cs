using System;
using System.Text;
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
            .PointInTimeId("pit-id")
            .RepoOwnedPointInTime();
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
        var afterOptions = new CommandOptions<Employee>().SearchAfter("after");
        var beforeOptions = new CommandOptions<Employee>().SearchBefore("before");
        var hit = new FindHit<Employee>(null, null, 0);
        hit.Data[ElasticDataKeys.Sorts] = new object?[] { null };

        object[]? after = afterOptions.GetSearchAfter();
        object[]? before = beforeOptions.GetSearchBefore();
        object[]? decoded = FindHitExtensions.DecodeSortToken(EncodeToken([null]), Serializer);
        object[]? sorts = hit.GetSorts();

        Assert.Equal(new object[] { "after" }, after);
        Assert.Equal(new object[] { "before" }, before);
        Assert.Equal(new object?[] { null }, decoded);
        Assert.Equal(new object?[] { null }, sorts);
    }

    [Fact]
    public async Task PageableQueryBuilder_WithDisabledPaging_IgnoresStaleCursor()
    {
        var options = new CommandOptions<Employee>().SearchAfterPaging(false);
        options.Values.Set(SearchAfterQueryExtensions.SearchAfterKey, new object?[] { "stale" });
        var context = new QueryBuilderContext<Employee>(new RepositoryQuery<Employee>(), options);

        await new PageableQueryBuilder().BuildAsync(context);
        SearchRequest request = context.Search;

        Assert.Null(request.SearchAfter);
    }

    [Fact]
    public void SearchAfterPagingMode_Disabled_ClearsPagingSession()
    {
        var options = CreateActivePagingSession();

        options.SearchAfterPaging(SearchAfterPagingMode.PointInTime, false);

        AssertPagingSessionCleared(options);
    }

    [Fact]
    public void SearchAfterPagingMode_WhenModeChangesFromLiveToPointInTime_ClearsPagingSessionState()
    {
        // Arrange
        var options = new CommandOptions<Employee>()
            .SearchAfterPaging(SearchAfterPagingMode.Live)
            .SearchAfter("after")
            .PointInTimeId("stale-pit-id")
            .RepoOwnedPointInTime();
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
    public void SearchAfterPagingMode_WhenModeChanges_ClearsPagingSessionState()
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
        var options = CreateActivePagingSession();

        options.SearchAfterPaging(false);

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
        string token = EncodeToken([null]);

        var options = new CommandOptions<Employee>().SearchAfter("a").SearchAfterToken(token, Serializer);

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
        var options = new CommandOptions<Employee>().SearchBefore("before").SearchAfterToken(EncodeToken(["after"]), Serializer);

        Assert.True(options.HasSearchAfter());
        Assert.False(options.HasSearchBefore());
    }

    [Fact]
    public void SearchAfterToken_WithMalformedToken_Throws()
    {
        // Arrange
        var options = new CommandOptions<Employee>();

        // Act / Assert
        Assert.ThrowsAny<Exception>(() => options.SearchAfterToken("not-a-token", Serializer));
    }

    [Fact]
    public void SearchAfterToken_WithNullSerializer_ThrowsArgumentNullException()
    {
        var options = new CommandOptions<Employee>();

        Assert.Throws<ArgumentNullException>(() => options.SearchAfterToken("token", null!));
    }

    [Fact]
    public void SearchAfterToken_WithNullToken_ClearsBothCursors()
    {
        var options = new CommandOptions<Employee>().SearchBefore("before").SearchAfterToken(null, Serializer);

        Assert.False(options.HasSearchAfter());
        Assert.False(options.HasSearchBefore());
    }

    [Fact]
    public void SearchAfter_WithAllNullValues_ClearsCursor()
    {
        var options = new CommandOptions<Employee>().SearchAfter("existing");

        options.SearchAfter(new object?[] { null, null });

        Assert.False(options.HasSearchAfter());
    }

    [Fact]
    public void SearchAfter_WithEmptyArray_ClearsCursor()
    {
        var options = new CommandOptions<Employee>().SearchAfter("a").SearchAfter();

        Assert.False(options.HasSearchAfter());
    }

    [Fact]
    public void SearchAfter_WithExistingBeforeCursor_SelectsAfterDirection()
    {
        var options = new CommandOptions<Employee>().SearchBefore("before").SearchAfter("after");

        Assert.True(options.HasSearchAfter());
        Assert.False(options.HasSearchBefore());
    }

    [Fact]
    public void SearchAfter_WithMixedNullValues_PreservesCursor()
    {
        var options = new CommandOptions<Employee>();

        options.SearchAfter(new object?[] { "value", null });

        Assert.Equal(new object?[] { "value", null }, options.GetSearchAfter());
    }

    [Fact]
    public void SearchAfter_WithNullArrayReference_ClearsCursor()
    {
        var options = new CommandOptions<Employee>().SearchAfter("a").SearchAfter(null);

        Assert.False(options.HasSearchAfter());
    }

    [Fact]
    public void SearchAfter_WithValues_EnablesPagingAndSetsCursor()
    {
        var options = new CommandOptions<Employee>().SearchAfter("a", "b");

        Assert.True(options.ShouldUseSearchAfterPaging());
        Assert.True(options.HasSearchAfter());
        Assert.Equal(new object?[] { "a", "b" }, options.GetSearchAfter());
    }

    [Fact]
    public void SearchBeforeToken_WithExistingAfterCursor_SelectsBeforeDirection()
    {
        var options = new CommandOptions<Employee>().SearchAfter("after").SearchBeforeToken(EncodeToken(["before"]), Serializer);

        Assert.False(options.HasSearchAfter());
        Assert.True(options.HasSearchBefore());
    }

    [Fact]
    public void SearchBeforeToken_WithNullSerializer_ThrowsArgumentNullException()
    {
        var options = new CommandOptions<Employee>();

        Assert.Throws<ArgumentNullException>(() => options.SearchBeforeToken("token", null!));
    }

    [Fact]
    public void SearchBefore_WithAllNullValues_ClearsCursor()
    {
        var options = new CommandOptions<Employee>().SearchBefore("existing");

        options.SearchBefore(new object?[] { null, null });

        Assert.False(options.HasSearchBefore());
    }

    [Fact]
    public void SearchBefore_WithExistingAfterCursor_SelectsBeforeDirection()
    {
        var options = new CommandOptions<Employee>().SearchAfter("after").SearchBefore("before");

        Assert.False(options.HasSearchAfter());
        Assert.True(options.HasSearchBefore());
    }

    [Fact]
    public void SearchBefore_WithMixedNullValues_PreservesCursor()
    {
        var options = new CommandOptions<Employee>();

        options.SearchBefore(new object?[] { "value", null });

        Assert.Equal(new object?[] { "value", null }, options.GetSearchBefore());
    }

    [Fact]
    public void SearchBefore_WithNullArrayReference_ClearsCursor()
    {
        var options = new CommandOptions<Employee>().SearchBefore("a").SearchBefore(null);

        Assert.False(options.HasSearchBefore());
    }

    [Fact]
    public void SearchBefore_WithValues_EnablesPagingAndSetsCursor()
    {
        var options = new CommandOptions<Employee>().SearchBefore("a", "b");

        Assert.True(options.ShouldUseSearchAfterPaging());
        Assert.True(options.HasSearchBefore());
        Assert.Equal(new object?[] { "a", "b" }, options.GetSearchBefore());
    }
}
