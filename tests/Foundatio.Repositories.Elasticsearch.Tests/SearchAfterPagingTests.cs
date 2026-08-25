using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Utility;
using Foundatio.Serializer;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class SearchAfterPagingTests : ElasticRepositoryTestBase
{
    private readonly DailyLogEventWithNoCachingRepository _repository;

    public SearchAfterPagingTests(ITestOutputHelper output) : base(output)
    {
        _repository = new DailyLogEventWithNoCachingRepository(_configuration);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    [Fact]
    public async Task FindAsync_WithPointInTimeAndDuplicateIdsAcrossIndexes_SupportsBackwardNavigation()
    {
        string id = ObjectId.GenerateNewId().ToString();
        string companyId = ObjectId.GenerateNewId().ToString();
        var olderDate = new DateTimeOffset(DateTime.UtcNow.Date.AddDays(-1), TimeSpan.Zero);
        var newerDate = olderDate.AddDays(1);
        var olderDocument = LogEventGenerator.Generate(id: id, companyId: companyId, createdUtc: olderDate.UtcDateTime, date: olderDate);
        var newerDocument = LogEventGenerator.Generate(id: id, companyId: companyId, createdUtc: newerDate.UtcDateTime, date: newerDate);

        await _repository.AddAsync(olderDocument, o => o.ImmediateConsistency());
        await _repository.AddAsync(newerDocument, o => o.ImmediateConsistency());

        var pointInTime = (ISupportPointInTime)_repository;
        string? pointInTimeId = null;

        try
        {
            var firstPage = await _repository.FindAsync(
                q => q.Index(olderDate.UtcDateTime, newerDate.UtcDateTime).SortAscending(e => e.CompanyId),
                o => o.PageLimit(1).SearchAfterPaging(SearchAfterPagingMode.PointInTime));
            pointInTimeId = firstPage.GetPointInTimeId();
            Assert.False(String.IsNullOrEmpty(pointInTimeId));
            string? searchAfterToken = firstPage.GetSearchAfterToken();
            Assert.False(String.IsNullOrEmpty(searchAfterToken));
            string firstIndex = Assert.IsType<string>(Assert.Single(firstPage.Hits).GetIndex());

            var secondPage = await _repository.FindAsync(
                q => q.Index(olderDate.UtcDateTime, newerDate.UtcDateTime).SortAscending(e => e.CompanyId),
                o => o.PageLimit(1)
                    .SearchAfterPaging(SearchAfterPagingMode.PointInTime)
                    .PointInTimeId(pointInTimeId)
                    .SearchAfterToken(searchAfterToken, _serializer));
            pointInTimeId = secondPage.GetPointInTimeId();
            Assert.False(String.IsNullOrEmpty(pointInTimeId));
            string? searchBeforeToken = secondPage.GetSearchBeforeToken();
            Assert.False(String.IsNullOrEmpty(searchBeforeToken));
            string secondIndex = Assert.IsType<string>(Assert.Single(secondPage.Hits).GetIndex());
            Assert.NotEqual(firstIndex, secondIndex);

            var backToFirstPage = await _repository.FindAsync(
                q => q.Index(olderDate.UtcDateTime, newerDate.UtcDateTime).SortAscending(e => e.CompanyId),
                o => o.PageLimit(1)
                    .SearchAfterPaging(SearchAfterPagingMode.PointInTime)
                    .PointInTimeId(pointInTimeId)
                    .SearchBeforeToken(searchBeforeToken, _serializer));
            pointInTimeId = backToFirstPage.GetPointInTimeId();
            Assert.False(String.IsNullOrEmpty(pointInTimeId));

            Assert.Equal(firstIndex, Assert.Single(backToFirstPage.Hits).GetIndex());
        }
        finally
        {
            if (!String.IsNullOrEmpty(pointInTimeId))
                await pointInTime.ClosePointInTimeAsync(pointInTimeId);
        }
    }
}

/// <summary>
/// Pins the cursor semantics of the <c>SearchAfter</c>/<c>SearchBefore</c> option extensions: a null
/// array reference or an empty array clears any stored cursor, while arrays containing values are
/// stored as-is. Clearing via an all-null cursor and preserving mixed-null cursors is pinned by
/// <c>SearchAfterQueryExtensionsTests</c>; the token variants have no such filter -- they round-trip
/// whatever sort values the previous page carried, all-null cursors included. These tests need no
/// Elasticsearch connection.
/// </summary>
public sealed class SearchAfterQueryExtensionTests
{
    private static readonly ITextSerializer Serializer = new SystemTextJsonSerializer();

    [Fact]
    public void SearchAfter_WithValues_EnablesPagingAndSetsCursor()
    {
        var options = new CommandOptions<Employee>().SearchAfter("a", "b");

        Assert.True(options.ShouldUseSearchAfterPaging());
        Assert.True(options.HasSearchAfter());
        Assert.Equal(new object[] { "a", "b" }, options.GetSearchAfter());
    }

    [Fact]
    public void SearchAfter_WithNullArrayReference_ClearsCursor()
    {
        var options = new CommandOptions<Employee>().SearchAfter("a").SearchAfter(null!);

        Assert.False(options.HasSearchAfter());
    }

    [Fact]
    public void SearchAfter_WithEmptyArray_ClearsCursor()
    {
        var options = new CommandOptions<Employee>().SearchAfter("a").SearchAfter();

        Assert.False(options.HasSearchAfter());
    }

    [Fact]
    public void SearchBefore_WithValues_EnablesPagingAndSetsCursor()
    {
        var options = new CommandOptions<Employee>().SearchBefore("a", "b");

        Assert.True(options.ShouldUseSearchAfterPaging());
        Assert.True(options.HasSearchBefore());
        Assert.Equal(new object[] { "a", "b" }, options.GetSearchBefore());
    }

    [Fact]
    public void SearchBefore_WithNullArrayReference_ClearsCursor()
    {
        var options = new CommandOptions<Employee>().SearchBefore("a").SearchBefore(null!);

        Assert.False(options.HasSearchBefore());
    }

    [Fact]
    public void SearchBeforeToken_WithAllNullValues_SetsCursorUnlikeRawPath()
    {
        // The token path must accept cursors the raw path clears (all-null cursors are removed by
        // SearchAfter/SearchBefore): tokens round-trip the exact sort values of the hit being
        // paged from.
        string token = EncodeToken(new object[] { null! });

        var options = new CommandOptions<Employee>().SearchAfter("a").SearchAfterToken(token, Serializer);

        Assert.True(options.HasSearchAfter());
        Assert.Equal(new object?[] { null }, options.GetSearchAfter());
    }

    [Fact]
    public void SearchAfterToken_WithNullToken_ClearsCursor()
    {
        var options = new CommandOptions<Employee>().SearchAfter("a").SearchAfterToken(null, Serializer);

        Assert.False(options.HasSearchAfter());
    }

    [Fact]
    public void SearchAfterPaging_Disabled_ResetsModeToLive()
    {
        var options = new CommandOptions<Employee>().SearchAfterPaging(SearchAfterPagingMode.PointInTime).SearchAfterPaging(false);

        Assert.False(options.ShouldUseSearchAfterPaging());
        Assert.Equal(SearchAfterPagingMode.Live, options.GetSearchAfterPagingMode());
    }

    private static string EncodeToken(object[] values)
    {
        string json = Serializer.SerializeToString(values);
        return Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(json))
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }
}
