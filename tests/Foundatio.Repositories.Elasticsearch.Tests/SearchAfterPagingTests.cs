using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Utility;
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
    public async Task CountAsync_WithPointInTimeSearchAfterPaging_ThrowsBeforeRequest()
    {
        // Arrange
        var options = new CommandOptions<LogEvent>().SearchAfterPaging(SearchAfterPagingMode.PointInTime);

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => _repository.CountAsync(new RepositoryQuery<LogEvent>(), options));

        // Assert
        Assert.Contains(nameof(_repository.CountAsync), exception.Message);
    }

    [Fact]
    public async Task ExistsAsync_WithEmptyIdAndPointInTimeSearchAfterPaging_ThrowsBeforeRequest()
    {
        // Arrange
        var options = new CommandOptions<LogEvent>().SearchAfterPaging(SearchAfterPagingMode.PointInTime);

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => _repository.ExistsAsync(String.Empty, options));

        // Assert
        Assert.Contains(nameof(_repository.ExistsAsync), exception.Message);
    }

    [Fact]
    public async Task ExistsAsync_WithIdAndPointInTimeSearchAfterPaging_ThrowsBeforeRequest()
    {
        // Arrange
        var options = new CommandOptions<LogEvent>().SearchAfterPaging(SearchAfterPagingMode.PointInTime);

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => _repository.ExistsAsync(ObjectId.GenerateNewId().ToString(), options));

        // Assert
        Assert.Contains(nameof(_repository.ExistsAsync), exception.Message);
    }

    [Fact]
    public async Task ExistsAsync_WithPointInTimeSearchAfterPaging_ThrowsBeforeRequest()
    {
        // Arrange
        var options = new CommandOptions<LogEvent>().SearchAfterPaging(SearchAfterPagingMode.PointInTime);

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => _repository.ExistsAsync(new RepositoryQuery<LogEvent>(), options));

        // Assert
        Assert.Contains(nameof(_repository.ExistsAsync), exception.Message);
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

    [Fact]
    public async Task FindAsync_WithSnapshotAndLiveSearchAfterPaging_ThrowsBeforeRequest()
    {
        // Arrange
        var options = new CommandOptions<LogEvent>()
            .SnapshotPaging()
            .SearchAfterPaging(SearchAfterPagingMode.Live);

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => _repository.FindAsync(new RepositoryQuery<LogEvent>(), options));

        // Assert
        Assert.Contains("cannot be used together", exception.Message);
    }

    [Fact]
    public async Task FindAsync_WithSnapshotAndPointInTimeSearchAfterPaging_ThrowsBeforeRequest()
    {
        // Arrange
        var options = new CommandOptions<LogEvent>()
            .SnapshotPaging()
            .SearchAfterPaging(SearchAfterPagingMode.PointInTime);

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => _repository.FindAsync(new RepositoryQuery<LogEvent>(), options));

        // Assert
        Assert.Contains("cannot be used together", exception.Message);
    }

    [Fact]
    public async Task FindOneAsync_WithPointInTimeSearchAfterPaging_ThrowsBeforeRequest()
    {
        // Arrange
        var options = new CommandOptions<LogEvent>().SearchAfterPaging(SearchAfterPagingMode.PointInTime);

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => _repository.FindOneAsync(new RepositoryQuery<LogEvent>(), options));

        // Assert
        Assert.Contains(nameof(_repository.FindOneAsync), exception.Message);
    }
}
