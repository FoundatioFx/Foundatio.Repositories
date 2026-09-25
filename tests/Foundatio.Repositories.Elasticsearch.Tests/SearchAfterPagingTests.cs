using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NextPageAsync_WithClosedPointInTime_ThrowsWithoutTruncatingResults(bool callerOwned)
    {
        await _repository.AddAsync(LogEventGenerator.Generate(), o => o.ImmediateConsistency());
        await _repository.AddAsync(LogEventGenerator.Generate(), o => o.ImmediateConsistency());
        var stats = await _client.Nodes.StatsAsync(cancellationToken: TestCancellationToken);
        Assert.True(stats.IsValidResponse);
        long baseline = stats.Nodes.Values.Sum(n => Assert.IsType<long>(n.Indices?.Search?.OpenContexts));
        var options = new CommandOptions<LogEvent>().PageLimit(1).SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        if (callerOwned)
        {
            var opened = await _client.OpenPointInTimeAsync("daily-logevents", p => p.KeepAlive("1m"), TestCancellationToken);
            Assert.True(opened.IsValidResponse);
            options.PointInTimeId(opened.Id);
        }

        try
        {
            var results = await _repository.FindAsync(new RepositoryQuery<LogEvent>(), options);
            Assert.True(results.HasMore);
            stats = await _client.Nodes.StatsAsync(cancellationToken: TestCancellationToken);
            Assert.True(stats.IsValidResponse);
            Assert.True(stats.Nodes.Values.Sum(n => Assert.IsType<long>(n.Indices?.Search?.OpenContexts)) > baseline);
            Assert.True(await _repository.ClosePointInTimeAsync(options.GetPointInTimeId()));

            var exception = await Assert.ThrowsAsync<DocumentException>(() => results.NextPageAsync());

            Assert.Contains("Status code 404", exception.Message);
            Assert.Contains("search_phase_execution_exception", exception.Message);
            Assert.True(results.HasMore);
            Assert.Single(results.Documents);
            stats = await _client.Nodes.StatsAsync(cancellationToken: TestCancellationToken);
            Assert.True(stats.IsValidResponse);
            Assert.Equal(baseline, stats.Nodes.Values.Sum(n => Assert.IsType<long>(n.Indices?.Search?.OpenContexts)));
        }
        finally
        {
            await _repository.ClosePointInTimeAsync(options.GetPointInTimeId());
        }
    }

    [Fact]
    public async Task CountAsync_WhenBeforeQueryEnablesPointInTimeSearchAfterPaging_ThrowsBeforeRequest()
    {
        // Arrange
        _repository.BeforeQuery.AddHandler((_, args) =>
        {
            args.Options.SearchAfterPaging(SearchAfterPagingMode.PointInTime);
            return Task.CompletedTask;
        });

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() =>
            _repository.CountAsync(new RepositoryQuery<LogEvent>(), new CommandOptions<LogEvent>()));

        // Assert
        Assert.Contains(nameof(_repository.CountAsync), exception.Message);
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
    public async Task ExistsAsync_WhenBeforeQueryEnablesPointInTimeSearchAfterPaging_ThrowsBeforeRequest()
    {
        // Arrange
        _repository.BeforeQuery.AddHandler((_, args) =>
        {
            args.Options.SearchAfterPaging(SearchAfterPagingMode.PointInTime);
            return Task.CompletedTask;
        });

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() =>
            _repository.ExistsAsync(new RepositoryQuery<LogEvent>(), new CommandOptions<LogEvent>()));

        // Assert
        Assert.Contains(nameof(_repository.ExistsAsync), exception.Message);
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
    public async Task FindAsync_WhenBeforeQueryDisablesPointInTimeSearchAfterPaging_DoesNotUsePointInTime()
    {
        // Arrange
        await _repository.AddAsync(LogEventGenerator.Generate(), o => o.ImmediateConsistency());
        _repository.BeforeQuery.AddHandler((_, args) =>
        {
            args.Options.SearchAfterPaging(false);
            return Task.CompletedTask;
        });
        var options = new CommandOptions<LogEvent>().SearchAfterPaging(SearchAfterPagingMode.PointInTime);

        // Act
        var results = await _repository.FindAsync(new RepositoryQuery<LogEvent>(), options);

        // Assert
        Assert.Null(results.GetPointInTimeId());
        Assert.False(options.ShouldUseSearchAfterPaging());
    }

    [Fact]
    public async Task FindAsync_WhenBeforeQueryEnablesPointInTimeSearchAfterPaging_UsesPointInTime()
    {
        // Arrange
        await _repository.AddAsync(LogEventGenerator.Generate(), o => o.ImmediateConsistency());
        await _repository.AddAsync(LogEventGenerator.Generate(), o => o.ImmediateConsistency());
        _repository.BeforeQuery.AddHandler((_, args) =>
        {
            args.Options.SearchAfterPaging(SearchAfterPagingMode.PointInTime);
            return Task.CompletedTask;
        });
        var options = new CommandOptions<LogEvent>().PageLimit(1);
        FindResults<LogEvent>? results = null;

        try
        {
            // Act
            results = await _repository.FindAsync(new RepositoryQuery<LogEvent>(), options);

            // Assert
            Assert.True(results.HasMore);
            Assert.False(String.IsNullOrEmpty(results.GetPointInTimeId()));
            Assert.True(options.ShouldUseSearchAfterPagingPointInTime());
        }
        finally
        {
            if (results is not null)
                await ((ISupportPointInTime)_repository).ClosePointInTimeAsync(results);
        }
    }

    [Theory]
    [InlineData(SearchAfterPagingMode.Live)]
    [InlineData(SearchAfterPagingMode.PointInTime)]
    public async Task FindAsync_WithAsyncQueryAndSearchAfterPaging_ThrowsBeforeRequest(SearchAfterPagingMode mode)
    {
        // Arrange
        var options = new CommandOptions<LogEvent>()
            .AsyncQuery(TimeSpan.Zero)
            .SearchAfterPaging(mode);

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() =>
            _repository.FindAsync(new RepositoryQuery<LogEvent>(), options));

        // Assert
        Assert.Contains("async", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(SearchAfterPagingMode.Live)]
    [InlineData(SearchAfterPagingMode.PointInTime)]
    public async Task FindAsync_WithAsyncQueryIdAndSearchAfterPaging_ThrowsBeforeRequest(SearchAfterPagingMode mode)
    {
        // Arrange
        var options = new CommandOptions<LogEvent>()
            .AsyncQueryId("async-query-id")
            .SearchAfterPaging(mode);

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() =>
            _repository.FindAsync(new RepositoryQuery<LogEvent>(), options));

        // Assert
        Assert.Contains("async", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task FindAsync_WithPointInTimeAndDuplicateIdsAcrossIndexes_SupportsBackwardNavigation()
    {
        // Arrange
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

        // Act / Assert
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
    public async Task FindOneAsync_WhenBeforeQueryEnablesPointInTimeSearchAfterPaging_ThrowsBeforeRequest()
    {
        // Arrange
        _repository.BeforeQuery.AddHandler((_, args) =>
        {
            args.Options.SearchAfterPaging(SearchAfterPagingMode.PointInTime);
            return Task.CompletedTask;
        });

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() =>
            _repository.FindOneAsync(new RepositoryQuery<LogEvent>(), new CommandOptions<LogEvent>()));

        // Assert
        Assert.Contains(nameof(_repository.FindOneAsync), exception.Message);
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
