using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Options;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class SearchAfterRequestTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FindAsync_WithExpiredPointInTime_ThrowsAndRespectsOwnership(bool callerOwned)
    {
        using var invoker = new StubInvoker(endpoint => GetPointInTimeResponse(endpoint, ErrorResponse, 404));
        using var configuration = new StubConfiguration(invoker);
        using var index = new Index<NonIdentityDocument>(configuration, "test-index");
        using var repository = new StubRepository(index);
        var options = new CommandOptions<NonIdentityDocument>().SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        if (callerOwned)
            options.PointInTimeId("caller-pit");

        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.FindAsync(new RepositoryQuery<NonIdentityDocument>(), options));

        Assert.Contains("expected failure", exception.Message);
        if (callerOwned)
            Assert.Single(invoker.Requests);
        else
            AssertClosed(invoker, "opened-pit");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FindAsync_WithMissingIndex_ReturnsEmptyResults(bool livePaging)
    {
        using var invoker = new StubInvoker(_ => (ErrorResponse, 404));
        using var configuration = new StubConfiguration(invoker);
        using var index = new Index<NonIdentityDocument>(configuration, "test-index");
        using var repository = new StubRepository(index);
        var options = new CommandOptions<NonIdentityDocument>().SearchAfterPaging(livePaging);

        var result = await repository.FindAsync(new RepositoryQuery<NonIdentityDocument>(), options);

        Assert.Empty(result.Hits);
        Assert.False(result.HasMore);
        Assert.Single(invoker.Requests);
    }

    [Theory]
    [InlineData("request")]
    [InlineData("conversion")]
    [InlineData("event")]
    public async Task NextPageAsync_AfterOwnedPointInTimeFailure_RejectsRetryWithoutRequest(string failure)
    {
        int searches = 0;
        using var invoker = new StubInvoker(endpoint =>
        {
            bool fail = endpoint.Uri.AbsolutePath is "/_search" && ++searches is 2;
            string response = fail ? failure switch
            {
                "request" => ErrorResponse,
                "conversion" => InvalidHitResponse,
                _ => PageResponse
            } : PageResponse;
            return GetPointInTimeResponse(endpoint, response, fail && failure is "request" ? 400 : 200);
        });
        using var configuration = new StubConfiguration(invoker);
        using var index = new Index<NonIdentityDocument>(configuration, "test-index");
        using var repository = new StubRepository(index);
        int events = 0;
        var expectedException = new InvalidOperationException("event failure");
        repository.AfterQuery.AddHandler((_, _) =>
        {
            if (++events is 2 && failure is "event")
                throw expectedException;
            return Task.CompletedTask;
        });
        var options = new CommandOptions<NonIdentityDocument>().PageLimit(1).SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        var page = await repository.FindAsync(new RepositoryQuery<NonIdentityDocument>(), options);

        var exception = await Record.ExceptionAsync(async () =>
        {
            await page.NextPageAsync();
        });

        Assert.NotNull(exception);
        if (failure is "event")
            Assert.Same(expectedException, exception);
        AssertClosed(invoker, "updated-pit");
        Assert.False(options.HasPointInTimeId());
        int requests = invoker.Requests.Count;
        await Assert.ThrowsAsync<QueryValidationException>(() => page.NextPageAsync());
        Assert.Equal(requests, invoker.Requests.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NextPageAsync_AfterRetainedPointInTimeFailure_UsesLatestId(bool callerOwned)
    {
        int searches = 0;
        using var invoker = new StubInvoker(endpoint =>
        {
            if (endpoint.Uri.AbsolutePath is "/_search")
                searches++;
            return GetPointInTimeResponse(endpoint, PageResponse.Replace("updated-pit", $"pit-{searches}"), closeFails: true);
        });
        using var configuration = new StubConfiguration(invoker);
        using var index = new Index<NonIdentityDocument>(configuration, "test-index");
        using var repository = new StubRepository(index);
        int events = 0;
        var expectedException = new InvalidOperationException("event failure");
        repository.AfterQuery.AddHandler((_, _) =>
        {
            if (++events is 2)
                throw expectedException;
            return Task.CompletedTask;
        });
        var options = new CommandOptions<NonIdentityDocument>().PageLimit(1).SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        if (callerOwned)
            options.PointInTimeId("caller-pit");
        var page = await repository.FindAsync(new RepositoryQuery<NonIdentityDocument>(), options);

        Assert.Same(expectedException, await Record.ExceptionAsync(async () =>
        {
            await page.NextPageAsync();
        }));
        Assert.Equal("pit-2", options.GetPointInTimeId());
        Assert.True(await page.NextPageAsync());

        using var request = JsonDocument.Parse(invoker.Requests[^1].Body!);
        Assert.Equal("pit-2", request.RootElement.GetProperty("pit").GetProperty("id").GetString());
        Assert.Equal(1, request.RootElement.GetProperty("search_after")[0].GetInt32());
        Assert.False(request.RootElement.TryGetProperty("from", out _));
        Assert.Equal(2, page.Page);
        if (callerOwned)
            Assert.DoesNotContain(invoker.Requests, r => r.Method is Elastic.Transport.HttpMethod.DELETE);
        else
            AssertClosed(invoker, "pit-2");
    }

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, true, false)]
    [InlineData(true, false, false)]
    [InlineData(true, true, false)]
    [InlineData(false, false, true)]
    [InlineData(false, true, true)]
    [InlineData(true, false, true)]
    [InlineData(true, true, true)]
    public async Task ScalarQuery_WithLiveCursors_BypassesCache(bool count, bool backward, bool enableInBeforeQuery)
    {
        int searches = 0;
        using var invoker = new StubInvoker(_ => (PageResponse.Replace("\"name\":\"a\"", $"\"name\":\"{++searches}\"").Replace("\"value\":2", $"\"value\":{searches}"), 200));
        using var configuration = new StubConfiguration(invoker);
        using var index = new Index<NonIdentityDocument>(configuration, "test-index");
        using var repository = new StubRepository(index);
        bool enablePaging = false;
        repository.BeforeQuery.AddHandler((_, args) =>
        {
            if (enablePaging && enableInBeforeQuery)
                SetCursor(args.Options, backward, searches);
            return Task.CompletedTask;
        });

        async Task<string> ExecuteAsync(bool cursor)
        {
            var options = new CommandOptions<NonIdentityDocument>().Cache("same-key");
            if (cursor && !enableInBeforeQuery)
                SetCursor(options, backward, searches);
            var query = new RepositoryQuery<NonIdentityDocument>();
            return count ? (await repository.CountAsync(query, options)).Total.ToString()
                : (await repository.FindOneAsync(query, options)).Document!.Name;
        }

        Assert.Equal("1", await ExecuteAsync(false));
        enablePaging = true;
        Assert.Equal("2", await ExecuteAsync(true));
        Assert.Equal("3", await ExecuteAsync(true));
        enablePaging = false;
        Assert.Equal("1", await ExecuteAsync(false));
        Assert.Equal(3, invoker.Requests.Count);
        for (int i = 1; i < invoker.Requests.Count; i++)
        {
            using var request = JsonDocument.Parse(invoker.Requests[i].Body!);
            Assert.Equal(i, request.RootElement.GetProperty("search_after")[0].GetInt32());
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ScalarQuery_WithPointInTimeEnabledByBeforeQuery_RejectsCachedResult(bool count)
    {
        using var invoker = new StubInvoker(_ => (PageResponse, 200));
        using var configuration = new StubConfiguration(invoker);
        using var index = new Index<NonIdentityDocument>(configuration, "test-index");
        using var repository = new StubRepository(index);
        var query = new RepositoryQuery<NonIdentityDocument>();
        async Task<object> ExecuteAsync()
        {
            var options = new CommandOptions<NonIdentityDocument>().Cache("same-key");
            if (count)
                return await repository.CountAsync(query, options);
            return await repository.FindOneAsync(query, options);
        }
        await ExecuteAsync();
        repository.BeforeQuery.AddHandler((_, args) =>
        {
            args.Options.SearchAfterPaging(SearchAfterPagingMode.PointInTime);
            return Task.CompletedTask;
        });

        await Assert.ThrowsAsync<QueryValidationException>(ExecuteAsync);

        Assert.Single(invoker.Requests);
    }

    private static void SetCursor(ICommandOptions options, bool backward, int cursor)
    {
        options.SearchAfterPaging();
        if (backward)
            options.SearchBefore(cursor);
        else
            options.SearchAfter(cursor);
    }

    [Theory]
    [InlineData(SearchAfterPagingMode.Live, SearchAfterPagingMode.PointInTime, false)]
    [InlineData(SearchAfterPagingMode.PointInTime, SearchAfterPagingMode.Live, false)]
    [InlineData(SearchAfterPagingMode.Live, SearchAfterPagingMode.Live, true)]
    [InlineData(SearchAfterPagingMode.PointInTime, SearchAfterPagingMode.PointInTime, true)]
    public async Task FindAsync_AfterAdvancingAndResettingSession_StartsWithoutOffset(SearchAfterPagingMode initialMode, SearchAfterPagingMode finalMode, bool disableFirst)
    {
        // Arrange
        using var invoker = new StubInvoker(endpoint => GetPointInTimeResponse(endpoint, PageResponse));
        using var configuration = new StubConfiguration(invoker);
        using var index = new Index<NonIdentityDocument>(configuration, "test-index");
        using var repository = new StubRepository(index);
        var options = new CommandOptions<NonIdentityDocument>().PageLimit(1).SearchAfterPaging(initialMode);
        var page = await repository.FindAsync(new RepositoryQuery<NonIdentityDocument>(), options);
        Assert.True(await page.NextPageAsync());
        Assert.Equal(2, options.GetPage());
        if (initialMode is SearchAfterPagingMode.PointInTime)
            await repository.ClosePointInTimeAsync(options.GetPointInTimeId());

        // Act
        if (disableFirst)
            options.SearchAfterPaging(false);
        options.SearchAfterPaging(finalMode);
        await repository.FindAsync(new RepositoryQuery<NonIdentityDocument>(), options);

        // Assert
        using var request = JsonDocument.Parse(invoker.Requests[^1].Body!);
        Assert.False(request.RootElement.TryGetProperty("from", out _));
        Assert.False(request.RootElement.TryGetProperty("search_after", out _));
        Assert.Equal(1, options.GetPage());
        Assert.Equal(1, options.GetLimit());
    }

    [Theory]
    [InlineData(false, 400)]
    [InlineData(false, 403)]
    [InlineData(false, 500)]
    [InlineData(true, 400)]
    [InlineData(true, 403)]
    [InlineData(true, 500)]
    public async Task FindAsync_WithAsyncError_PreservesDocumentException(bool poll, int status)
    {
        // Arrange
        using var invoker = new StubInvoker(_ => (ErrorResponse, status));
        using var configuration = new StubConfiguration(invoker);
        using var index = new Index<NonIdentityDocument>(configuration, "test-index");
        using var repository = new StubRepository(index);
        var options = new CommandOptions<NonIdentityDocument>();
        if (poll)
            options.AsyncQueryId("async-id");
        else
            options.AsyncQuery(TimeSpan.Zero);

        // Act
        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.FindAsync(new RepositoryQuery<NonIdentityDocument>(), options));

        // Assert
        Assert.Contains("expected failure", exception.Message);
        Assert.Single(invoker.Requests);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FindAsync_WithAsyncNotFound_PreservesMissingResultBehavior(bool poll)
    {
        // Arrange
        using var invoker = new StubInvoker(_ => (ErrorResponse, 404));
        using var configuration = new StubConfiguration(invoker);
        using var index = new Index<NonIdentityDocument>(configuration, "test-index");
        using var repository = new StubRepository(index);
        var options = poll ? new CommandOptions<NonIdentityDocument>().AsyncQueryId("missing-id")
            : new CommandOptions<NonIdentityDocument>().AsyncQuery();

        // Act / Assert
        if (poll)
            await Assert.ThrowsAsync<AsyncQueryNotFoundException>(() => repository.FindAsync(new RepositoryQuery<NonIdentityDocument>(), options));
        else
            Assert.Empty((await repository.FindAsync(new RepositoryQuery<NonIdentityDocument>(), options)).Documents);
        Assert.Single(invoker.Requests);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FindAsync_WithCallerOwnedPointInTime_DoesNotClose(bool fail)
    {
        // Arrange
        using var invoker = new StubInvoker(_ => (fail ? ErrorResponse : SearchResponse, fail ? 400 : 200));
        using var configuration = new StubConfiguration(invoker);
        using var index = new Index<NonIdentityDocument>(configuration, "test-index");
        using var repository = new StubRepository(index);
        var options = new CommandOptions<NonIdentityDocument>().SearchAfterPaging(SearchAfterPagingMode.PointInTime).PointInTimeId("caller-pit");

        // Act
        if (fail)
            await Assert.ThrowsAsync<DocumentException>(() => repository.FindAsync(new RepositoryQuery<NonIdentityDocument>(), options));
        else
            await repository.FindAsync(new RepositoryQuery<NonIdentityDocument>(), options);

        // Assert
        Assert.Single(invoker.Requests);
        Assert.Equal("/_search", invoker.Requests[0].Path);
        Assert.False(options.IsRepoOwnedPointInTime());
    }

    [Theory]
    [InlineData("request", "opened-pit", false)]
    [InlineData("conversion", "updated-pit", false)]
    [InlineData("event", "updated-pit", false)]
    [InlineData("event", "updated-pit", true)]
    public async Task FindAsync_WithOwnedPointInTimeFailure_ClosesLatestIdWithoutMaskingException(string failure, string expectedId, bool closeFails)
    {
        // Arrange
        string searchResponse = failure switch
        {
            "request" => ErrorResponse,
            "conversion" => InvalidHitResponse,
            _ => SearchResponse
        };
        using var invoker = new StubInvoker(endpoint => GetPointInTimeResponse(endpoint, searchResponse, failure is "request" ? 400 : 200, closeFails));
        using var configuration = new StubConfiguration(invoker);
        using var index = new Index<NonIdentityDocument>(configuration, "test-index");
        using var repository = new StubRepository(index);
        var expectedException = new InvalidOperationException("event failure");
        if (failure is "event")
            repository.AfterQuery.AddHandler((_, _) => throw expectedException);
        var options = new CommandOptions<NonIdentityDocument>().SearchAfterPaging(SearchAfterPagingMode.PointInTime);

        // Act
        var exception = await Record.ExceptionAsync(async () =>
        {
            await repository.FindAsync(new RepositoryQuery<NonIdentityDocument>(), options);
        });

        // Assert
        Assert.NotNull(exception);
        if (failure is "event")
            Assert.Same(expectedException, exception);
        else if (failure is "request")
            Assert.IsType<DocumentException>(exception);
        AssertClosed(invoker, expectedId);
        Assert.Equal(closeFails, options.HasPointInTimeId());
    }

    [Fact]
    public async Task FindAsync_WithOwnedPointInTimeTerminalPage_ClosesLatestIdAndResetsSession()
    {
        // Arrange
        using var invoker = new StubInvoker(endpoint => GetPointInTimeResponse(endpoint, SearchResponse));
        using var configuration = new StubConfiguration(invoker);
        using var index = new Index<NonIdentityDocument>(configuration, "test-index");
        using var repository = new StubRepository(index);
        var options = new CommandOptions<NonIdentityDocument>().PageNumber(3).SearchAfterPaging(SearchAfterPagingMode.PointInTime);

        // Act
        await repository.FindAsync(new RepositoryQuery<NonIdentityDocument>(), options);

        // Assert
        AssertClosed(invoker, "updated-pit");
        Assert.False(options.ShouldUseSearchAfterPaging());
        Assert.False(options.HasPointInTimeId());
        Assert.Equal(1, options.GetPage());
        Assert.Equal("/test-index/_pit", invoker.Requests[0].Path);
        Assert.Equal("/_search", invoker.Requests[1].Path);
    }

    private static void AssertClosed(StubInvoker invoker, string expectedId)
    {
        var close = Assert.Single(invoker.Requests, r => r.Method is Elastic.Transport.HttpMethod.DELETE);
        Assert.Equal("/_pit", close.Path);
        using var json = JsonDocument.Parse(close.Body!);
        Assert.Equal(expectedId, json.RootElement.GetProperty("id").GetString());
    }

    private static (string Json, int Status) GetPointInTimeResponse(Endpoint endpoint, string searchResponse, int searchStatus = 200, bool closeFails = false)
    {
        if (endpoint.Method is Elastic.Transport.HttpMethod.DELETE)
            return (CloseResponse, closeFails ? 500 : 200);
        if (endpoint.Uri.AbsolutePath.EndsWith("/_pit", StringComparison.Ordinal))
            return (OpenResponse, 200);
        if (endpoint.Uri.AbsolutePath is not "/_search")
            searchResponse = searchResponse.Replace("\"pit_id\":\"updated-pit\",", String.Empty);
        return (searchResponse, searchStatus);
    }

    private const string CloseResponse = """{"succeeded":true,"num_freed":1}""";
    private const string ErrorResponse = """{"error":{"type":"test_error","reason":"expected failure"},"status":400}""";
    private const string InvalidHitResponse = """{"pit_id":"updated-pit","took":1,"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"hits":[null]}}""";
    private const string OpenResponse = """{"id":"opened-pit"}""";
    private const string PageResponse = """{"pit_id":"updated-pit","took":1,"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"total":{"value":2,"relation":"eq"},"hits":[{"_index":"test-index","_id":"1","_source":{"name":"a"},"sort":[1]},{"_index":"test-index","_id":"2","_source":{"name":"b"},"sort":[2]}]}}""";
    private const string SearchResponse = """{"pit_id":"updated-pit","took":1,"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}""";

    private sealed class StubConfiguration(StubInvoker invoker) : ElasticConfiguration
    {
        protected override ElasticsearchClient CreateElasticClient()
        {
            return new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker)
                .DisableDirectStreaming().MaximumRetries(0));
        }
    }

    private sealed class StubRepository(IIndex index) : ElasticReadOnlyRepositoryBase<NonIdentityDocument>(index)
    {
        protected override async Task<SearchRequestDescriptor<NonIdentityDocument>> CreateSearchDescriptorAsync(IRepositoryQuery query, ICommandOptions options)
        {
            var context = new QueryBuilderContext<NonIdentityDocument>(query, options);
            await new PageableQueryBuilder().BuildAsync(context);
            return context.Search.Indices("test-index").IgnoreUnavailable(true);
        }
    }

    private sealed class StubInvoker(Func<Endpoint, (string Json, int Status)> response)
        : InMemoryRequestInvoker(null, 200, null, "application/json", new Dictionary<string, IEnumerable<string>> { ["x-elastic-product"] = ["Elasticsearch"] }), IRequestInvoker
    {
        public List<(Elastic.Transport.HttpMethod Method, string Path, byte[]? Body)> Requests { get; } = [];

        async Task<TResponse> IRequestInvoker.RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
        {
            var (json, status) = response(endpoint);
            var result = await BuildResponseAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken, Encoding.UTF8.GetBytes(json), status);
            Requests.Add((endpoint.Method, endpoint.Uri.AbsolutePath, postData?.WrittenBytes));
            return result;
        }
    }
}
