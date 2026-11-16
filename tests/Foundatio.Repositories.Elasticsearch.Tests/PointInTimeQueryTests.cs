using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Xunit;
using HttpMethod = Elastic.Transport.HttpMethod;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class PointInTimeQueryTests
{
    private const string AllowedIndex = "pit-query-allowed";
    private const string ExcludedIndex = "pit-query-excluded";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PointInTime_WhenConfigureQueryReturnsIndexRestriction_UsesOnlyConfiguredIndexes(bool patch)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        var query = new RepositoryQuery<TestDocument>();
        var visited = new List<string>();
        var options = new CommandOptions().Cache(false).PageLimit(10);

        if (patch)
        {
            long modified = await repository.PatchAllAsync(query, new ActionPatch<TestDocument>(document =>
            {
                visited.Add(document.Id);
                return false;
            }), options);
            Assert.Equal(0, modified);
        }
        else
        {
            var results = await repository.FindAsync(query, options.SearchAfterPaging(SearchAfterPagingMode.PointInTime));
            visited.AddRange(results.Documents.Select(document => document.Id));
        }

        Assert.Equal(new[] { AllowedIndex }, configuration.Invoker.OpenedIndexes);
        Assert.Equal(new[] { "allowed" }, visited);
        Assert.Equal(1, repository.ConfigureQueryCalls);
        Assert.Empty(query.GetElasticIndexes());
        Assert.Equal(1, configuration.Invoker.ClosedPointInTimes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PointInTime_WhenDescriptorOverrideSkipsBase_DoesNotReuseIndexesFromFailedQuery(bool cloneOptions)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration) { ThrowAfterBaseSearchConfiguration = true };
        var options = new CommandOptions().Cache(false).PageLimit(10).SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        await Assert.ThrowsAsync<InvalidOperationException>(() => repository.FindAsync(new RepositoryQuery<TestDocument>(), options));
        repository.ThrowAfterBaseSearchConfiguration = false;
        repository.SkipBaseSearchConfiguration = true;
        var nextOptions = cloneOptions ? options.Clone() : options;

        var results = await repository.FindAsync(new RepositoryQuery<TestDocument>().Index(ExcludedIndex), nextOptions);

        Assert.Equal(new[] { ExcludedIndex }, configuration.Invoker.OpenedIndexes);
        Assert.Equal("excluded", Assert.Single(results.Documents).Id);
        Assert.Equal(1, repository.ConfigureQueryCalls);
        Assert.Equal(1, configuration.Invoker.ClosedPointInTimes);
    }

    [Theory]
    [InlineData("BeforeQuery", false, false)]
    [InlineData("BeforeQuery", false, true)]
    [InlineData("BeforeQuery", true, false)]
    [InlineData("BeforeQuery", true, true)]
    [InlineData("Descriptor", false, false)]
    [InlineData("Descriptor", false, true)]
    [InlineData("Descriptor", true, false)]
    [InlineData("Descriptor", true, true)]
    [InlineData("AfterQuery", false, false)]
    [InlineData("AfterQuery", false, true)]
    [InlineData("AfterQuery", true, false)]
    [InlineData("AfterQuery", true, true)]
    public async Task PointInTime_WhenSecondPageFails_ClosesOnlyTheOwnedContext(string failureStage, bool callerOwned, bool cloneOptions)
    {
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 2;
        using var repository = new TestRepository(configuration);
        var options = new CommandOptions().Cache(false).PageLimit(1).SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        if (callerOwned)
        {
            var opened = await configuration.Client.OpenPointInTimeAsync(AllowedIndex, descriptor => descriptor
                .KeepAlive(TimeSpan.FromMinutes(1).ToElasticDuration()), TestContext.Current.CancellationToken);
            Assert.True(opened.IsValidResponse);
            options.PointInTimeId(opened.Id);
        }

        var results = await repository.FindAsync(new RepositoryQuery<TestDocument>(), options);
        Assert.True(results.HasMore);
        Assert.Single(results.Documents);
        string? pointInTimeId = results.GetPointInTimeId();
        Assert.False(String.IsNullOrEmpty(pointInTimeId));
        Assert.Equal(!callerOwned, options.IsRepoOwnedPointInTime());
        Assert.Equal(0, configuration.Invoker.ClosedPointInTimes);
        repository.CloneOptions = cloneOptions;
        var failure = new InvalidOperationException("The second-page query failed.");
        if (failureStage is "BeforeQuery")
            repository.BeforeQuery.AddHandler((_, _) => Task.FromException(failure));
        else if (failureStage is "AfterQuery")
            repository.AfterQuery.AddHandler((_, _) => Task.FromException(failure));
        else
            repository.SearchConfigurationException = failure;

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => results.NextPageAsync());

        Assert.Same(failure, thrown);
        Assert.Equal(failureStage is "AfterQuery" ? 2 : 1, configuration.Invoker.SearchRequests);
        Assert.False(options.IsRepoOwnedPointInTime());
        if (callerOwned)
        {
            Assert.Empty(configuration.Invoker.ClosedPointInTimeIds);
            Assert.Equal(pointInTimeId, options.GetPointInTimeId());
        }
        else
        {
            Assert.Equal(pointInTimeId, Assert.Single(configuration.Invoker.ClosedPointInTimeIds));
            Assert.Null(options.GetPointInTimeId());
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task PointInTime_WhenClonedOptionsCompleteSecondPage_ClearsOnlyOwnedContext(bool callerOwned, bool cloneOptions)
    {
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 2;
        using var repository = new TestRepository(configuration);
        var options = new CommandOptions().Cache(false).PageLimit(1).SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        if (callerOwned)
        {
            var opened = await configuration.Client.OpenPointInTimeAsync(AllowedIndex, descriptor => descriptor
                .KeepAlive(TimeSpan.FromMinutes(1).ToElasticDuration()), TestContext.Current.CancellationToken);
            Assert.True(opened.IsValidResponse);
            options.PointInTimeId(opened.Id);
        }

        var results = await repository.FindAsync(new RepositoryQuery<TestDocument>(), options);
        Assert.Equal("allowed-0", Assert.Single(results.Documents).Id);
        Assert.True(results.HasMore);
        string? pointInTimeId = results.GetPointInTimeId();
        Assert.False(String.IsNullOrEmpty(pointInTimeId));
        Assert.Equal(!callerOwned, options.IsRepoOwnedPointInTime());
        Assert.Empty(configuration.Invoker.ClosedPointInTimeIds);
        repository.CloneOptions = cloneOptions;

        Assert.True(await results.NextPageAsync());

        Assert.Equal("allowed-1", Assert.Single(results.Documents).Id);
        Assert.False(results.HasMore);
        Assert.False(options.IsRepoOwnedPointInTime());
        if (callerOwned)
        {
            Assert.Empty(configuration.Invoker.ClosedPointInTimeIds);
            Assert.Equal(pointInTimeId, options.GetPointInTimeId());
        }
        else
        {
            Assert.Equal(pointInTimeId, Assert.Single(configuration.Invoker.ClosedPointInTimeIds));
            Assert.Null(options.GetPointInTimeId());
        }
        Assert.False(await results.NextPageAsync());
        Assert.Equal(2, configuration.Invoker.SearchRequests);
        Assert.Equal(callerOwned ? 0 : 1, configuration.Invoker.ClosedPointInTimes);
    }

    public sealed class TestDocument : IIdentity
    {
        public string Id { get; set; } = null!;
        public string? Value { get; set; }
    }

    private sealed class TestRepository(TestConfiguration configuration) : ElasticRepositoryBase<TestDocument>(new TestIndex(configuration))
    {
        public int ConfigureQueryCalls { get; private set; }
        public bool CloneOptions { get; set; }
        public bool SkipBaseSearchConfiguration { get; set; }
        public bool ThrowAfterBaseSearchConfiguration { get; set; }
        public Exception? SearchConfigurationException { get; set; }

        protected override ICommandOptions<TestDocument> ConfigureOptions(ICommandOptions<TestDocument>? options)
        {
            return base.ConfigureOptions(CloneOptions ? options?.Clone() : options);
        }

        protected override IRepositoryQuery ConfigureQuery(IRepositoryQuery<TestDocument> query)
        {
            ConfigureQueryCalls++;
            var configured = query.Clone<RepositoryQuery<TestDocument>>().Index(AllowedIndex);
            return base.ConfigureQuery(configured);
        }

        protected override async Task<SearchRequestDescriptor<TestDocument>> ConfigureSearchDescriptorAsync(SearchRequestDescriptor<TestDocument> search, IRepositoryQuery query, ICommandOptions options)
        {
            if (SkipBaseSearchConfiguration)
                return search.Size(options.GetLimit() + 1);

            var configured = await base.ConfigureSearchDescriptorAsync(search, query, options);
            if (ThrowAfterBaseSearchConfiguration)
                throw new InvalidOperationException("The descriptor override failed after base configuration.");
            if (SearchConfigurationException is { } failure)
                throw failure;

            return configured;
        }
    }

    private sealed class TestIndex : Index<TestDocument>
    {
        public TestIndex(TestConfiguration configuration) : base(configuration, "pit-query-tests")
        {
            HasMultipleIndexes = true;
        }

        public override string[] GetIndexesByQuery(IRepositoryQuery query)
        {
            var indexes = query.GetElasticIndexes();
            return indexes.Count > 0 ? indexes.ToArray() : [AllowedIndex, ExcludedIndex];
        }
    }

    private sealed class TestConfiguration : ElasticConfiguration
    {
        public PointInTimeInvoker Invoker { get; } = new();
        protected override IRequestInvoker CreateRequestInvoker() => Invoker;
    }

    private sealed class PointInTimeInvoker : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _owner = new();
        private string[] _currentIndexes = [];
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };
        public ResponseFactory ResponseFactory => _owner.ResponseFactory;
        public List<string> OpenedIndexes { get; } = [];
        public List<string> ClosedPointInTimeIds { get; } = [];
        public int ClosedPointInTimes => ClosedPointInTimeIds.Count;
        public int SearchRequests { get; private set; }
        public int HitsPerIndex { get; set; } = 1;

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            object response;
            if (endpoint.PathAndQuery.Contains("_mapping", StringComparison.Ordinal))
            {
                response = new Dictionary<string, object>
                {
                    ["pit-query-tests"] = new
                    {
                        mappings = new { properties = new { id = new { type = "keyword" }, value = new { type = "keyword" } } }
                    }
                };
            }
            else if (endpoint.PathAndQuery.Contains("_pit", StringComparison.Ordinal))
            {
                if (endpoint.Method is HttpMethod.DELETE)
                {
                    Assert.NotNull(postData);
                    using var requestStream = new MemoryStream();
                    postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                    using var request = JsonDocument.Parse(requestStream.ToArray());
                    ClosedPointInTimeIds.Add(request.RootElement.GetProperty("id").GetString()!);
                    response = new { succeeded = true, num_freed = 1 };
                }
                else
                {
                    string path = endpoint.PathAndQuery.Split('?', 2)[0];
                    _currentIndexes = Uri.UnescapeDataString(path.Trim('/').Split('/')[0]).Split(',');
                    OpenedIndexes.AddRange(_currentIndexes);
                    response = new { id = "point-in-time" };
                }
            }
            else
            {
                Assert.Contains("_search", endpoint.PathAndQuery, StringComparison.Ordinal);
                SearchRequests++;
                Assert.NotNull(postData);
                using var requestStream = new MemoryStream();
                postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                using var request = JsonDocument.Parse(requestStream.ToArray());
                string? searchAfter = request.RootElement.TryGetProperty("search_after", out var sortValues)
                    ? sortValues[0].GetString()
                    : null;
                var hits = _currentIndexes.SelectMany(index => Enumerable.Range(0, HitsPerIndex).Select(ordinal =>
                {
                    string id = index is AllowedIndex ? "allowed" : "excluded";
                    if (HitsPerIndex > 1)
                        id = $"{id}-{ordinal}";
                    return new { _index = index, _id = id, _source = new { id, value = "before" }, sort = new[] { id } };
                })).Where(hit => searchAfter is null || String.CompareOrdinal(hit._id, searchAfter) > 0).ToArray();
                response = new { took = 1, timed_out = false, pit_id = "point-in-time", hits = new { total = new { value = _currentIndexes.Length * HitsPerIndex, relation = "eq" }, hits } };
            }

            byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(response);
            using var stream = new MemoryStream(bytes);
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, 200, _headers, stream, "application/json", bytes.Length, null, null);
        }

        public Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
            where TResponse : TransportResponse, new()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Request<TResponse>(endpoint, boundConfiguration, postData));
        }

        public void Dispose() => ((IDisposable)_owner).Dispose();
    }
}
