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

    public sealed class TestDocument : IIdentity
    {
        public string Id { get; set; } = null!;
        public string? Value { get; set; }
    }

    private sealed class TestRepository(TestConfiguration configuration) : ElasticRepositoryBase<TestDocument>(new TestIndex(configuration))
    {
        public int ConfigureQueryCalls { get; private set; }
        public bool SkipBaseSearchConfiguration { get; set; }
        public bool ThrowAfterBaseSearchConfiguration { get; set; }

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
        public int ClosedPointInTimes { get; private set; }

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
                    ClosedPointInTimes++;
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
                var hits = _currentIndexes.Select(index =>
                {
                    string id = index is AllowedIndex ? "allowed" : "excluded";
                    return new { _index = index, _id = id, _source = new { id, value = "before" } };
                }).ToArray();
                response = new { took = 1, timed_out = false, pit_id = "point-in-time", hits = new { total = new { value = hits.Length, relation = "eq" }, hits } };
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
