using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ProjectionCacheTests
{
    [Theory]
    [InlineData("Include", false)]
    [InlineData("Include", true)]
    [InlineData("Exclude", false)]
    [InlineData("Exclude", true)]
    [InlineData("IncludeMask", false)]
    [InlineData("IncludeMask", true)]
    [InlineData("ExcludeMask", false)]
    [InlineData("ExcludeMask", true)]
    public async Task GetByIdAsync_WithCallerSourceFiltering_DoesNotReplaceUnfilteredIdCache(string restriction, bool customCacheKey)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        var options = new CommandOptions().Cache();
        if (customCacheKey)
            options.CacheKey("projected-document");
        switch (restriction)
        {
            case "Include":
                options.Include("name");
                break;
            case "Exclude":
                options.Exclude("detail");
                break;
            case "IncludeMask":
                options.IncludeMask("name");
                break;
            case "ExcludeMask":
                options.ExcludeMask("detail");
                break;
        }

        var projected = await repository.GetByIdAsync("one", options);
        Assert.NotNull(projected);
        Assert.Equal("full name", projected.Name);
        Assert.Null(projected.Detail);
        Assert.Equal("3:10", projected.Version);
        if (customCacheKey)
        {
            var cachedProjection = await repository.GetByIdAsync("one", options);
            Assert.NotNull(cachedProjection);
            Assert.Null(cachedProjection.Detail);
            Assert.Equal(1, configuration.Invoker.Reads);
        }

        var full = await repository.GetByIdAsync("one", new CommandOptions().Cache());

        Assert.NotNull(full);
        Assert.Equal("full name", full.Name);
        Assert.Equal("full detail", full.Detail);
        Assert.Equal("3:10", full.Version);
        Assert.Equal(2, configuration.Invoker.Reads);
        var cachedFull = await repository.GetByIdAsync("one", new CommandOptions().Cache());
        Assert.NotNull(cachedFull);
        Assert.Equal("full detail", cachedFull.Detail);
        Assert.Equal(2, configuration.Invoker.Reads);
    }

    [Fact]
    public async Task GetByIdAsync_WithRepositoryDefaultExclude_StillCachesTheDefaultDocument()
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration, defaultExclude: true);

        var first = await repository.GetByIdAsync("one", new CommandOptions().Cache());
        var second = await repository.GetByIdAsync("one", new CommandOptions().Cache());

        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.Equal("full name", second.Name);
        Assert.Null(first.Detail);
        Assert.Null(second.Detail);
        Assert.Equal(1, configuration.Invoker.Reads);
    }

    [Theory]
    [InlineData("Include")]
    [InlineData("Exclude")]
    [InlineData("IncludeMask")]
    [InlineData("ExcludeMask")]
    public async Task FindOneAsync_WithQuerySourceFiltering_DoesNotReplaceUnfilteredIdCache(string restriction)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        var query = new RepositoryQuery<TestDocument>().Id("one");
        switch (restriction)
        {
            case "Include":
                query.Include("name");
                break;
            case "Exclude":
                query.Exclude("detail");
                break;
            case "IncludeMask":
                query.IncludeMask("name");
                break;
            case "ExcludeMask":
                query.ExcludeMask("detail");
                break;
        }
        var options = new CommandOptions().Cache("projected-query").ImmediateConsistency();

        var projected = await repository.FindOneAsync(query, options);
        Assert.NotNull(projected.Document);
        Assert.Equal("full name", projected.Document.Name);
        Assert.Null(projected.Document.Detail);
        Assert.Equal("3:10", projected.Document.Version);
        var cachedProjection = await repository.FindOneAsync(query, options);
        Assert.NotNull(cachedProjection.Document);
        Assert.Null(cachedProjection.Document.Detail);
        Assert.Equal(1, configuration.Invoker.Searches);

        var full = await repository.GetByIdAsync("one", new CommandOptions().Cache());

        Assert.NotNull(full);
        Assert.Equal("full name", full.Name);
        Assert.Equal("full detail", full.Detail);
        Assert.Equal("3:10", full.Version);
        Assert.Equal(1, configuration.Invoker.Reads);
    }

    [Theory]
    [InlineData("Include")]
    [InlineData("Exclude")]
    public async Task FindOneAsync_WhenConfigureQueryReturnsAProjectedQuery_DoesNotReplaceUnfilteredIdCache(string restriction)
    {
        using var configuration = new TestConfiguration();
        var configuredQuery = new RepositoryQuery<TestDocument>().Id("one");
        if (restriction is "Include")
            configuredQuery.Include("name");
        else
            configuredQuery.Exclude("detail");
        using var repository = new TestRepository(configuration, configuredQuery: configuredQuery);
        var query = new RepositoryQuery<TestDocument>().Id("one");
        var options = new CommandOptions().Cache("configured-projection").ImmediateConsistency();

        var projected = await repository.FindOneAsync(query, options);

        Assert.NotNull(projected.Document);
        Assert.Equal("full name", projected.Document.Name);
        Assert.Null(projected.Document.Detail);
        Assert.Equal("3:10", projected.Document.Version);
        Assert.Equal(1, repository.ConfigureQueryCalls);
        Assert.Equal(1, configuration.Invoker.Searches);
        Assert.Empty(query.GetIncludes());
        Assert.Empty(query.GetExcludes());

        var full = await repository.GetByIdAsync("one", new CommandOptions().Cache());

        Assert.NotNull(full);
        Assert.Equal("full name", full.Name);
        Assert.Equal("full detail", full.Detail);
        Assert.Equal("3:10", full.Version);
        Assert.Equal(1, configuration.Invoker.Reads);
        Assert.Equal(1, repository.ConfigureQueryCalls);
    }

    [Fact]
    public async Task FindOneAsync_WithRepositoryDefaultExclude_StillPopulatesTheDefaultIdCache()
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration, defaultExclude: true);
        var query = new RepositoryQuery<TestDocument>().Id("one");
        var options = new CommandOptions().Cache("default-query").ImmediateConsistency();

        var first = await repository.FindOneAsync(query, options);
        var cachedById = await repository.GetByIdAsync("one", new CommandOptions().Cache());

        Assert.NotNull(first.Document);
        Assert.NotNull(cachedById);
        Assert.Equal("full name", cachedById.Name);
        Assert.Null(first.Document.Detail);
        Assert.Null(cachedById.Detail);
        Assert.Equal(1, configuration.Invoker.Searches);
        Assert.Equal(0, configuration.Invoker.Reads);

        await repository.InvalidateCacheAsync("one");
        var repeated = await repository.FindOneAsync(query,
            new CommandOptions().Cache("default-query-repeat").ImmediateConsistency());
        var cachedAfterRepeat = await repository.GetByIdAsync("one", new CommandOptions().Cache());

        Assert.NotNull(repeated.Document);
        Assert.NotNull(cachedAfterRepeat);
        Assert.Equal("full name", cachedAfterRepeat.Name);
        Assert.Null(repeated.Document.Detail);
        Assert.Null(cachedAfterRepeat.Detail);
        Assert.Equal(2, configuration.Invoker.Searches);
        Assert.Equal(0, configuration.Invoker.Reads);
    }

    public sealed class TestDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Name { get; set; }
        public string? Detail { get; set; }
        public string? Version { get; set; }
    }

    private sealed class TestRepository : ElasticRepositoryBase<TestDocument>
    {
        private readonly IRepositoryQuery<TestDocument>? _configuredQuery;

        public TestRepository(TestConfiguration configuration, bool defaultExclude = false, IRepositoryQuery<TestDocument>? configuredQuery = null)
            : base(new Index<TestDocument>(configuration, "projection-cache-tests"))
        {
            _configuredQuery = configuredQuery;
            if (defaultExclude)
                AddDefaultExclude(document => document.Detail);
        }

        public int ConfigureQueryCalls { get; private set; }

        protected override IRepositoryQuery ConfigureQuery(IRepositoryQuery<TestDocument> query)
        {
            ConfigureQueryCalls++;
            return base.ConfigureQuery(_configuredQuery ?? query);
        }
    }

    private sealed class TestConfiguration : ElasticConfiguration
    {
        public SourceFilteringInvoker Invoker { get; } = new();
        protected override IRequestInvoker CreateRequestInvoker() => Invoker;
    }

    private sealed class SourceFilteringInvoker : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _owner = new();
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };
        public ResponseFactory ResponseFactory => _owner.ResponseFactory;
        public int Reads { get; private set; }
        public int Searches { get; private set; }

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            object response;
            if (endpoint.PathAndQuery.Contains("_mapping", StringComparison.Ordinal))
            {
                response = new Dictionary<string, object>
                {
                    ["projection-cache-tests"] = new
                    {
                        mappings = new { properties = new { id = new { type = "keyword" }, name = new { type = "keyword" }, detail = new { type = "keyword" } } }
                    }
                };
            }
            else if (endpoint.PathAndQuery.Contains("_refresh", StringComparison.Ordinal))
            {
                response = new { _shards = new { total = 1, successful = 1, failed = 0 } };
            }
            else if (endpoint.PathAndQuery.Contains("_search", StringComparison.Ordinal))
            {
                Searches++;
                Assert.NotNull(postData);
                using var requestStream = new MemoryStream();
                postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                using var request = JsonDocument.Parse(requestStream.ToArray());
                var sourceFilter = request.RootElement.GetProperty("_source");
                var includes = ReadFields(sourceFilter, "includes");
                var excludes = ReadFields(sourceFilter, "excludes");
                var hit = new { _index = "projection-cache-tests", _id = "one", _seq_no = 10, _primary_term = 3, _source = FilterSource(includes, excludes) };
                response = new { took = 1, timed_out = false, hits = new { total = new { value = 1, relation = "eq" }, hits = new[] { hit } } };
            }
            else
            {
                Assert.Contains("/_doc/one", endpoint.PathAndQuery, StringComparison.Ordinal);
                Reads++;
                var parameters = new Uri($"http://localhost{endpoint.PathAndQuery}").Query.TrimStart('?')
                    .Split('&', StringSplitOptions.RemoveEmptyEntries)
                    .Select(parameter => parameter.Split('=', 2))
                    .ToDictionary(pair => Uri.UnescapeDataString(pair[0]), pair => pair.Length > 1 ? Uri.UnescapeDataString(pair[1]) : String.Empty);
                var includes = parameters.TryGetValue("_source_includes", out string? include) ? include.Split(',').ToHashSet() : [];
                var excludes = parameters.TryGetValue("_source_excludes", out string? exclude) ? exclude.Split(',').ToHashSet() : [];
                response = new { _index = "projection-cache-tests", _id = "one", found = true, _seq_no = 10, _primary_term = 3, _source = FilterSource(includes, excludes) };
            }

            byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(response);
            using var stream = new MemoryStream(bytes);
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, 200, _headers, stream, "application/json", bytes.Length, null, null);
        }

        private static Dictionary<string, string> FilterSource(HashSet<string> includes, HashSet<string> excludes)
        {
            return new Dictionary<string, string> { ["id"] = "one", ["name"] = "full name", ["detail"] = "full detail" }
                .Where(field => (includes.Count is 0 || includes.Contains(field.Key)) && !excludes.Contains(field.Key))
                .ToDictionary(field => field.Key, field => field.Value);
        }

        private static HashSet<string> ReadFields(JsonElement sourceFilter, string name)
        {
            if (!sourceFilter.TryGetProperty(name, out var value))
                return [];

            return value.ValueKind is JsonValueKind.String
                ? [value.GetString()!]
                : value.EnumerateArray().Select(field => field.GetString()!).ToHashSet();
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
