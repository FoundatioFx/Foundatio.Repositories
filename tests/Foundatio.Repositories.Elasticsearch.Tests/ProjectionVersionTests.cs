using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ProjectionVersionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FindAsAsync_WithVersionedProjectionOnNonVersionedRepository_RequestsAndRestoresMetadata(bool asyncQuery)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        var options = new CommandOptions().Cache(false);
        if (asyncQuery)
            options.AsyncQuery(TimeSpan.FromMinutes(1));

        var result = await repository.FindAsAsync<ExplicitVersionProjection>(new RepositoryQuery(), options);

        Assert.Equal((asyncQuery, true), Assert.Single(configuration.Invoker.Requests));
        var hit = Assert.Single(result.Hits);
        Assert.NotNull(hit.Document);
        Assert.Equal("3:42", hit.Version);
        Assert.Equal(hit.Version, ((IVersioned)hit.Document).Version);
        Assert.Equal("business-7", hit.Document.Version);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FindAsAsync_WithNonVersionedProjection_DoesNotRequestMetadata(bool asyncQuery)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        var options = new CommandOptions().Cache(false);
        if (asyncQuery)
            options.AsyncQuery(TimeSpan.FromMinutes(1));

        var result = await repository.FindAsAsync<NonVersionedDocument>(new RepositoryQuery(), options);

        Assert.Equal((asyncQuery, false), Assert.Single(configuration.Invoker.Requests));
        var hit = Assert.Single(result.Hits);
        Assert.NotNull(hit.Document);
        Assert.Null(hit.Version);
        Assert.Equal("business-7", hit.Document.Version);
    }

    private sealed class NonVersionedDocument : IIdentity
    {
        public string Id { get; set; } = null!;
        public string? Version { get; set; }
    }

    private sealed class ExplicitVersionProjection : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Version { get; set; }
        string? IVersioned.Version { get; set; } = "9:9";
    }

    private sealed class TestRepository : ElasticReadOnlyRepositoryBase<NonVersionedDocument>
    {
        public TestRepository(TestConfiguration configuration) : base(new Index<NonVersionedDocument>(configuration, "projection-tests"))
        {
            DisableCache();
        }
    }

    private sealed class TestConfiguration : ElasticConfiguration
    {
        public ProjectionResponseInvoker Invoker { get; } = new();

        protected override IRequestInvoker CreateRequestInvoker() => Invoker;
    }

    private sealed class ProjectionResponseInvoker : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _responseFactoryOwner = new();
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };
        public ResponseFactory ResponseFactory => _responseFactoryOwner.ResponseFactory;
        public List<(bool IsAsync, bool RequestedVersion)> Requests { get; } = [];

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            string body;
            if (endpoint.PathAndQuery.Contains("_mapping", StringComparison.Ordinal))
            {
                body = """{"projection-tests":{"mappings":{"properties":{"id":{"type":"keyword"}}}}}""";
            }
            else
            {
                Assert.Contains("_search", endpoint.PathAndQuery, StringComparison.Ordinal);
                Assert.NotNull(postData);
                using var requestStream = new MemoryStream();
                postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                using var request = JsonDocument.Parse(requestStream.ToArray());
                bool requestedVersion = request.RootElement.TryGetProperty("seq_no_primary_term", out var version) && version.GetBoolean();
                bool isAsync = endpoint.PathAndQuery.Contains("_async_search", StringComparison.Ordinal);
                Requests.Add((isAsync, requestedVersion));

                var hit = new Dictionary<string, object?>
                {
                    ["_index"] = "projection-tests",
                    ["_id"] = "one",
                    ["_score"] = 1,
                    ["_source"] = new { id = "one", version = "business-7" }
                };
                if (requestedVersion)
                {
                    hit["_primary_term"] = 3;
                    hit["_seq_no"] = 42;
                }

                var searchResponse = new
                {
                    took = 1,
                    timed_out = false,
                    _shards = new { total = 1, successful = 1, skipped = 0, failed = 0 },
                    hits = new { total = new { value = 1, relation = "eq" }, max_score = 1, hits = new[] { hit } }
                };
                body = isAsync
                    ? JsonSerializer.Serialize(new { id = "query-one", is_partial = false, is_running = false, start_time_in_millis = 1, expiration_time_in_millis = 60001, response = searchResponse })
                    : JsonSerializer.Serialize(searchResponse);
            }

            byte[] bytes = Encoding.UTF8.GetBytes(body);
            using var stream = new MemoryStream(bytes);
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, 200, _headers, stream, "application/json", bytes.Length, null, null);
        }

        public Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
            where TResponse : TransportResponse, new()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Request<TResponse>(endpoint, boundConfiguration, postData));
        }

        public void Dispose() => ((IDisposable)_responseFactoryOwner).Dispose();
    }
}
