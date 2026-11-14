using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Xunit;
using HttpMethod = Elastic.Transport.HttpMethod;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class WriteTransportTests
{
    [Theory]
    [InlineData(502, false)]
    [InlineData(502, true)]
    [InlineData(504, false)]
    [InlineData(504, true)]
    public async Task PatchAsync_WithGatewayFailureAfterScriptApplied_DoesNotRetry(int status, bool customTransport)
    {
        // Arrange
        using var configuration = new TestConfiguration(status, applyBeforeFailure: true, customTransport: customTransport);
        using var repository = new TestRepository(configuration);

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync("one", new ScriptPatch("ctx._source.count++")));

        // Assert
        Assert.Equal(1, configuration.Invoker.WriteRequests);
        Assert.Equal(1, configuration.Invoker.AppliedWrites);
        var failedNode = Assert.Single(configuration.Client.ElasticsearchClientSettings.NodePool.Nodes, n => !n.IsAlive);
        Assert.Equal(configuration.Invoker.LastNode, failedNode.Uri.Host);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task PatchAsync_WithUnstructuredServiceUnavailableAfterScriptApplied_DoesNotRetry(bool bulk, bool json)
    {
        // Arrange
        using var configuration = new TestConfiguration(503, applyBeforeFailure: true);
        configuration.Invoker.UnstructuredFailure = true;
        configuration.Invoker.JsonFailure = json;
        using var repository = new TestRepository(configuration);

        // Act
        var exception = await Record.ExceptionAsync(async () =>
        {
            if (bulk)
                _ = await repository.PatchAsync(new Ids(new[] { "one", "two" }), new ScriptPatch("ctx._source.count++"), o => o.Retry(1));
            else
                _ = await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++"), o => o.Retry(1));
        });

        // Assert
        Assert.Equal(1, configuration.Invoker.WriteRequests);
        Assert.Equal(1, configuration.Invoker.AppliedWrites);
        Assert.IsType<DocumentException>(exception);
    }

    [Theory]
    [InlineData("Add")]
    [InlineData("Save")]
    [InlineData("BulkSave")]
    [InlineData("Remove")]
    [InlineData("BulkRemove")]
    [InlineData("PartialPatch")]
    [InlineData("BulkPatch")]
    [InlineData("PipelinePatch")]
    [InlineData("PatchAllByQuery")]
    [InlineData("RemoveAllByQuery")]
    public async Task WriteAsync_WithGatewayFailure_DoesNotRetryOnAnotherNode(string operation)
    {
        // Arrange
        using var configuration = new TestConfiguration(502);
        using var repository = new TestRepository(configuration, operation is "PipelinePatch");

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => WriteAsync(repository, operation, 0));

        // Assert
        Assert.Equal(1, configuration.Invoker.WriteRequests);
    }

    [Fact]
    public async Task PatchAsync_WithRetryZeroAndServiceUnavailable_SendsOnce()
    {
        // Arrange
        using var configuration = new TestConfiguration(503);
        using var repository = new TestRepository(configuration);

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync("one", new ScriptPatch("ctx._source.count++"), o => o.Retry(0)));

        // Assert
        Assert.Equal(1, configuration.Invoker.WriteRequests);
        Assert.Equal(0, configuration.Invoker.AppliedWrites);
    }

    [Fact]
    public async Task PatchAsync_WithServiceUnavailableAndRetryBudget_RetriesThroughRunner()
    {
        // Arrange
        using var configuration = new TestConfiguration(503);
        using var repository = new TestRepository(configuration);

        // Act
        bool modified = await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++"), o => o.Retry(1));

        // Assert
        Assert.True(modified);
        Assert.Equal(2, configuration.Invoker.WriteRequests);
        Assert.Equal(1, configuration.Invoker.AppliedWrites);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetAsync_AfterWriteClientInitialized_PreservesReadTransportRetries(bool customTransport)
    {
        // Arrange
        using var configuration = new TestConfiguration(502, customTransport: customTransport);
        using var repository = new TestRepository(configuration);
        var client = configuration.Client;
        var settings = client.ElasticsearchClientSettings;
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync("one", new ScriptPatch("ctx._source.count++")));
        configuration.Invoker.FailNextRead = true;

        // Act
        var response = await client.GetAsync<TestDocument>(new GetRequest("transport-tests", "one"), TestContext.Current.CancellationToken);

        // Assert
        Assert.True(response.IsValidResponse);
        Assert.Equal(2, configuration.Invoker.ReadRequests);
        Assert.Same(settings, client.ElasticsearchClientSettings);
        Assert.Null(settings.MaxRetries);
        Assert.Equal(0, configuration.Invoker.AppliedWrites);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(0)]
    public async Task PatchAsync_WithCustomTransport_UsesConfiguredTransport(int? maxRetries)
    {
        // Arrange
        using var configuration = new TestConfiguration(200, customTransport: true, maxRetries: maxRetries);
        using var repository = new TestRepository(configuration);

        // Act
        Assert.True(await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++")));

        // Assert
        Assert.NotNull(configuration.CustomTransport);
        Assert.Equal(1, configuration.CustomTransport.Requests);
        Assert.Equal(1, configuration.Invoker.AppliedWrites);
    }

    [Fact]
    public async Task PatchAsync_WithCustomTransportCopyingRequestConfiguration_UsesConfiguredTransport()
    {
        // Arrange
        using var configuration = new TestConfiguration(200, customTransport: true, copyRequestConfiguration: true);
        using var repository = new TestRepository(configuration);

        // Act
        Assert.True(await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++")));

        // Assert
        Assert.NotNull(configuration.CustomTransport);
        Assert.Equal(1, configuration.CustomTransport.Requests);
        Assert.Equal(1, configuration.Invoker.WriteRequests);
        Assert.Equal(1, configuration.Invoker.AppliedWrites);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchAsync_WithInvalidResponseAfterSendingToSingleNode_DoesNotRetry(bool customTransport)
    {
        // Arrange
        var exception = new HttpRequestException(HttpRequestError.InvalidResponse, "Received an invalid status line: 'unexpected'");
        using var configuration = new TestConfiguration(200, applyBeforeFailure: true, customTransport: customTransport, singleNode: true, failureException: exception);
        using var repository = new TestRepository(configuration);

        // Act
        var failure = await Record.ExceptionAsync(async () =>
        {
            _ = await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++"));
        });

        // Assert
        Assert.Equal(1, configuration.Invoker.WriteRequests);
        Assert.Equal(1, configuration.Invoker.AppliedWrites);
        Assert.IsType<DocumentException>(failure);
    }

    [Fact]
    public async Task PatchAsync_WithExcludedAndDeadNodes_SelectsOneEligibleNode()
    {
        // Arrange
        using var configuration = new TestConfiguration(200, includeExcludedNode: true);
        using var repository = new TestRepository(configuration);
        configuration.Client.ElasticsearchClientSettings.NodePool.Nodes.Single(n => n.Uri.Host is "node-one").MarkDead(DateTimeOffset.UtcNow.AddMinutes(1));

        // Act
        Assert.True(await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++")));

        // Assert
        Assert.Equal(1, configuration.Invoker.WriteRequests);
        Assert.Equal("node-two", configuration.Invoker.LastNode);
    }

    [Fact]
    public void WriteTransport_WhenPoolIsReseeded_UpdatesTheSharedSourcePool()
    {
        // Arrange
        using var sourcePool = new SniffingNodePool([new Uri("http://node-one:9200")]);
        using var invoker = new RecordingInvoker(200, false);
        var settings = new ElasticsearchClientSettings(sourcePool, invoker);
        ITransport transport = new WriteTransport(new ElasticsearchClient(settings).Transport);

        // Act
        transport.Configuration.NodePool.Reseed([new Node(new Uri("http://replacement:9200"))]);

        // Assert
        Assert.Equal("replacement", Assert.Single(sourcePool.Nodes).Uri.Host);
        Assert.True(sourcePool.SniffedOnStartup);
        Assert.Equal("replacement", Assert.Single(transport.Configuration.NodePool.CreateView()).Uri.Host);
    }

    [Fact]
    public async Task PatchAsync_WithSniffOnConnectionFailure_DoesNotResendAfterReseeding()
    {
        // Arrange
        using var configuration = new TestConfiguration(502, applyBeforeFailure: true, sniffOnConnectionFailure: true);
        using var repository = new TestRepository(configuration);

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync("one", new ScriptPatch("ctx._source.count++")));

        // Assert
        Assert.Equal(1, configuration.Invoker.WriteRequests);
        Assert.Equal(1, configuration.Invoker.AppliedWrites);
        Assert.Equal(1, configuration.Invoker.SniffRequests);
        Assert.Equal("replacement", Assert.Single(configuration.Client.ElasticsearchClientSettings.NodePool.Nodes).Uri.Host);
    }

    [Fact]
    public async Task PatchAsync_WhenReseedingRetainsFailedNode_PreservesSharedNodeHealth()
    {
        // Arrange
        using var configuration = new TestConfiguration(502, sniffOnConnectionFailure: true);
        using var repository = new TestRepository(configuration);
        configuration.Invoker.RetainNodesOnSniff = true;

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync("one", new ScriptPatch("ctx._source.count++")));

        // Assert
        Assert.Equal(1, configuration.Invoker.WriteRequests);
        Assert.Equal(1, configuration.Invoker.SniffRequests);
        Assert.Single(configuration.Client.ElasticsearchClientSettings.NodePool.Nodes, node => !node.IsAlive);
    }

    [Fact]
    public async Task PatchAsync_WithHealthySniffingPool_PreservesNodeRotation()
    {
        // Arrange
        using var configuration = new TestConfiguration(200, sniffOnConnectionFailure: true);
        using var repository = new TestRepository(configuration);

        // Act
        Assert.True(await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++")));
        Assert.True(await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++")));

        // Assert
        Assert.Equal(2, configuration.Invoker.WriteNodes.Distinct().Count());
        Assert.Equal(0, configuration.Invoker.SniffRequests);
    }

    [Fact]
    public async Task PatchAsync_WithPingingEnabled_DoesNotPingHealthyNodesAgain()
    {
        // Arrange
        using var configuration = new TestConfiguration(200, singleNode: true, enablePings: true);
        using var repository = new TestRepository(configuration);

        // Act
        Assert.True(await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++")));
        Assert.True(await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++")));

        // Assert
        Assert.Equal(2, configuration.Invoker.WriteRequests);
        Assert.Equal(1, configuration.Invoker.PingRequests);
        Assert.True(Assert.Single(configuration.Client.ElasticsearchClientSettings.NodePool.Nodes).IsAlive);
    }

    [Fact]
    public async Task PatchAsync_WithExcludedSeedNodes_DiscoversEligibleNodeBeforeSelectingWriteTarget()
    {
        // Arrange
        using var configuration = new TestConfiguration(200, sniffOnStartup: true);
        using var repository = new TestRepository(configuration);

        // Act
        Assert.True(await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++")));
        Assert.True(await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++")));

        // Assert
        Assert.Equal(2, configuration.Invoker.WriteRequests);
        Assert.Equal(1, configuration.Invoker.SniffRequests);
        Assert.Equal(1, configuration.Invoker.PingRequests);
        Assert.Equal("replacement", configuration.Invoker.LastNode);
    }

    [Fact]
    public async Task PatchAsync_WhenStartupDiscoveryFails_RetriesOnlyTheUnsentWrite()
    {
        // Arrange
        using var configuration = new TestConfiguration(200, sniffOnStartup: true);
        using var repository = new TestRepository(configuration);
        configuration.Invoker.SniffFailuresRemaining = 1;

        // Act
        Assert.True(await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++"), o => o.Retry(1)));

        // Assert
        Assert.Equal(1, configuration.Invoker.WriteRequests);
        Assert.Equal(1, configuration.Invoker.AppliedWrites);
        Assert.Equal(2, configuration.Invoker.SniffRequests);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchAsync_WithConfiguredAuthenticationAndHooks_PreservesSettingsAndResourceOwnership(bool customTransport)
    {
        // Arrange
        var configuration = new TestConfiguration(200, customTransport: customTransport);
        using (configuration)
        {
            using (var repository = new TestRepository(configuration))
            {
                // Act
                Assert.True(await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++")));
                Assert.True(await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++")));

                // Assert
                Assert.Equal(2, configuration.BeforeRequestCalls);
                Assert.Same(configuration.Authentication, configuration.Invoker.LastAuthentication);
                Assert.Equal(TimeSpan.FromSeconds(37), configuration.Invoker.LastRequestTimeout);
                if (customTransport)
                    Assert.Equal(2, configuration.CustomTransport!.Requests);
            }

            Assert.Equal(0, configuration.Invoker.DisposeCount);
        }

        Assert.Equal(1, configuration.Invoker.DisposeCount);
    }

    private static Task WriteAsync(TestRepository repository, string operation, int retry)
    {
        var document = new TestDocument { Id = "one" };
        TestDocument[] documents = [document, new TestDocument { Id = "two" }];
        return operation switch
        {
            "Add" => repository.AddAsync(document, o => o.Retry(retry)),
            "Save" => repository.SaveAsync(document, o => o.Retry(retry)),
            "BulkSave" => repository.SaveAsync(documents, o => o.Retry(retry)),
            "Remove" => repository.RemoveAsync(document, o => o.Retry(retry)),
            "BulkRemove" => repository.RemoveAsync(documents, o => o.Retry(retry)),
            "PartialPatch" => repository.PatchAsync("one", new PartialPatch(new { count = 1 }), o => o.Retry(retry)),
            "BulkPatch" => repository.PatchAsync(new Ids(new[] { "one", "two" }), new ScriptPatch("ctx._source.count++"), o => o.Retry(retry)),
            "PipelinePatch" => repository.PatchAsync("one", new ScriptPatch("ctx._source.count++"), o => o.Retry(retry)),
            "PatchAllByQuery" => repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"), o => o.UseUpdateByQuery().Retry(retry)),
            "RemoveAllByQuery" => repository.RemoveAllAsync(q => q, o => o.Retry(retry)),
            _ => throw new ArgumentOutOfRangeException(nameof(operation))
        };
    }

    private sealed class TestDocument : IIdentity
    {
        public string Id { get; set; } = null!;
        public int Count { get; set; }
    }

    private sealed class TestRepository : ElasticRepositoryBase<TestDocument>
    {
        public TestRepository(TestConfiguration configuration, bool usePipeline = false) : base(new TestIndex(configuration))
        {
            DisableCache();
            NotificationsEnabled = false;
            DefaultPipeline = usePipeline ? "test-pipeline" : null;
        }
    }

    private sealed class TestIndex(TestConfiguration configuration) : Index<TestDocument>(configuration, "transport-tests")
    {
        public override Task EnsureIndexAsync(object? target) => Task.CompletedTask;
    }

    private sealed class TestConfiguration(int failureStatus, bool applyBeforeFailure = false, bool customTransport = false, int? maxRetries = null, bool singleNode = false, bool includeExcludedNode = false, Exception? failureException = null, bool sniffOnConnectionFailure = false, bool enablePings = false, bool sniffOnStartup = false, bool copyRequestConfiguration = false) : ElasticConfiguration
    {
        public RecordingInvoker Invoker { get; } = new(failureStatus, applyBeforeFailure, failureException);
        public AuthorizationHeader Authentication { get; } = new BasicAuthentication("test", "test");
        public RecordingTransport? CustomTransport { get; private set; }
        public int BeforeRequestCalls { get; private set; }

        protected override NodePool CreateConnectionPool()
        {
            if (singleNode)
                return new SingleNodePool(new Uri("http://node-one:9200"));

            if (sniffOnConnectionFailure)
                return new SniffingNodePool([new Uri("http://node-one:9200"), new Uri("http://node-two:9200")]);

            if (sniffOnStartup)
                return new SniffingNodePool([new Node(new Uri("http://seed:9200"), ["master"])]);

            return includeExcludedNode
                ? new StaticNodePool([new Uri("http://excluded:9200"), new Uri("http://node-one:9200"), new Uri("http://node-two:9200")])
                : new StaticNodePool([new Uri("http://node-one:9200"), new Uri("http://node-two:9200")]);
        }

        protected override IRequestInvoker CreateRequestInvoker() => Invoker;

        protected override void ConfigureSettings(ElasticsearchClientSettings settings)
        {
            base.ConfigureSettings(settings);
            settings.DisablePing(!enablePings).DeadTimeout(TimeSpan.Zero).MaxDeadTimeout(TimeSpan.Zero)
                .Authentication(Authentication).RequestTimeout(TimeSpan.FromSeconds(37));
            settings.OnBeforeRequest((ElasticsearchClient client, Elastic.Clients.Elasticsearch.Requests.Request request, EndpointPath path, ref PostData? data, ref IRequestConfiguration? configuration) =>
            {
                BeforeRequestCalls++;
            });
            if (maxRetries is { } retries)
                settings.MaximumRetries(retries);
            if (includeExcludedNode)
                settings.NodePredicate(node => node.Uri.Host is not "excluded");
            if (sniffOnConnectionFailure)
                settings.SniffOnStartup(false).SniffOnConnectionFault(true);
            if (sniffOnStartup)
                settings.SniffOnStartup(true).NodePredicate(node => node.Uri.Host is not "seed");
        }

        protected override ElasticsearchClient CreateElasticClient()
        {
            var client = base.CreateElasticClient();
            if (!customTransport)
                return client;

            CustomTransport = new RecordingTransport(client.Transport, copyRequestConfiguration);
            return new ElasticsearchClient(CustomTransport);
        }
    }

    private sealed class RecordingTransport(ITransport<IElasticsearchClientSettings> inner, bool copyRequestConfiguration) : ITransport<IElasticsearchClientSettings>
    {
        public int Requests { get; private set; }
        public IElasticsearchClientSettings Configuration => inner.Configuration;
        ITransportConfiguration ITransport.Configuration => Configuration;

        public TResponse Request<TResponse>(in EndpointPath path, PostData? postData, Action<Activity>? configureActivity, IRequestConfiguration? localConfiguration)
            where TResponse : TransportResponse, new()
        {
            Requests++;
            return inner.Request<TResponse>(path, postData, configureActivity, CopyConfiguration(localConfiguration));
        }

        public Task<TResponse> RequestAsync<TResponse>(in EndpointPath path, PostData? postData, Action<Activity>? configureActivity, IRequestConfiguration? localConfiguration, CancellationToken cancellationToken = default)
            where TResponse : TransportResponse, new()
        {
            Requests++;
            return inner.RequestAsync<TResponse>(path, postData, configureActivity, CopyConfiguration(localConfiguration), cancellationToken);
        }

        private IRequestConfiguration? CopyConfiguration(IRequestConfiguration? configuration) =>
            copyRequestConfiguration && configuration is not null ? new RequestConfiguration(configuration) : configuration;
    }

    private sealed class RecordingInvoker(int failureStatus, bool applyBeforeFailure, Exception? failureException = null) : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _responseFactoryOwner = new();
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };

        public ResponseFactory ResponseFactory => _responseFactoryOwner.ResponseFactory;
        public int WriteRequests { get; private set; }
        public int ReadRequests { get; private set; }
        public int SniffRequests { get; private set; }
        public int SniffFailuresRemaining { get; set; }
        public bool RetainNodesOnSniff { get; set; }
        public bool UnstructuredFailure { get; set; }
        public bool JsonFailure { get; set; }
        public int PingRequests { get; private set; }
        public int AppliedWrites { get; private set; }
        public bool FailNextRead { get; set; }
        public AuthorizationHeader? LastAuthentication { get; private set; }
        public TimeSpan LastRequestTimeout { get; private set; }
        public int DisposeCount { get; private set; }
        public string? LastNode { get; private set; }
        public List<string?> WriteNodes { get; } = [];

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            LastAuthentication = boundConfiguration.AuthenticationHeader;
            LastRequestTimeout = boundConfiguration.RequestTimeout;
            LastNode = endpoint.Node?.Uri.Host;
            string path = endpoint.PathAndQuery;
            int status = 200;
            string body;
            if (endpoint.Method is HttpMethod.HEAD)
            {
                PingRequests++;
                body = "{}";
            }
            else if (path.Contains("_nodes", StringComparison.Ordinal))
            {
                SniffRequests++;
                if (SniffFailuresRemaining > 0)
                {
                    SniffFailuresRemaining--;
                    status = 502;
                }
                body = RetainNodesOnSniff
                    ? """{"nodes":{"one":{"name":"node-one","roles":["data","ingest","master"],"http":{"publish_address":"node-one:9200"}},"two":{"name":"node-two","roles":["data","ingest","master"],"http":{"publish_address":"node-two:9200"}}}}"""
                    : """{"nodes":{"node":{"name":"replacement","roles":["data","ingest","master"],"http":{"publish_address":"replacement:9200"}}}}""";
            }
            else if (endpoint.Method is HttpMethod.GET)
            {
                ReadRequests++;
                status = FailNextRead ? 502 : 200;
                FailNextRead = false;
                body = """{"_index":"transport-tests","_id":"one","_version":1,"found":true,"_source":{"id":"one","count":1}}""";
            }
            else
            {
                WriteRequests++;
                WriteNodes.Add(endpoint.Node?.Uri.Host);
                if (WriteRequests is 1)
                    status = failureStatus;
                if (status is 200 || applyBeforeFailure)
                    AppliedWrites++;

                if (WriteRequests is 1 && failureException is not null)
                    return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, failureException, null, _headers, Stream.Null, null, -1, null, null);

                body = path.Contains("_bulk", StringComparison.Ordinal)
                    ? """{"took":1,"errors":false,"items":[{"update":{"_index":"transport-tests","_id":"one","status":200,"result":"updated"}},{"update":{"_index":"transport-tests","_id":"two","status":200,"result":"updated"}}]}"""
                    : path.Contains("_delete_by_query", StringComparison.Ordinal)
                        ? """{"took":1,"timed_out":false,"total":1,"deleted":1,"version_conflicts":0,"failures":[]}"""
                        : path.Contains("_update_by_query", StringComparison.Ordinal)
                            ? """{"took":1,"timed_out":false,"total":1,"updated":1,"version_conflicts":0,"failures":[]}"""
                            : """{"_index":"transport-tests","_id":"one","_version":2,"result":"updated","_seq_no":1,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}""";
            }

            bool unstructured = status is not 200 && UnstructuredFailure;
            if (status is not 200)
            {
                string errorType = status is 503 ? "unavailable_shards_exception" : "injected_fault";
                body = unstructured
                    ? JsonFailure ? """{"message":"upstream connection terminated"}""" : "upstream connect error or disconnect/reset before headers. reset reason: connection termination"
                    : $$"""{"error":{"type":"{{errorType}}","reason":"injected {{status}}"},"status":{{status}}}""";
            }

            byte[] bytes = Encoding.UTF8.GetBytes(body);
            using var stream = new MemoryStream(bytes);
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, status, unstructured ? null : _headers, stream, unstructured && !JsonFailure ? "text/plain" : "application/json", bytes.Length, null, null);
        }

        public Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
            where TResponse : TransportResponse, new()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Request<TResponse>(endpoint, boundConfiguration, postData));
        }

        public void Dispose()
        {
            DisposeCount++;
            ((IDisposable)_responseFactoryOwner).Dispose();
        }
    }
}
