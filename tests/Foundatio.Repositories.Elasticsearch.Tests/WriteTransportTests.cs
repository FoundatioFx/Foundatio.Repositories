using System;
using System.Collections.Generic;
using System.IO;
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
    [InlineData(502)]
    [InlineData(504)]
    public async Task PatchAsync_WithGatewayFailureAfterScriptApplied_DoesNotRetry(int status)
    {
        // Arrange
        using var configuration = new TestConfiguration(status, applyBeforeFailure: true);
        using var repository = new TestRepository(configuration);

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync("one", new ScriptPatch("ctx._source.count++")));

        // Assert
        Assert.Equal(1, configuration.Invoker.WriteRequests);
        Assert.Equal(1, configuration.Invoker.AppliedWrites);
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

    [Fact]
    public async Task GetAsync_AfterWriteClientInitialized_PreservesReadTransportRetries()
    {
        // Arrange
        using var configuration = new TestConfiguration(502);
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

    private sealed class TestConfiguration(int failureStatus, bool applyBeforeFailure = false) : ElasticConfiguration
    {
        public RecordingInvoker Invoker { get; } = new(failureStatus, applyBeforeFailure);

        protected override NodePool CreateConnectionPool() => new StaticNodePool([new Uri("http://node-one:9200"), new Uri("http://node-two:9200")]);

        protected override IRequestInvoker CreateRequestInvoker() => Invoker;

        protected override void ConfigureSettings(ElasticsearchClientSettings settings)
        {
            base.ConfigureSettings(settings);
            settings.DisablePing().DeadTimeout(TimeSpan.Zero).MaxDeadTimeout(TimeSpan.Zero);
        }
    }

    private sealed class RecordingInvoker(int failureStatus, bool applyBeforeFailure) : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _responseFactoryOwner = new();
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };

        public ResponseFactory ResponseFactory => _responseFactoryOwner.ResponseFactory;
        public int WriteRequests { get; private set; }
        public int ReadRequests { get; private set; }
        public int AppliedWrites { get; private set; }
        public bool FailNextRead { get; set; }

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            string path = endpoint.PathAndQuery;
            int status = 200;
            string body;
            if (endpoint.Method is HttpMethod.GET)
            {
                ReadRequests++;
                status = FailNextRead ? 502 : 200;
                FailNextRead = false;
                body = """{"_index":"transport-tests","_id":"one","_version":1,"found":true,"_source":{"id":"one","count":1}}""";
            }
            else
            {
                WriteRequests++;
                if (WriteRequests is 1)
                    status = failureStatus;
                if (status is 200 || applyBeforeFailure)
                    AppliedWrites++;

                body = path.Contains("_bulk", StringComparison.Ordinal)
                    ? """{"took":1,"errors":false,"items":[{"update":{"_index":"transport-tests","_id":"one","status":200,"result":"updated"}},{"update":{"_index":"transport-tests","_id":"two","status":200,"result":"updated"}}]}"""
                    : path.Contains("_delete_by_query", StringComparison.Ordinal)
                        ? """{"took":1,"timed_out":false,"total":1,"deleted":1,"version_conflicts":0,"failures":[]}"""
                        : path.Contains("_update_by_query", StringComparison.Ordinal)
                            ? """{"took":1,"timed_out":false,"total":1,"updated":1,"version_conflicts":0,"failures":[]}"""
                            : """{"_index":"transport-tests","_id":"one","_version":2,"result":"updated","_seq_no":1,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}""";
            }

            if (status is not 200)
                body = $$"""{"error":{"type":"injected_fault","reason":"injected {{status}}"},"status":{{status}}}""";

            byte[] bytes = Encoding.UTF8.GetBytes(body);
            using var stream = new MemoryStream(bytes);
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, status, _headers, stream, "application/json", bytes.Length, null, null);
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
