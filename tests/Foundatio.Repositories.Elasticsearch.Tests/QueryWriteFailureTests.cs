using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class QueryWriteFailureTests
{
    [Fact]
    public async Task PatchAsync_WithUnstructuredServiceUnavailableWhileReading_RetriesReadBeforeWriting()
    {
        // Arrange
        const string document = """{"docs":[{"_index":"query-tests","_id":"one","found":true,"_source":{"id":"one","count":0}}]}""";
        const string saved = """{"took":1,"errors":false,"items":[{"index":{"_index":"query-tests","_id":"one","status":200,"result":"updated","_seq_no":1,"_primary_term":1}}]}""";
        using var configuration = new TestConfiguration(("_mget", 503, "upstream connection terminated"), ("_mget", 200, document), ("_bulk", 200, saved));
        using var repository = new TestRepository(configuration);

        // Act
        Assert.True(await repository.PatchAsync("one", new ActionPatch<TestDocument>(document => document.Count++), o => o.Retry(1)));

        // Assert
        Assert.Equal(3, configuration.Invoker.Requests);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RemoveAsync_WithMissingUnversionedDocuments_DoesNotRaiseRemovalEvents(bool bulk)
    {
        // Arrange
        const string singleResponse = """{"_index":"query-tests","_id":"one","result":"not_found","_version":2,"_seq_no":1,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}""";
        const string bulkResponse = """{"took":1,"errors":false,"items":[{"delete":{"_index":"query-tests","_id":"one","status":404,"result":"not_found"}},{"delete":{"_index":"query-tests","_id":"two","status":404,"result":"not_found"}}]}""";
        using var configuration = new TestConfiguration(bulk ? ("_bulk", 200, bulkResponse) : ("_doc/one", 404, singleResponse));
        using var repository = new TestRepository(configuration);
        int removed = 0;
        repository.DocumentsRemoved.AddHandler((_, args) =>
        {
            removed += args.Documents.Count;
            return Task.CompletedTask;
        });
        TestDocument[] documents = bulk ? [new TestDocument { Id = "one" }, new TestDocument { Id = "two" }] : [new TestDocument { Id = "one" }];

        // Act
        await repository.RemoveAsync(documents, o => o.Retry(0));

        // Assert
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Equal(0, removed);
    }

    [Theory]
    [InlineData(200, true, false)]
    [InlineData(200, false, true)]
    [InlineData(500, false, true)]
    public async Task RemoveAllAsync_WithPartialQueryFailure_NotifiesCompletedDeletesAndThrows(int status, bool timedOut, bool itemFailure)
    {
        // Arrange
        string response = QueryResponse(deleted: 1, timedOut, itemFailure);
        using var configuration = new TestConfiguration(("_delete_by_query", status, response));
        using var repository = new TestRepository(configuration);

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.RemoveAllAsync(q => q, o => o.Retry(0)));

        // Assert
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Equal(1, repository.QueryNotifications);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task PatchAsync_WithPipelineAndQueryFailure_ThrowsInsteadOfReportingNoop(bool timedOut, bool itemFailure)
    {
        // Arrange
        using var configuration = new TestConfiguration(("_update_by_query", 200, QueryResponse(deleted: 0, timedOut, itemFailure)));
        using var repository = new TestRepository(configuration, usePipeline: true);

        // Act & Assert
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync("one", new ScriptPatch("ctx._source.count++"), o => o.Retry(0)));
        Assert.Equal(1, configuration.Invoker.Requests);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task PatchAsync_WithPipelineAndPartiallyAppliedQuery_NotifiesBeforeThrowing(bool timedOut, bool itemFailure)
    {
        // Arrange
        using var configuration = new TestConfiguration(("_update_by_query", 200, QueryResponse(deleted: 0, timedOut, itemFailure, updated: 1)));
        using var repository = new TestRepository(configuration, usePipeline: true);
        int changed = 0;
        repository.DocumentsChanged.AddHandler((_, args) =>
        {
            changed++;
            return Task.CompletedTask;
        });

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync("one", new ScriptPatch("ctx._source.count++"), o => o.Retry(1)));

        // Assert
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Equal(1, changed);
    }

    [Fact]
    public async Task PatchAsync_WithPipelineAndVersionConflict_RequestsConflictCountsAndRetries()
    {
        // Arrange
        const string conflict = """{"took":1,"timed_out":false,"total":1,"updated":0,"deleted":0,"version_conflicts":1,"failures":[]}""";
        using var configuration = new TestConfiguration(("_update_by_query", 200, conflict), ("_update_by_query", 200, QueryResponse(0, false, false, updated: 1)));
        using var repository = new TestRepository(configuration, usePipeline: true);

        // Act
        Assert.True(await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++"), o => o.Retry(1)));

        // Assert
        Assert.Equal(2, configuration.Invoker.Requests);
        Assert.All(configuration.Invoker.RequestBodies, body =>
        {
            using var document = JsonDocument.Parse(body);
            Assert.Equal("proceed", document.RootElement.GetProperty("conflicts").GetString());
        });
    }

    [Theory]
    [InlineData(400, 0)]
    [InlineData(502, 1)]
    [InlineData(503, 1)]
    [InlineData(504, 1)]
    public async Task PatchAllAsync_WithFailedTaskSubmission_NotifiesOnlyWhenOutcomeMayBeUnknown(int status, int expectedNotifications)
    {
        // Arrange
        string response = status is 503 ? "upstream connect error or disconnect/reset before headers" : $$"""{"error":{"type":"injected_failure","reason":"injected"},"status":{{status}}}""";
        using var configuration = new TestConfiguration(("_update_by_query", status, response));
        using var repository = new TestRepository(configuration);

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"), o => o.UseUpdateByQuery().Retry(1)));

        // Assert
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Equal(expectedNotifications, repository.QueryNotifications);
    }

    [Fact]
    public async Task PatchAllAsync_WithTimedOutTask_NotifiesAndThrowsInsteadOfReportingSuccess()
    {
        // Arrange
        const string completedTask = """
            {"completed":true,"task":{"node":"node","id":1,"type":"transport","action":"indices:data/write/update/byquery","status":{"total":2,"updated":1,"created":0,"deleted":0,"version_conflicts":0}},"response":{"took":1,"timed_out":true,"total":2,"updated":1,"created":0,"deleted":0,"version_conflicts":0,"failures":[]}}
            """;
        using var configuration = new TestConfiguration(("_update_by_query", 200, """{"task":"node:1"}"""), ("_tasks", 200, completedTask));
        using var repository = new TestRepository(configuration);

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"), o => o.UseUpdateByQuery().Retry(0)));

        // Assert
        Assert.Equal(2, configuration.Invoker.Requests);
        Assert.Equal(1, repository.QueryNotifications);
    }

    [Theory]
    [InlineData(429, 0)]
    [InlineData(429, 1)]
    [InlineData(503, 0)]
    [InlineData(503, 1)]
    public async Task PatchAllAsync_WithTransientTaskSubmission_RespectsRetryBudget(int status, int retries)
    {
        // Arrange
        const string completedTask = """
            {"completed":true,"task":{"node":"node","id":1,"type":"transport","action":"indices:data/write/update/byquery","status":{"total":1,"updated":1,"created":0,"deleted":0,"version_conflicts":0}},"response":{"took":1,"timed_out":false,"total":1,"updated":1,"created":0,"deleted":0,"version_conflicts":0,"failures":[]}}
            """;
        string errorType = status is 503 ? "unavailable_shards_exception" : "es_rejected_execution_exception";
        using var configuration = new TestConfiguration(
            ("_update_by_query", status, $$"""{"error":{"type":"{{errorType}}","reason":"injected"},"status":{{status}}}"""),
            ("_update_by_query", 200, """{"task":"node:1"}"""),
            ("_tasks", 200, completedTask));
        using var repository = new TestRepository(configuration);

        // Act
        if (retries is 0)
            await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"),
                o => o.UseUpdateByQuery().Retry(retries)));
        else
            Assert.Equal(1, await repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"),
                o => o.UseUpdateByQuery().Retry(retries)));

        // Assert
        Assert.Equal(retries is 0 ? 1 : 3, configuration.Invoker.Requests);
        Assert.Equal(retries is 0 ? 0 : 1, repository.QueryNotifications);
    }

    private static string QueryResponse(int deleted, bool timedOut, bool itemFailure, int updated = 0)
    {
        string failures = itemFailure
            ? """[{"index":"query-tests","id":"two","status":500,"cause":{"type":"exception","reason":"injected failure"}}]"""
            : "[]";
        return $$"""{"took":1,"timed_out":{{timedOut.ToString().ToLowerInvariant()}},"total":2,"updated":{{updated}},"deleted":{{deleted}},"version_conflicts":0,"failures":{{failures}}}""";
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
            DefaultPipeline = usePipeline ? "test-pipeline" : null;
        }

        public int QueryNotifications { get; private set; }

        protected override Task SendQueryNotificationsAsync(ChangeType changeType, IRepositoryQuery query, ICommandOptions options)
        {
            QueryNotifications++;
            return Task.CompletedTask;
        }
    }

    private sealed class TestIndex(TestConfiguration configuration) : Index<TestDocument>(configuration, "query-tests")
    {
        public override Task EnsureIndexAsync(object? target) => Task.CompletedTask;
    }

    private sealed class TestConfiguration(params (string Path, int Status, string Body)[] responses) : ElasticConfiguration
    {
        public CannedResponseInvoker Invoker { get; } = new(responses);

        protected override IRequestInvoker CreateRequestInvoker() => Invoker;
    }

    private sealed class CannedResponseInvoker(params (string Path, int Status, string Body)[] responses) : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _responseFactoryOwner = new();
        private readonly Queue<(string Path, int Status, string Body)> _responses = new(responses);
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };
        public ResponseFactory ResponseFactory => _responseFactoryOwner.ResponseFactory;
        public int Requests { get; private set; }
        public List<string> RequestBodies { get; } = [];

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            int status;
            string body;
            if (endpoint.PathAndQuery.Contains("_mapping", StringComparison.Ordinal))
            {
                status = 200;
                body = """{"query-tests":{"mappings":{"properties":{"id":{"type":"keyword"}}}}}""";
            }
            else
            {
                Requests++;
                using var requestStream = new MemoryStream();
                postData?.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                RequestBodies.Add(Encoding.UTF8.GetString(requestStream.ToArray()));
                var response = _responses.Dequeue();
                Assert.Contains(response.Path, endpoint.PathAndQuery, StringComparison.Ordinal);
                status = response.Status;
                body = response.Body;
            }

            byte[] bytes = Encoding.UTF8.GetBytes(body);
            using var stream = new MemoryStream(bytes);
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, status, _headers, stream, body.StartsWith('{') ? "application/json" : "text/plain", bytes.Length, null, null);
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
