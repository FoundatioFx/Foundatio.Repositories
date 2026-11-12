using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Bulk;
using Elastic.Transport;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class BulkResultTests
{
    private static readonly ElasticsearchClient _client = new(new ElasticsearchClientSettings(new InMemoryRequestInvoker()));

    private static async Task<BulkResult> GetBulkResultAsync(string responseJson, int statusCode = 200)
    {
        if (statusCode is not 200)
        {
            var invoker = new InMemoryRequestInvoker(Encoding.UTF8.GetBytes(responseJson), statusCode, null, "application/json", null);
            var client = new ElasticsearchClient(new ElasticsearchClientSettings(invoker));
            var failed = await client.BulkAsync(new BulkRequest { Operations = [new BulkDeleteOperation("1") { Index = "employees" }] });
            return BulkResult.From(failed);
        }

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(responseJson));
        var response = _client.ElasticsearchClientSettings.RequestResponseSerializer.Deserialize<BulkResponse>(stream);
        Assert.NotNull(response);
        return BulkResult.From(response);
    }

    [Fact]
    public async Task From_WithMixedItemErrors_ClassifiesEachItemInRequestOrder()
    {
        // Arrange
        const string json = """
            {"took":1,"errors":true,"items":[
              {"index":{"_index":"employees","_id":"ok","status":200,"result":"updated","_seq_no":5,"_primary_term":1}},
              {"update":{"_index":"employees","_id":"noop","status":200,"result":"noop","_seq_no":3,"_primary_term":1}},
              {"index":{"_index":"employees","_id":"conflict","status":409,"error":{"type":"version_conflict_engine_exception","reason":"[conflict]: version conflict, required seqNo [1], primary term [1]. current document has seqNo [2] and primary term [1]"}}},
              {"index":{"_index":"employees","_id":"missing","status":409,"error":{"type":"version_conflict_engine_exception","reason":"[missing]: version conflict, required seqNo [1], primary term [1]. but no document was found"}}},
              {"update":{"_index":"employees","_id":"gone","status":404,"error":{"type":"document_missing_exception","reason":"[gone]: document missing"}}},
              {"index":{"_index":"employees","_id":"throttled","status":429,"error":{"type":"es_rejected_execution_exception","reason":"rejected"}}},
              {"index":{"_index":"employees","_id":"unavailable","status":503,"error":{"type":"unavailable_shards_exception","reason":"unavailable"}}},
              {"index":{"_index":"employees","_id":"gateway","status":504,"error":{"type":"timeout","reason":"gateway timeout"}}},
              {"index":{"_index":"logs","_id":"occ","status":400,"error":{"type":"o_c_c_not_supported_exception","reason":"Optimistic concurrency control is not supported on indices with sequence numbers disabled"}}},
              {"index":{"_index":"employees","_id":"bad","status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse"}}},
              {"index":{"_index":"other","_id":"ok","status":201,"result":"created","_seq_no":0,"_primary_term":1}}
            ]}
            """;

        // Act
        var result = await GetBulkResultAsync(json);

        // Assert
        Assert.Null(result.RequestFailure);
        Assert.Equal(
            [WriteOutcome.Succeeded, WriteOutcome.Noop, WriteOutcome.Conflict, WriteOutcome.Missing, WriteOutcome.Missing, WriteOutcome.Transient, WriteOutcome.Transient, WriteOutcome.Fatal, WriteOutcome.VersioningNotSupported, WriteOutcome.Fatal, WriteOutcome.Succeeded],
            result.Items.Select(i => i.Outcome));
        Assert.Equal("1:5", result.Items[0].Version.ToVersionString());
        Assert.Equal(("employees", "ok"), (result.Items[0].Index, result.Items[0].Id));
        Assert.Equal(("other", "ok"), (result.Items[10].Index, result.Items[10].Id));
        Assert.Equal("logs", result.Items[8].Index);
    }

    [Fact]
    public async Task ToWriteResults_WithFewerItemsThanOperations_MarksTheRestFatal()
    {
        // Arrange
        var result = await GetBulkResultAsync("""{"took":1,"errors":false,"items":[{"index":{"_index":"employees","_id":"1","status":200,"result":"updated","_seq_no":1,"_primary_term":1}}]}""");

        // Act
        var results = result.ToWriteResults(3);

        // Assert
        Assert.Equal([WriteOutcome.Succeeded, WriteOutcome.Fatal, WriteOutcome.Fatal], results.Select(r => r.Outcome));
    }

    [Theory]
    [InlineData(503, true)]
    [InlineData(429, true)]
    [InlineData(502, false)]
    [InlineData(504, false)]
    [InlineData(500, false)]
    public async Task From_WithRequestFailure_AppliesItToEveryOperation(int status, bool transient)
    {
        // Act
        var result = await GetBulkResultAsync($$"""{"error":{"type":"some_exception","reason":"failed"},"status":{{status}}}""", status);

        // Assert
        Assert.NotNull(result.RequestFailure);
        Assert.Equal(status, result.RequestFailure.Status);
        Assert.All(result.ToWriteResults(2), r => Assert.Equal(transient ? WriteOutcome.Transient : WriteOutcome.Fatal, r.Outcome));
    }

    [Theory]
    [InlineData("[1]: version conflict, required seqNo [5], primary term [1]. current document has seqNo [6] and primary term [2]", "1:5", "2:6")]
    [InlineData("[1]: version conflict, required seqNo [0], primary term [1]. but no document was found", "1:0", null)]
    [InlineData("something else", null, null)]
    [InlineData(null, null, null)]
    public void ParseConflictVersions_WithReason_ReturnsExpectedAndCurrent(string? reason, string? expected, string? current)
    {
        var (parsedExpected, parsedCurrent) = ElasticWriteErrors.ParseConflictVersions(reason);

        Assert.Equal(expected, parsedExpected);
        Assert.Equal(current, parsedCurrent);
    }

    [Theory]
    [InlineData(429, true)]
    [InlineData(503, true)]
    [InlineData(502, false)]
    [InlineData(504, false)]
    [InlineData(409, false)]
    [InlineData(400, false)]
    [InlineData(null, false)]
    public void IsTransientStatus_WithStatus_ReturnsExpected(int? status, bool expected)
    {
        Assert.Equal(expected, ElasticWriteErrors.IsTransientStatus(status));
    }

    [Fact]
    public void ClassifyRequestFailure_WithoutStatus_IsTransientOnlyWhenRequestWasNotSent()
    {
        var refused = new HttpRequestException(HttpRequestError.ConnectionError, "connection refused", new SocketException((int)SocketError.ConnectionRefused));
        var dns = new HttpRequestException(HttpRequestError.NameResolutionError, "no such host");
        var noNodes = new TransportException(PipelineFailure.NoNodesAttempted, "no nodes", (Exception?)null);
        var reset = new HttpRequestException(HttpRequestError.ResponseEnded, "response ended");
        var timeout = new TaskCanceledException("timed out", new TimeoutException());

        Assert.Equal(WriteOutcome.Transient, WriteOutcomeClassifier.ClassifyRequestFailure(null, refused));
        Assert.Equal(WriteOutcome.Transient, WriteOutcomeClassifier.ClassifyRequestFailure(null, new TransportException(PipelineFailure.BadResponse, "wrapped", refused)));
        Assert.Equal(WriteOutcome.Transient, WriteOutcomeClassifier.ClassifyRequestFailure(null, dns));
        Assert.Equal(WriteOutcome.Transient, WriteOutcomeClassifier.ClassifyRequestFailure(null, noNodes));
        Assert.Equal(WriteOutcome.Fatal, WriteOutcomeClassifier.ClassifyRequestFailure(null, reset));
        Assert.Equal(WriteOutcome.Fatal, WriteOutcomeClassifier.ClassifyRequestFailure(null, timeout));
        Assert.Equal(WriteOutcome.Fatal, WriteOutcomeClassifier.ClassifyRequestFailure(null, null));
    }
}
