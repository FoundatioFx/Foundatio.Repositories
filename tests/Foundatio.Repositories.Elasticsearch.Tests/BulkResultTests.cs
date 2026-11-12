using System.IO;
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
    public async Task From_WithMixedItemErrors_ClassifiesEachItem()
    {
        // Arrange
        const string json = """
            {"took":1,"errors":true,"items":[
              {"index":{"_index":"employees","_id":"ok","status":200,"result":"updated","_seq_no":5,"_primary_term":1}},
              {"update":{"_index":"employees","_id":"noop","status":200,"result":"noop","_seq_no":3,"_primary_term":1}},
              {"index":{"_index":"employees","_id":"conflict","status":409,"error":{"type":"version_conflict_engine_exception","reason":"[conflict]: version conflict, required seqNo [1], primary term [1]. current document has seqNo [2] and primary term [1]"}}},
              {"index":{"_index":"employees","_id":"missing","status":409,"error":{"type":"version_conflict_engine_exception","reason":"[missing]: version conflict, required seqNo [1], primary term [1]. but no document was found"}}},
              {"index":{"_index":"employees","_id":"throttled","status":429,"error":{"type":"es_rejected_execution_exception","reason":"rejected"}}},
              {"index":{"_index":"employees","_id":"unavailable","status":503,"error":{"type":"unavailable_shards_exception","reason":"unavailable"}}},
              {"index":{"_index":"logs","_id":"occ","status":400,"error":{"type":"o_c_c_not_supported_exception","reason":"Optimistic concurrency control is not supported on indices with sequence numbers disabled"}}},
              {"index":{"_index":"employees","_id":"bad","status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse"}}}
            ]}
            """;

        // Act
        var result = await GetBulkResultAsync(json);

        // Assert
        Assert.False(result.HasTransportError);
        Assert.Equal(["ok", "noop"], result.SuccessfulIds);
        Assert.Equal(["noop"], result.NoopIds);
        Assert.Equal(["conflict"], result.ConflictIds);
        Assert.Equal(["missing"], result.MissingIds);
        Assert.Equal(["throttled", "unavailable"], result.RetryableIds);
        Assert.Equal(["occ"], result.VersioningNotSupportedIds);
        Assert.Equal(["bad"], result.FatalIds);
        Assert.Equal(1, result.ModifiedCount);
        Assert.False(result.IsSuccess);
        Assert.Equal("logs", result.Errors["occ"].Index);
    }

    [Fact]
    public async Task From_WithOnlyMissingDocumentConflicts_IsSuccess()
    {
        // Arrange
        const string json = """
            {"took":1,"errors":true,"items":[
              {"delete":{"_index":"employees","_id":"1","status":409,"error":{"type":"version_conflict_engine_exception","reason":"[1]: version conflict, required seqNo [0], primary term [1]. but no document was found"}}}
            ]}
            """;

        // Act
        var result = await GetBulkResultAsync(json);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(["1"], result.MissingIds);
    }

    [Fact]
    public async Task From_WithTransportFailure_ReportsStatusCode()
    {
        // Act
        var result = await GetBulkResultAsync("""{"error":{"type":"cluster_block_exception","reason":"blocked"},"status":503}""", 503);

        // Assert
        Assert.True(result.HasTransportError);
        Assert.Equal(503, result.TransportStatusCode);
        Assert.False(result.IsSuccess);
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
    [InlineData(502, true)]
    [InlineData(503, true)]
    [InlineData(504, true)]
    [InlineData(409, false)]
    [InlineData(400, false)]
    [InlineData(null, false)]
    public void IsTransientStatus_WithStatus_ReturnsExpected(int? status, bool expected)
    {
        Assert.Equal(expected, ElasticWriteErrors.IsTransientStatus(status));
    }
}
