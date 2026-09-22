from pathlib import Path
import sys

TEST = Path('tests/Foundatio.Repositories.Elasticsearch.Tests/Configuration/IndexCompatibilityTests.TaskResponse.cs')
SOURCE = Path('src/Foundatio.Repositories.Elasticsearch/Repositories')
TEST_CONTENT = r'''using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Exceptions;
using Foundatio.Resilience;
using Foundatio.Serializer;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    private const string EmptyTaskResponse = """{"total":0,"created":0,"updated":0,"deleted":0,"noops":0,"version_conflicts":0,"failures":[]}""";
    private static readonly string[] TaskCounterNames = ["total", "created", "updated", "deleted", "noops", "version_conflicts"];

    [Theory]
    [InlineData("null")]
    [InlineData("true")]
    [InlineData("false")]
    [InlineData("\"1\"")]
    [InlineData("1.5")]
    [InlineData("-1")]
    [InlineData("9223372036854775808")]
    [InlineData("{}")]
    public void TaskResponse_RejectsMalformedJsonCounters(string value)
    {
        foreach (string counter in TaskCounterNames)
        {
            // Arrange
            using var document = JsonDocument.Parse(EmptyTaskResponse.Replace($"\"{counter}\":0", $"\"{counter}\":{value}", StringComparison.Ordinal));
            var dictionary = document.RootElement.EnumerateObject().ToDictionary(property => property.Name, property => (object)property.Value);

            // Act & Assert
            Assert.Null(ElasticReindexTaskResponseReader.ReadStatus(document.RootElement));
            Assert.Null(ElasticReindexTaskResponseReader.ReadCompleted(document.RootElement));
            Assert.Null(ElasticReindexTaskResponseReader.ReadStatus(dictionary));
            Assert.Null(ElasticReindexTaskResponseReader.ReadCompleted(dictionary));
        }
    }

    [Theory]
    [InlineData("boolean")]
    [InlineData("string")]
    [InlineData("double")]
    [InlineData("single")]
    [InlineData("decimal")]
    [InlineData("negative")]
    [InlineData("overflow")]
    [InlineData("null")]
    public void TaskResponse_DoesNotCoerceDictionaryCounters(string kind)
    {
        foreach (string counter in TaskCounterNames)
        {
            // Arrange
            var values = TaskCounterNames.ToDictionary(name => name, _ => (object)0L);
            values["failures"] = Array.Empty<object>();
            values[counter] = kind switch
            {
                "boolean" => true,
                "string" => "1",
                "double" => 1.5d,
                "single" => 1.5f,
                "decimal" => 1.5m,
                "negative" => -1L,
                "overflow" => UInt64.MaxValue,
                "null" => null!,
                _ => throw new ArgumentOutOfRangeException(nameof(kind))
            };

            // Act & Assert
            Assert.Null(ElasticReindexTaskResponseReader.ReadStatus(values));
            Assert.Null(ElasticReindexTaskResponseReader.ReadCompleted(values));
        }
    }

    [Theory]
    [InlineData("byte")]
    [InlineData("sbyte")]
    [InlineData("short")]
    [InlineData("ushort")]
    [InlineData("int")]
    [InlineData("uint")]
    [InlineData("long")]
    [InlineData("ulong")]
    public void TaskResponse_PreservesSupportedIntegerCounters(string kind)
    {
        // Arrange
        object one = kind switch
        {
            "byte" => (byte)1,
            "sbyte" => (sbyte)1,
            "short" => (short)1,
            "ushort" => (ushort)1,
            "int" => 1,
            "uint" => 1U,
            "long" => 1L,
            "ulong" => 1UL,
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };
        var values = TaskCounterNames.ToDictionary(name => name, _ => (object)0L);
        values["total"] = one;
        values["created"] = one;
        values["failures"] = Array.Empty<object>();

        // Act
        var status = ElasticReindexTaskResponseReader.ReadStatus(values);
        var response = ElasticReindexTaskResponseReader.ReadCompleted(values);

        // Assert
        Assert.NotNull(status);
        Assert.NotNull(response);
        Assert.Equal(1L, status.Value.Total);
        Assert.Equal(1L, response.Total);
        Assert.Equal(1L, response.Created);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("\"true\"")]
    [InlineData("1")]
    [InlineData("{}")]
    [InlineData("[]")]
    public void TaskResponse_RejectsMalformedTimeoutFlags(string value)
    {
        // Arrange
        using var document = JsonDocument.Parse(EmptyTaskResponse.Replace("\"failures\":[]", $"\"timed_out\":{value},\"failures\":[]", StringComparison.Ordinal));
        var dictionary = document.RootElement.EnumerateObject().ToDictionary(property => property.Name, property => (object)property.Value);

        // Act & Assert
        Assert.Null(ElasticReindexTaskResponseReader.ReadCompleted(document.RootElement));
        Assert.Null(ElasticReindexTaskResponseReader.ReadCompleted(dictionary));
    }

    [Fact]
    public void TaskResponse_WireCountersDoNotUseApplicationNamingPolicy()
    {
        // Arrange
        using var document = JsonDocument.Parse("""{"total":3,"created":2,"updated":0,"deleted":0,"noops":0,"version_conflicts":1,"failures":[]}""");
        var serializer = new SystemTextJsonSerializer(new JsonSerializerOptions { PropertyNameCaseInsensitive = false });

        // Act
        var response = ElasticReindexTaskResponseReader.ReadCompleted(document.RootElement, serializer);

        // Assert
        Assert.NotNull(response);
        Assert.Equal(3L, response.Total);
        Assert.Equal(2L, response.Created);
        Assert.Equal(1L, response.VersionConflicts);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task TaskResponse_CompatibilityRunnerChecksTimeoutWithOrWithoutBufferedBody(bool timedOut, bool bufferResponse)
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(
            new StubResponse(200, """{"task":"node:1"}""", Request: "POST /_reindex"),
            new StubResponse(200, CreateCompletedTaskResponse(timedOut), Request: "GET /_tasks/node:1"));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker)
            .DisableDirectStreaming(bufferResponse));
        var runner = new ElasticReindexTaskRunner(client, TimeProvider.System);
        int terminated = 0;

        // Act & Assert
        if (timedOut)
        {
            var exception = await Assert.ThrowsAsync<RepositoryException>(() => runner.RunCompatibilityReindexAsync(
                "employees-v1", "reindexed-v9-employees-v1", null, null, (_, _) => Task.CompletedTask, () => terminated++, TestContext.Current.CancellationToken));
            Assert.Contains("Timed out: True", exception.Message, StringComparison.Ordinal);
        }
        else
        {
            var response = await runner.RunCompatibilityReindexAsync(
                "employees-v1", "reindexed-v9-employees-v1", null, null, (_, _) => Task.CompletedTask, () => terminated++, TestContext.Current.CancellationToken);
            Assert.Equal(1L, response.Total);
            Assert.Equal(1L, response.Created);
        }
        Assert.Equal(1, terminated);
        Assert.Equal(0, invoker.RemainingResponses);
        Assert.Equal(2, invoker.Requests.Count);
    }

    [Fact]
    public async Task TaskResponse_SchemaReindexDoesNotPromoteTimedOutCopy()
    {
        // Arrange
        string completed = CreateCompletedTaskResponse(true);
        var invoker = new SequenceRequestInvoker(
            new StubResponse(200, """{"employees-v1":{"aliases":{"employees":{}}}}""", Request: "GET /employees-v1"),
            new StubResponse(200, """{"employees-v1":{"aliases":{"employees":{}}}}""", Request: "GET /employees-v1/_alias"),
            new StubResponse(200, """{"took":0,"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}"""),
            new StubResponse(200, """{"task":"node:1"}""", Request: "POST /_reindex"),
            new StubResponse(200, completed, Request: "GET /_tasks/node:1"),
            new StubResponse(200, """{"nodes":{}}""", Request: "POST /_tasks/node:1/_cancel"),
            new StubResponse(200, completed, Request: "GET /_tasks/node:1"));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker));
        var reindexer = new ElasticReindexer(client, new SystemTextJsonSerializer(), TimeProvider.System, new ResiliencePolicyProvider());
        var workItem = new ReindexWorkItem { OldIndex = "employees-v1", NewIndex = "employees-v2", Alias = "employees", TimestampField = "updatedUtc", DeleteOld = true };

        // Act
        await reindexer.ReindexAsync(workItem);

        // Assert
        Assert.DoesNotContain(invoker.Requests, request => request.Contains("/_aliases", StringComparison.Ordinal) || request.StartsWith("DELETE ", StringComparison.Ordinal));
        Assert.Equal(0, invoker.RemainingResponses);
    }

    private static string CreateCompletedTaskResponse(bool timedOut)
    {
        return $$"""
            {
              "completed":true,
              "task":{"node":"node","id":1,"type":"transport","action":"indices:data/write/reindex",
                "status":{"total":1,"created":1,"updated":0,"deleted":0,"noops":0,"version_conflicts":0},
                "start_time_in_millis":1,"running_time_in_nanos":1,"cancellable":true,"cancelled":false,"headers":{}},
              "response":{"total":1,"created":1,"updated":0,"deleted":0,"noops":0,"version_conflicts":0,
                "timed_out":{{timedOut.ToString().ToLowerInvariant()}},"failures":[]}
            }
            """;
    }
}
'''

def replace_once(text, old, new):
    if text.count(old) != 1:
        raise RuntimeError(f'Expected exactly one occurrence: {old[:100]!r}; found {text.count(old)}')
    return text.replace(old, new)

if sys.argv[1] == 'tests':
    if TEST.exists():
        raise RuntimeError('Refusing to overwrite an existing task-response test file')
    TEST.write_text(TEST_CONTENT)
elif sys.argv[1] == 'fix':
    path = SOURCE / 'ElasticReindexTaskResponseReader.cs'
    text = path.read_text()
    start = text.index('        if (response is JsonElement element)')
    end = text.index('        if (response is not IDictionary<string, object> values)', start)
    text = text[:start] + '''        if (response is JsonElement element)
        {
            if (!TryReadStatus(element, out var status)
                || !TryReadTimedOut(element, out bool elementTimedOut)
                || !element.TryGetProperty("failures", out var elementFailures)
                || elementFailures.ValueKind is not JsonValueKind.Array)
            {
                return null;
            }

            var parsedFailures = serializer is null
                ? elementFailures.Deserialize<IReadOnlyCollection<ElasticReindexTaskFailure>>(SerializerOptions)
                : serializer.Deserialize<IReadOnlyCollection<ElasticReindexTaskFailure>>(elementFailures.GetRawText());
            if (parsedFailures is null)
                return null;

            return new ElasticReindexTaskResponse
            {
                Total = status.Total,
                Created = status.Created,
                Updated = status.Updated,
                Deleted = status.Deleted,
                Noops = status.Noops,
                VersionConflicts = status.VersionConflicts,
                TimedOut = elementTimedOut,
                Failures = parsedFailures
            };
        }

''' + text[end:]
    text = replace_once(text, '            || !TryReadFailures(values, serializer, out var failures))', '            || !TryReadTimedOut(values, out bool timedOut)\n            || !TryReadFailures(values, serializer, out var failures))')
    text = replace_once(text, '            VersionConflicts = versionConflicts,\n            Failures = failures', '            VersionConflicts = versionConflicts,\n            TimedOut = timedOut,\n            Failures = failures')
    start = text.index('    private static bool HasCompletedResponseShape(')
    end = text.index('    private static bool TryReadStatus(', start)
    text = text[:start] + text[end:]
    start = text.index('    private static bool TryReadInt64(JsonElement ')
    end = text.index('    private static string? ReadString(', start)
    text = text[:start] + '''    private static bool TryReadInt64(JsonElement element, string propertyName, out long number)
    {
        number = 0;
        return element.TryGetProperty(propertyName, out var value)
            && value.ValueKind is JsonValueKind.Number
            && value.TryGetInt64(out number)
            && number >= 0;
    }

    private static bool TryReadInt64(IDictionary<string, object> values, string key, out long number)
    {
        number = 0;
        if (!values.TryGetValue(key, out object? value) || value is null)
            return false;

        if (value is JsonElement element)
            return element.ValueKind is JsonValueKind.Number && element.TryGetInt64(out number) && number >= 0;

        // Wire counters are integers, not values that happen to convert or round to integers.
        switch (value)
        {
            case byte or sbyte or short or ushort or int or uint or long:
                number = Convert.ToInt64(value);
                return number >= 0;
            case ulong unsigned when unsigned <= Int64.MaxValue:
                number = (long)unsigned;
                return true;
            default:
                return false;
        }
    }

    private static bool TryReadTimedOut(JsonElement element, out bool timedOut)
    {
        timedOut = false;
        if (!element.TryGetProperty("timed_out", out var value))
            return true;

        if (value.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
            return false;

        timedOut = value.GetBoolean();
        return true;
    }

    private static bool TryReadTimedOut(IDictionary<string, object> values, out bool timedOut)
    {
        timedOut = false;
        if (!values.TryGetValue("timed_out", out object? value))
            return true;

        if (value is bool boolean)
        {
            timedOut = boolean;
            return true;
        }

        if (value is JsonElement element && element.ValueKind is JsonValueKind.True or JsonValueKind.False)
        {
            timedOut = element.GetBoolean();
            return true;
        }

        return false;
    }

''' + text[end:]
    text = replace_once(text, '    public long VersionConflicts { get; init; }\n', '    public long VersionConflicts { get; init; }\n\n    [JsonPropertyName("timed_out")]\n    public bool TimedOut { get; init; }\n')
    path.write_text(text)
    path = SOURCE / 'ElasticReindexTaskRunner.cs'
    text = path.read_text()
    text = replace_once(text, 'if (failures > 0 || result.VersionConflicts > 0', 'if (result.TimedOut || failures > 0 || result.VersionConflicts > 0')
    text = replace_once(text, 'Version conflicts: {result.VersionConflicts}, Failures: {failures}.', 'Version conflicts: {result.VersionConflicts}, Failures: {failures}, Timed out: {result.TimedOut}.')
    path.write_text(text)
    path = SOURCE / 'ElasticReindexer.cs'
    text = path.read_text()
    text = replace_once(text, '                    taskSuccess = true;\n                    break;', '''                    if (lastReindexResponse.TimedOut)
                    {
                        _logger.LogError("Reindex task {ReindexTaskId} timed out while reindexing {OldIndex} -> {NewIndex}",
                            result.Task.FullyQualifiedId, workItem.OldIndex, workItem.NewIndex);
                        break;
                    }

                    taskSuccess = true;
                    break;''')
    path.write_text(text)
else:
    raise RuntimeError('Expected tests or fix')
