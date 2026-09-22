from pathlib import Path
import sys
root=Path(sys.argv[1])
p=root/'src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexer.cs'
s=p.read_text().replace(".Append('\n');", ".Append('\\n');")
s=s.replace("the source's sequence number, not by the copy's own counters", "a complete primary-shard checkpoint vector, not by the copy's own counters")
s=s.replace("The source's max <c>_seq_no</c>", "Each primary shard's <c>max_seq_no</c>")
p.write_text(s)
# Existing tests must assert the deliberately stricter retention/recovery contract, not the old unsafe behavior.
p=root/'tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexTests.cs';s=p.read_text()
start=s.index('    public async Task QueuedQuiescedReindex_WhenPromotedWithoutCompletionRecord_RecordsCompletionAndAcknowledges()');end=s.index('\n    }',start)
part=s[start:end].replace('RecordsCompletionAndAcknowledges','RefusesToInventCompletionEvidence').replace('Assert.Equal(0, disposition.Abandoned);','Assert.Equal(1, disposition.Abandoned);').replace('Assert.Equal(1, disposition.Completed);','Assert.Equal(0, disposition.Completed);').replace('Assert.True(await reindexer.HasCompletionEvidenceAsync','Assert.False(await reindexer.HasCompletionEvidenceAsync')
part=part.replace('// Assert - acknowledged rather than abandoned or dead-lettered','// Assert - a work-item flag cannot establish what verified this physical generation').replace('// And completion is now durable, so a further redelivery resolves without re-deriving anything','// No completion evidence is fabricated from alias topology.')
s=s[:start]+part+s[end:]
start=s.index('    public async Task QuiescedReindex_WhenReconcileFails_RestoresWritesToTheSource()');end=s.index('\n    }',start)
part=s[start:end].replace('RestoresWritesToTheSource','RetainsFenceForOperatorRecovery').replace('Assert.True(write.IsValidResponse);','Assert.False(write.IsValidResponse);\n        Assert.Equal(403, write.ApiCallDetails.HttpStatusCode);').replace('// Assert - the failure surfaced and the source accepts writes again','// Assert - failure does not automatically reopen an uncertain source.')
s=s[:start]+part+s[end:]
# Remove obsolete safety explanations immediately preceding the two changed tests.
import re
for method in ['QueuedQuiescedReindex_WhenPromotedWithoutCompletionRecord_RefusesToInventCompletionEvidence','QuiescedReindex_WhenReconcileFails_RetainsFenceForOperatorRecovery']:
 pattern=r'    /// <summary>\n(?:(?!    \[Fact\]).)*?    \[Fact\]\n    public async Task '+method
 s=re.sub(pattern, '    [Fact]\n    public async Task '+method, s, flags=re.S)
p.write_text(s)
p=root/'tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexConsistencyEvidenceTests.cs'
p.write_text(r'''using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;
using Foundatio.Repositories.Exceptions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ReindexConsistencyEvidenceTests
{
    private const string Page = """{"_scroll_id":"scroll","timed_out":false,"_shards":{"total":2,"successful":2,"failed":0},"hits":{"hits":[{"_index":"target","_id":"shared","fields":{"_routing":["tenant-a"]}},{"_index":"target","_id":"shared","fields":{"_routing":["tenant-b"]}}]}}""";

    [Fact]
    public void PagePreservesRoutedDuplicateIdentities()
    {
        var page = ReindexEvidence.ReadPage(Page, "target");
        Assert.Equal(new[] { new ReindexDocumentIdentity("shared", "tenant-a"), new ReindexDocumentIdentity("shared", "tenant-b") }, page.Ids);
    }

    [Theory]
    [InlineData("timeout")]
    [InlineData("missing-timeout")]
    [InlineData("partial-shards")]
    [InlineData("failed-shards")]
    [InlineData("missing-scroll")]
    [InlineData("missing-hits")]
    [InlineData("null-hits")]
    [InlineData("foreign-index")]
    [InlineData("bad-routing")]
    public void PageRejectsIncompleteEvidence(string fault)
    {
        var page = JsonNode.Parse(Page)!.AsObject();
        switch (fault)
        {
            case "timeout": page["timed_out"] = true; break;
            case "missing-timeout": page.Remove("timed_out"); break;
            case "partial-shards": page["_shards"]!["successful"] = 1; break;
            case "failed-shards": page["_shards"]!["failed"] = 1; break;
            case "missing-scroll": page.Remove("_scroll_id"); break;
            case "missing-hits": page.Remove("hits"); break;
            case "null-hits": page["hits"]!["hits"] = null; break;
            case "foreign-index": page["hits"]!["hits"]![0]!["_index"] = "other"; break;
            case "bad-routing": page["hits"]!["hits"]![0]!["fields"]!["_routing"] = new JsonArray(1); break;
        }
        Assert.Throws<RepositoryException>(() => ReindexEvidence.ReadPage(page.ToJsonString(), "target"));
    }

    [Theory]
    [InlineData("error")]
    [InlineData("missing-found")]
    [InlineData("invalid-found")]
    [InlineData("missing-item")]
    [InlineData("wrong-id")]
    [InlineData("wrong-index")]
    [InlineData("wrong-routing")]
    public void MultiGetDoesNotConfuseItemFailureWithAbsence(string fault)
    {
        var response = JsonNode.Parse("""{"docs":[{"_index":"source","_id":"id","found":false}]}""")!.AsObject();
        var item = response["docs"]![0]!.AsObject();
        switch (fault)
        {
            case "error": item["error"] = new JsonObject { ["type"] = "unavailable_shards_exception" }; break;
            case "missing-found": item.Remove("found"); break;
            case "invalid-found": item["found"] = "false"; break;
            case "missing-item": response["docs"] = new JsonArray(); break;
            case "wrong-id": item["_id"] = "other"; break;
            case "wrong-index": item["_index"] = "other"; break;
            case "wrong-routing": item["_routing"] = "other"; break;
        }
        Assert.Throws<RepositoryException>(() => ReindexEvidence.ReadMissing(response.ToJsonString(), "source", new[] { new ReindexDocumentIdentity("id", "tenant-a") }));
    }

    [Fact]
    public void MultiGetRetainsTheRoutingOfTheMissingIdentity()
    {
        var requested = new[] { new ReindexDocumentIdentity("shared", "a"), new ReindexDocumentIdentity("shared", "b") };
        const string response = """{"docs":[{"_index":"source","_id":"shared","found":false},{"_index":"source","_id":"shared","found":true}]}""";
        Assert.Equal(requested[0], Assert.Single(ReindexEvidence.ReadMissing(response, "source", requested)));
    }

    [Theory]
    [InlineData("errors")]
    [InlineData("failed-item")]
    [InlineData("missing-item")]
    [InlineData("wrong-id")]
    [InlineData("wrong-index")]
    [InlineData("wrong-result")]
    [InlineData("missing-status")]
    public void BulkRequiresEveryDeletionToSucceed(string fault)
    {
        var response = JsonNode.Parse("""{"errors":false,"items":[{"delete":{"_index":"target","_id":"id","status":200,"result":"deleted"}}]}""")!.AsObject();
        var item = response["items"]![0]!["delete"]!.AsObject();
        switch (fault)
        {
            case "errors": response["errors"] = true; break;
            case "failed-item": item["status"] = 429; break;
            case "missing-item": response["items"] = new JsonArray(); break;
            case "wrong-id": item["_id"] = "other"; break;
            case "wrong-index": item["_index"] = "other"; break;
            case "wrong-result": item["result"] = "updated"; break;
            case "missing-status": item.Remove("status"); break;
        }
        Assert.Throws<RepositoryException>(() => ReindexEvidence.RequireDeletes(response.ToJsonString(), "target", new[] { new ReindexDocumentIdentity("id", null) }));
    }

    [Theory]
    [InlineData(200, "deleted")]
    [InlineData(404, "not_found")]
    public void BulkAcceptsOnlyAcknowledgedTerminalDeleteOutcomes(int status, string result)
    {
        string response = JsonSerializer.Serialize(new { errors = false, items = new[] { new { delete = new { _index = "target", _id = "id", status, result } } } });
        ReindexEvidence.RequireDeletes(response, "target", new[] { new ReindexDocumentIdentity("id", null) });
    }

    [Fact]
    public void CheckpointDetectsWritesBelowAnotherShardsMaximum()
    {
        var before = new ReindexSourceCheckpoint("generation", new Dictionary<int, long> { [0] = 1000, [1] = 10 });
        var after = new ReindexSourceCheckpoint("generation", new Dictionary<int, long> { [0] = 1000, [1] = 11 });
        Assert.False(before.Matches(after));
        Assert.False(before.Matches(before with { Uuid = "recreated" }));
    }

    [Theory]
    [InlineData("missing-primary")]
    [InlineData("duplicate-primary")]
    [InlineData("failed-shards")]
    [InlineData("wrong-generation")]
    public void CheckpointRejectsIncompleteOrForeignGenerations(string fault)
    {
        const string settings = """{"source":{"settings":{"index.uuid":"generation","index.number_of_shards":"2"}}}""";
        var stats = JsonNode.Parse("""{"_shards":{"total":2,"successful":2,"failed":0},"indices":{"source":{"uuid":"generation","shards":{"0":[{"routing":{"primary":true},"seq_no":{"max_seq_no":1000}}],"1":[{"routing":{"primary":true},"seq_no":{"max_seq_no":10}}]}}}}""")!.AsObject();
        var shards = stats["indices"]!["source"]!["shards"]!.AsObject();
        switch (fault)
        {
            case "missing-primary": shards.Remove("1"); break;
            case "duplicate-primary": shards["1"]!.AsArray().Add(shards["1"]![0]!.DeepClone()); break;
            case "failed-shards": stats["_shards"]!["failed"] = 1; break;
            case "wrong-generation": stats["indices"]!["source"]!["uuid"] = "recreated"; break;
        }
        Assert.Throws<RepositoryException>(() => ReindexSourceCheckpoint.Parse("source", settings, stats.ToJsonString()));
    }

    [Fact]
    public async Task ScriptedQuiescenceFailsBeforeAnyElasticsearchRequest()
    {
        int requests = 0;
        var invoker = new SequenceRequestInvoker((500, "{}"));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker).OnRequestCompleted(_ => requests++));
        var reindexer = new ElasticReindexer(client, new Foundatio.Serializer.SystemTextJsonSerializer());
        await Assert.ThrowsAsync<NotSupportedException>(() => reindexer.ReindexAsync(new ReindexWorkItem
        {
            OldIndex = "source", NewIndex = "target", Alias = "logical", QuiesceSource = true, Script = "ctx._source.migrated = true"
        }, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(0, requests);
    }

    [Fact]
    public async Task RetainedWriteBlockDoesNotReleaseDuringDisposal()
    {
        int requests = 0;
        var invoker = new SequenceRequestInvoker(
            (200, """{"source":{"settings":{"index":{}}}}"""),
            (200, """{"acknowledged":true,"shards_acknowledged":true,"indices":[{"name":"source","blocked":true}]}"""));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker).OnRequestCompleted(_ => requests++));
        var block = await IndexWriteBlock.ApplyAsync(client, "source", NullLogger.Instance, TestContext.Current.CancellationToken);
        block.Retain();
        await block.ReleaseAsync();
        await block.DisposeAsync();
        Assert.Equal(2, requests);
    }
}
''')
