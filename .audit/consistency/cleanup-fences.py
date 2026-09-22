from pathlib import Path
import sys
root=Path(sys.argv[1])
p=root/'src/Foundatio.Repositories.Elasticsearch/Repositories/IndexWriteBlock.cs';s=p.read_text()
old='''    public static async Task<IndexWriteBlock> ApplyAsync(ElasticsearchClient client, string index, ILogger logger, TimeSpan releaseTimeout, CancellationToken cancellationToken = default)
    {'''
new='''    public static Task<IndexWriteBlock> ApplyAsync(ElasticsearchClient client, string index, ILogger logger, TimeSpan releaseTimeout, CancellationToken cancellationToken = default)
        => ApplyCoreAsync(client, index, logger, releaseTimeout, false, cancellationToken);

    /// <summary>Retains migration fencing responsibility before dispatch, including an ambiguous acquisition.</summary>
    public static Task<IndexWriteBlock> ApplyRetainedAsync(ElasticsearchClient client, string index, ILogger logger, CancellationToken cancellationToken = default)
        => ApplyCoreAsync(client, index, logger, DefaultReleaseTimeout, true, cancellationToken);

    private static async Task<IndexWriteBlock> ApplyCoreAsync(ElasticsearchClient client, string index, ILogger logger,
        TimeSpan releaseTimeout, bool retain, CancellationToken cancellationToken)
    {'''
assert s.count(old)==1;s=s.replace(old,new)
old='        var block = new IndexWriteBlock(client, index, wasAlreadyBlocked, releaseTimeout, logger);'
assert s.count(old)==1;s=s.replace(old,old+'\n        if (retain)\n            block.Retain();')
s=s.replace('/// If applying or confirming a new block fails, acquisition attempts to release it before propagating\n/// the original exception.', '/// Ordinary scoped acquisition attempts release after failure. Migration acquisition retains the\n/// fence before dispatch, including when the acquisition response is lost or unconfirmed.')
p.write_text(s)
p=root/'src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexer.cs';s=p.read_text()
s=s.replace('await IndexWriteBlock.ApplyAsync(_client, workItem.OldIndex, _logger, cancellationToken)', 'await IndexWriteBlock.ApplyRetainedAsync(_client, workItem.OldIndex, _logger, cancellationToken)')
a=s.index('    private async Task<bool> VerifyDocumentCountsAsync(');b=s.index('    private async Task DeleteOldIndexAsync(',a)
part=s[a:b]
old='        if (!newDocCountResponse.IsValidResponse || !oldDocCountResponse.IsValidResponse)'
new='''        if (!refreshResponse.IsValidResponse || refreshResponse.Shards is null
            || refreshResponse.Shards.Failed > 0 || refreshResponse.Shards.Successful <= 0
            || !newDocCountResponse.IsValidResponse || newDocCountResponse.Shards is null
            || newDocCountResponse.Shards.Total <= 0 || newDocCountResponse.Shards.Failed > 0
            || newDocCountResponse.Shards.Successful != newDocCountResponse.Shards.Total
            || !oldDocCountResponse.IsValidResponse || oldDocCountResponse.Shards is null
            || oldDocCountResponse.Shards.Total <= 0 || oldDocCountResponse.Shards.Failed > 0
            || oldDocCountResponse.Shards.Successful != oldDocCountResponse.Shards.Total)'''
assert part.count(old)==1;part=part.replace(old,new);s=s[:a]+part+s[b:];p.write_text(s)
p=root/'tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexCleanupFenceTests.cs'
p.write_text(r'''using System;
using System.Reflection;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;
using Foundatio.Repositories.Exceptions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ReindexCleanupFenceTests
{
    [Theory]
    [InlineData(200, "{}")]
    [InlineData(500, "{}")]
    [InlineData(200, "{\"acknowledged\":true,\"shards_acknowledged\":false,\"indices\":[]}")]
    public async Task AmbiguousMigrationBlockAcquisitionDoesNotDispatchAnUnblock(int status, string body)
    {
        int requests = 0;
        var invoker = new SequenceRequestInvoker(
            (200, """{"source":{"settings":{"index":{}}}}"""),
            (status, body));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker)
            .OnRequestCompleted(_ => requests++));
        await Assert.ThrowsAsync<RepositoryException>(() => IndexWriteBlock.ApplyRetainedAsync(client, "source", NullLogger.Instance, TestContext.Current.CancellationToken));
        Assert.Equal(2, requests);
    }

    [Theory]
    [InlineData("refresh")]
    [InlineData("target")]
    [InlineData("source")]
    public async Task PostCutoverPartialShardEvidenceDoesNotAuthorizeSourceDeletion(string partial)
    {
        const string completeRefresh = """{"_shards":{"total":2,"successful":2,"failed":0}}""";
        const string failedRefresh = """{"_shards":{"total":2,"successful":1,"failed":1}}""";
        const string completeCount = """{"count":10,"_shards":{"total":2,"successful":2,"failed":0}}""";
        const string partialCount = """{"count":10,"_shards":{"total":2,"successful":1,"failed":1}}""";
        var invoker = new SequenceRequestInvoker(
            (200, partial == "refresh" ? failedRefresh : completeRefresh),
            (200, partial == "target" ? partialCount : completeCount),
            (200, partial == "source" ? partialCount : completeCount));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker));
        var reindexer = new ElasticReindexer(client, new Foundatio.Serializer.SystemTextJsonSerializer());
        var verify = typeof(ElasticReindexer).GetMethod("VerifyDocumentCountsAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        Func<int, string?, Task> progress = (_, _) => Task.CompletedTask;
        var task = (Task<bool>)verify.Invoke(reindexer, new object[]
        {
            new ReindexWorkItem { OldIndex = "source", NewIndex = "target", Alias = "logical" }, progress, TestContext.Current.CancellationToken
        })!;
        Assert.False(await task);
    }
}
''')
for name in ['docs/guide/index-management.md','.agents/skills/foundatio-repositories/references/index-lifecycle.md']:
 p=root/name
 with p.open('a') as f:
  f.write('\nMigration write-block acquisition now retains its fence responsibility before dispatch. An invalid, partial or lost acquisition response no longer triggers an automatic unblock in the quiesced path; the outcome requires inspection. The ordinary scoped block helper retains its historical release-on-disposal behavior for non-migration callers. Post-cutover count verification also retains the source on incomplete refresh/count shard evidence; equal numeric counts do not override partial responses.\n')
