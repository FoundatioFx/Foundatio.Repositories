from pathlib import Path
import sys
root=Path(sys.argv[1])
p=root/'tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexConsistencyIntegrationTests.cs'
s=p.read_text().replace('    private async Task CleanupAsync(ReindexWorkItem item)\n    {\n        await RawAsync', '    private Task CleanupAsync(ReindexWorkItem item)\n    {\n        return RawAsync')
p.write_text(s)
p=root/'src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexer.cs'
s=p.read_text()
s=s.replace('and the model cannot catch up; use QuiesceSource or stop writers', 'and these changes cannot be caught up; use QuiesceSource or stop writers')
needle='    private async Task SwitchAliasesAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, int progress, CancellationToken cancellationToken)\n    {'
assert s.count(needle)==1
s=s.replace(needle,needle+'\n        cancellationToken.ThrowIfCancellationRequested();')
needle='    private async Task<Dictionary<string, AliasDefinition>> GetIndexAliasesAsync(string index, CancellationToken cancellationToken)\n    {'
assert s.count(needle)==1
s=s.replace(needle,needle+'\n        cancellationToken.ThrowIfCancellationRequested();')
s=s.replace('!response.IsValidResponse || response.Shards is null || response.Shards.Failed > 0 || response.Shards.Successful != response.Shards.Total', '!response.IsValidResponse || response.Shards is null || response.Shards.Total <= 0 || response.Shards.Failed > 0 || response.Shards.Successful != response.Shards.Total')
old='''        if (!refreshResponse.IsValidResponse)
            _logger.LogWarning("Failed to refresh {OldIndex} and {NewIndex} before copying, so the copy may read stale data: {Error}", workItem.OldIndex, workItem.NewIndex, refreshResponse.ElasticsearchServerError);'''
assert s.count(old)==1
s=s.replace(old,'''        cancellationToken.ThrowIfCancellationRequested();
        // Refresh includes unavailable replicas in total; require successful work with no shard failures,
        // rather than incorrectly requiring replicas to be assigned on a single-node test/deployment.
        if (!refreshResponse.IsValidResponse || refreshResponse.Shards is null
            || refreshResponse.Shards.Failed > 0 || refreshResponse.Shards.Successful <= 0)
            throw new ReindexIncompleteException(workItem.OldIndex, workItem.NewIndex,
                "source refresh did not provide complete successful shard evidence; refusing to copy potentially stale data");''')
p.write_text(s)
p=root/'src/Foundatio.Repositories.Elasticsearch/Repositories/ReindexCompletionUnknownException.cs'
s=p.read_text();old='''/// A migration run with <see cref="Jobs.ReindexWorkItem.QuiesceSource"/> does not reach this state. Promotion
/// there happens only after reconciliation and verification succeeded, so a missing record can be re-derived
/// rather than escalated.'''
assert old in s
s=s.replace(old,'''/// This also applies to <see cref="Jobs.ReindexWorkItem.QuiesceSource"/> migrations. A redelivered work-item
/// flag cannot prove which attempt verified the promoted physical generation. Missing durable evidence is
/// never reconstructed from that flag alone.''');p.write_text(s)
p=root/'src/Foundatio.Repositories.Elasticsearch/Repositories/ReindexEvidence.cs';s=p.read_text()
old='''                throw new RepositoryException("Reconciliation bulk delete did not positively account for a requested identity.");'''
new=old+'''

            if (status is 200 && (!item.TryGetProperty("_shards", out var shards)
                || !TryInteger(shards, "failed", out long failed) || failed is not 0
                || !TryInteger(shards, "successful", out long successful) || successful <= 0))
                throw new RepositoryException("Reconciliation bulk delete returned incomplete shard evidence.");'''
assert s.count(old)==1;s=s.replace(old,new);p.write_text(s)
p=root/'tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexConsistencyEvidenceTests.cs'
s=p.read_text()
s=s.replace('new { _index = "target", _id = "id", status, result }', 'new { _index = "target", _id = "id", status, result, _shards = new { successful = 1, failed = 0 } }')
insert='''
    [Theory]
    [InlineData(0, 0)]
    [InlineData(1, 1)]
    public void BulkRejectsIncompleteShardAcknowledgement(int successful, int failed)
    {
        string body = JsonSerializer.Serialize(new { errors = false, items = new[]
        {
            new { delete = new { _index = "target", _id = "id", status = 200, result = "deleted", _shards = new { successful, failed } } }
        }});
        Assert.Throws<RepositoryException>(() => ReindexEvidence.RequireDeletes(body, "target", new[] { new ReindexDocumentIdentity("id", null) }));
    }
'''
pos=s.rfind('\n}');s=s[:pos]+insert+s[pos:];p.write_text(s)
for path in ['docs/guide/index-management.md','.agents/skills/foundatio-repositories/references/index-lifecycle.md']:
 p=root/path;p.write_text(p.read_text().replace('Unscriped quiesced','Unscripted quiesced'))
