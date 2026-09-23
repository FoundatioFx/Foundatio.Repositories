from pathlib import Path

def replace(path, old, new, count=1):
    p=Path(path)
    s=p.read_text()
    assert s.count(old)==count,(path,old,s.count(old))
    p.write_text(s.replace(old,new))

replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexDispatchTests.cs', 'new SystemTextJsonSerializer()', 'new Foundatio.Serializer.SystemTextJsonSerializer()', 2)
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexTests.cs', 'GetAliasAsync(partition, TestCancellationToken)', 'GetAliasAsync(Indices.Index(partition), TestCancellationToken)')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexTests.cs', 'bool serving = aliases.Aliases.Values.Any(a => a.Aliases.Count > 0);', 'Assert.NotNull(aliases.Aliases);\n                bool serving = aliases.Aliases.Values.Any(a => a.Aliases.Count > 0);')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexTests.cs', 'bool serving = aliases.Values.Values.Any(a => a.Aliases.Count > 0);', 'Assert.NotNull(aliases.Values);\n                bool serving = aliases.Values.Values.Any(a => a.Aliases.Count > 0);')
p='src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexer.cs'
replace(p, '''        if (countsVerified && workItem.DeleteOld && workItem.OldIndex != workItem.NewIndex)
            await DeleteOldIndexAsync(workItem, progressCallbackAsync, cancellationToken).AnyContext();

        controller.Complete();''', '''        bool cleanupConfirmed = true;
        if (countsVerified && workItem.DeleteOld && workItem.OldIndex != workItem.NewIndex)
            cleanupConfirmed = await DeleteOldIndexAsync(workItem, progressCallbackAsync, cancellationToken).AnyContext();

        if (cleanupConfirmed)
            controller.Complete();''')
replace(p, '''    private async Task DeleteOldIndexAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, CancellationToken cancellationToken)
    {
        var deleteIndexResponse = await _client.Indices.DeleteAsync(Indices.Index(workItem.OldIndex), cancellationToken).AnyContext();''', '''    private async Task<bool> DeleteOldIndexAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, CancellationToken cancellationToken)
    {
        var deleteIndexResponse = await _client.Indices.DeleteAsync(Indices.Index(workItem.OldIndex),
            d => d.RequestConfiguration(r => r.ForceNode(ReindexTransport.SelectSingleAttemptNode(_client)).MaxRetries(0)), cancellationToken).AnyContext();''')
replace(p, '''        if (deleteIndexResponse.IsValidResponse)
        {
            await progressCallbackAsync(99, $"Deleted index: {workItem.OldIndex}").AnyContext();
            return;
        }''', '''        if (deleteIndexResponse.IsValidResponse && deleteIndexResponse.Acknowledged)
        {
            await progressCallbackAsync(99, $"Deleted index: {workItem.OldIndex}").AnyContext();
            return true;
        }''')
replace(p, '''        await progressCallbackAsync(99, $"Failed to delete old index {workItem.OldIndex}: {deleteIndexResponse.ElasticsearchServerError}").AnyContext();
    }''', '''        await progressCallbackAsync(99, $"Failed to delete old index {workItem.OldIndex}: {deleteIndexResponse.ElasticsearchServerError}").AnyContext();
        // Completion still stands, but a delayed deletion is not permission for another controller
        // to reuse these physical names. Preserve admission until cleanup is reconciled.
        return false;
    }''')
p='tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexDispatchTests.cs'
replace(p, '    [Fact]\n    public void SingleAttemptNode_RespectsEligibility()', '''    [Theory]
    [InlineData(200, "{\\"acknowledged\\":true}", true)]
    [InlineData(200, "{\\"acknowledged\\":false}", false)]
    [InlineData(503, "{}", false)]
    public async Task SourceCleanup_RequiresPositiveAcknowledgementWithoutTransportRetries(int status, string body, bool confirmed)
    {
        var invoker = new SequenceRequestInvoker((status, body), (status, body), (status, body));
        var reindexer = new ElasticReindexer(new ElasticsearchClient(Settings(invoker)), new Foundatio.Serializer.SystemTextJsonSerializer());
        var method = typeof(ElasticReindexer).GetMethod("DeleteOldIndexAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        Func<int, string?, Task> progress = (_, _) => Task.CompletedTask;

        var actual = await (Task<bool>)method.Invoke(reindexer, [Work(), progress, TestContext.Current.CancellationToken])!;

        Assert.Equal(confirmed, actual);
        Assert.Equal(1, invoker.RequestsReceived);
    }

    [Fact]
    public void SingleAttemptNode_RespectsEligibility()''')
p='docs/guide/reindex-safety.md'
s=Path(p).read_text()
s += '\nA completed migration whose optional source deletion is unacknowledged still has completion evidence, but retains controller admission until that potentially delayed deletion is reconciled. Completion and cleanup are separate outcomes; do not reuse physical index names while cleanup remains uncertain.\n'
Path(p).write_text(s)
