from pathlib import Path
p = Path('src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexer.cs')
s = p.read_text()
start = s.index('        var refreshResponse = await _client.Indices.RefreshAsync(', s.index('private async Task<bool> VerifyDocumentCountsAsync'))
end = s.index('\n        return false;\n    }', start)
s = s[:start] + '''        long sourceCount;
        long destinationCount;
        try
        {
            await RefreshForCopyAsync(workItem, cancellationToken).AnyContext();
            destinationCount = await GetDocumentCountAsync(workItem.NewIndex, cancellationToken).AnyContext();
            sourceCount = await GetDocumentCountAsync(workItem.OldIndex, cancellationToken).AnyContext();
        }
        catch (RepositoryException ex)
        {
            // The alias already moved, so retain the source rather than declaring partial refresh/count
            // evidence sufficient for cleanup. This advisory path does not certify live-write consistency.
            _logger.LogWarning(ex, "Could not verify the reindex of {OldIndex} -> {NewIndex}; keeping the source because refresh or count evidence was incomplete.", workItem.OldIndex, workItem.NewIndex);
            return false;
        }

        await progressCallbackAsync(98, $"Old Docs: {sourceCount} New Docs: {destinationCount}").AnyContext();
        if (destinationCount >= sourceCount)
            return true;

        long missing = sourceCount - destinationCount;
        // Scripts and writes through the already-switched alias can legitimately reduce output cardinality.
        _logger.LogWarning("Reindex of {OldIndex} -> {NewIndex} left the destination {MissingCount:N0} document(s) short ({OldCount:N0} -> {NewCount:N0}). This can be legitimate, so it is not treated as a failure, but {OldIndex} will not be deleted.",
            workItem.OldIndex, workItem.NewIndex, missing, sourceCount, destinationCount, workItem.OldIndex);
''' + s[end:]
p.write_text(s)
p = Path('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexSafetyTests.cs')
s = p.read_text()
old = '    [Fact]\n    public async Task QueuedLock_WhenContendedWithoutCallerCancellation_ReturnsNoLock()'
assert old in s
s = s.replace(old, '''    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task DefaultCleanup_RequiresCompleteRefreshAndCountEvidence(int failedStep)
    {
        const string refresh = """{"_shards":{"total":1,"successful":1,"failed":0}}""";
        const string count = """{"count":5,"_shards":{"total":1,"successful":1,"failed":0}}""";
        var responses = new[] { (200, refresh), (200, refresh), (200, count), (200, count) };
        if (failedStep >= 0)
            responses[failedStep] = (200, """{"count":5,"_shards":{"total":1,"successful":0,"failed":1}}""");
        int requests = 0;
        var settings = new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://in-memory.invalid:9200")),
                new SequenceRequestInvoker(responses))
            .MaximumRetries(0).OnRequestCompleted(_ => requests++);
        var reindexer = new ElasticReindexer(new ElasticsearchClient(settings), new Foundatio.Serializer.SystemTextJsonSerializer());
        var method = typeof(ElasticReindexer).GetMethod("VerifyDocumentCountsAsync", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        Func<int, string?, Task> progress = (_, _) => Task.CompletedTask;

        bool mayDeleteSource = await (Task<bool>)method.Invoke(reindexer, [WorkItem(), progress, TestContext.Current.CancellationToken])!;

        Assert.Equal(failedStep < 0, mayDeleteSource);
        Assert.Equal(failedStep < 0 ? 4 : failedStep + 1, requests);
    }

''' + old)
p.write_text(s)
