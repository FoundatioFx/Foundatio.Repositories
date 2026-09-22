from pathlib import Path

def replace(path, old, new):
    file = Path(path)
    text = file.read_text()
    assert old in text, (path, old)
    file.write_text(text.replace(old, new))

replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexSafetyTests.cs',
'''        var work = WorkItem();
        work.Alias = new string('a', 250);
        work.OldIndex = new string('b', 250);
        work.NewIndex = new string('c', 250);''',
'''        var work = WorkItem() with
        {
            Alias = new string('a', 250),
            OldIndex = new string('b', 250),
            NewIndex = new string('c', 250)
        };''')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexSafetyTests.cs',
'''        switch (option)
        {
            case "range": work.StartUtc = new DateTime(2026, 9, 22, 0, 0, 0, DateTimeKind.Utc); break;
            case "timestamp": work.TimestampField = "updatedUtc"; break;
            case "script": work.Script = "ctx.op = 'noop';"; break;
            case "quiesce": work.QuiesceSource = true; break;
        }''',
'''        work = option switch
        {
            "range" => work with { StartUtc = new DateTime(2026, 9, 22, 0, 0, 0, DateTimeKind.Utc) },
            "timestamp" => work with { TimestampField = "updatedUtc" },
            "script" => work with { Script = "ctx.op = 'noop';" },
            "quiesce" => work with { QuiesceSource = true },
            _ => throw new ArgumentException("Unknown test option", nameof(option))
        };''')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexSafetyTests.cs',
'''        work.ReindexBatchSize = 10;
        work.ReindexRequestsPerSecond = 2;
        work.DeleteOld = true;''',
'''        work = work with { ReindexBatchSize = 10, ReindexRequestsPerSecond = 2, DeleteOld = true };''')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexTests.cs',
'''Assert.Equal("none", storedResponse.Source.Transformation);''',
'''Assert.Equal(ElasticReindexer.GetTransformationFingerprint(workItem), storedResponse.Source.Transformation);''')
path = Path('src/Foundatio.Repositories.Elasticsearch/Configuration/ElasticConfiguration.cs')
text = path.read_text()
start = text.index('                // Do not replay an incomplete migration:')
end = text.index('\n            }\n\n            if (failures', start)
text = text[:start] + '''                // Whole-migration replay is unsafe after an ambiguous cutover, regardless of exception type.
                // Recovery must inspect durable completion and task/block state, not merely the index version.
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await outdatedIndex.ReindexAsync((progress, message) =>
                            progressCallbackAsync?.Invoke(progress / outdatedIndexes.Count, message) ?? Task.CompletedTask, cancellationToken)
                        .AnyContext();
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Reindex of {IndexName} did not complete; automatic replay was not attempted", outdatedIndex.Name);
                    (failures ??= []).Add(ex);
                }''' + text[end:]
path.write_text(text)
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ElasticConfigurationReindexTests.cs',
'''    private sealed class ObservedIndex''',
'''    [Fact]
    public async Task ReindexAsync_WhenAnUnexpectedFailureMayFollowCutover_DoesNotRetryIntoSuccess()
    {
        using var configuration = new ElasticConfiguration();
        int attempts = 0;
        var failure = new InvalidOperationException("simulated response lost after promotion");
        var index = new ObservedIndex(configuration, "first", _ =>
        {
            if (++attempts is 1)
                throw failure;
        });
        configuration.AddIndex(index);

        var aggregate = await Assert.ThrowsAsync<AggregateException>(() => configuration.ReindexAsync(cancellationToken: TestContext.Current.CancellationToken));

        Assert.Same(failure, Assert.Single(aggregate.InnerExceptions));
        Assert.Equal(1, attempts);
    }

    private sealed class ObservedIndex''')
