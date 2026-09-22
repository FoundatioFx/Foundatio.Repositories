from pathlib import Path

def replace(path, old, new):
    file = Path(path)
    text = file.read_text()
    assert old in text, (path, old)
    file.write_text(text.replace(old, new))

replace('tests/Foundatio.Repositories.Elasticsearch.Tests/DeleteByQueryRetryTests.cs',
        'using Foundatio.Messaging;', 'using Foundatio.Messaging;\nusing Foundatio.Parsers.ElasticQueries;')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/DeleteByQueryRetryTests.cs',
        'new Configuration.Index<Identity>(configuration, "identity")', 'new StubIndex(configuration)')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/DeleteByQueryRetryTests.cs',
        '    private sealed class StubConfiguration',
'''    private sealed class StubIndex(ElasticConfiguration configuration) : Configuration.Index<Identity>(configuration, "identity")
    {
        protected override ElasticMappingResolver CreateMappingResolver() => ElasticMappingResolver.NullInstance;
    }

    private sealed class StubConfiguration''')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ElasticReindexerTests.cs',
        'using Foundatio.Repositories.Elasticsearch.Jobs;',
'''using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;''')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ElasticReindexerTests.cs',
        'new ElasticReindexer(null!, new SystemTextJsonSerializer())', 'CreateValidationReindexer()')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ElasticReindexerTests.cs',
        'public sealed class ElasticReindexerTests\n{',
'''public sealed class ElasticReindexerTests
{
    private static ElasticReindexer CreateValidationReindexer()
    {
        var settings = new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://in-memory.invalid:9200")),
                new SequenceRequestInvoker((500, "{}")))
            .MaximumRetries(0)
            .OnRequestCompleted(_ => throw new InvalidOperationException("Input validation must not issue requests."));
        return new ElasticReindexer(new ElasticsearchClient(settings), new SystemTextJsonSerializer());
    }
''')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexTests.cs',
        'GetWorkItemLockAsync_WhenAliasIsAlreadyLocked_AbandonsWorkItem()', 'GetWorkItemLockAsync_WhenCallerCancelsWhileAliasIsLocked_PropagatesCancellation()')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexTests.cs',
'''        // Act - a cancelled token stands in for the acquire timeout elapsing
        using var cancelledSource = new CancellationTokenSource();
        await cancelledSource.CancelAsync();
        var workItemLock = await handler.GetWorkItemLockAsync(workItem, cancelledSource.Token);

        // Assert - no lock means the queue redelivers rather than running two reindexes at once
        Assert.Null(workItemLock);''',
'''        // Caller cancellation is not a contention timeout: preserve it for shutdown handling.
        using var cancelledSource = new CancellationTokenSource();
        await cancelledSource.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => handler.GetWorkItemLockAsync(workItem, cancelledSource.Token));''')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexSafetyTests.cs',
        'public sealed class ReindexSafetyTests\n{',
'''public sealed class ReindexSafetyTests
{
    [Fact]
    public async Task QueuedLock_WhenContendedWithoutCallerCancellation_ReturnsNoLock()
    {
        using var configuration = new Foundatio.Repositories.Elasticsearch.Configuration.ElasticConfiguration(lockProvider: new DenyingLockProvider());
        var handler = new ReindexWorkItemHandler(configuration);

        Assert.Null(await handler.GetWorkItemLockAsync(WorkItem(), TestContext.Current.CancellationToken));
    }
''')
