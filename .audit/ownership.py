from pathlib import Path
import sys

path = Path('tests/Foundatio.Repositories.Elasticsearch.Tests/SearchAfterRequestTests.cs')
if sys.argv[1] == 'tests':
    anchor = '    private static void AssertClosed(StubInvoker invoker, string expectedId)'
    test = '''    [Theory]
    [InlineData(false, "disable")]
    [InlineData(true, "disable")]
    [InlineData(false, "live")]
    [InlineData(true, "live")]
    [InlineData(false, "restart")]
    [InlineData(true, "restart")]
    [InlineData(false, "replace")]
    [InlineData(true, "replace")]
    public async Task FindAsync_WhenBeforeQueryAbandonsExistingPointInTime_RespectsOwnership(bool callerOwned, string change)
    {
        using var invoker = new StubInvoker(endpoint => GetPointInTimeResponse(endpoint, PageResponse));
        using var configuration = new StubConfiguration(invoker);
        using var index = new Index<NonIdentityDocument>(configuration, "test-index");
        using var repository = new StubRepository(index);
        var options = new CommandOptions<NonIdentityDocument>().PageLimit(1).SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        if (callerOwned)
            options.PointInTimeId("prior-pit");
        else
            options.RepositoryOwnedPointInTimeId("prior-pit");
        repository.BeforeQuery.AddHandler((_, args) =>
        {
            switch (change)
            {
                case "disable":
                    args.Options.SearchAfterPaging(false);
                    break;
                case "live":
                    args.Options.SearchAfterPaging(SearchAfterPagingMode.Live);
                    break;
                case "restart":
                    args.Options.SearchAfterPaging(false).SearchAfterPaging(SearchAfterPagingMode.PointInTime);
                    break;
                case "replace":
                    args.Options.PointInTimeId("replacement-pit");
                    break;
            }
            return Task.CompletedTask;
        });

        var page = await repository.FindAsync(new RepositoryQuery<NonIdentityDocument>(), options);

        Assert.True(page.HasMore);
        if (!callerOwned && change is not "replace")
            AssertClosed(invoker, "prior-pit");
        else
            Assert.DoesNotContain(invoker.Requests, r => r.Method is Elastic.Transport.HttpMethod.DELETE);
        Assert.Equal(change is "restart" or "replace" ? "updated-pit" : null, options.GetPointInTimeId());
        Assert.Equal(change is "restart", options.IsRepoOwnedPointInTime());
    }

'''
    text = path.read_text()
    assert text.count(anchor) == 1
    path.write_text(text.replace(anchor, test + anchor))
elif sys.argv[1] == 'fix':
    path = Path('src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReadOnlyRepositoryBase.cs')
    text = path.read_text()
    old = '''            ValidateSearchAfterContinuation(options, continuation);
            pointInTime = options.GetPointInTimeState();'''
    new = '''            ValidateSearchAfterContinuation(options, continuation);
            if (!ReferenceEquals(pointInTime, options.GetPointInTimeState()))
                await TryCloseRepositoryOwnedPointInTimeAsync(options, pointInTime).AnyContext();
            pointInTime = options.GetPointInTimeState();'''
    assert text.count(old) == 1
    text = text.replace(old, new)
    assert text.count('CloseRepositoryOwnedPointInTimeAfterFailureAsync') == 2
    text = text.replace('CloseRepositoryOwnedPointInTimeAfterFailureAsync', 'TryCloseRepositoryOwnedPointInTimeAsync')
    text = text.replace('Failed to close repository-owned point in time after a paging failure; it will expire after its keep-alive window', 'Failed to close repository-owned point in time; it will expire after its keep-alive window')
    path.write_text(text)
else:
    raise ValueError(sys.argv[1])
