from pathlib import Path

def replace(path, old, new):
    file = Path(path)
    text = file.read_text()
    assert old in text, (path, old)
    file.write_text(text.replace(old, new))

replace('src/Foundatio.Repositories.Elasticsearch/Configuration/Index.cs',
        'cref="ReindexAsync"', 'cref="ReindexAsync(Func{int, string, Task}, CancellationToken)"')
replace('src/Foundatio.Repositories.Elasticsearch/Jobs/ReindexWorkItem.cs',
        '<see cref="ElasticReindexer.ReindexAsync"/>', '<see cref="ElasticReindexer"/>.<c>ReindexAsync</c>')
replace('src/Foundatio.Repositories.Elasticsearch/Configuration/VersionedIndex.cs',
'''        currentVersion = await GetCurrentVersionAsync().AnyContext();
        if (currentVersion < 0 || currentVersion >= Version)
        {
            await reindexLock.DisposeAsync().AnyContext();
            return null;
        }

        return new ReindexLease(reindexLock, currentVersion);''',
'''        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            currentVersion = await GetCurrentVersionAsync().AnyContext();
            cancellationToken.ThrowIfCancellationRequested();
            if (currentVersion >= 0 && currentVersion < Version)
                return new ReindexLease(reindexLock, currentVersion);
        }
        catch
        {
            await reindexLock.DisposeAsync().AnyContext();
            throw;
        }

        await reindexLock.DisposeAsync().AnyContext();
        return null;''')
replace('src/Foundatio.Repositories.Elasticsearch/Configuration/VersionedIndex.cs',
'''    protected async Task<ReindexLease?> TryAcquireReindexLeaseAsync(CancellationToken cancellationToken = default)
    {
        int currentVersion = await GetCurrentVersionAsync().AnyContext();''',
'''    protected async Task<ReindexLease?> TryAcquireReindexLeaseAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        int currentVersion = await GetCurrentVersionAsync().AnyContext();
        cancellationToken.ThrowIfCancellationRequested();''')
replace('src/Foundatio.Repositories.Elasticsearch/Jobs/ReindexWorkItemHandler.cs',
'''        return await _lockProvider.TryAcquireAsync(ElasticReindexer.GetLockName(reindexWorkItem.Alias), LockDuration, acquireTimeoutSource.Token).AnyContext();''',
'''        var reindexLock = await _lockProvider.TryAcquireAsync(ElasticReindexer.GetLockName(reindexWorkItem.Alias), LockDuration, acquireTimeoutSource.Token).AnyContext();
        if (cancellationToken.IsCancellationRequested)
        {
            if (reindexLock is not null)
                await reindexLock.DisposeAsync().AnyContext();
            cancellationToken.ThrowIfCancellationRequested();
        }
        return reindexLock;''')
