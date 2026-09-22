from pathlib import Path
import subprocess

control = Path(__file__).resolve().parent
path = Path('tests/Foundatio.Repositories.Elasticsearch.Tests/DeleteByQueryRetryIntegrationTests.cs')
assert not path.exists()
text = (control / path.name).read_text()
old = '''        var settings = await _client.Indices.PutSettingsAsync(_configuration.Identities.Name,
            s => s.Settings(index => index.RefreshInterval(new Duration("-1"))), TestCancellationToken);
        Assert.True(settings.IsValidResponse, settings.DebugInformation);

        try
        {
'''
new = '''        try
        {
            var settings = await _client.Indices.PutSettingsAsync(_configuration.Identities.Name,
                s => s.Settings(index => index.RefreshInterval(new Duration("-1"))), TestCancellationToken);
            Assert.True(settings.IsValidResponse, settings.DebugInformation);

'''
assert text.count(old) == 1
path.write_text(text.replace(old, new))
subprocess.run(['git', 'add', str(path)], check=True)
subprocess.run(['git', 'diff', '--cached', '--check'], check=True)
subprocess.run(['git', 'commit', '-m', 'test: verify deterministic retry recovery against Elasticsearch'], check=True)

path = Path('docs/guide/caching.md')
text = path.read_bytes().decode('utf-8')
old = '''// Read from cache only (don't write)
o.ReadCache()

// Disable caching for this operation
o.Cache(false)
```
'''
new = '''// Read from cache without writing new entries
o.Cache(false).ReadCache()

// Write refreshed entries without reading existing entries
o.Cache().ReadCache(false)

// Disable both document/result cache reads and writes for this operation
o.Cache(false).ReadCache(false)
```

`ReadCache(bool)` controls reads independently of writes and takes precedence over `Cache()`.
The parameterless `ReadCache()` enables reads; it does not turn off writes that were already enabled.
Likewise, `Cache(false)` does not clear a previously explicit `ReadCache()` setting. These options
control document and query-result caches, not internal mapping or soft-delete bookkeeping.
See [Multi-Get Error Options](/guide/configuration#multi-get-error-options) for strict reads and their
cache, fallback, and freshness boundaries.
'''
assert text.count(old) == 1
text = text.replace(old, new)
assert text.count(': base(cache: cache, loggerFactory: loggerFactory)') == 1
text = text.replace(': base(cache: cache, loggerFactory: loggerFactory)', ': base(cacheClient: cache, loggerFactory: loggerFactory)')
path.write_bytes(text.encode('utf-8'))
subprocess.run(['git', 'add', str(path)], check=True)
subprocess.run(['git', 'diff', '--cached', '--check'], check=True)
subprocess.run(['git', 'commit', '-m', 'docs: align caching examples with independent read controls'], check=True)
