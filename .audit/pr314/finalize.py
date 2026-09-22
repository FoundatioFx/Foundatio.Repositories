from pathlib import Path
import subprocess


def replace_once(path, old, new):
    text = path.read_bytes().decode('utf-8')
    assert text.count(old) == 1, f'Expected one replacement in {path}: {old[:100]}'
    path.write_bytes(text.replace(old, new).encode('utf-8'))


root = Path('tests/Foundatio.Repositories.Elasticsearch.Tests')
retry = root / 'DeleteByQueryRetryTests.cs'
mget = root / 'MultiGetResponseValidationTests.cs'
for path in (retry, mget):
    replace_once(path,
        'new Dictionary<string, IEnumerable<string>> { ["X-Elastic-Product"] = ["Elasticsearch"] }',
        'new Dictionary<string, IEnumerable<string>>(StringComparer.OrdinalIgnoreCase) { ["x-elastic-product"] = ["Elasticsearch"] }')

replace_once(retry,
    '''            throw new InvalidOperationException($"Unexpected synchronous request: {endpoint.Uri}");''',
    '''            if (typeof(TResponse) != typeof(GetMappingResponse))
                throw new InvalidOperationException($"Unexpected synchronous request: {endpoint.Uri}");

            byte[] mapping = Encoding.UTF8.GetBytes("""
                {"identity":{"mappings":{"properties":{"id":{"type":"keyword"}}}}}
                """);
            return BuildResponse<TResponse>(endpoint, boundConfiguration, postData, mapping);''')
replace_once(retry,
    '''        Assert.Equal(10, deleted);
        Assert.Equal(3, invoker.DeleteRequests.Count);''',
    '''        Assert.Equal(10, deleted);
        Assert.DoesNotContain(Log.LogEntries, entry => entry.LogLevel >= LogLevel.Error);
        Assert.Equal(3, invoker.DeleteRequests.Count);''')
subprocess.run(['git', 'add', str(retry), str(mget)], check=True)
subprocess.run(['git', 'diff', '--cached', '--check'], check=True)
subprocess.run(['git', 'commit', '-m', 'test: honor Elasticsearch transport headers and mapping requests'], check=True)
