from pathlib import Path
import shutil
import subprocess

control = Path(__file__).resolve().parent

def commit(message, paths):
    subprocess.run(['git', 'add', *map(str, paths)], check=True)
    subprocess.run(['git', 'diff', '--cached', '--check'], check=True)
    subprocess.run(['git', 'commit', '-m', message], check=True)

def replace_once(path, old, new):
    path = Path(path)
    text = path.read_bytes().decode('utf-8')
    assert text.count(old) == 1, f'Expected one replacement in {path}: {old[:80]}'
    path.write_bytes(text.replace(old, new).encode('utf-8'))

path = Path('tests/Foundatio.Repositories.Elasticsearch.Tests/RepositoryTests.cs')
text = path.read_bytes().decode('utf-8')
names = [
    'RemoveAllAsync_DeleteByQuery_AccumulatesDeletedCountAcrossConflictRetries',
    'RemoveAllAsync_DeleteByQuery_FiresNotificationsOnceAfterAllRetries',
    'RemoveAllAsync_DeleteByQuery_ReturnsPartialCountAndLogsWarnWhenConflictsNeverResolve',
    'RemoveAllAsync_DeleteByQuery_RetriesUntilNoConflictsAndDeletesAllDocuments',
]
for name in names:
    marker = '    [Fact]\n    public async Task ' + name + '()'
    assert text.count(marker) == 1, f'Expected exactly one {name}'
    start = text.index(marker)
    end = text.index('    [Fact]\n', start + len(marker))
    text = text[:start] + text[end:]
start = text.index('    private Task StartConflictWriterAsync(')
assert text[start:].count('private ') == 2, 'Unexpected changes after the conflict-writer helper'
text = text[:start].rstrip() + '\n}\n'
assert 'StartConflictWriterAsync' not in text and 'StopWriterAsync' not in text
assert 'RemoveAllAsync_DeleteByQuery_SkipsRetryWhenNoVersionConflicts' in text
path.write_bytes(text.encode('utf-8'))
retry_tests = path.parent / 'DeleteByQueryRetryTests.cs'
shutil.copyfile(control / retry_tests.name, retry_tests)
commit('test: make delete-by-query retry coverage deterministic', [path, retry_tests])

mget_tests = path.parent / 'MultiGetResponseValidationTests.cs'
shutil.copyfile(control / mget_tests.name, mget_tests)
commit('test: cover incomplete multi-get responses and cache preservation', [mget_tests])

extensions = Path('src/Foundatio.Repositories.Elasticsearch/Extensions/ElasticIndexExtensions.cs')
text = extensions.read_bytes().decode('utf-8')
start = text.index('    public static IReadOnlyCollection<MultiGetError> GetItemErrors<T>')
end = text.index('    private static readonly long _epochTicks', start)
text = text[:start] + (control / 'GetItemErrors.cs').read_text() + text[end:]
extensions.write_bytes(text.encode('utf-8'))
repository = Path('src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReadOnlyRepositoryBase.cs')
replace_once(repository, 'multiGetResults.GetItemErrors(_logger)', 'multiGetResults.GetItemErrors(docOperations, _logger)')
commit('fix: reject incomplete and mismatched strict multi-get responses', [extensions, repository])

configuration = Path('docs/guide/configuration.md')
anchor = 'The internal fallback does not read or write query-result cache entries.'
replace_once(configuration, anchor, 'Strict reads also require one MGET response item per requested operation, in request order, with matching response IDs. Null items, mismatched or duplicate response IDs, found documents without a source, and missing documents with an unexpected source throw `DocumentException` before fallback or cache writes. Projected sources may omit their ID property; correlation uses Elasticsearch response metadata. This validation does not run for the default non-strict path.\n\n' + anchor)
troubleshooting = Path('docs/guide/troubleshooting.md')
anchor = 'An explicit `found: false` response still represents a normal missing document.'
replace_once(troubleshooting, anchor, anchor + ' Strict reads also reject incomplete or mismatched MGET response items before writing document-result cache entries. Strict mode does not bypass existing positive cache hits, make fallback searches real-time, or provide transactional guarantees; see [Multi-Get Error Options](/guide/configuration#multi-get-error-options). Never interpret a `DocumentException` as an empty batch for a destructive decision.')
options = Path('src/Foundatio.Repositories.Elasticsearch/Options/ElasticCommandOptions.cs')
anchor = '        /// Existing document cache reads, consistency settings, and soft-delete filters still apply;'
replace_once(options, anchor, '        /// Incomplete or mismatched MGET responses are rejected before fallback and cache writes.\n        /// Found items must include a source; projected sources may omit their ID property.\n' + anchor)
skill = Path('.agents/skills/foundatio-repositories/SKILL.md')
anchor = 'For time-series/parent-child repositories the throw is deferred until after the multi-index fallback query, so it only fires for ids still unresolved afterward.'
replace_once(skill, anchor, 'Valid item errors are deferred until after any time-series/parent-child fallback; incomplete or mismatched MGET responses fail before fallback. This is not a freshness or transaction guarantee: use `Cache(false).ReadCache(false)` to bypass document cache hits and never treat `DocumentException` as an empty batch. See the configuration guide for the full contract.')
commit('docs: document strict multi-get response validation boundaries', [configuration, troubleshooting, options, skill])
