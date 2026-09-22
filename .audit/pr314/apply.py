from pathlib import Path
import shutil
import subprocess

control = Path(__file__).resolve().parent
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
assert text.rstrip().endswith('}'), 'Missing class closing brace'
text = text[:start].rstrip() + '\n}\n'
assert 'StartConflictWriterAsync' not in text and 'StopWriterAsync' not in text
assert 'RemoveAllAsync_DeleteByQuery_SkipsRetryWhenNoVersionConflicts' in text
path.write_bytes(text.encode('utf-8'))
shutil.copyfile(control / 'DeleteByQueryRetryTests.cs', path.parent / 'DeleteByQueryRetryTests.cs')
subprocess.run(['git', 'add', str(path), str(path.parent / 'DeleteByQueryRetryTests.cs')], check=True)
subprocess.run(['git', 'diff', '--cached', '--check'], check=True)
subprocess.run(['git', 'commit', '-m', 'test: make delete-by-query retry coverage deterministic'], check=True)
