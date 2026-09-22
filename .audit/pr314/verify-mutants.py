from pathlib import Path
import os
import subprocess
import xml.etree.ElementTree as ET

root = Path(os.environ['GITHUB_WORKSPACE']) / 'audit-results'
project = 'tests/Foundatio.Repositories.Elasticsearch.Tests/Foundatio.Repositories.Elasticsearch.Tests.csproj'
build = ['dotnet', 'build', 'Foundatio.Repositories.slnx', '--configuration', 'Release', '--no-restore']
mutants = [
    ('accumulation', 'src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticRepositoryBase.cs', 'totalDeleted += response.Deleted ?? 0;', 'totalDeleted = response.Deleted ?? 0;', '*DeleteByQueryRetryTests', 'AccumulatesDeletedCountAndNotifiesOnceAcrossRetriesAsync'),
    ('budget', 'src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticRepositoryBase.cs', 'int maxAttempts = options.GetRetryCount() + 1;', 'int maxAttempts = 1;', '*DeleteByQueryRetryTests', 'AccumulatesDeletedCountAndNotifiesOnceAcrossRetriesAsync'),
    ('mget-validation', 'src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReadOnlyRepositoryBase.cs', 'multiGetResults.GetItemErrors(docOperations, _logger)', 'multiGetResults.GetItemErrors(_logger)', '*MultiGetResponseValidationTests', 'RejectsMalformedResponsesBeforeCachingAsync'),
]
for name, filename, old, new, test_class, expected_failure in mutants:
    path = Path(filename)
    original = path.read_bytes()
    text = original.decode('utf-8')
    assert text.count(old) == 1, f'Invalid mutant target: {name}'
    try:
        path.write_bytes(text.replace(old, new).encode('utf-8'))
        subprocess.run(build, check=True)
        results = root / ('mutation-' + name)
        result = subprocess.run(['dotnet', 'test', '--project', project, '--configuration', 'Release', '--no-build', '--filter-class', test_class, '--report-xunit-trx', '--results-directory', str(results)])
        assert result.returncode == 2, f'{name}: expected test-failure exit 2, got {result.returncode}'
        failed = []
        for report in results.rglob('*.trx'):
            tree = ET.parse(report)
            failed.extend(node.get('testName', '') for node in tree.iter() if node.tag.endswith('UnitTestResult') and node.get('outcome') == 'Failed')
        assert any(expected_failure in test for test in failed), f'{name}: intended assertion was not exercised: {failed}'
        message = f'VERIFIED MUTANT {name}: {len(failed)} failing tests, including {expected_failure}'
        print(message, flush=True)
        with (root / 'mutation-summary.txt').open('a') as summary:
            summary.write(message + '\n')
    finally:
        path.write_bytes(original)
subprocess.run(['git', 'diff', '--exit-code'], check=True)
subprocess.run(build, check=True)
