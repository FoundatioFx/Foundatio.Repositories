from pathlib import Path
import os
import runpy
import subprocess
import sys

runpy.run_path('.audit/pr307/prepare.py', run_name='__main__')
runpy.run_path('.audit/pr307/ownership.py', run_name='__main__')
runpy.run_path('.audit/pr307/cancellation.py', run_name='__main__')

if sys.argv[1] == 'tests':
    path = Path('tests/Foundatio.Repositories.Elasticsearch.Tests/Configuration/IndexCompatibilityTests.TaskResponse.cs')
    text = path.read_text()
    text = text.replace('using System.Collections.Generic;\n', '').replace('using Foundatio.Serializer;\n', '')
    assert text.count('new SystemTextJsonSerializer(') == 2
    text = text.replace('new SystemTextJsonSerializer(', 'new Foundatio.Serializer.SystemTextJsonSerializer(')
    text = text.replace('TimeProvider.System, new ResiliencePolicyProvider()', 'TimeProvider.System, resiliencePolicyProvider: new ResiliencePolicyProvider()')
    assert text.count('return $$"""') == 1
    assert text.count('{{timedOut.ToString().ToLowerInvariant()}}') == 1
    text = text.replace('return $$"""', 'return """')
    text = text.replace('{{timedOut.ToString().ToLowerInvariant()}}', 'TIMED_OUT')
    text = text.replace('            """;', '            """.Replace("TIMED_OUT", timedOut ? "true" : "false", StringComparison.Ordinal);')
    for kind, value in [('byte','(byte)1'), ('sbyte','(sbyte)1'), ('short','(short)1'), ('ushort','(ushort)1'), ('int','1'), ('uint','1U'), ('long','1L'), ('ulong','1UL')]:
        old = f'"{kind}" => {value},'
        assert text.count(old) == 1
        text = text.replace(old, f'"{kind}" => (object){value},')
    path.write_text(text)
    path = Path('tests/Foundatio.Repositories.Elasticsearch.Tests/Configuration/IndexCompatibilityTests.ErrorLineage.cs')
    text = path.read_text()
    text = text.replace('TaskResponse_ErrorLineage', 'ErrorLineage_')
    text = text.replace('new StubResponse(200, $$"""', 'new StubResponse(200, """')
    text = text.replace('{{target}}', 'TARGET').replace('{{nativeError}}', 'SOURCE').replace('{{marker}}', 'MARKER')
    old = '                """),\n            new StubResponse(200, """{"nodes":{}}"""));'
    new = '''                """.Replace("TARGET", target, StringComparison.Ordinal)
                    .Replace("SOURCE", nativeError, StringComparison.Ordinal)
                    .Replace("MARKER", marker, StringComparison.Ordinal)),
            new StubResponse(200, """{"nodes":{}}"""));'''
    assert text.count(old) == 1
    path.write_text(text.replace(old, new))

if sys.argv[1] == 'fix' and os.environ.get('GITHUB_JOB') == 'materialize':
    subprocess.run(['git', 'add',
        'src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexTaskCancellation.cs',
        'src/Foundatio.Repositories.Elasticsearch/Configuration/ElasticIndexCompatibilityUpgrader.cs',
        'tests/Foundatio.Repositories.Elasticsearch.Tests/Configuration/IndexCompatibilityTests.CancellationEvidence.cs',
        'tests/Foundatio.Repositories.Elasticsearch.Tests/Configuration/IndexCompatibilityTests.Task.cs',
        'tests/Foundatio.Repositories.Elasticsearch.Tests/Configuration/IndexCompatibilityTests.CutoverSafety.cs'], check=True)
