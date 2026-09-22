from pathlib import Path
import runpy
import sys

runpy.run_path('.audit/pr307/prepare.py', run_name='__main__')
runpy.run_path('.audit/pr307/ownership.py', run_name='__main__')

if sys.argv[1] == 'tests':
    path = Path('tests/Foundatio.Repositories.Elasticsearch.Tests/Configuration/IndexCompatibilityTests.TaskResponse.cs')
    text = path.read_text()
    text = text.replace('using System.Collections.Generic;\n', '')
    assert text.count('return $$"""') == 1
    assert text.count('{{timedOut.ToString().ToLowerInvariant()}}') == 1
    text = text.replace('return $$"""', 'return """')
    text = text.replace('{{timedOut.ToString().ToLowerInvariant()}}', 'TIMED_OUT')
    text = text.replace('            """;', '            """.Replace("TIMED_OUT", timedOut ? "true" : "false", StringComparison.Ordinal);')
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
