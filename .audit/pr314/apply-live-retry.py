from pathlib import Path
import shutil
import subprocess

control = Path(__file__).resolve().parent
path = Path('tests/Foundatio.Repositories.Elasticsearch.Tests/DeleteByQueryRetryIntegrationTests.cs')
assert not path.exists()
shutil.copyfile(control / path.name, path)
subprocess.run(['git', 'add', str(path)], check=True)
subprocess.run(['git', 'diff', '--cached', '--check'], check=True)
subprocess.run(['git', 'commit', '-m', 'test: verify deterministic retry recovery against Elasticsearch'], check=True)
