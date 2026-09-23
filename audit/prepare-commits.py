"""Prepare verified source/tests and documentation commits; never move a branch."""
import hashlib
import json
import os
import subprocess
import urllib.request
from pathlib import Path

BASE = '63bfaf4a7d546fc24a5e8225996c24ae9fbff16f'
ROOT = 'https://api.github.com/repos/FoundatioFx/Foundatio.Repositories'

def api(path, data=None):
    request = urllib.request.Request(ROOT + path,
        data=None if data is None else json.dumps(data).encode(),
        headers={'Authorization': 'Bearer ' + os.environ['GH_TOKEN'],
                 'Accept': 'application/vnd.github+json',
                 'X-GitHub-Api-Version': '2022-11-28',
                 'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)

pr = api('/pulls/327')
assert pr['state'] == 'open' and pr['head']['ref'] == 'fix/reindex-data-loss-phase0'
assert pr['head']['sha'] == BASE, 'PR advanced; reconcile instead of overwriting.'
subprocess.run(['git', 'add', 'src', 'tests', 'docs', '.agents'], check=True)
subprocess.run(['git', 'diff', '--cached', '--check'], check=True)
paths = subprocess.check_output(['git', 'diff', '--cached', '--name-only', '-z']).decode().strip('\0').split('\0')
expected = json.loads(Path('../candidate-files.json').read_text())
assert sorted(paths) == sorted(expected)
for path in paths:
    assert Path(path).parts[0] in ('src', 'tests', 'docs', '.agents')
    assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == expected[path], path

messages = [
    'fix(reindex): exclude stale controllers and retain uncertain write fences\n\nUse effective single-node transport dispatch for asynchronous copy and alias mutations. Require positive, identity-bound task termination and use relocation-aware reindex status on Elasticsearch 9.5+. Never steal a prior controller or infer termination from task 404.\n\nReuse the safety journal for non-expiring conditional admission across alias/source/destination resources and maintenance. Cache lease expiry cannot authorize overlapping controllers. Retain source write blocks after final reconciliation starts, including committed and uncertain cutover. Add multi-node transport, cache-lease expiry, maintenance, competing-controller, and retired-source regressions.',
    'docs(reindex): define conservative recovery and verification evidence\n\nDocument single dispatch, durable no-takeover admission, maintenance coordination, retained retired-source fences, and manual recovery after uncertainty. Distinguish journal admission from server-side fencing, preserve the pinned provider limitation, and specify evidence needed for a real deployment case study. No workflow or publishing changes.'
]
groups = [[p for p in paths if p.startswith(('src/', 'tests/'))], [p for p in paths if p.startswith(('docs/', '.agents/'))]]
parent = BASE
base_tree = api('/git/commits/' + BASE)['tree']['sha']
manifest = {'base_sha': BASE, 'validation_run': os.environ['GITHUB_RUN_ID'], 'commits': [], 'files': expected}
for message, files in zip(messages, groups):
    assert files
    tree = api('/git/trees', {'base_tree': base_tree, 'tree': [
        {'path': p, 'mode': '100644', 'type': 'blob', 'content': Path(p).read_text()} for p in files]})['sha']
    commit = api('/git/commits', {'message': message, 'tree': tree, 'parents': [parent]})['sha']
    manifest['commits'].append({'sha': commit, 'tree': tree, 'title': message.splitlines()[0], 'files': files})
    parent, base_tree = commit, tree
manifest['head_sha'] = parent
Path('../commits.json').write_text(json.dumps(manifest, indent=2) + '\n')
Path('../reviewed.patch').write_bytes(subprocess.check_output(['git', 'diff', '--cached', '--binary']))
print('Prepared two unreferenced commits after validation; no branch moved.')
