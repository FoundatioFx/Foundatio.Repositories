"""Create reviewed Git objects after validation. Never move a branch or merge a PR."""
import hashlib
import json
import os
import subprocess
import urllib.request
from pathlib import Path

BASE = '091b2ba0a6c24806568c42897c1078f0b00dae5c'
REPO = 'FoundatioFx/Foundatio.Repositories'
ROOT = f'https://api.github.com/repos/{REPO}'

def api(path, data=None):
    req = urllib.request.Request(ROOT + path,
        data=None if data is None else json.dumps(data).encode(),
        headers={'Authorization': 'Bearer ' + os.environ['GH_TOKEN'],
                 'Accept': 'application/vnd.github+json',
                 'X-GitHub-Api-Version': '2022-11-28',
                 'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.load(response)

# Guard against preparing a replacement for a branch that has moved during validation.
pr = api('/pulls/327')
assert pr['head']['sha'] == BASE, 'PR changed during validation; reconcile before preparing commits.'
assert pr['head']['ref'] == 'fix/reindex-data-loss-phase0'
assert pr['state'] == 'open'
subprocess.run(['git', 'add', 'src', 'tests', 'docs', '.agents'], check=True)
subprocess.run(['git', 'diff', '--cached', '--check'], check=True)
paths = subprocess.check_output(['git', 'diff', '--cached', '--name-only', '-z']).decode().strip('\0').split('\0')
assert paths and all(Path(p).parts[0] in ('src', 'tests', 'docs', '.agents') for p in paths)
assert all(Path(p).is_file() for p in paths), 'Unexpected deletion'

groups = [[], [], [], []]
for path in paths:
    if path.startswith(('docs/', '.agents/')):
        group = 3
    elif path.endswith(('/DeleteByQueryRetryTests.cs', '/RepositoryTests.cs')):
        group = 2
    elif '/Configuration/' in path or path.endswith('/ElasticConfigurationReindexTests.cs'):
        group = 0
    else:
        group = 1
    groups[group].append(path)

messages = [
    'fix(api): preserve reindex overloads and cancellation semantics\n\nRetain legacy CLR signatures and virtual dispatch while adding cancellation-aware overloads. Release locks after failed version rechecks, propagate cancellation, invalidate configuration markers, and never replay an ambiguous whole migration into apparent success.',
    'fix(reindex): verify cutover and recover durable migration ownership\n\nCompare complete per-primary checkpoints, examine every changed-ID page, validate prerequisite refreshes and every reconciliation item, preserve routing, and account for scripted deletes and noops. Block source writes only for final reconciliation; rebuild scripted destinations before verified cutover.\n\nBind completion evidence to exact migration semantics and destination generation. Never fabricate completion from alias state or a new quiesce flag. Persist task and block ownership before side effects, fence stale journal updates, confirm task termination, and fail closed on ambiguous recovery. Add deterministic response and live-cluster recovery regressions.',
    'test(repository): make delete-conflict retry coverage deterministic\n\nReplace timing-dependent conflict writers with ordered transport responses exercising the real repository retry path, cumulative counts, bounded exhaustion, and notifications. Keep live-cluster no-conflict deletion coverage.',
    'docs(reindex): document barriers deletes and recovery contracts\n\nDocument first-pass availability, full second-pass write outage, hard and soft delete semantics, scripted destination rebuilds, exact completion identity, and durable task/block recovery. Preserve historical RCA evidence while clearly superseding obsolete guarantees. Add an operational runbook and distinguish default best-effort behavior from verified quiesced cutover.'
]
parent = BASE
base_tree = api('/git/commits/' + BASE)['tree']['sha']
manifest = {'base_sha': BASE, 'validation_run': os.environ['GITHUB_RUN_ID'], 'commits': [], 'files': {}}
for message, files in zip(messages, groups):
    assert files
    entries = []
    for path in files:
        content = Path(path).read_bytes()
        manifest['files'][path] = hashlib.sha256(content).hexdigest()
        entries.append({'path': path, 'mode': '100644', 'type': 'blob', 'content': content.decode()})
    tree = api('/git/trees', {'base_tree': base_tree, 'tree': entries})['sha']
    commit = api('/git/commits', {'message': message, 'tree': tree, 'parents': [parent]})['sha']
    manifest['commits'].append({'sha': commit, 'title': message.splitlines()[0], 'tree': tree, 'files': files})
    parent, base_tree = commit, tree
manifest['head_sha'] = parent
Path('../commits.json').write_text(json.dumps(manifest, indent=2) + '\n')
Path('../reviewed.patch').write_bytes(subprocess.check_output(['git', 'diff', '--cached', '--binary']))
print('Prepared four unreferenced commits; no branch was updated.')
