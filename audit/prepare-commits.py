"""Prepare one documentation-only commit after validation; never update a branch."""
import hashlib
import json
import os
import subprocess
import urllib.request
from pathlib import Path

BASE = 'b458dd81d75095428c4d438ed93a32a707739cd9'
ROOT = 'https://api.github.com/repos/FoundatioFx/Foundatio.Repositories'
EXPECTED = {
    '.agents/skills/foundatio-repositories/SKILL.md',
    '.agents/skills/foundatio-repositories/references/index-lifecycle.md',
    'docs/guide/index-management.md',
    'docs/guide/jobs.md',
    'docs/guide/reindex-safety.md',
    'docs/guide/troubleshooting.md',
}

def api(path, data=None):
    request = urllib.request.Request(
        ROOT + path,
        data=None if data is None else json.dumps(data).encode(),
        headers={
            'Authorization': 'Bearer ' + os.environ['GH_TOKEN'],
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28',
            'Content-Type': 'application/json',
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)

pr = api('/pulls/327')
assert pr['head']['sha'] == BASE, 'PR moved during validation; reconcile before preparing a commit.'
assert pr['head']['ref'] == 'fix/reindex-data-loss-phase0'
assert pr['state'] == 'open'
subprocess.run(['git', 'add', 'src', 'tests', 'docs', '.agents'], check=True)
subprocess.run(['git', 'diff', '--cached', '--check', BASE], check=True)
paths = subprocess.check_output(['git', 'diff', '--cached', '--name-only', '-z', BASE]).decode().strip('\0').split('\0')
assert set(paths) == EXPECTED, paths
assert all(Path(path).is_file() for path in paths)
subprocess.run(['git', 'diff', '--cached', '--quiet', BASE, '--', 'src', 'tests', '.github', 'AGENTS.md'], check=True)
expected_tree = subprocess.check_output(['git', 'write-tree']).decode().strip()
base_tree = api('/git/commits/' + BASE)['tree']['sha']
entries = []
manifest = {
    'base_sha': BASE,
    'validation_run': os.environ['GITHUB_RUN_ID'],
    'commits': [],
    'files': {},
    'tested_tree': expected_tree,
}
for path in sorted(paths):
    content = Path(path).read_bytes()
    manifest['files'][path] = hashlib.sha256(content).hexdigest()
    entries.append({'path': path, 'mode': '100644', 'type': 'blob', 'content': content.decode()})
tree = api('/git/trees', {'base_tree': base_tree, 'tree': entries})['sha']
assert tree == expected_tree, 'Prepared tree differs from the validated source.'
message = (
    'docs(reindex): tighten operating contract and recovery guidance\n\n'
    'Correct stale replay, handler-constructor, sequence-number, and cancellation claims across the guides and agent reference. '
    'Define verified cutover without claiming checksum validation or universal lease fencing.\n\n'
    'Document exclusive destinations, scripts and ingest-pipeline limits, producer retry/idempotency requirements, '
    'full second-pass outage costs, read-only diagnostics, state lifecycles, and least-privilege rollout checks. '
    'Keep all workflow files, audit infrastructure, runtime code, and dependencies unchanged.'
)
commit = api('/git/commits', {'message': message, 'tree': tree, 'parents': [BASE]})['sha']
manifest['commits'].append({'sha': commit, 'title': message.splitlines()[0], 'tree': tree, 'files': sorted(paths)})
manifest['head_sha'] = commit
Path('../commits.json').write_text(json.dumps(manifest, indent=2) + '\n')
Path('../reviewed.patch').write_bytes(subprocess.check_output(['git', 'diff', '--cached', '--binary', BASE]))
print('Prepared one unreferenced documentation commit. No branch or workflow was changed by this job.')
