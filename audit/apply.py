"""Apply only the three-gate candidate to its exact reviewed base."""
import base64
import hashlib
import json
import lzma
import runpy
import subprocess
from pathlib import Path

BASE = '63bfaf4a7d546fc24a5e8225996c24ae9fbff16f'
subprocess.run(['git', 'checkout', '--detach', BASE], check=True)
payload_root = Path('../payload/audit')
encoded = ''.join((payload_root / f'three-gate-{i}.txt').read_text().strip() for i in range(3))
raw = lzma.decompress(base64.b64decode(encoded, validate=True))
assert hashlib.sha256(raw).hexdigest() == '5f6b039d4ca5efac8d8fe26b73adc75fb769ebe322d7719c330ab5467e6d5905'
entries = json.loads(raw)
for entry in entries:
    path = Path(entry['path'])
    assert not path.is_absolute() and '..' not in path.parts
    assert path.parts[0] in ('src', 'tests', 'docs', '.agents')
    old = path.read_bytes() if path.exists() else b''
    assert hashlib.sha256(old).hexdigest() == entry['base'], path
    lines = old.decode().splitlines(keepends=True)
    for start, end, text in reversed(entry['edits']):
        lines[start:end] = [text]
    new = ''.join(lines).encode()
    assert hashlib.sha256(new).hexdigest() == entry['result'], path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(new)
runpy.run_path(str(payload_root / 'threegate-corrections.py'))
Path('../candidate-files.json').write_text(json.dumps({e['path']: hashlib.sha256(Path(e['path']).read_bytes()).hexdigest() for e in entries}, indent=2) + '\n')
print(f'Applied {len(entries)} reviewed files on {BASE}; no workflow files changed.')
