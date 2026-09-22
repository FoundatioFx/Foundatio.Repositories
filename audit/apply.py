import base64, hashlib, json, lzma, runpy, shutil
from pathlib import Path
payload_root = Path('../payload')
payload = ''.join((payload_root / f'audit/changes-{i}.txt').read_text().strip() for i in range(1, 5))
raw = lzma.decompress(base64.b64decode(payload, validate=True))
assert hashlib.sha256(raw).hexdigest() == 'd88236f4904d34ac0955ee9d78a292821b355cffb990092c0950dad315e661ef'
for entry in json.loads(raw):
    path = Path(entry['path'])
    assert not path.is_absolute() and '..' not in path.parts
    old = path.read_bytes() if path.exists() else b''
    assert hashlib.sha256(old).hexdigest() == entry['base'], path
    lines = old.decode().splitlines(keepends=True)
    for start, end, text in reversed(entry['edits']):
        lines[start:end] = [text]
    new = ''.join(lines).encode()
    assert hashlib.sha256(new).hexdigest() == entry['result'], path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(new)
runpy.run_path(str(payload_root / 'audit/followup.py'))
for file in sorted((payload_root / 'extra').rglob('*')):
    if file.is_file():
        dest = file.relative_to(payload_root / 'extra')
        assert dest.parts[0] in ('src', 'tests', 'docs', '.agents')
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(file, dest)
for script in sorted((payload_root / 'audit').glob('followup-*.py')):
    runpy.run_path(str(script))
