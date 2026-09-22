import base64, hashlib, json, lzma
from pathlib import Path
payload = ''.join(Path(f'../payload/audit/finalchanges-{i}.txt').read_text().strip() for i in range(1, 5))
raw = lzma.decompress(base64.b64decode(payload, validate=True))
assert hashlib.sha256(raw).hexdigest() == '32a58c0017b8f68ff828f561a2b7083e5694585cf7f9af2446827b3e42846520'
for entry in json.loads(raw):
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
