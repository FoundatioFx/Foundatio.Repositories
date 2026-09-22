#!/usr/bin/env bash
set -euo pipefail
cd repo
mkdir -p ../evidence
git config user.name 'Blake Niemyjski'
git config user.email 'bniemyjski@gmail.com'
python - <<'PY'
import base64, hashlib, pathlib, re, subprocess, zlib
workflow = subprocess.check_output(['git', 'show', '05c1a46e67f7d8b8d0e828cf45f0c4a5d94295f8:.github/workflows/pr-308-audit.yml'], text=True)
payload = re.search(r"payload = '([A-Za-z0-9+/=]+)'", workflow).group(1)
patches = [(payload, '9ef0f6469d59fdaedee7a8d9ebea97847160aea07ed302250a94143344e3de38')]
for name, checksum in [('live-fix', '9a750e73d97361750c68b38e6d27759ddc7a9de1d7b74686c128eae3f0951d6a'), ('scalar-tests', '42faa3a11decb219420c51dfeb1e6fbdea0fc32ed4539ad1c5169b660e1f4498')]:
    patches.append((pathlib.Path(f'../tools/.audit/{name}.b64').read_text().strip(), checksum))
for payload, checksum in patches:
    patch = zlib.decompress(base64.b64decode(payload, validate=True))
    assert hashlib.sha256(patch).hexdigest() == checksum
    subprocess.run(['git', 'am', '--committer-date-is-author-date'], input=patch, check=True)
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip() == '71e47fcae9b6808e0b9f559703de9e74227cfdf1'
PY
dotnet build Foundatio.Repositories.slnx --configuration Release -warnaserror 2>&1 | tee ../evidence/baseline-build.log
assembly=tests/Foundatio.Repositories.Elasticsearch.Tests/bin/Release/net10.0/Foundatio.Repositories.Elasticsearch.Tests.dll
set +e
dotnet "$assembly" -class Foundatio.Repositories.Elasticsearch.Tests.SearchAfterRequestTests -result-xml ../evidence/scalar-baseline.xml 2>&1 | tee ../evidence/scalar-baseline.log
status=${PIPESTATUS[0]}
set -e
test "$status" -eq 1
python - <<'PY'
import xml.etree.ElementTree as ET
tests = ET.parse('../evidence/scalar-baseline.xml').findall('.//test')
failed = [t for t in tests if t.get('result') == 'Fail']
expected = ('ScalarQuery_WithIncompleteLiveCursorSearch', 'ScalarQuery_WithIncompatibleLivePaging', 'ScalarQuery_WithCompleteLiveCursorSearch')
assert len(tests) == 136, len(tests)
assert len(failed) == 36, [(t.get('name'), t.get('result')) for t in tests]
assert all(any(name in t.get('name', '') for name in expected) for t in failed)
assert all(t.get('result') in ('Pass', 'Fail') for t in tests)
print('Confirmed 36 expected scalar failures; 100 existing/live tests passed.')
PY
python ../tools/.audit/finish.py scalar
git diff --check
git add src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReadOnlyRepositoryBase.cs
git commit -m 'fix: enforce complete responses and compatible modes for scalar cursors'
python ../tools/.audit/ownership.py tests
dotnet build Foundatio.Repositories.slnx --configuration Release -warnaserror 2>&1 | tee ../evidence/ownership-build.log
set +e
dotnet "$assembly" -class Foundatio.Repositories.Elasticsearch.Tests.SearchAfterRequestTests -result-xml ../evidence/ownership-baseline.xml 2>&1 | tee ../evidence/ownership-baseline.log
status=${PIPESTATUS[0]}
set -e
test "$status" -eq 1
python - <<'PY'
import xml.etree.ElementTree as ET
tests = ET.parse('../evidence/ownership-baseline.xml').findall('.//test')
failed = [t for t in tests if t.get('result') == 'Fail']
assert len(tests) == 144, len(tests)
assert len(failed) == 3, [(t.get('name'), t.get('result')) for t in tests if t.get('result') != 'Pass']
assert all('WhenBeforeQueryAbandonsExistingPointInTime' in t.get('name', '') for t in failed)
assert all(t.get('result') in ('Pass', 'Fail') for t in tests)
print('Confirmed three orphaned owned-PIT cases; caller-ownership controls and all preceding tests passed.')
PY
python ../tools/.audit/ownership.py fix
git diff --check
git add src tests
git commit -m 'fix: close owned PITs abandoned by successful BeforeQuery resets'
python ../tools/.audit/finish.py docs
git diff --check
git add src docs .agents
git commit -m 'docs: clarify cursor session identity, scalar safety, and traversal ownership'
test -z "$(git status --porcelain)"
git diff --exit-code 9ed59304eec0a092a58c617c8370e62eab1c5935 HEAD -- .github
dotnet build Foundatio.Repositories.slnx --configuration Release -warnaserror 2>&1 | tee ../evidence/final-build.log
dotnet "$assembly" -class Foundatio.Repositories.Elasticsearch.Tests.SearchAfterRequestTests -result-xml ../evidence/request-tests.xml 2>&1 | tee ../evidence/request-tests.log
mapfile -t changed < <(git diff --name-only 9ed59304eec0a092a58c617c8370e62eab1c5935 HEAD -- '*.cs')
dotnet format Foundatio.Repositories.slnx --no-restore --verify-no-changes --include "${changed[@]}" 2>&1 | tee ../evidence/format.log
python ../tools/.audit/consumer.py 2>&1 | tee ../evidence/consumer.log
git diff --check
test -z "$(git status --porcelain)"
git log -6 --format=fuller > ../evidence/commits.txt
git diff --stat 9ed59304eec0a092a58c617c8370e62eab1c5935 > ../evidence/diffstat.txt
git bundle create ../evidence/candidate.bundle HEAD
git rev-parse HEAD > ../evidence/candidate-sha.txt
git format-patch --stdout 9ed59304eec0a092a58c617c8370e62eab1c5935..HEAD > ../evidence/fixes.patch
