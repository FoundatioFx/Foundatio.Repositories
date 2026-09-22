#!/usr/bin/env bash
set -euo pipefail
mkdir -p .audit/results
sudo sysctl -w vm.max_map_count=1048576
snapshot_dir="$RUNNER_TEMP/pr307-snapshots"
mkdir -p "$snapshot_dir"
chmod 777 "$snapshot_dir"
docker volume create pr307-chain-data >/dev/null
base=http://localhost:9200
api() { curl --fail-with-body -sS -H 'Content-Type: application/json' "$@"; }
start() {
  local version=$1
  docker run -d --name pr307-chain -p 127.0.0.1:9200:9200 -v pr307-chain-data:/usr/share/elasticsearch/data -v "$snapshot_dir:/snapshots" -e discovery.type=single-node -e xpack.security.enabled=false -e "cluster.name=foundatio-pr307-chain-${GITHUB_RUN_ID}" -e path.repo=/snapshots -e 'ES_JAVA_OPTS=-Xms1g -Xmx1g' "docker.elastic.co/elasticsearch/elasticsearch:$version"
  for attempt in $(seq 1 120); do
    if api "$base/_cluster/health?wait_for_status=yellow&timeout=1s" > .audit/results/chain-health.json; then
      if python -c 'import json; assert not json.load(open(".audit/results/chain-health.json"))["timed_out"]'; then break; fi
    fi
    sleep 2
  done
  api "$base/" | tee ".audit/results/server-$version.json"
  python - "$version" <<'PY'
import json,sys
v=sys.argv[1]
d=json.load(open(f'.audit/results/server-{v}.json'))
assert d['version']['number']==v
assert d['cluster_name'].startswith('foundatio-pr307-chain-')
PY
}
stop() {
  docker logs pr307-chain > ".audit/results/chain-server-$1.log" 2>&1
  docker stop --time 60 pr307-chain >/dev/null
  docker rm pr307-chain >/dev/null
}
restore_snapshot() {
  local phase=$1 physical=$2
  api -X PUT "$base/_snapshot/pr307-audit" -d '{"type":"fs","settings":{"location":"/snapshots","compress":true}}'
  api -X PUT "$base/_snapshot/pr307-audit/phase-$phase?wait_for_completion=true" -d "{\"indices\":\"$physical\",\"include_global_state\":false}" > ".audit/results/snapshot-$phase.json"
  python - "$phase" <<'PY'
import json,sys
s=json.load(open(f'.audit/results/snapshot-{sys.argv[1]}.json'))['snapshot']
assert s['state']=='SUCCESS' and s['shards']['failed']==0
PY
  api -X DELETE "$base/$physical"
  api -X POST "$base/_snapshot/pr307-audit/phase-$phase/_restore?wait_for_completion=true" -d '{"include_global_state":false,"include_aliases":true}' > ".audit/results/restore-$phase.json"
  python - "$phase" <<'PY'
import json,sys
r=json.load(open(f'.audit/results/restore-{sys.argv[1]}.json'))['snapshot']
assert r['shards']['failed']==0
PY
  api "$base/_cluster/health?wait_for_status=yellow&timeout=30s"
  api "$base/compatibility-major-chain/_count" > ".audit/results/restored-count-$phase.json"
  python - "$phase" <<'PY'
import json,sys
assert json.load(open(f'.audit/results/restored-count-{sys.argv[1]}.json'))['count']==2
PY
}
start 7.17.29
api -X PUT "$base/compatibility-major-chain-v1" -d '{"settings":{"number_of_shards":2,"number_of_replicas":0},"mappings":{"properties":{"message":{"type":"keyword"},"sequence":{"type":"integer"},"nested":{"properties":{"enabled":{"type":"boolean"}}},"tags":{"type":"keyword"}}},"aliases":{"compatibility-major-chain":{}}}'
api -X PUT "$base/compatibility-major-chain-v1/_doc/1?routing=tenant-1&refresh=true" -d '{"message":"first","sequence":1,"nested":{"enabled":true},"tags":["a","b"]}'
api -X PUT "$base/compatibility-major-chain-v1/_doc/2?routing=tenant-2&refresh=true" -d '{"message":"second","sequence":2,"nested":{"enabled":true},"tags":["a","b"]}'
restore_snapshot 7 compatibility-major-chain-v1
stop 7
for entry in '8:8.19.15' '9:9.5.0'; do
  major=${entry%%:*}
  version=${entry#*:}
  start "$version"
  FOUNDATIO_COMPATIBILITY_CHAIN_MAJOR="$major" ELASTICSEARCH_URL="$base" dotnet test --project tests/Foundatio.Repositories.Elasticsearch.Tests/Foundatio.Repositories.Elasticsearch.Tests.csproj -c Release --no-build --filter-method '*UpgradeIndexCompatibilityAsync_AcrossSequentialMajors_PreservesDataAndCanonicalNames' | tee ".audit/results/chain-$major.log"
  grep -Eq 'succeeded: 1([^0-9]|$)' ".audit/results/chain-$major.log"
  grep -Eq 'skipped: 0([^0-9]|$)' ".audit/results/chain-$major.log"
  restore_snapshot "$major" "reindexed-v$major-compatibility-major-chain-v1"
  api -X PUT "$base/compatibility-major-chain/_doc/smoke?routing=smoke&refresh=true" -d '{"message":"smoke","sequence":3}'
  api "$base/compatibility-major-chain/_doc/smoke?routing=smoke" > ".audit/results/smoke-$major.json"
  python - "$major" <<'PY'
import json,sys
r=json.load(open(f'.audit/results/smoke-{sys.argv[1]}.json'))
assert r['found'] and r['_source']['message']=='smoke'
PY
  api -X DELETE "$base/compatibility-major-chain/_doc/smoke?routing=smoke&refresh=true"
  stop "$major"
done
printf 'Persistent 7.17.29 -> 8.19.15 -> 9.5.0 chain and per-phase snapshot restores passed.\n' | tee .audit/results/chain-summary.txt
