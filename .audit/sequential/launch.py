from pathlib import Path
import shutil

root = Path('../harness/.audit/sequential')
shutil.copyfile(root / 'SingleNodeDispatchExtensions.cs', 'src/Foundatio.Repositories.Elasticsearch/Extensions/SingleNodeDispatchExtensions.cs')
p = Path('src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexTaskRunner.cs')
s = p.read_text()
old = 'd.RequestConfiguration(request => request.OpaqueId(opaqueId));'
assert s.count(old) == 1
p.write_text(s.replace(old, 'd.RequestConfiguration(request => request.OpaqueId(opaqueId).SingleNodeDispatch(_client));'))
p = Path('docs/guide/index-management.md')
s = p.read_text()
needle = 'Task copy results are validated independently of application serializer naming policy.'
assert s.count(needle) == 1
p.write_text(s.replace(needle, 'Asynchronous compatibility-copy submission selects one eligible node and disables failover for that dispatch, without changing retries for ordinary client requests. The pinned transport does not honor request-local `MaxRetries(0)` alone on a multi-node pool. A lost response can hide an accepted task; `X-Opaque-Id` is correlation, not deduplication. Configure proxies and outer application policies not to replay these submissions. Unknown launch outcomes require inspection, not automatic resubmission.\n\n' + needle))
p = Path('.agents/skills/foundatio-repositories/references/index-lifecycle.md')
s = p.read_text()
needle = '**Task evidence is fail-closed:**'
assert s.count(needle) == 1
p.write_text(s.replace(needle, '**Asynchronous launch:** compatibility reindex selects one eligible node for one dispatch; request-local retry zero alone is ineffective in the pinned transport. Preserve node predicates and explicit forced-node configuration. Do not replay ambiguous submissions through proxies or outer resilience policies; the opaque header does not deduplicate tasks.\n\n' + needle))
