from pathlib import Path

path = Path('docs/guide/index-management.md')
text = path.read_text()
old = 'A task listing alone is not proof of termination, and cleanup safety is never inferred after a restart.'
new = 'A task listing or task HTTP 404 is not proof of termination: an unavailable owner node or missing stored result can also produce 404. Cleanup requires a successful task response explicitly reporting `completed: true`, and cleanup safety is never inferred after a restart.'
assert text.count(old) == 1
text = text.replace(old, new)
anchor = '#### Maintenance-window contract\n\n'
assert text.count(anchor) == 1
text = text.replace(anchor, anchor + 'Task copy results are validated independently of application serializer naming policy. Counters must be nonnegative integers; booleans, numeric strings, fractional values, and overflow are rejected rather than coerced. A completed task with `timed_out: true` is not accepted as a successful copy, even when its counters appear complete.\n\n')
path.write_text(text)

path = Path('.agents/skills/foundatio-repositories/references/index-lifecycle.md')
text = path.read_text()
anchor = '### Explicit Index Compatibility Upgrades (ES Major-Version Upgrades)\n\n'
assert text.count(anchor) == 1
text = text.replace(anchor, anchor + '**Task evidence is fail-closed:** copy counters are parsed independently of application serializer naming policy and must be nonnegative integers. A completed but timed-out copy is rejected. Task HTTP 404 does not prove termination; only a successful task read with `completed: true` authorizes current-attempt cleanup. Missing results retain artifacts for inspection. Native names are checked before removing generated prefixes, including when authenticating error-index provenance.\n\n')
path.write_text(text)

path = Path('src/Foundatio.Repositories.Elasticsearch/Configuration/IElasticConfigurationCompatibility.cs')
text = path.read_text()
old = '    /// Ambiguous requests retain the marked artifacts for manual reconciliation.\n'
new = old + '    /// A task HTTP 404 is ambiguous, not positive termination evidence. Timed-out copy results and malformed\n    /// counters are rejected. Cleanup requires a successful task read that explicitly confirms completion.\n'
assert text.count(old) == 1
text = text.replace(old, new)
old = '    /// Cancellation may be reported after a cutover has committed; inspect the original physical source before retrying.\n'
new = old + '    /// Original cancellation is preserved when inspection or reset also fails; secondary errors remain in\n    /// the inner exception. An independent cleanup timeout does not change an unrelated failure into cancellation.\n'
assert text.count(old) == 1
text = text.replace(old, new)
path.write_text(text)
