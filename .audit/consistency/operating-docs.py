from pathlib import Path
import sys,re
root=Path(sys.argv[1]);mode=sys.argv[2]
def section(path,start,end,replacement):
 p=root/path;s=p.read_text(encoding='utf-8-sig');a=s.index(start);b=s.index(end,a);p.write_text(s[:a]+replacement+'\n\n'+s[b:])
if mode=='327':
 section('docs/guide/index-management.md','#### Recovering from a rolling restart mid-upgrade','#### When do writes flip', '''#### Recovering from a rolling restart mid-upgrade

A process restart does not establish that its Elasticsearch task stopped. Stop competing migration/alias managers and inspect exact source/destination generations, outstanding task IDs, alias topology and completion evidence before retrying. A missing task, expired lease, empty listing or equal document counts is not permission to recopy, delete an artifact or unblock a source.

Migration leases use independent bounded renewal as well as progress-boundary checks. After a process dies its lease eventually expires, but its server-side task may survive. A new lease owner is not automatically authorized to overwrite that task's destination. The Foundatio provider fix in #573 is required to detect failed compare-and-renew; renewal itself is not storage-enforced fencing.

A new first pass performs a full copy unless the caller explicitly supplies a start time; it does not infer a completed prefix from the destination's newest timestamp. Full recopy is still unsafe if an earlier task or live writer can mutate that destination. The queued path refuses a promoted destination without valid generation-bound completion evidence, including quiesced work items. A work-item flag cannot reconstruct the earlier attempt's provenance.

A quiesced source stays blocked after reconciliation failure or after retirement. Do not clear the block on a retired source merely to suppress write errors from stale clients. The current code does not implement a complete durable attempt journal with safe automatic takeover; see [Recovery protocol and remaining release gates](../design/reindex-consistency.md).''')
 section('docs/guide/index-management.md','For a **time-series index the block is applied per partition**','## Best Practices', '''For a **time-series index the active migration blocks one partition at a time**. A successfully migrated partition accepts new writes through its destination alias. Retained old physical partitions remain blocked so stale writers cannot add data that would later be discarded.

Under this ordering the source is fully rescanned while blocked; the outage is proportional to the whole reconciliation workload, not merely the changed documents. Every destination identity is reconciled with routing preserved. Multi-get errors never mean absence, bulk HTTP success never substitutes for item success, and partial search/refresh/count evidence refuses promotion. Both count deficits and surpluses fail after reconciliation.

Arbitrary nonempty transformation scripts are rejected before any Elasticsearch request in quiesced mode. A script can change membership, IDs or routing, and no count-only check can verify the expected transformed result. Use a fresh blocked-source rebuild or a transformation-aware migration protocol rather than skipping deletion checks.

Once the source is fenced for reconciliation, success and failure retain that fence until source deletion or explicit operator recovery. An invalid or lost alias response does not prove that traffic stayed on the source. Inspect both generations and outstanding work before failback; never reopen a retired source to stale clients.

A low-downtime mutable-data migration requires backfill plus durable ordered change replay, including deletes/tombstones and a final writer boundary. Simply moving a write block to a later full pass does not supply those guarantees. The default non-quiesced ordering remains best effort under concurrent writes.''')
 p=root/'docs/guide/index-management.md';s=p.read_text()
 s=s.replace('If a run is interrupted — a process restart, a failure on one partition, a lost lock — just **run it again**. It picks up the remaining old partitions and continues, oldest first. A partition whose reindex failed keeps its old index (the delete is gated on success), so nothing is lost.', 'If a run is interrupted, inspect outstanding tasks, physical generations and alias/completion evidence before retrying. Retaining the source prevents destructive cleanup of an unverified copy, but does not prove a second task can safely reuse the destination.')
 s=s.replace('the catch-up pass rescans the whole blocked source until it converges', 'the catch-up pass rescans the whole blocked source and reconciles routed document identities')
 p.write_text(s)
 p=root/'docs/guide/jobs.md';s=p.read_text()
 s=s.replace('The quiesced recovery exception to this rule is described below.', 'The same rule applies to quiesced migrations.')
 s=s.replace('For the default ordering, a promoted destination', 'For either ordering, a promoted destination')
 s=s.replace('The current implementation can write a missing completion record and acknowledge a promoted work item when that work item has `QuiesceSource = true`.', 'A promoted work item without a matching completion record is refused even when `QuiesceSource = true`.')
 s=s.replace('The exact-destination guard does not close this separate recovery gap.', 'No completion record is fabricated from the flag.')
 s=s.replace('The handler sets `AutoRenewLockOnProgress = true`, which automatically renews the distributed lock whenever progress is reported', 'The handler renews its caller-owned work-item lease independently every 30 seconds with bounded attempts, and retains progress-boundary renewal. Loss cancels linked work; already-dispatched requests still require reconciliation')
 p.write_text(s)
 p=root/'docs/guide/troubleshooting.md';s=p.read_text()
 s=s.replace('Note that a **quiesced** migration does not land here: promotion under `QuiesceSourceOnReindex` happens only after reconciliation and verification succeeded, so a redelivered work item can safely re-derive the completion record and acknowledge.', 'This also applies to **quiesced** migrations. A redelivered work-item flag is not proof that the promoted physical generation was verified; missing completion evidence is never reconstructed from that flag.')
 s=s.replace('Something bypassed the block (a direct write to the versioned index name, or another block-clearing process); retry after identifying it', 'Inspect generation changes, block removal and competing administrators; direct writes to the same blocked physical index do not bypass its block. Reconcile before retrying')
 s=s.replace('Retry; a shortfall against a blocked source is not a race, so investigate the copy logs for rejected documents', 'Retain the fence and both artifacts; inspect failed items, task termination and alias topology before retrying')
 p.write_text(s)
 section('docs/guide/troubleshooting.md','### An index is stuck read-only after a reindex','## Notification Issues', '''### An index is stuck read-only after a reindex

A `403 cluster_block_exception` on the old physical source may be **intentional retirement**, not failed cleanup. Quiesced reconciliation retains the old source fence on both success and failure. The successfully promoted destination is the write target; stale clients must stop addressing the retired generation.

Inspect the exact physical index settings, alias membership, source/target UUIDs, task IDs and durable completion evidence. Do not infer a failed cutover from a lost response, or infer task termination from HTTP404 or an empty task listing. A pre-existing administrator block is never owned by the migration.

Only consider unblocking an intact source after establishing a proven pre-cutover failure and ruling out every outstanding cutover/target-writing request. Otherwise keep both artifacts for manual reconciliation. Do not unblock a retired source to accommodate stale writers. There is no general-purpose automatic reset that can establish these facts after a crash; see [Recovery protocol and remaining release gates](../design/reindex-consistency.md).''')
 p=root/'src/Foundatio.Repositories.Elasticsearch/Jobs/ReindexWorkItem.cs';s=p.read_text(encoding='utf-8-sig')
 a=s.index('    /// <summary>\n    /// Blocks writes to the source index');b=s.index('    public bool QuiesceSource',a)
 s=s[:a]+'''    /// <summary>
    /// Reconciles an unscripted copy behind a source write fence before alias promotion. Defaults to false.
    /// Nonempty transformation scripts are rejected before any Elasticsearch request in this mode.
    /// </summary>
    /// <remarks>
    /// The blocked reconciliation scans the whole source and destination, so outage cost is not delta-only.
    /// Gate application writers and drain accepted work first. Routing and complete item/shard evidence are
    /// required, and both count deficits and surpluses refuse promotion. Retained source generations stay
    /// blocked on success or failure until deletion or deliberate operator recovery. A lost cutover response
    /// does not establish which generation serves traffic; a work-item flag does not prove completion.
    /// This does not implement online change replay, automatic crash takeover, or storage-enforced fencing.
    /// </remarks>
'''+s[b:];p.write_text(s)
 p=root/'src/Foundatio.Repositories.Elasticsearch/Configuration/DailyIndex.cs';s=p.read_text()
 s=s.replace('// Per partition: the block is applied when this work item runs and released before the next\n                // iteration, so the write outage covers one period at a time, not the whole batch.', '// Per partition: the active destination resumes writes after cutover; a retained retired\n                // source stays blocked to reject stale writers.')
 p.write_text(s)
p=root/'docs/design/reindex-consistency.md';p.parent.mkdir(parents=True,exist_ok=True)
p.write_text('''# Reindex consistency: protocol and remaining release gates

Status: **the generation/attempt journal and automatic takeover protocol below are not implemented**. This document is an explicit acceptance contract, not a claim that the current code provides crash-safe online migration. Related work: Repositories #307/#327 and Foundatio #573.

## Invariants

A lease owner must be able to detect failed renewal. Losing ownership removes permission to initiate cutover, delete artifacts, unblock a source or fabricate completion. Independent heartbeats improve lease liveness but cannot revoke a request already sent to Elasticsearch.

Task submission and task completion are separate facts. A lost launch response must not be blindly retried. `X-Opaque-Id` is correlation, not deduplication. HTTP404 and complete-looking task listings are observations, not proof that every delayed request or relocated writer is gone. Terminal task evidence does not by itself prove the copy is correct or sufficiently replicated.

An exact copy is verified against physical generations, not mutable names. Routed document identity, all expected primary shards, transformation/configuration identity, intended aliases and destination redundancy must be accounted for. Counts cannot establish equality of identities or contents.

## Required durable attempt record

Persist, before launch, a unique attempt ID; logical lock resource; cluster identity; source and destination UUIDs; transformation and relevant mapping/settings fingerprints; intended alias definitions; and ownership generation. Persist task IDs and relocation lineage when learned. A launch with no confirmed task ID remains unknown, not not-started.

State transitions must use optimistic concurrency on the record and explicit allowed prior states: planned, launch intent, copying, verified, cutover intent, committed, completed, uncertain. Verification records must bind to the destination UUID and expected alias topology. Commit intent must be durable before submitting destructive alias/delete operations. Completion must follow verified cutover and required finalization, never be reconstructed from a newly supplied request flag.

Until takeover is proven safe, an unresolved attempt forbids another controller from reusing its target. An updated record or a renewed Redis lease is not an Elasticsearch fencing token. The design must either enforce mutation authority at a single coordinator/gateway that rejects stale generations, or forbid automatic takeover and retain artifacts until outstanding requests are reconciled. Ordinary UUID prechecks alone do not make a later alias update conditional on that UUID.

## Recovery acceptance matrix

| Observation | Permitted action |
| --- | --- |
| Authoritative terminal copy failure, no cutover dispatch, stable authenticated generations | A separately verified reset may discard only the private target; do not generalize this to restarts |
| Task404, unknown launch, node/task failures, missing lineage or duplicate submission | Retain artifacts and authority restrictions; no inferred termination |
| Cutover intent with lost response | Independently inspect exact expected alias topology and physical generations; never assume source is still live |
| Proven committed target bound to durable verified record | Finalize that generation only; do not recopy into the live target |
| Recreated target with the same name | Reject stale records and require manual reconciliation |
| Lost lease while requests are outstanding | Stop new guarded mutations; durable ownership transition alone cannot revoke old requests |

## Required fault tests

Exercise lease expiry/reacquisition and delayed old-owner resumption; failed/late renewal; duplicate asynchronous launch after a lost response; task relocation with original and successor IDs; missing stored task result; delayed alias request after controller restart; source or destination delete/recreate with the same name; missing replicas at destructive cutover; scripted membership/identity changes; same-ID different-routing documents; partial mget/bulk/search/refresh/count results; quiet-shard updates; and changed identities beyond the first page.

Kill and restart the coordinator at every durable transition. Tests must assert what was **not** mutated, not only which exception was raised. Validate the actual combined release tree, not independently green branches. A two-node happy-path suite is not a network-partition or crash-recovery certification.

## Strict offline and low-downtime protocols

Strict offline replacement gates writers, establishes a dedicated source write block, refreshes, copies to an isolated target, verifies task/item/shard/content evidence and replica readiness, then switches routing and retires old writers. Retained old physical indexes remain blocked. For #307 the canonical old physical name becomes an alias, so source deletion is part of cutover and a verified snapshot is required before it.

Low-downtime mutable migration instead needs durable snapshot-boundary plus change capture, ordered idempotent replay including deletions/tombstones, a final writer gate and replay boundary, verification, destination redundancy and controlled retirement. Direct dual writes without atomic durable intent leave a split-write window. Tombstone retention must span every active consumer's replay boundary. A full second scan while blocked remains an O(dataset) outage, not a delta-only migration.

Use immutable historical partitions to reduce the active write window only when late arrivals, corrections and retention jobs are controlled. Slicing, throughput changes and newer relocation-aware task APIs need explicit child-task/lineage accounting. Elasticsearch 9.5's reindex-specific cancellation API can follow shutdown relocations, but its documented404 is still ambiguous; do not downgrade the older-server safety rule.

## Release boundary

The cache ownership repair must be released and consumed, not merely opened as an upstream PR. Shared migration code must be reconciled before combining #307/#327/#308. Require exact-head full suites, fault tests, packaged consumers, persistent-major/snapshot rehearsal and the deployed cluster's own upgrade checks. Current partial implementations and this design document are not production approval.
''')
