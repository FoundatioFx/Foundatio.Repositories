# Reindex Safety and Recovery

Use this guide before migrating mutable production data. An atomic alias change is not an atomic copy of a changing index, and equal counts are not proof of equal documents.

## Choose the consistency contract

`QuiesceSourceOnReindex = true` on an index, or `QuiesceSource = true` on an exact physical work item, enables verified cutover. It is opt-in: existing work items default to the historical non-blocking ordering.

| Mode | Ordering | Appropriate operating assumption |
| --- | --- | --- |
| Quiesced | Live copy, block source, full final copy and deletion reconciliation, verify, promote | Mutable data; producers retain and retry rejected writes |
| Default | Copy, promote, timestamp/ObjectId catch-up | Externally stopped writes, or an explicitly accepted best-effort non-blocking contract |

Default mode still has concurrent-write limitations: catch-up can overwrite newer destination data or resurrect a destination deletion. Hard deletes during the first pass cannot be found by a timestamp range. Per-primary checkpoint and changed-ID guards detect specific unsafe cases; they are not write barriers or a replacement for change-data capture.

## Why blocking starts at the second pass

The first pass remains writable. Only after it finishes does the reindexer apply `PUT /{source}/_block/write`. Unlike merely setting a boolean setting, the dedicated API waits for in-flight writes to finish on the shards before reporting success. The library validates acknowledgement and exact-index confirmation.

The remaining order is refresh, full-source rescan, deletion reconciliation, verification, alias switch, owned-block release, durable completion, optional source deletion. Reads continue throughout. Writes to the blocked physical source receive `403 cluster_block_exception`, including writes addressed through aliases. The library does not enqueue those rejected application operations: producers must retain and retry them through the logical alias.

The final scan is deliberately not only a timestamp delta. Timestamps can be unchanged or backdated; ObjectIds encode creation time rather than update time; hard-deleted documents are no longer searchable. Therefore outage duration depends on full source/destination size, routing lookups, transformation cost, and throttling. No duration has been established for 5–500 GB production indexes. Benchmark a representative disposable copy before scheduling a maintenance window. Daily/monthly reindexing bounds the block to one dated partition at a time.

## Hard deletes and routed documents

Suppose pass one copies document A, then A is physically deleted from the source. Another `_reindex` never sees A and cannot remove the stale destination copy. Simply copying twice is insufficient.

For an unscripted quiesced migration, the library scans every destination `(id, routing)` and checks it against the now-blocked source. It deletes only confirmed missing identities. Equal-sized indexes are checked too: a missing document and an extra document can cancel in the count. Both `_mget` item failures and bulk-delete item failures stop promotion; HTTP 200 alone is insufficient. Routing accompanies both lookups and deletes, including parent/child documents.

For scripted migration, output can change membership through `ctx.op = 'noop'` or `'delete'`. The unpromoted destination is cleared before the final full scripted copy. This removes first-pass remnants even when a previously copied document becomes a no-op, is intentionally deleted, or disappears physically. Final accounting includes `deleted`, and expected output is `total - noops - deleted`. Under- and over-counts both fail verification.

Use deterministic transformations producing unique destination identities in the named destination. Do not redirect script output to unrelated indexes. Keep mappings and ingest pipelines stable during migration; externally changed transforms are not captured merely by hashing the work-item script. Quiesced `StartUtc` is rejected because partial-source copying cannot establish the full-copy contract.

## Soft deletes are data, not a synchronization protocol

An application flag such as `isDeleted` keeps a searchable source document representing the deletion. The final full quiesced copy carries the flag even when `updatedUtc` does not change. Physical-index copying does not apply repository query filters, so retained soft-deleted records are copied unless a transformation intentionally drops them.

Soft deletes alone do not make the default mode lossless. A timestamp delta misses flags written without a qualifying timestamp; filtering deleted records out of the source merely leaves stale destination copies; purging tombstones before reconciliation turns them back into unobservable hard deletes. Restore/undelete operations, changed routing, stale post-cutover writes, and concurrent cleanup/retention also need coordination. Elasticsearch's internal soft-delete history is not an application tombstone stream consumed by this reindexer.

Retain tombstones through migration and its recovery window, keep deletion/restoration timestamps correct for any timestamp-based tooling, and pause destructive retention or index recreation during the cutover. For continuous zero-write-outage exact replication, a separate durable change stream with ordering, delete events, and destination version fencing is required; this implementation does not provide that protocol.

## Durable evidence and replay gates

`foundatio-reindex-completions` records alias/source/destination identity, destination UUID, and a versioned fingerprint of script, timestamp field, start range, and quiesce mode. Its hashed ID is bounded even for long index names. A partial-range result cannot satisfy a full-range request, nor a default result a quiesced request. Old key/fingerprint formats are not silently certified as the new contract.

A matching completion may be acknowledged without recopying even after the alias advances. A promoted destination without matching evidence is unconfirmed **in either mode**. A new quiesce flag proves nothing about the earlier attempt. An unavailable completion read is a failure, not evidence that copying is safe. A source that no longer owns an existing alias cannot promote an unrecorded stale migration backwards.

`foundatio-reindex-safety` records side effects that outlive a worker:

- **Task intent and known task ID:** intent is persisted before asynchronous dispatch. A retry stops and confirms a known prior task before launching another. Missing task ID after ambiguous dispatch remains fenced. A cancellation acknowledgement is followed by a terminal-state check.
- **Write-block intent and ownership:** source UUID and ownership token bind a confirmed block to one physical generation. A later attempt can recover a confirmed same-migration block; it preserves an operator block. An unconfirmed application intent or mismatched generation requires inspection.

Journal updates and deletion use owner-token checks plus sequence-number/primary-term concurrency control. A stale worker cannot erase newer ownership. Cleanup has an independent bounded timeout; failure to confirm cleanup leaves evidence and loud diagnostics rather than silently authorizing overlap. Do not put these shared indexes under ordinary source-index retention or delete their records to force progress.

## Recovery procedure

Stop competing migration workers and acquire the same `reindex:{alias}` distributed lock before intervention. Inspect exact physical names, UUIDs, alias definitions, task status, block settings, completion records, and safety records. Compare document IDs/routing and relevant content as well as counts.

For a known task ID, confirm completion or stop it and verify terminal state. For unknown dispatch, inspect active reindex tasks before clearing an intent. For a stuck source block, establish ownership and generation before removing it. Do not clear a pre-existing operator block, an unconfirmed ownership intent, or a newer worker's record automatically.

If traffic has already moved, do not blindly recopy the old source over the live destination or roll the alias back: either action can discard newer writes or resurrect deletions. Validate and accept the completed result explicitly, or design recovery into a fresh destination using the authoritative application data. A missing record after genuine success is intentionally escalated rather than manufactured.

The direct configuration dispatcher discovers outdated index versions; it does not scan or certify existing current versions. Use explicit physical work items for interrupted-attempt recovery. Whole-migration retries are not applied automatically after exceptions. Queue redelivery still obeys these replay gates and eventually follows the configured dead-letter policy.

## Production prerequisites

Use a shared distributed lock provider across direct, queued, and maintenance paths. Low-level `ElasticReindexer` callers must supply equivalent coordination. Keep callbacks reliable: callback exceptions are failures, not rollback instructions. After non-blocking alias promotion begins, caller cancellation no longer aborts required catch-up; process termination can still interrupt it and requires durable-state recovery.

All application writes should use the intended aliases. External operators and retention jobs must not remove blocks, recreate indexes, mutate aliases, or change transformations concurrently outside that coordination. A boolean Elasticsearch write-block setting has no independent owner identity; the journal cannot atomically arbitrate an operator ignoring the lock.

Provision permissions for source reads/metadata/refresh/statistics/write-block management, destination create/write/delete/refresh/aliases, task inspection/cancellation, and read/create/write/delete on both shared state indexes. Verify the application's least-privilege role on a disposable staging cluster; the integration suite's security-disabled clusters do not certify a production role policy. Keep backups and preserve the old index until the result is accepted when rollback/recovery requirements demand it.

## Elasticsearch references

[Add an index block](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-add-block), [reindex documents](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex), [multi-get partial results and routing](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-mget), and [task cancellation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-tasks-cancel) define the underlying server behavior.
