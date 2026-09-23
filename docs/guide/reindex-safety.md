# Reindex Safety and Recovery

Use this guide when changing index versions, reviewing a migration, or recovering an interrupted copy. It describes the library's migration contract, not an end-to-end transaction across Elasticsearch, application queues, and caches. An atomic alias change is not an atomic copy of a changing index.

## Current release gates

This guide describes the implementation at runtime revision `b458dd81`, not unconditional production approval. The [sequential consistency review](https://github.com/FoundatioFx/Foundatio.Repositories/pull/307#issuecomment-5786782362) tracks additional release-blocking work that documentation and the existing single-node test suites do not resolve:

- **Effective single dispatch:** the pinned transport can ignore request-local `MaxRetries(0)`. A three-node regression reports multiple asynchronous submissions after 502/503/504 responses. The journal can record a returned task ID without knowing about sibling tasks created by transport retries. Validate effective no-retry dispatch and complete task lineage; do not infer one launch from that flag or treat an empty task lookup as universal termination proof. See the [execution finding](https://github.com/FoundatioFx/Foundatio.Repositories/pull/327#issuecomment-5786927969) and the [transport binding](https://github.com/elastic/elastic-transport-net/blob/78dc23fe779254d1a06ef52bef44e3941c6fb049/src/Elastic.Transport/Components/Pipeline/BoundConfiguration.cs).
- **Lease loss and stale controllers:** the package references Foundatio 13.0.4; a separately prepared provider correction is not automatically consumed. Verify that renewal reports lost ownership and that delayed controllers cannot mutate protected resources. Journal optimistic concurrency is not data-plane fencing.
- **Retired-source protection:** the current implementation releases its owned source block after promotion, and cleanup also runs after ambiguous outcomes. Stale concrete-index writers can then write into the retired source and have those writes stranded or deleted. Retaining the retired fence and distinguishing definite pre-cutover failure from committed/unknown cutover are active code-review gates, not features provided by this documentation update.

The original 18 resolved review threads and passing 1,012-test suites do not close these newer gates. Keep them open until the actual combined release tree and consumed dependencies have regression evidence. Reconcile the current runtime fixes with the sequential review before accepting a release; an older parallel candidate is not a substitute for that integration.

## Choose the consistency contract

| Mode | Ordering | Operating requirement |
| --- | --- | --- |
| Quiesced (`QuiesceSourceOnReindex = true`) | Live copy, block source, full final copy and deletion reconciliation, verify, promote | Exclusive migration ownership; producers retain and retry blocked operations |
| Default (`false`) | Copy, promote, timestamp/ObjectId catch-up | Externally stop writes for the whole migration, or explicitly accept best-effort consistency |

**The intended verified mode for mutable data is quiescence when the final-pass write interruption is acceptable; the release gates above still need to close.** Default mode can overwrite a newer destination update or resurrect a destination deletion during post-promotion catch-up. Timestamps, ObjectIds, soft-delete flags, and equal document counts do not remove those races.

“Verified” means the final task accounted for its source documents, the expected output count matched, deletion reconciliation succeeded where applicable, and a complete per-primary source checkpoint remained unchanged before promotion. It is **not** a byte-for-byte checksum, proof of business-level transformation correctness, or certification of every cluster failure mode. A completion record attests that protocol; it is not continuous validation of the live destination.

## Configure and start a migration

Configure quiescence on the registered index before starting a new migration. This example assumes the application's existing `Employee` model and `IElasticConfiguration`:

```csharp
using Foundatio.Repositories.Elasticsearch.Configuration;

public sealed class EmployeeIndex : VersionedIndex<Employee>
{
    public EmployeeIndex(IElasticConfiguration configuration)
        : base(configuration, "employees", version: 2)
    {
        QuiesceSourceOnReindex = true;
        DiscardIndexesOnReindex = false;
    }
}
```

`DiscardIndexesOnReindex = false` retains the old index for acceptance and recovery. It does not make rolling back safe after new writes reach the destination. `DiscardExpiredIndexes` on time-series indexes is a separate retention policy and must also be coordinated.

Register the index with the application's configuration as usual. Configure the destination mappings/settings, then call the direct path with an explicit cancellation token:

```csharp
await configuration.ConfigureIndexesAsync(beginReindexingOutdated: false);
await configuration.ReindexAsync(progressCallbackAsync: null,
    cancellationToken: cancellationToken);
```

The direct dispatcher discovers **outdated versions**; it does not audit an already-current version. A direct call can also return without migrating when another worker holds the lock. Confirm the expected physical destination and durable outcome; successful startup or a progress percentage alone is not evidence that this particular migration completed.

For queued execution, register a `ReindexWorkItemHandler` and run a worker. Supply exact physical names per partition and opt in on the work item itself; changing an index property does not rewrite already-enqueued items:

```csharp
using Foundatio.Repositories.Elasticsearch.Jobs;

await queue.EnqueueAsync(new ReindexWorkItem
{
    OldIndex = "employees-v1",
    NewIndex = "employees-v2",
    Alias = "employees",
    QuiesceSource = true,
    DeleteOld = false
});
```

Here `queue` is the application's configured work-item queue. Prepare the destination before enqueuing; the handler is not a mapping/template deployment mechanism. Enqueuing alone does not execute the copy. See [Jobs](/guide/jobs#reindexworkitemhandler) for worker behavior and [Index Management](/guide/index-management#what-actually-triggers-a-reindex) for direct and time-series entry points.

## Preconditions and unsupported combinations

Use distinct, exact physical source and destination names in the same cluster. This contract covers ordinary versioned indexes and dated daily/monthly partitions, not arbitrary wildcard sources, remote reindexing, or data-stream migration. Elasticsearch requires `_source` and does not copy mappings, settings, shard counts, or replicas automatically; configure and test the intended destination first ([reindex API](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-reindex)).

The destination must be **exclusively owned by this migration** until promotion: no application writers, readers depending on its contents, or unrelated data. Alias checks do not discover clients using physical names. Scripted quiesced migration deletes all destination documents before the final copy; a shared destination is not supported. Prefer a newly provisioned destination. Unscripted reconciliation assumes source and destination preserve document IDs and routing.

Keep mappings, scripts, ingest pipelines, and their dependencies stable for both passes and recovery. A pipeline can run even when `ReindexWorkItem.Script` is null; review the destination's default/final pipelines. An identity-changing or document-dropping pipeline is not covered by unscripted membership reconciliation. Do not infer support merely because aggregate counts happen to match.

Quiesced `StartUtc` is rejected because a partial source range cannot establish the full-copy contract. Scripted output must be deterministic, remain in the named destination, and produce unique output identities. Do not use a new script, destination generation, or quiesce flag to “approve” an earlier unknown result.

## Why blocking starts at the second pass

```text
First full copy with a writable source
  -> block source writes and drain in-flight writes
  -> refresh and perform the full final copy
  -> reconcile deletions and verify the result
  -> switch aliases
  -> release the owned block
  -> persist completion
  -> optionally delete the old source
```

The first pass is not write-blocked. `PUT /{source}/_block/write` starts the final-pass barrier; the dedicated API waits for in-flight writes on the shards. Merely setting `index.blocks.write` does not provide that drain guarantee. The library requires acknowledgement and exact-index block confirmation ([index blocks](https://www.elastic.co/docs/reference/elasticsearch/index-settings/index-block)).

Hold the barrier through verification **and** alias promotion. Releasing it after the scan but before promotion opens another missed-write window. Reads remain available while the write block is held. Writes targeting the physical source, including through its aliases, receive `403 cluster_block_exception`.

The final pass is a full rescan, not a timestamp delta: timestamps may be unchanged/backdated, ObjectIds encode creation rather than update time, and hard-deleted records are no longer searchable. The first pass therefore does not turn the write outage into a changed-documents-only operation. Scripted copies additionally clear and rebuild their first-pass output. This trades more I/O for an explicit final-state reconciliation; it is not a minimal-I/O migration.

### Capacity and outage planning

Reserve space for the source, destination, replicas, and transient indexing/merge work. Document deletion is not a promise of immediate disk reclamation. Include the second full pass, destination scans and routing lookups, script execution, and configured throttling in the outage budget. Lower `ReindexBatchSize` for large documents; lower `ReindexRequestsPerSecond` to reduce sustained load, recognizing that this also lengthens the blocked phase.

No write-outage duration has been established for 5–500 GB production indexes. Benchmark a representative disposable copy with the production mappings, routing, scripts, shard count, and application load. Daily/monthly migrations block one physical partition at a time; late-arriving updates and deletes to older partitions still need the same protocol. There is no global atomic cutover across all partitions.

## Retry rejected application writes safely

The library does not buffer rejected application operations. Keep their durable source or queue entry until the individual operation is confirmed. A bulk request may succeed at HTTP level while some items fail: inspect each item, retain only the failed operations for retry, and never acknowledge the entire batch from the HTTP status alone ([bulk API](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-bulk)).

Retry only a confirmed **migration-owned write block** under the application's bounded backoff, jitter, and alerting policy. Do not retry every `403`: authorization failures, disk-watermark blocks, and operator blocks need different remedies. Retry through the logical alias so a post-cutover retry resolves to the new destination. Do not cache physical write targets across the migration; the retained old source may be writable again after its owned block is released.

Use stable document IDs and application-level idempotency or version checks. A timeout after a write is not proof that it failed. Replaying a non-idempotent increment, script, or external side effect can duplicate work. Preserve update/delete/restore ordering for each logical entity; a delayed older retry must not overwrite a later value or undo a deletion. Apply backpressure when queues grow rather than silently dropping work or retrying indefinitely.

## Hard deletes and routed documents

Suppose pass one copies document A, then A is physically deleted from the source. Another `_reindex` never sees A and cannot remove the stale destination copy. Copying twice is insufficient.

For an unscripted quiesced migration, every destination `(id, routing)` is checked against the blocked source, including when aggregate counts match. Only confirmed missing identities are removed. `_mget` errors are not absences; incomplete search results and failed bulk-delete items stop promotion. Stored routing is explicitly requested when `_source` is disabled and is supplied to both lookups and deletes ([multi-get API](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-mget)).

For a scripted quiesced migration, `ctx.op = 'noop'` means “do not write this document,” not “remove its earlier copy.” The unpromoted destination is emptied before the full final scripted pass. That removes first-pass remnants from conditional no-ops, intentional deletes, and hard-deleted source records. Task accounting includes `deleted`; expected final output is `total - noops - deleted`, and both under- and over-counts fail verification.

## Soft deletes are data, not a synchronization protocol

An application flag such as `isDeleted` keeps a source document representing the deletion. The full quiesced copy carries that flag even if `updatedUtc` does not change. Physical-index copying does not apply ordinary repository query filters, so retained soft-deleted records are copied unless a transformation deliberately drops them. Check that the destination application's queries still exclude them.

| Event | Required treatment |
| --- | --- |
| Flag changes without a timestamp update | Full quiesced copy sees it; timestamp-only catch-up can miss it |
| Copy filters out tombstones | Reconcile/remove the earlier active destination copy; skipping the tombstone is insufficient |
| Tombstones are purged | Treat as hard deletion, not an update a later timestamp query can discover |
| Delete is followed by restore or another update | Preserve entity operation ordering through producer retries and cutover |

Retain tombstones through migration and its recovery window, and coordinate destructive retention. Elasticsearch's internal soft-delete history is not an application change stream consumed by this reindexer. Continuous exact replication without a write interruption needs a separate durable ordered change stream, deletion events, and destination version fencing; this PR does not implement that protocol.

## Durable evidence and replay gates

`foundatio-reindex-completions` stores migration identity, destination UUID, and a versioned fingerprint of script, timestamp field, start range, and quiesce mode. The hashed ID is bounded. A partial-range result cannot satisfy a full-range request, nor a default result a quiesced request. Throttling and cleanup options do not change which documents were copied. Old key/fingerprint formats are not automatically upgraded or certified.

A matching completion may acknowledge a duplicate even after the alias advances. A promoted destination without matching evidence remains unconfirmed **in either mode**. An unavailable completion read fails rather than authorizing replay. An unrecorded stale source cannot move an existing alias backwards.

`foundatio-reindex-safety` is a small internal journal of side effects that outlive a worker, not a general-purpose workflow engine:

- Task dispatch intent is stored before starting an asynchronous copy. The implementation checks the recorded task before another dispatch and keeps unknown dispatch fenced. This does not establish that every task from an ambiguous or retried submission is known; the single-dispatch and task-lineage release gates above remain open. Cancellation acknowledgement alone is not termination ([task cancellation](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-tasks-cancel)).
- Block intent and confirmed ownership bind the migration to a source UUID. Confirmed same-migration orphaned blocks can be recovered; operator-owned, ambiguous, or wrong-generation blocks are preserved for inspection.

Journal updates/deletion use ownership tokens and Elasticsearch sequence-number/primary-term checks. These protect journal mutations, **not every write or alias mutation against a worker that has lost its distributed lease**. Continuous exclusive coordination is a prerequisite. Neither the journal nor a boolean index block can arbitrate an operator or stale process that ignores it.

Completion and safety records have different lifecycles. Completion records remain useful while duplicate work items can still arrive; active or ambiguous safety records must not expire merely because a worker stopped. No automatic retention policy is provided for these shared indexes. Do not delete their records to force progress, or include them in broad source-index cleanup patterns.

## Cancellation and outcome handling

| Observed outcome | Meaning and next action |
| --- | --- |
| Failure/cancellation before any promotion request | Source remains authoritative; confirm task termination and block cleanup before a controlled retry |
| Alias request sent but response lost, rejected, or cancelled | Outcome may be ambiguous; inspect exact alias state rather than assuming rollback |
| Destination promoted without matching completion | Stop replay; inspect both indexes and durable state, in either mode |
| Completion exists but source remains | Migration can be complete while optional cleanup was skipped/failed; handle cleanup separately |
| Direct dispatcher returns without doing work | It may have skipped a current version or lost the lock race; verify the physical migration independently |

Before cutover, caller cancellation is observed and cleanup uses independent bounded tokens. Once **default-mode** promotion begins, required catch-up does not inherit caller cancellation. There is no fixed total shutdown bound for a task that keeps progressing, and a process kill can still interrupt it. In quiesced mode, cancellation during the alias request can leave a verified destination promoted but no completion record; that is an inspection case, not permission to recopy.

Progress callbacks execute inside the migration and participate in lock renewal. Keep them fast and reliable; do not couple optional UI/telemetry delivery to a failing network call that throws through the migration. Callback exceptions are not rollback instructions. A callback failure after promotion, source cleanup, or completion persistence can produce an exception after irreversible work already succeeded.

## Recovery procedure

Stop competing migration workers and obtain the same `reindex:{alias}` lock before intervention. Preserve the exception, exact source/destination names, UUIDs, aliases, work-item options, task ID, and journal IDs. Inspect IDs/routing and relevant content as well as counts.

These Elasticsearch Console requests are read-only. Substitute the actual index names and task ID; they do not cancel a task, remove a block, or certify a result:

```http
GET /employees-v1,employees-v2/_settings?filter_path=*.settings.index.uuid,*.settings.index.blocks,*.settings.index.default_pipeline,*.settings.index.final_pipeline
GET /employees-v1,employees-v2/_alias
GET /employees-v1/_stats?level=shards
GET /_tasks?actions=*reindex&detailed=true
GET /_tasks/REPLACE_WITH_NODE_ID:REPLACE_WITH_TASK_ID
```

Read a known safety record directly using the journal ID from diagnostics. Its internal fields are stored with `dynamic: false`; a term query on an unmapped field is not a reliable way to prove no ownership record exists. `ElasticReindexer.GetCompletionId(workItem)` returns the public completion-record ID. A missing document and an unavailable/unauthorized index are different outcomes; preserve that distinction.

For a known task, confirm terminal state or stop it and verify termination. For unknown dispatch, examine active tasks before considering any journal change. For a stuck block, establish ownership and physical generation before removing it. If promotion already happened, stop treating the old source as an automatically authoritative repair source: blindly recopying or rolling aliases back can destroy newer writes or resurrect deletes. Recover into a fresh destination from authoritative application data where necessary.

The library has no public “force complete” or automatic rollback API. Do not fabricate completion documents. An operator may accept an inspected result and retire a stale queue item using application operations, or plan a new recovery migration. A retry permitted by the gates copies from the source again; it never derives a resume watermark from arbitrary destination documents. The direct dispatcher does not enumerate already-promoted failed attempts for recovery.

<a id="production-prerequisites"></a>

## Production prerequisites and acceptance

Use a shared distributed lock provider across direct, queued, and maintenance paths, and maintain ownership for the whole operation. The in-memory default is not distributed coordination. Low-level `ElasticReindexer` callers must supply equivalent locking. Keep request/callback stalls below the lease-renewal budget and treat lease loss as an operational failure; safety records are not a replacement for lease fencing.

Coordinate retention, mapping/pipeline changes, index recreation, alias changes, and operator block changes. Alias maintenance sharing the lock does **not** mean destructive retention is fully serialized with migration. Before deploying mixed library versions, drain or pause migration workers: older code does not understand the new journal/replay contract. Existing queued items keep their serialized options, including `QuiesceSource = false` when absent.

Use a dedicated migration identity rather than adding broad administration rights to ordinary application writers. Verify the actual role against the following API families on disposable staging:

| Resource | Operations to authorize and verify |
| --- | --- |
| Physical source and its aliases | Reads, settings/mappings/aliases, shard statistics, refresh, write-block management; index deletion only when cleanup is enabled |
| Physical destination and aliases | Index/mapping provisioning, writes, refresh, counts, routing-aware reads/deletes, alias updates |
| Tasks | Inspect and cancel the migration's tasks; task cancellation requires cluster `manage`, not just `monitor` |
| Shared state and failure indexes | Provisioning and required document reads/writes/deletes; restrict access to failure documents that may contain source data |

The [reindex](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-reindex), [aliases](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-indices-update-aliases), and [task APIs](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-tasks-cancel) have different privilege requirements. Precreating a state index does not eliminate all create-index requests in the current implementation. Avoid a blanket `superuser` workaround; test both success and denial/cleanup paths with the real credentials. Field/document-level visibility restrictions cannot be used to certify a complete physical-index copy. Use TLS, do not log credentials or unnecessary document content, and protect backups and error artifacts.

Server-side copying bypasses repository per-document events and application cache invalidation. Plan cache refresh/invalidation after transformed data becomes live, and re-read documents before using pre-cutover optimistic-concurrency tokens. A physical-index sequence number is not a portable application version across a reindex.

Before deleting the retained source, verify aliases and metadata, representative transformed content, routed reads/writes, delete/restore behavior, cache behavior, producer backlog/retries, completion evidence, and restored writes through the alias. Record the acceptance decision. Keeping the old index is recovery material, not a safe automatic rollback once the destination has accepted writes.

The previously executed tests used .NET 10 with the pinned 8.x client against disposable Elasticsearch 8.19.0 and 9.5.0 servers. That is not a separate 9.x typed-client test, a production-role test, or a large-index benchmark. Validate the deployed package/server combination, least-privilege role, replica durability requirements, and measured outage before rollout.

## Elasticsearch references

The [reindex API](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-reindex), [index-block semantics](https://www.elastic.co/docs/reference/elasticsearch/index-settings/index-block), [aliases API](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-indices-update-aliases), [bulk API](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-bulk), [multi-get API](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-mget), and [task cancellation API](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-tasks-cancel) are the server contracts underlying this implementation. These references describe Elasticsearch behavior, not a separate certification of this library.
