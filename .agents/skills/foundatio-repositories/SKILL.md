---
name: foundatio-repositories
description: >
  Use when querying, counting, patching, or paginating data through
  Foundatio.Repositories Elasticsearch abstractions. Also use when configuring
  index mappings, managing index lifecycle (VersionedIndex, DailyIndex,
  MonthlyIndex), bumping index versions, planning reindex operations, or
  setting up retention policies. Covers filter expressions, aggregation queries,
  partial and script patches, search-after pagination, and schema versioning.
  Apply when working with IRepository, ISearchableRepository, FindAsync,
  CountAsync, PatchAsync, PatchAllAsync, RemoveAllAsync, or
  ConfigureIndexesAsync. Supports both v8+ (Elastic.Clients.Elasticsearch /
  ElasticsearchClient) and v7 (NEST / IElasticClient) with version-specific
  references. Also use when migrating from NEST to v8. Never use raw
  ElasticsearchClient or IElasticClient directly -- always use repository
  methods.
compatibility: ".NET 8+. Requires Elastic.Clients.Elasticsearch (v8+) or NEST (v7)."
---

# Foundatio Repositories

High-level Elasticsearch repository pattern for .NET. Interface-first, with built-in caching, messaging, patch operations, and soft deletes. **Never use raw `ElasticsearchClient` or `IElasticClient` directly** -- always use repository methods.

## Documentation via context7

Use context7 MCP for complete, up-to-date API docs and examples:

```text
query-docs(libraryId="/foundatiofx/foundatio.repositories", query="How to use PatchAllAsync with ScriptPatch to update documents by query")
```

Related libraries:

```text
query-docs(libraryId="/foundatiofx/foundatio.parsers", query="How to build aggregation expressions with nested terms and date histograms")
```

Query with specific questions, not single keywords. Both libraries are indexed with full guide content.

## Version Detection and Routing

**Step 1 -- Detect version** (check package references, usings, or client type in the user's codebase):

| Signal | Version |
| --- | --- |
| `Elastic.Clients.Elasticsearch`, `ElasticsearchClient`, `ElasticsearchClientSettings`, `NodePool` | **v8+** (current) |
| `NEST`, `IElasticClient`, `ConnectionSettings`, `IConnectionPool` | **v7** (maintenance mode) |

**Step 2 -- Read the right reference file based on version and task:**

| Task | v8+ | v7 (NEST) |
| --- | --- | --- |
| Queries, patches, aggregations, pagination, mappings | Read [references/patterns.md](references/patterns.md) | Read [references/patterns-v7.md](references/patterns-v7.md) |
| Index types, versioning, retention, reindex scripts | Read [references/index-lifecycle.md](references/index-lifecycle.md) | Read [references/index-lifecycle.md](references/index-lifecycle.md) |
| Migrating from v7 to v8 | Read [references/upgrading-from-nest.md](references/upgrading-from-nest.md) | Read [references/upgrading-from-nest.md](references/upgrading-from-nest.md) |

Do NOT read both patterns files. Read exactly one based on the detected version.

## Repository Hierarchy

```text
IReadOnlyRepository<T>
  ├─ ISearchableReadOnlyRepository<T>
  └─ IRepository<T>  (T : IIdentity)
       └─ ISearchableRepository<T>  (also extends ISearchableReadOnlyRepository<T>)
```

### IReadOnlyRepository&lt;T&gt;

| Method | Returns |
| --- | --- |
| `GetByIdAsync(id, options?)` | `Task<T>` |
| `GetByIdsAsync(ids, options?)` | `Task<IReadOnlyCollection<T>>` |
| `GetAllAsync(options?)` | `Task<FindResults<T>>` (pageable) |
| `ExistsAsync(id, options?)` | `Task<bool>` |
| `CountAsync(options?)` | `Task<CountResult>` |
| `InvalidateCacheAsync(...)` | Invalidate by document, documents, key, or keys |

### ISearchableReadOnlyRepository&lt;T&gt; : IReadOnlyRepository&lt;T&gt;

| Method | Returns |
| --- | --- |
| `FindAsync(query, options?)` | `Task<FindResults<T>>` (pageable) |
| `FindAsAsync<TResult>(query, options?)` | `Task<FindResults<TResult>>` (projection) |
| `FindOneAsync(query, options?)` | `Task<FindHit<T>>` |
| `CountAsync(query, options?)` | `Task<CountResult>` (with aggregations) |
| `ExistsAsync(query, options?)` | `Task<bool>` |

### IRepository&lt;T&gt; : IReadOnlyRepository&lt;T&gt; where T : IIdentity

| Method | Returns |
| --- | --- |
| `AddAsync(doc/docs, options?)` | `Task<T>` or `Task` (bulk) |
| `SaveAsync(doc/docs, options?)` | `Task<T>` or `Task` (bulk) |
| `PatchAsync(id, operation, options?)` | `Task<bool>` (true if modified) |
| `PatchAsync(ids, operation, options?)` | `Task<long>` (count modified) |
| `RemoveAsync(id/ids/doc/docs, options?)` | `Task` |
| `RemoveAllAsync(options?)` | `Task<long>` |

**Events:** `DocumentsAdding`, `DocumentsAdded`, `DocumentsSaving`, `DocumentsSaved`, `DocumentsRemoving`, `DocumentsRemoved`, `DocumentsChanging`, `DocumentsChanged`

### ISearchableRepository&lt;T&gt; : IRepository&lt;T&gt;, ISearchableReadOnlyRepository&lt;T&gt;

| Method | Returns |
| --- | --- |
| `PatchAllAsync(query, operation, options?)` | `Task<long>` (count modified) |
| `RemoveAllAsync(query, options?)` | `Task<long>` (count removed) |
| `BatchProcessAsync(query, processFunc, options?)` | `Task<long>` (count processed) |
| `BatchProcessAsAsync<TResult>(query, processFunc, options?)` | `Task<long>` (projected batches) |

### Model Interfaces

| Interface | Provides | Automatic Behavior |
| --- | --- | --- |
| `IIdentity` | `string Id` | Required for `IRepository<T>` |
| `IHaveCreatedDate` | `DateTime CreatedUtc` | Auto-set on Add |
| `IHaveDates` | `CreatedUtc` + `DateTime UpdatedUtc` | Auto-set on Add, Save, and all Patches |
| `ISupportSoftDeletes` | `bool IsDeleted` | Filtered by default, Remove sets flag |
| `IVersioned` | `string Version` | Optimistic concurrency on Save |

## Gotchas

- **Unmapped fields silently return zero results**: `.Dynamic(false)` is standard. Adding a model property without a corresponding mapping in `ConfigureIndexMapping` means queries on it silently return empty results with no error.
- **`.Index(start, end)` is only for DailyIndex/MonthlyIndex**: It routes to physical daily/monthly shards. On `VersionedIndex` (single index) it is a no-op. Always pair with `.DateRange()`.
- **Painless uses `==` not `===`**: The `===` operator does not exist in Painless. Always use `==` for equality in ScriptPatch scripts.
- **`NextPageAsync()` mutates in-place**: It returns `Task<bool>` and replaces `.Documents`/`.Hits` on the same result object. Do not hold references to the previous page.
- **`PatchAllAsync` notification behavior depends on path**: Cached/batch paths send per-ID notifications. Uncached `ScriptPatch`/`PartialPatch` with a filter-only query sends a single type-level `EntityChanged` with `Id = null`. Design subscribers to be idempotent.
- **Cache invalidation limits on ScriptPatch/PartialPatch**: These execute server-side and only invalidate cache by document ID. Custom `InvalidateCacheAsync` overrides based on document properties will NOT fire. Use `ActionPatch` if you need full document-based cache invalidation.
- **`ctx.op = 'none'` for script noops**: Elasticsearch does not auto-detect noops for script updates. Your script must explicitly set `ctx.op = 'none'` to skip the write.
- **Automatic `UpdatedUtc` on patches**: Models implementing `IHaveDates` get `UpdatedUtc` set automatically on every patch. `PartialPatch` almost always reports `modified = true` even if no other field changed.
- **`ImmediateConsistency()` is for tests only**: It triggers an Elasticsearch index refresh after writes. Never use in production.
- **`ExistsAsync(query)` is a dirty read**: Uses the Search API (`size: 0`), NOT the realtime Document Exists API. After a write without `ImmediateConsistency`, it can return stale results.
- **`ExistsAsync(id)` is real-time even with soft deletes**: Uses the GET API with a source filter for `IsDeleted`.
- **Register repositories as singletons**: Repository instances maintain internal state (index configuration, cache references).
- **`FieldEquals` with multiple values is OR**: `.FieldEquals(e => e.Field, "A", "B")` produces an OR filter, not AND.
- **`FieldContains` is token matching, NOT wildcard**: `FieldContains(f => f.Name, "Er")` will NOT match "Eric". Use `FilterExpression("field:pattern*")` for prefix/wildcard matching.
- **`FieldNot` is AND-NOT**: Multiple conditions inside `FieldNot` mean NOT A AND NOT B. For NOT (A AND B), nest `FieldAnd` inside `FieldNot`.
- **Range operators + time-series indexes**: `FieldLessThanOrEqual(f => f.CreatedUtc, now)` does NOT narrow which daily/monthly indexes are queried. Always pair with `.Index(start, end)`.
- **`FieldEquals` on analyzed text fields throws**: If the field has no `.keyword` sub-field, `FieldEquals` throws `QueryValidationException`. Use `FieldContains` for full-text search.
- **`PatchAsync(Ids, ...)` requires `Ids` type**: Use `new Ids(id1, id2)` -- `string[]` does not implicitly convert to `Ids`.
- **`PatchAllAsync` with filter-only queries sends type-level notifications**: `EntityChanged` message has `Id = null`. Subscribers that need specific document IDs should re-query.
- **Current reindex release gates remain open**: runtime `b458dd81` still needs effective multi-node single-dispatch enforcement, consumed lost-lease handling/stale-controller protection, and safe retired-source fencing. A local `MaxRetries(0)` or a task 404 is not proof of single launch or complete termination. Follow the [current gates](../../../docs/guide/reindex-safety.md#current-release-gates); passing earlier suites and resolved older threads do not establish production approval.
- **An incomplete reindex is not safe to replay blindly**: `ReindexIncompleteException` reports failed copy/accounting/verification or unconfirmed promotion; `ReindexCompletionUnknownException` reports insufficient recovery evidence. Inspect physical aliases and durable task/block state before retrying. `ElasticConfiguration.ReindexAsync` attempts outdated indexes once and aggregates failures, but propagates cancellation; it clears its configuration marker in `finally`. `DailyIndex.ReindexAsync` stops at the first failed partition. See [reindex safety](../../../docs/guide/reindex-safety.md#cancellation-and-outcome-handling).
- **A permitted retry recopies from the beginning**: never derive a resume watermark from destination contents. Replay first requires exact-destination, completion, alias, task, and block checks; never recopy the old source over an already-promoted destination. `StartUtc` is explicit partial-copy input and is rejected with quiescence. See [recovery](../../../docs/guide/reindex-safety.md#recovery-procedure).
- **Counts are not content checksums**: default-mode count mismatch is advisory and retains the source; failed/partial refresh or count evidence cannot authorize cleanup. Quiesced verification requires complete final task accounting, exact expected cardinality, deletion reconciliation, and stable per-primary checkpoints. Scripts, pipelines, and application invariants still require validation.
- **The `-error` failure index is searchable, and `index` is not the source**: Failed documents land in `ElasticReindexer.GetFailureIndexName(destination)` with an explicit `dynamic: strict` mapping (`ReindexFailure`). Query `source_index` to find where to replay from — `index` is the *destination* Elasticsearch reported the failed write against. The document body (`source`) is stored but `enabled: false`, so it round-trips in `_source` but is not queryable. Records are keyed by source document id, so retries overwrite instead of duplicating.
- **Reindex failure detection does not depend on `DisableDirectStreaming()`**: The per-document failure list and completeness counters exist only in the raw task-status body, which the transport drops by default. Reindex asks for that body on just that one request and treats a missing or unparseable one as an abandoned pass rather than a clean one — previously an unreadable body read as "no failures", so a reindex that copied nothing reported success and flipped the alias onto the empty destination. Don't "fix" this by disabling direct streaming globally; it buffers every response.
- **Both queued-handler constructors check exact destinations**: prefer `new ReindexWorkItemHandler(configuration)` for its configured time provider, logging, and lock provider. Both constructors check durable completion and exact physical destination aliases, including dated partitions; neither compares the worker's schema version or aggregate partition version to decide replay. Lock acquisition is bounded; see [queued execution](references/index-lifecycle.md#the-queued-path-behaves-like-the-direct-one).
- **Alias maintenance shares the reindex lock and skips when it's held**: `MaintainAsync` (what `MaintainIndexesJob` calls) takes `reindex:{alias}` around its alias update and returns early if a reindex has it. Without that, `DailyIndex` maintenance acting on a stale snapshot could revert a reindex's cutover, and a partition whose version no longer matches its current version has its aliases *removed* — so it would silently stop being queried. `VersionedIndex` maintenance re-checks the alias **under** the lock, since a cutover removes it from the old version before adding it to the new one and "repairing" that gap would point the alias back at the old version. The acquire timeout is short by design (a reindex holds the lock far longer than any sane wait), and `DeleteOldIndexesAsync` stays outside the lock. Don't call `MaintainAsync` from code already holding that lock.
- **Index locks are only as distributed as your cache**: `ElasticConfiguration` defaults to a `CacheLockProvider` over an in-memory cache when given neither a cache client nor a lock provider, so `reindex:{alias}` serializes only *within one process* — two instances can both reindex and both flip the same alias. Pass a distributed cache (e.g. Redis) before running more than one instance; the constructor logs a warning when it falls back.
- **Unreachable changes prevent default-mode promotion**: use complete source-UUID-bound per-primary checkpoints, not a global maximum. An initially empty source still needs a guard. These checks reject particular unsafe copies but do not establish a write barrier. See [catch-up](references/index-lifecycle.md#second-pass-catch-up).
- **ObjectIds describe creation, not update or deletion**: every page of changed identities must be inspected against its shard baseline; one page or one sampled ID is not proof. Old-ID updates can be refused, but deleted records no longer appear in that search. Use quiescence for mutable data rather than treating the default guard as complete replication.
- **The default ordering is not lossless — three defects, two hitting `IHaveDates`**: the alias switches *before* the catch-up pass, so (A) in-place updates missed by the ObjectId range — now refused; (B) **stale overwrite**: catch-up copies with `Conflicts.Proceed` + internal versioning + no `op_type`, so it unconditionally clobbers a post-cutover write with the older source copy — invisible to `version_conflicts` and to count comparison; (C) **resurrected deletes**: no slicing mode expresses a delete, so a post-cutover delete is re-copied — and the resurrection makes counts *match*. B and C affect **all models including `IHaveDates`**. Quiescing addresses their ordering under exclusive ownership, but does not close the separate dispatch, lease-loss, and retirement release gates.
- **Quiescence starts after the live first pass**: block and drain source writes, perform a full final copy, reconcile routed hard deletes, verify, then promote. Scripted copies empty the migration-exclusive destination before the final copy so no-op/delete remnants cannot survive. Producers must retain/retry blocked operations through aliases; no write-outage duration is guaranteed. See [reindex safety](../../../docs/guide/reindex-safety.md).
- **Promotion never proves completion in either mode**: missing/mismatched durable evidence stays unconfirmed; never recreate a completion record from a new `QuiesceSource` flag. Fingerprints bind script, timestamp field, start range, and quiesce mode; matching also checks destination UUID. Task/block journals protect replay and ownership but are not a general workflow engine or a replacement for continuous distributed coordination. See [completion records](references/index-lifecycle.md#completion-records).
- **Losing the reindex lock race is a skip, not a failure**: the real lock providers *throw* `LockAcquisitionTimeoutException` from the acquire-with-timeout overload rather than returning null, so `TryAcquireReindexLeaseAsync` catches it (and handles a null return, and an elapsed acquire timeout) and returns "nothing to do". This matters because `ElasticConfiguration.ReindexAsync` aggregates failures and throws: left to propagate, two instances starting together would fail startup on whichever one lost a race that the lock exists to arbitrate. Acquisition is also cancellable — otherwise a cancelled reindex would block for the full 30-minute acquire timeout before noticing.
- **Cancellation is not rollback**: before promotion it is observed with independent bounded task/block cleanup. Once default-mode alias promotion starts, required catch-up does not inherit caller cancellation. A cancelled/lost quiesced alias request can have applied already. Callback exceptions can occur after irreversible work, and unknown outcomes require inspection. See [cancellation](../../../docs/guide/reindex-safety.md#cancellation-and-outcome-handling).
- **Patch `DocumentsChanged` event has empty document list**: Only single-document `ActionPatch` populates `args.Documents`. All other patch types have an empty list.
- **Patches do not fire `DocumentsSaving`/`DocumentsSaved`**: Patch operations only fire `DocumentsChanged`.
- **Patches do not detect soft-delete transitions**: Even if a patch sets `IsDeleted = true`, the `ChangeType` is always `Saved`. Soft-delete detection requires `SaveAsync` with `OriginalsEnabled = true`.
- **Large documents can make `ReindexAsync` trip Elasticsearch's indexing pressure limit**: The default reindex batch size (1000 docs) can produce a bulk sub-request bigger than a node's `indexing_pressure.memory.limit` (10% of heap), causing `es_rejected_execution_exception` ("rejected execution of coordinating operation"). Set `ReindexBatchSize`/`ReindexRequestsPerSecond` (must be positive and finite, or `ReindexAsync` throws `ArgumentOutOfRangeException`; a `null` work item throws `ArgumentNullException`) on the index to throttle. Task-status polling backs off exponentially with jitter (1s → 30s cap, +/-25%) on failure. A low `ReindexRequestsPerSecond` also extends the reindex's stall-detection timeout (default 10 minutes) so a healthy but slow, throttled reindex isn't cancelled as falsely "stalled". See [index-lifecycle.md](references/index-lifecycle.md#reindexasync).
