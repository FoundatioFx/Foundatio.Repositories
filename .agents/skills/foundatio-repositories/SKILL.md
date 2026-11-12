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
- **`PatchAllAsync` sends per-ID notifications**: Every path sends one `EntityChanged` per modified ID, per batch, except opt-in `o.UseUpdateByQuery()` with a filter-only query, which sends one type-level message with `Id = null`. Design subscribers to be idempotent.
- **Cache invalidation limits on ScriptPatch/PartialPatch**: These execute server-side and only invalidate cache by document ID. Custom `InvalidateCacheAsync` overrides based on document properties will NOT fire. Use `ActionPatch` if you need full document-based cache invalidation.
- **`ctx.op = 'none'` for script noops**: Elasticsearch does not auto-detect noops for script updates. Your script must explicitly set `ctx.op = 'none'` to skip the write.
- **Automatic `UpdatedUtc` on patches**: Models implementing `IHaveDates` get `UpdatedUtc` set automatically on every patch. `PartialPatch` almost always reports `modified = true` even if no other field changed.
- **`ImmediateConsistency()` is for tests only**: It triggers an Elasticsearch index refresh after writes. Never use in production.
- **`ExistsAsync(query)` is a dirty read**: Uses the Search API (`size: 0`), NOT the realtime Document Exists API. After a write without `ImmediateConsistency`, it can return stale results.
- **`ExistsAsync(id)` is real-time even with soft deletes**: Uses the GET API with a source filter for `IsDeleted`.
- **Versioning is checked on `SaveAsync` and `RemoveAsync(T)`**: `Version` is `"primaryTerm:seqNo"` from Elasticsearch metadata. Both send it as `if_seq_no`/`if_primary_term` and throw `VersionConflictDocumentException` on 409, never retried. `RemoveAsync(id/ids)` and `RemoveAllAsync` don't check. Saving an existing `IVersioned` document with an empty `Version` throws `DocumentException` ("has no version") unless `SkipVersionCheck()`. Re-read with `o.Cache(false)` before retrying a conflict. Map `VersionConflictDocumentException` to HTTP 409.
- **Patches re-apply on conflict; use `IfVersion` to fail instead**: For `IVersioned` entities patches never lose a concurrent change (last-write-wins per changed field). For non-`IVersioned` entities, JsonPatch/ActionPatch/pipeline PartialPatch have **no conflict protection**: a concurrent change between the read and the write is silently overwritten (Script/Partial via the Update API still apply server-side). `o.IfVersion(version)` on `PatchAsync(id)` throws `VersionConflictDocumentException` if the document changed since that version; `PatchAsync(ids)`/`PatchAllAsync` reject it with `ArgumentException`.
- **`VersionCheck(false)` == `SkipVersionCheck()`**: Applies to `SaveAsync`, `RemoveAsync(T)`, and JSON/action/pipeline partial patches (single and batch).
- **`Retry(n)` means n retries after the first attempt everywhere** (default 10, `Retry(0)` fails fast): `retry_on_conflict` for Script/Partial patches, re-read-and-re-apply rounds for JSON/action patches (grouped: one mget + one bulk per round), delete-by-query passes, and 429/502/503/504 retries for `AddAsync`/`SaveAsync`/`RemoveAsync`.
- **`Version` is never stored in `_source`**: The ES source serializer drops `IVersioned.Version` from root documents (nested `IVersioned` objects keep theirs) and ignores old stored values. Raw Elasticsearch queries must use `seq_no_primary_term=true`; never query/sort/aggregate on `version`. Custom source serializers must call `options.ExcludeDocumentVersionFromSource()`. Never `[JsonIgnore]` `Version` — the cache needs it. Reindex doesn't preserve _seq_no/_primary_term: a pre-reindex version is NOT guaranteed to 409 (the new index can assign the same pair, and the stale save then overwrites). Always re-read after a reindex.
- **`PatchAllAsync` ScriptPatch uses the bulk Update API**: Update-by-query is opt-in (`o.UseUpdateByQuery()`, cache disabled) and throws `VersionConflictDocumentException` when it skipped documents unless `o.ProceedOnQueryConflicts()`. Make scripts idempotent when an operation can be retried or run in parallel.
- **Batch writes throw after processing successes**: `PatchAllAsync`, `PatchAsync(ids)`, bulk `SaveAsync`/`RemoveAsync` invalidate and notify successful documents, then throw with counts and ids for the rest. Delete-by-query leftovers throw unless `ProceedOnQueryConflicts()`.
- **`IVersioned` on indexes without sequence numbers is unsupported**: ES 9.4+ time series backing indexes (`index.disable_sequence_numbers`) return `-2`/`0` sentinels (read as `null` version) and reject OCC and the Update API; the repository throws `DocumentVersioningNotSupportedException`.- **Register repositories as singletons**: Repository instances maintain internal state (index configuration, cache references).
- **`FieldEquals` with multiple values is OR**: `.FieldEquals(e => e.Field, "A", "B")` produces an OR filter, not AND.
- **`FieldContains` is token matching, NOT wildcard**: `FieldContains(f => f.Name, "Er")` will NOT match "Eric". Use `FilterExpression("field:pattern*")` for prefix/wildcard matching.
- **`FieldNot` is AND-NOT**: Multiple conditions inside `FieldNot` mean NOT A AND NOT B. For NOT (A AND B), nest `FieldAnd` inside `FieldNot`.
- **Range operators + time-series indexes**: `FieldLessThanOrEqual(f => f.CreatedUtc, now)` does NOT narrow which daily/monthly indexes are queried. Always pair with `.Index(start, end)`.
- **`FieldEquals` on analyzed text fields throws**: If the field has no `.keyword` sub-field, `FieldEquals` throws `QueryValidationException`. Use `FieldContains` for full-text search.
- **`PatchAsync(Ids, ...)` requires `Ids` type**: Use `new Ids(id1, id2)` -- `string[]` does not implicitly convert to `Ids`.
- **Patch `DocumentsChanged` event has empty document list**: Only single-document `ActionPatch` populates `args.Documents`. All other patch types have an empty list.
- **Patches do not fire `DocumentsSaving`/`DocumentsSaved`**: Patch operations only fire `DocumentsChanged`.
- **Patches do not detect soft-delete transitions**: Even if a patch sets `IsDeleted = true`, the `ChangeType` is always `Saved`. Soft-delete detection requires `SaveAsync` with `OriginalsEnabled = true`.
- **Large documents can make `ReindexAsync` trip Elasticsearch's indexing pressure limit**: The default reindex batch size (1000 docs) can produce a bulk sub-request bigger than a node's `indexing_pressure.memory.limit` (10% of heap), causing `es_rejected_execution_exception` ("rejected execution of coordinating operation"). Set `ReindexBatchSize`/`ReindexRequestsPerSecond` (must be positive and finite, or `ReindexAsync` throws `ArgumentOutOfRangeException`; a `null` work item throws `ArgumentNullException`) on the index to throttle. Task-status polling backs off exponentially with jitter (1s → 30s cap, +/-25%) on failure. A low `ReindexRequestsPerSecond` also extends the reindex's stall-detection timeout (default 10 minutes) so a healthy but slow, throttled reindex isn't cancelled as falsely "stalled". See [index-lifecycle.md](references/index-lifecycle.md#reindexasync).
