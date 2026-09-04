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
- **v8+ Live `SearchAfterPaging()` requires a sort**: The repository adds the model id's sort-safe mapped field (such as `id.sort`) when possible. If the model has no `IIdentity` or the index sets `HasSortableIdField = false`, Live mode requires an explicit stable, unique sort and throws `QueryValidationException` when none exists. Point-in-time mode makes Elasticsearch's implicit `_shard_doc` tiebreaker explicit after caller sorts, keeping the complete sort tuple reversible for backward cursors.
- **Search-after sessions have strict invariants**: Direction is exclusive, cursor value count must match the final sort count, and snapshot or async queries cannot be mixed with search-after in either mode. Search-after bypasses query-result caching because cache keys do not contain cursor tuples. Raw null/empty/all-null arrays clear the cursor; decoded tokens preserve null sort values. Changing modes clears cursor/PIT/warning state, while reapplying the same mode preserves it; the final mode is evaluated after `BeforeQuery` handlers. `PointInTimeId(...)` always establishes caller ownership. Scalar `FindOneAsync`, `CountAsync`, and all `ExistsAsync` overloads reject PIT search-after options. Close caller-owned PITs when abandoning traversal; repository-owned PITs are closed on completion or failure.
- **Paging resets include the page number**: Disabling search-after or changing Live/PIT mode resets to page one and preserves the page limit. Backward paging must preserve multivalue selection and reverse missing placement as well as direction. Failed PIT searches (including HTTP 404) throw `DocumentException`. After successful failure cleanup, `NextPageAsync()` rejects the cleared session; restart with `FindAsync`. Retained sessions use the latest options PIT ID. Scalar Live cursors bypass caches after `BeforeQuery`. Backward query builds copy every sort variant and preserve settings, including date formats. PIT cleanup tests must assert the latest PIT ID is closed or inspect open search contexts, not scroll counts.
- **Externally-managed daily/monthly indexes are read-only adapters**: Use `ElasticReadOnlyRepositoryBase<T>`. Write methods can call `EnsureIndexAsync()` and create library-owned versioned partitions. Queries without `.Index(...)` and large-range fallbacks require the external system to maintain the `{Name}` umbrella alias. Override `MappingIndexPattern` and `GetIndexDate()` together; the pattern is authoritative and malformed wildcard matches returning `DateTime.MaxValue` are ignored.
- **`PatchAsync(Ids, ...)` requires `Ids` type**: Use `new Ids(id1, id2)` -- `string[]` does not implicitly convert to `Ids`.
- **`PatchAllAsync` with filter-only queries sends type-level notifications**: `EntityChanged` message has `Id = null`. Subscribers that need specific document IDs should re-query.
- **Patch `DocumentsChanged` event has empty document list**: Only single-document `ActionPatch` populates `args.Documents`. All other patch types have an empty list.
- **Patches do not fire `DocumentsSaving`/`DocumentsSaved`**: Patch operations only fire `DocumentsChanged`.
- **Patches do not detect soft-delete transitions**: Even if a patch sets `IsDeleted = true`, the `ChangeType` is always `Saved`. Soft-delete detection requires `SaveAsync` with `OriginalsEnabled = true`.
- **Large documents can make `ReindexAsync` trip Elasticsearch's indexing pressure limit**: The default reindex batch size (1000 docs) can produce a bulk sub-request bigger than a node's `indexing_pressure.memory.limit` (10% of heap), causing `es_rejected_execution_exception` ("rejected execution of coordinating operation"). Set `ReindexBatchSize`/`ReindexRequestsPerSecond` (must be positive and finite, or `ReindexAsync` throws `ArgumentOutOfRangeException`; a `null` work item throws `ArgumentNullException`) on the index to throttle. Task-status polling backs off exponentially with jitter (1s → 30s cap, +/-25%) on failure. A low `ReindexRequestsPerSecond` also extends the reindex's stall-detection timeout (default 10 minutes) so a healthy but slow, throttled reindex isn't cancelled as falsely "stalled". See [index-lifecycle.md](references/index-lifecycle.md#reindexasync).
