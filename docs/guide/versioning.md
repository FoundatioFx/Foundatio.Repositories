# Versioning

Foundatio.Repositories provides optimistic concurrency control (OCC) for documents that implement `IVersioned`. This guide explains what the version is, which operations check it, how retries work, and the limits Elasticsearch puts on version checks.

::: tip Upgrading
Several behaviors changed in this release. See [Upgrading](#upgrading-breaking-changes) at the end of this page.
:::

## Enabling Versioning

Implement `IVersioned` on your entity:

```csharp
using Foundatio.Repositories.Models;

public class Employee : IIdentity, IHaveDates, IVersioned
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public DateTime CreatedUtc { get; set; }
    public DateTime UpdatedUtc { get; set; }
    public string Version { get; set; } = string.Empty;  // Required by IVersioned
}
```

## What the Version Is

`Version` is an `ElasticDocumentVersion` serialized as `"primaryTerm:sequenceNumber"`, e.g. `"1:42"`. Both numbers come from Elasticsearch **hit metadata**, never from the document body:

| Field | Meaning |
| --- | --- |
| `_seq_no` | Increases with **every operation on the shard**, not per document. Two consecutive writes to the same document can go from `1:42` to `1:57` if other documents on that shard were written in between. |
| `_primary_term` | Increases each time a new primary takes over the shard (failover). |

Versions are ordered by primary term first, then by sequence number. They're only comparable for the **same document in the same index**.

An `ElasticDocumentVersion` is **empty** when the primary term is not positive or the sequence number is negative. That covers a missing version, `"0:0"`, and the `-2`/`0` sentinels returned by indexes with sequence numbers disabled. Empty versions are equal to each other, sort first, and are never sent to Elasticsearch as `if_seq_no`/`if_primary_term`. Reads set `Version` to `null` when the version is empty.

::: tip `_version` is something else
Elasticsearch also has `_version`, a per-document write counter. Since Elasticsearch 7.0 it can't be used for optimistic concurrency, because it isn't safe across primary failover. The supported mechanism is `if_seq_no` + `if_primary_term` ([Elasticsearch docs](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/optimistic-concurrency-control)). That's what this library uses. External versioning (`version_type=external`) isn't used: it's meant for a system where another store owns the version, it can't be combined with `if_seq_no`, and the Update API doesn't support it.
:::

## Read-Modify-Write Cycle

```csharp
// 1. Read document (Version is populated from hit metadata)
var employee = await repository.GetByIdAsync(id);
Console.WriteLine(employee.Version);  // e.g., "1:42"

// 2. Modify document
employee.Name = "John Smith";

// 3. Save document. Elasticsearch receives if_seq_no=42 & if_primary_term=1.
await repository.SaveAsync(employee);
// Success: employee.Version is updated from the write response (e.g., "1:57")
// Mismatch: VersionConflictDocumentException (HTTP 409)
```

```mermaid
sequenceDiagram
    participant A as Process A
    participant B as Process B
    participant ES as Elasticsearch

    A->>ES: GetByIdAsync (version 1:42)
    B->>ES: GetByIdAsync (version 1:42)
    A->>ES: SaveAsync (if_seq_no=42)
    ES-->>A: Success (new version 1:57)
    B->>ES: SaveAsync (if_seq_no=42)
    ES-->>B: 409 -> VersionConflictDocumentException
```

Saving an existing `IVersioned` document whose `Version` is empty throws a `DocumentException` ("has no version") instead of silently overwriting the stored document. Load the document first, or use `SkipVersionCheck()` when an overwrite is intended.

## Where the Version Comes From

The version is metadata. It is **not stored in `_source`**: the Elasticsearch source serializer leaves `IVersioned.Version` out of the root document, and ignores a `version` value already stored there by older releases.

| Read | How `Version` is set |
| --- | --- |
| `GetByIdAsync` / `GetByIdsAsync` | From GET/mget metadata. `null` when missing. |
| `FindAsync` / `FindOneAsync` / `SearchAsync` | From hit metadata (`seq_no_primary_term=true` is requested automatically). |
| Cache hit | From the cached `FindHit.Version`, which was taken from metadata when the entry was cached. |
| `FindAsAsync<TResult>` | A `TResult` that implements `IVersioned` gets `Version` from hit metadata. Otherwise only `hit.Version` carries it. |
| `top_hits` aggregations | Built from `_source` only, so they carry no version. |

`hit.Version` (on `FindHit<T>`) and `hit.Document.Version` always agree for repository reads.

- Only the **root** document type registered with a repository or index is affected. A nested object that implements `IVersioned` (for example a metadata object with its own business version) keeps its `version` field. Don't reuse `IVersioned` for business counters on the root document.
- A proxy property such as `public string Version { get => MetaData.Version; set => MetaData.Version = value; }` still writes `metaData.version`, because the nested object is serialized normally.
- If you replace the Elasticsearch source serializer, call `options.ExcludeDocumentVersionFromSource()` on its `JsonSerializerOptions`, or override `ElasticConfiguration.ConfigureSourceSerializerOptions` and call the base method. Never add `[JsonIgnore]` to `Version`: the cache serializer needs it.
- Documents written by older releases keep a stale `version` field in `_source` until their next full write (`SaveAsync`, JSON/action patches). Script and partial patches merge into the stored document and leave the old field in place. It is ignored on reads. To remove it, run an update-by-query with `ctx._source.remove('version')`.

### Debugging versions

Query metadata directly instead of `_source`:

```http
GET employees/_doc/123
GET employees/_search?seq_no_primary_term=true
{ "query": { "ids": { "values": ["123"] } } }
```

In Kibana Discover, add the `_seq_no` and `_primary_term` columns. Range queries and sorts on `_seq_no` work on indexes with sequence numbers enabled.

### Versions after reindex

Reindexing (schema migrations via `VersionedIndex`, `ReindexAsync`, moving documents between daily or monthly indexes) writes documents into a new index with fresh sequence numbers. Elasticsearch does **not** preserve `_seq_no`/`_primary_term`. A version captured before the reindex is **not guaranteed** to cause a 409 on the next save: it's rejected only if the new index happens to assign a different pair. The new index can assign the same pair (sequence numbers restart in the new index, so a document updated only a few times before the reindex can easily land on the same numbers), and then the stale copy passes the check and overwrites the migrated document. Always re-read documents after a reindex instead of reusing versions captured before it.

### Caching and versions

Cached reads return the version from when the entry was cached. If the document changed since, saving the cached copy fails with `VersionConflictDocumentException`, which is the safe outcome. Re-read with `o.Cache(false)` and retry. Script and partial patches only invalidate the cache, since the new version isn't known client-side. The cache population race (stale data cached after invalidation) is tracked in [#323](https://github.com/FoundatioFx/Foundatio.Repositories/issues/323).

## Concurrency Model per Operation

"Checks your `Version`" means the write is rejected when the stored document changed after the version you hold. "Never loses a concurrent change" means a write that loses a race is re-applied on the latest document instead of overwriting it.

::: warning Entities that don't implement `IVersioned`
The version checks below only exist for `IVersioned` entities. For any other entity, `SaveAsync` and the read-modify-write patches (JSON, action and pipeline partial patches, single or batch) write without a check, so a change made by someone else between the read and the write is **silently overwritten**. Script and partial patches through the Update API are still applied server-side to the latest document.
:::

| Operation | Mechanism | Checks your `Version`? | Retries (`Retry(n)`, default 10) |
| --- | --- | --- | --- |
| `AddAsync` | `op_type=create` | No (duplicate id: `DuplicateDocumentException`) | 429/502/503/504 |
| `SaveAsync` | index with `if_seq_no`/`if_primary_term` | **Yes**, unless `SkipVersionCheck()` | 429/502/503/504 only. 409 is never retried |
| `RemoveAsync(T)` | delete with `if_seq_no`/`if_primary_term` | **Yes**, unless `SkipVersionCheck()` | 429/502/503/504 only |
| `RemoveAsync(ids)` | loads documents, deletes without a check | No | 429/502/503/504 |
| `PatchAsync(id)` ScriptPatch / PartialPatch | Update API with `retry_on_conflict` | Only with `IfVersion(v)` (then no `retry_on_conflict`) | Server-side conflict retries + transient retries |
| `PatchAsync(id)` JsonPatch / ActionPatch / pipeline PartialPatch | GET, apply, index with `if_seq_no` from the GET | Only with `IfVersion(v)` (fails instead of retrying) | Re-read and re-apply on conflict |
| `PatchAsync(id)` ScriptPatch with a default pipeline | update-by-query on the id | No (`IfVersion` not supported) | Re-run on conflict |
| `PatchAsync(ids)` / `PatchAllAsync` JsonPatch / ActionPatch / pipeline PartialPatch | bulk index with `if_seq_no` from the read, grouped retry rounds | No; `SkipVersionCheck()` writes without a check | Grouped rounds: one mget + one bulk per round |
| `PatchAsync(ids)` / `PatchAllAsync` ScriptPatch / PartialPatch | bulk Update API with `retry_on_conflict` | No | Server-side conflict retries + transient rounds |
| `PatchAllAsync` ScriptPatch with `UseUpdateByQuery()` (cache disabled) | update-by-query, `conflicts=proceed` | Snapshot only | None. Throws on conflicts unless `ProceedOnQueryConflicts()` |
| `RemoveAllAsync` (no listeners, no cache) | delete-by-query, `conflicts=proceed` | Snapshot only | Up to n more passes with backoff. Throws on leftovers unless `ProceedOnQueryConflicts()` |
| `RemoveAllAsync` (listeners or cache) | batched `RemoveAsync` without a check | No | 429/502/503/504 |

For `IVersioned` entities, patches never lose a concurrent change: a patch that loses a race is re-applied to the latest document. That makes patches last-write-wins **per field you change**, not for your whole copy of the document. Use `IfVersion` when the patch must only apply to the version you read. Script and partial patches through the Update API behave this way for every entity; JSON, action and pipeline partial patches on entities that don't implement `IVersioned` have **no conflict protection** and overwrite concurrent changes.

### Expected-version patches (`IfVersion`)

```csharp
var employee = await repository.GetByIdAsync(id);
// ... show the document to a user, who edits one field ...
await repository.PatchAsync(id, new PartialPatch(new { Name = "New" }), o => o.IfVersion(employee.Version));
// Throws VersionConflictDocumentException if anyone changed the document since it was read.
```

- Script and partial patches send `if_seq_no`/`if_primary_term` to the Update API. Elasticsearch rejects them together with `retry_on_conflict`, so there are no server-side conflict retries.
- JSON and action patches compare the version from their GET with the expected one before writing.
- `IfVersion` is only supported by `PatchAsync(id)`. `PatchAsync(ids)` and `PatchAllAsync` throw `ArgumentException`.

### Grouped conflict retries

For JSON, action and pipeline partial patches on many documents, a bulk write can return 409 (changed since read) or 429/503 for some items. Those documents are re-read **together** with one multi-get, the patch is re-applied in memory (an action patch that no longer changes the document is not written), and they are re-sent in one bulk request with their fresh versions. This repeats for up to `Retry(n)` rounds with exponential backoff (50 ms doubling, capped at 2 s) and jitter.

- Documents deleted in the meantime are skipped.
- Cache invalidation, notifications and the `UpdateCallback` fire once per document, in the round it succeeded.
- Documents still failing after the last round are reported in a `VersionConflictDocumentException` (or `DocumentException` for other failures) with counts and up to 10 ids, **after** the successes are invalidated and notified.

Compared with re-patching each conflicting id on its own, a grouped round is roughly 8x faster for 10 documents and 50-100x faster for 100-500 documents (see `benchmarks/`).

### Query-based writes check against a snapshot

Update-by-query and delete-by-query take a snapshot of matching documents, then write each one with a sequence-number check against **that snapshot**. A concurrent change is never overwritten, but the document is skipped and counted as a version conflict. Elasticsearch scripts can't read `_seq_no`, so a script can't compare against a caller-supplied version, and update-by-query doesn't return which ids conflicted.

- `PatchAllAsync` with `ScriptPatch` therefore uses the bulk Update API by default, which re-runs the script on the latest document per id. Update-by-query is an explicit opt-in (`o.UseUpdateByQuery()`, ignored when caching is enabled) for large data sets; it throws when any document was skipped unless you set `o.ProceedOnQueryConflicts()`.
- Re-running a whole query to pick up skipped documents is unsafe for non-idempotent scripts (`ctx._source.count++`), because documents that already succeeded get the script again. Prefer idempotent scripts (`ctx._source.count = params.count`) for any operation that may be retried or run in parallel.
- Delete-by-query is idempotent, so `RemoveAllAsync` re-runs it up to `Retry(n)` more times with backoff and a refresh until no conflicts remain.
- Running two query-based writes in parallel on overlapping documents is safe but each may skip documents the other changed. Documents that start matching after the operation begins are not included.

## Retry Options

`o.Retry(n)` means **n retries after the first attempt** on every path. The default is 10. `Retry(0)` makes a single attempt and fails fast.

| Path | What a retry is |
| --- | --- |
| Script / partial patches (Update API) | `retry_on_conflict = n` (server-side), plus up to n rounds for 429/502/503/504 |
| JSON / action / pipeline partial patches | Re-read and re-apply, up to n times (grouped per round for batches) |
| `AddAsync` / `SaveAsync` / `RemoveAsync` | Up to n retries of transient failures (429/502/503/504, transport errors). Version conflicts are not retried |
| Delete-by-query | Up to n more passes while conflicts remain |

Retries use exponential backoff starting at 50 ms, capped at 2 seconds, with jitter.

## Handling Version Conflicts

```csharp
try
{
    await repository.SaveAsync(employee);
}
catch (VersionConflictDocumentException)
{
    // Someone else changed the document. Re-read and decide what to do.
}
```

Map `VersionConflictDocumentException` to HTTP 409 in APIs. It derives from `DocumentException`, so a generic handler returns 500 unless you catch it first.

### Retry pattern

Re-read **without the cache** on each attempt, so you don't keep retrying with the same stale version:

```csharp
public async Task<Employee> UpdateWithRetryAsync(string id, Action<Employee> update, int maxAttempts = 5)
{
    for (int attempt = 1; ; attempt++)
    {
        var employee = await repository.GetByIdAsync(id, o => o.Cache(false))
            ?? throw new DocumentNotFoundException(id);

        update(employee);

        try
        {
            return await repository.SaveAsync(employee);
        }
        catch (VersionConflictDocumentException) when (attempt < maxAttempts)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(50 * Math.Pow(2, attempt) + Random.Shared.Next(25)));
        }
    }
}
```

When the change doesn't depend on values you read, a patch does this for you:

```csharp
await repository.PatchAsync(id, new ActionPatch<Employee>(e => e.Name = "John Smith"));
await repository.PatchAsync(id, new ScriptPatch("ctx._source.counter++"));
```

## Skipping the Version Check

```csharp
await repository.SaveAsync(employee, o => o.SkipVersionCheck());
await repository.SaveAsync(employee, o => o.VersionCheck(false)); // same thing
```

`SkipVersionCheck()` (or `VersionCheck(false)`) applies to `SaveAsync`, `RemoveAsync(T)`, and the JSON, action and pipeline partial patches on one or many documents. Skip the check only when lost updates are acceptable: trusted imports, migrations, or genuine last-write-wins data.

## Bulk Operations and Partial Failures

When saving several versioned documents in one call, some may succeed while others conflict:

1. **Successful documents** are fully processed: versions updated, events fired, cache populated, notifications sent.
2. **Conflicting documents** are left untouched in Elasticsearch and in the cache.
3. A `VersionConflictDocumentException` is thrown after the successes are processed. The message includes the conflict count and ids.

## Indexes Without Sequence Numbers (Elasticsearch 9.4+)

Elasticsearch 9.4 disables sequence numbers by default (`index.disable_sequence_numbers=true`, a final setting) for new **time_series data stream backing indexes**. Indexes can also be created with the setting. On those indexes:

- Responses carry the sentinel values `_seq_no: -2, _primary_term: 0`. The repository treats them as an empty version, so `Version` is `null`, and never sends them back as `if_seq_no`.
- Writes with `if_seq_no` fail with HTTP 400 (`o_c_c_not_supported_exception`) and the Update API with HTTP 400 (`update_not_supported_exception`). Both are reported as `DocumentVersioningNotSupportedException` with an explanation.
- Saving an `IVersioned` document with no version throws (see above) instead of silently overwriting.
- Update-by-query and delete-by-query run with no conflict detection.

**`IVersioned` repositories aren't supported on such indexes.** Either don't implement `IVersioned` for time-series data, or set `index.disable_sequence_numbers: false` in the index template. `SkipVersionCheck()` saves and JSON/action patches still work. Standard and logsdb indexes keep sequence numbers, and may use `index.seq_no.index_options=doc_values_only` (9.1+), where OCC still works but `_seq_no` range queries are slower.

## Logging

Conflicts and retries are logged with structured fields and never include document bodies:

- **Warning** per conflict: operation, index, document id, attempt and budget, expected version, current version (parsed from the 409 reason) and outcome (`Retrying`/`Failed`).
- **Warning** per transient retry and per delete-by-query pass with conflicts.
- **Information** summary per grouped retry round.
- **Error** when a retry budget is exhausted.

## Upgrading (Breaking Changes)

- **`RemoveAsync(T)` checks the version.** Removing an `IVersioned` document whose copy is stale now throws `VersionConflictDocumentException`. Use `SkipVersionCheck()` to delete regardless, or `RemoveAsync(id)`, which loads the document and deletes it without a check.
- **`VersionCheck(bool)` is fixed.** `VersionCheck(false)` now skips the check and `VersionCheck(true)` checks; they were inverted. Code that called `VersionCheck(true)` to skip the check must change to `SkipVersionCheck()`.
- **`PatchAllAsync` with `ScriptPatch` uses the bulk Update API by default** instead of update-by-query when caching is disabled. Results are the same, but every matching document is patched even under concurrent writes. Opt back in with `o.UseUpdateByQuery()`; it now throws `VersionConflictDocumentException` when documents were skipped unless `o.ProceedOnQueryConflicts()` is set.
- **`RemoveAllAsync` delete-by-query throws on leftover conflicts** after the retry budget, instead of only logging. Use `o.ProceedOnQueryConflicts()` to restore logging only.
- **`Version` is no longer stored in `_source`.** The `"primaryTerm:sequenceNumber"` format and the property itself are unchanged. Raw Elasticsearch clients must request `seq_no_primary_term=true` instead of reading `version` from `_source`. Queries, sorts or aggregations on `version` will stop matching new documents.
- **`Retry(n)` means n retries everywhere.** Patch paths that used the resilience policy made n attempts before; they now make n + 1. Single-document `AddAsync`/`SaveAsync`/`RemoveAsync` now retry 429/502/503/504.
- **Empty versions.** Reads return `null` instead of `"0:0"` when there is no version, two empty `ElasticDocumentVersion` values are now equal, and saving an existing `IVersioned` document with no version throws unless the check is skipped.
- **Failures are no longer silent.** `PatchAllAsync` used to stop and return without an exception when a bulk update item failed; it now processes every batch and throws at the end.

## Best Practices

1. Handle `VersionConflictDocumentException` wherever you call `SaveAsync`, `RemoveAsync(T)` or `IfVersion` patches on an `IVersioned` type. Map it to HTTP 409 in APIs.
2. Retry by **re-reading** (bypass the cache) and re-applying the change. Use exponential backoff with jitter and a small attempt budget.
3. Prefer patches for changes that don't depend on a value you read, and `IfVersion` patches when they do.
4. Make scripts idempotent when an operation may be retried or run in parallel.
5. Treat `Version` as an opaque string. Compare versions with `ElasticDocumentVersion` only for the same document in the same index.

## Next Steps

- [CRUD Operations](/guide/crud-operations) - Save operations with versioning
- [Patch Operations](/guide/patch-operations) - Patch concurrency behavior
- [Caching](/guide/caching) - Cache interaction
- [Troubleshooting](/guide/troubleshooting) - Diagnosing conflicts
