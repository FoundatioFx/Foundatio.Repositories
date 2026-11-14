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
    public string? Version { get; set; }  // Set by the repository; null until the document is stored
}
```

## What the Version Is

`Version` is an `ElasticDocumentVersion` serialized as `"primaryTerm:sequenceNumber"`, e.g. `"1:42"`. Both numbers come from Elasticsearch **hit metadata**, never from the document body:

| Field | Meaning |
| --- | --- |
| `_seq_no` | Increases with **every operation on the shard**, not per document. Two consecutive writes to the same document can go from `1:42` to `1:57` if other documents on that shard were written in between. |
| `_primary_term` | Increases each time a new primary takes over the shard (failover). |

Versions are ordered by primary term first, then by sequence number. They're only comparable for the **same document in the same index**.

An `ElasticDocumentVersion` is **empty** when the primary term is not positive or the sequence number is negative. That covers a missing version, `"0:0"`, and the `-2`/`0` sentinels returned by indexes with sequence numbers disabled. Empty versions are equal to each other, sort first, and are never sent to Elasticsearch as `if_seq_no`/`if_primary_term`. `ToVersionString()` and the implicit `string` conversion both return `null` for an empty version (only `ToString()` returns `"0:0"`), and reads set `Version` to `null` when there is no version.

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

### Documents without a version

An `IVersioned` document whose `Version` is empty has never been read from Elasticsearch, so it can't be checked against a stored copy:

- **`SaveAsync`** writes it with `op_type=create`. If a document with that id already exists, the save fails with `DuplicateDocumentException` (the same exception `AddAsync` throws), and the stored document is left unchanged. This applies to single and bulk saves.
- **`RemoveAsync(T)`** throws a `DocumentException` ("has no version") before anything is deleted, because a delete has no create fallback.
- **`SkipVersionCheck()`** makes both unconditional: the save indexes (overwrites) the document and the remove deletes it.

## Where the Version Comes From

The concurrency token comes from Elasticsearch's `_primary_term` and `_seq_no` metadata. The model and its configured serializer control the JSON document, just as they do for other properties. An existing public `Version` property continues to serialize normally; its stored value is not authoritative because it predates the write response.

| Read | How `IVersioned.Version` is set |
| --- | --- |
| `GetByIdAsync` / `GetByIdsAsync` | From GET/mget metadata. `null` when missing. A multi-get item error (for example an unavailable shard) throws `DocumentException`; it is never treated as "not found". |
| `FindAsync` / `FindOneAsync` / `SearchAsync` | From hit metadata (`seq_no_primary_term=true` is requested automatically). |
| Cache hit | From the cached `FindHit.Version` envelope, including clearing a stale model token when that envelope has no version. |
| `FindAsAsync<TResult>` | A versioned `TResult` requests metadata even when the repository's document type is nonversioned. For other result types, any requested metadata remains available on `hit.Version`. |
| `top_hits` aggregations | Deserialized from `_source` without repository metadata synchronization. A source property named `Version` may be stale; read the document through the repository before saving it. |

For ordinary repository reads, `hit.Version` and `((IVersioned)hit.Document).Version` agree. The envelope preserves the token even when the model omits it from JSON.

### Choosing the model's JSON contract

Use an explicit interface implementation when the token should be accessible through `IVersioned` but absent from the model's public JSON contract:

```csharp
public sealed class Document : IIdentity, IVersioned
{
    public string Id { get; set; } = null!;
    public string? Name { get; set; }

    string? IVersioned.Version { get; set; }
}
```

An explicit concurrency property can coexist with an ordinary public business property named `Version`. The repository reads and writes the interface property; normal serialization and patches use the business property. System.Text.Json omits an unannotated explicit implementation when serializing the concrete model. Serializing an object as the interface itself, or supplying a custom serializer, uses that selected contract.

Alternatively, keep direct property access and opt out of JSON with System.Text.Json's `[JsonIgnore]`:

```csharp
[JsonIgnore]
public string? Version { get; set; }
```

Both choices work with repository caching because the token is stored separately in `FindHit.Version` and restored when a cached document is returned. Neither change is required for existing public properties. A model-only JSON round trip outside the repository does not preserve an omitted token.

The repository does not rewrite source serializer contracts or require document-type registration. Nested properties, extension data, number handling, object population, reference handling and source-generated metadata follow the configured serializer's normal rules. Custom serializers need no version-exclusion adapter.

JSON and pipeline partial patches deserialize through the configured source serializer. After a client-side patch, the repository restores the token captured with the read before performing the conditional write. Changing a JSON property cannot replace the expected concurrency token. Use `IfVersion` to require a specific version; an independent business `Version` remains ordinary patchable data.

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
The version checks below only exist for `IVersioned` entities, except `IfVersion`, which works for any entity. For any other entity, `SaveAsync` and the read-modify-write patches (JSON, action and pipeline partial patches, single or batch) write without a check, so a change made by someone else between the read and the write is **silently overwritten**. Script and partial patches through the Update API are still applied server-side to the latest document.
:::

| Operation | Mechanism | Checks your `Version`? | Retries (`Retry(n)`, default 10) |
| --- | --- | --- | --- |
| `AddAsync` | `op_type=create` | No (duplicate id: `DuplicateDocumentException`) | 429/503 and unsent requests |
| `SaveAsync` | index with `if_seq_no`/`if_primary_term`; `op_type=create` when `Version` is empty | **Yes**, unless `SkipVersionCheck()` | 429/503 and unsent requests. 409 is never retried |
| `RemoveAsync(T)` | delete with `if_seq_no`/`if_primary_term` | **Yes**, unless `SkipVersionCheck()`. Empty `Version` throws | 429/503 and unsent requests |
| `RemoveAsync(ids)` | loads documents, deletes without a check | No | 429/503 and unsent requests |
| `PatchAsync(id)` ScriptPatch / PartialPatch | Update API with `retry_on_conflict` | Only with `IfVersion(v)` (then no `retry_on_conflict`) | Server-side conflict retries + transient retries |
| `PatchAsync(id)` JsonPatch / ActionPatch / pipeline PartialPatch | GET, apply, index with `if_seq_no` from the GET | Only with `IfVersion(v)` (fails instead of re-reading) | Re-read and re-apply on conflict |
| `PatchAsync(id)` ScriptPatch with a default pipeline | update-by-query on the id | No (`IfVersion` not supported) | Re-run on conflict |
| `PatchAsync(ids)` / `PatchAllAsync` JsonPatch / ActionPatch / pipeline PartialPatch | bulk index with `if_seq_no` from the read, grouped retry rounds | No; `SkipVersionCheck()` writes without a check | Grouped rounds: one mget + one bulk per round |
| `PatchAsync(ids)` / `PatchAllAsync` ScriptPatch / PartialPatch | bulk Update API with `retry_on_conflict` | No | Server-side conflict retries + transient rounds |
| `PatchAsync(ids)` / `PatchAllAsync` ScriptPatch with a default pipeline | update-by-query **per document id** (the Update API can't run pipelines) | No | Re-run on conflict. One request per document; none refreshes. The indexes that were written are refreshed once after the last document (also when a document failed), unless the consistency is eventual |
| `PatchAllAsync` ScriptPatch with `UseUpdateByQuery()` (cache disabled) | one update-by-query task, `conflicts=proceed` | Snapshot only | Transient submission failures only; a started task is never resubmitted. Throws on conflicts unless `ProceedOnQueryConflicts()` |
| `RemoveAllAsync` (no listeners, no cache) | delete-by-query, `conflicts=proceed` | Snapshot only | Up to n more passes with backoff. Throws on leftovers unless `ProceedOnQueryConflicts()` |
| `RemoveAllAsync` (listeners or cache) | batched `RemoveAsync` without a check | No | 429/503 and unsent requests |

In this table, retryable 503 responses contain an Elasticsearch error type or come from an Elasticsearch bulk item. An unstructured gateway 503 has an ambiguous outcome and is reported without replaying the write.

For `IVersioned` entities, patches never lose a concurrent change: a patch that loses a race is re-applied to the latest document. That makes patches last-write-wins **per field you change**, not for your whole copy of the document. Use `IfVersion` when the patch must only apply to the version you read. Script and partial patches through the Update API behave this way for every entity; JSON, action and pipeline partial patches on entities that don't implement `IVersioned` have **no conflict protection** and overwrite concurrent changes.

### Expected-version patches (`IfVersion`)

```csharp
var employee = await repository.GetByIdAsync(id);
// ... show the document to a user, who edits one field ...
await repository.PatchAsync(id, new PartialPatch(new { Name = "New" }), o => o.IfVersion(employee.Version));
// Throws VersionConflictDocumentException if anyone changed the document since it was read.
```

- Every write sends the expected version as `if_seq_no`/`if_primary_term`, including writes re-sent after a transient failure. The document is never re-read to pick up a newer version, so a concurrent change always fails the patch.
- Script and partial patches send it to the Update API. Elasticsearch rejects it together with `retry_on_conflict`, so there are no server-side conflict retries.
- JSON and action patches read the document, compare its version with the expected one, and write with the expected version.
- `IfVersion` works for entities that don't implement `IVersioned` too: the check is enforced by Elasticsearch, not by the entity.
- `IfVersion` can't be combined with `SkipVersionCheck()` (`ArgumentException`), and isn't supported for script patches with a default pipeline.
- `IfVersion` is only supported by `PatchAsync(id)`. `PatchAsync(ids)` (with any number of ids, including one) and `PatchAllAsync` throw `ArgumentException`, because one expected version can't apply to several documents. Patch each document by id instead.
- `AddAsync`, `SaveAsync`, `RemoveAsync` and `RemoveAllAsync` also throw `ArgumentException` when `IfVersion` is set, before anything is written: a save or remove already checks the `Version` of the document you pass in, so a second expected version would be silently ignored.

### Grouped conflict retries

For JSON, action and pipeline partial patches on many documents, a bulk write can return 409 (changed since read) or 429/503 for some items. Conflicting documents are re-read **together** with one multi-get, the patch is re-applied in memory (an action patch that no longer changes the document is not written), and they are re-sent in one bulk request with their fresh versions. Transient failures are re-sent unchanged. This repeats for up to `Retry(n)` rounds with exponential backoff (50 ms doubling, capped at 2 s) and jitter.

The initial read for a multi-ID pipeline `PartialPatch` also uses real-time multi-get when the index and required routing are known. Only unresolved locations use search discovery; a mixed batch preserves real-time visibility for its resolvable IDs. The single-ID overload still requires a resolvable location. JSON/action patches across multiple indexes and `PatchAllAsync` retain search visibility. See [Patch by IDs](/guide/patch-operations#patch-by-ids).

- Documents deleted in the meantime are skipped.
- Cache invalidation, notifications and the `UpdateCallback` fire once per document, after its batch completes.
- Documents still failing after the last round are reported in one exception with counts and up to 10 ids, **after** the successes are invalidated and notified (see [Failure handling](#failure-handling)).

`benchmarks/Foundatio.Repositories.Benchmarks` measures this through `PatchAllAsync` on a repository, with every document changed by a concurrent writer right before the first bulk write. The per-id baseline is the previous behavior: write the batch once, then patch each conflicting document again with `PatchAsync(id)`. Short job, Elasticsearch 9.5 on a laptop:

| Conflicting documents | Per-id | Grouped | Allocated (per-id / grouped) |
| --- | --- | --- | --- |
| 10 | 49 ms | 80 ms | 501 KB / 233 KB |
| 100 | 276 ms | 76 ms | 4.2 MB / 1.4 MB |
| 500 | 2,569 ms | 103 ms | 20.8 MB / 6.3 MB |

For a handful of documents the grouped round is slower, because it waits for the retry backoff before re-reading; from about 30 conflicting documents on, one multi-get and one bulk request per round win by a wide margin.

### Query-based writes check against a snapshot

Update-by-query and delete-by-query take a snapshot of matching documents, then write each one with a sequence-number check against **that snapshot**. A concurrent change is never overwritten, but the document is skipped and counted as a version conflict. Elasticsearch scripts can't read `_seq_no`, so a script can't compare against a caller-supplied version, and update-by-query doesn't return which ids conflicted.

- `PatchAllAsync` with `ScriptPatch` therefore uses the bulk Update API by default, which re-runs the script on the latest document per id. Update-by-query is an explicit opt-in (`o.UseUpdateByQuery()`, ignored when caching is enabled) for large data sets; it throws when any document was skipped unless you set `o.ProceedOnQueryConflicts()`. If its task can't be found while the repository waits for it, a `DocumentException` is thrown: the number of patched documents is unknown, so it is never reported as zero.
- Re-running a whole query to pick up skipped documents is unsafe for non-idempotent scripts (`ctx._source.count++`), because documents that already succeeded get the script again. Prefer idempotent scripts (`ctx._source.count = params.count`) for any operation that may be retried or run in parallel.
- Delete-by-query is idempotent, so `RemoveAllAsync` re-runs it up to `Retry(n)` more times with backoff and a refresh until no conflicts remain.
- Running two query-based writes in parallel on overlapping documents is safe but each may skip documents the other changed. Documents that start matching after the operation begins are not included.

## Write State Machines

Every write runs through the same round runner. Each document (or, for delete-by-query, the whole query) is an item, identified by its index **and** id, so the same id in two indexes is two items.

```mermaid
stateDiagram-v2
    [*] --> Pending
    Pending --> Succeeded: written
    Pending --> Noop: nothing to change
    Pending --> Missing: 404, or 409 "no document was found"
    Pending --> VersioningNotSupported: sequence numbers disabled
    Pending --> Fatal: any other failure
    Pending --> Transient: 429, Elasticsearch 503 error, request not sent
    Pending --> Conflict: 409
    Transient --> Pending: budget left (re-send unchanged)
    Transient --> Cancelled: repository disposed during backoff
    Conflict --> Pending: budget left and re-read allowed (fresh version)
    Conflict --> Missing: re-read finds the document gone
    Conflict --> Fatal: re-read or re-applied patch fails
    Conflict --> Noop: re-applied patch no longer changes the document
    Pending --> Cancelled: repository disposed
    Transient --> [*]: budget used
    Conflict --> [*]: budget used, or re-read not allowed
    Succeeded --> [*]
    Noop --> [*]
    Missing --> [*]
    VersioningNotSupported --> [*]
    Fatal --> [*]
    Cancelled --> [*]
```

- **Rounds and budget.** A round sends every pending item once. `Retry(n)` allows n more rounds (n + 1 total), separated by exponential backoff (50 ms doubling, capped at 2 s) with ±25% jitter. Items still `Transient` or `Conflict` after the last round keep that as their outcome.
- **Who re-reads a conflict.** Client-side patches re-read conflicting documents with one multi-get and re-apply the patch (unless `IfVersion` is set). Pipeline script patches re-run update-by-query on the id. Delete-by-query refreshes and runs the query again. Saves, removes and Update API patches never re-read: a 409 is terminal (Update API patches have already used `retry_on_conflict`).
- **Re-read results.** A document that's gone ends `Missing`; a multi-get item error, a patch that throws, or a failed multi-get ends `Fatal`; an action patch that no longer changes the re-read document ends `Noop`. A transient multi-get failure re-sends the stale items, which conflict again and are re-read in the next round.
- **No item is dropped.** When the repository is disposed, every pending item ends `Cancelled` (including items waiting out a backoff after a `Transient` round); when sending fails with an exception, every pending item ends `Fatal` with it.

### Transient vs. reported failures

Automatic retries are limited to rejection responses and transport evidence that the write was not sent:

| Failure | Outcome |
| --- | --- |
| HTTP 429 (Too Many Requests), HTTP 503 with a parsed Elasticsearch error type, or an Elasticsearch bulk item reporting 503 | Retried |
| Connection refused, DNS failure, TLS handshake failure, no node attempted (the request was never sent) | Retried |
| HTTP 502, HTTP 504, or HTTP 503 without an Elasticsearch error type (a proxy or gateway may already have forwarded the write) | Reported |
| Timeouts, connection reset, or any failure without a status after the request was sent | Reported |
| HTTP 409 | Conflict (see above), never retried by the transport |

A reported failure may or may not have been applied. Re-read the document before retrying it yourself.

### Failure handling

After the runner finishes, the repository invalidates the cache, raises events and sends notifications for every item that succeeded, and only then throws **one** exception for the items that didn't. The exception is chosen with a single precedence:

1. `OperationCanceledException` when any item was cancelled.
2. `DocumentVersioningNotSupportedException` when the index has sequence numbers disabled.
3. `DocumentException` ("index ... does not exist") when a version-checked save or add hit an index that doesn't exist, for example one deleted after the repository created it.
4. `DuplicateDocumentException` when every conflict was a create (`AddAsync`, or `SaveAsync` without a version); otherwise `VersionConflictDocumentException`. For saves, a document deleted since it was read also counts as a conflict.
5. `DocumentNotFoundException` for patches by id whose document doesn't exist.
6. `DocumentException` for everything else, with the first error reason.

Messages include the number of failed and succeeded documents and up to 10 ids. A document that is already gone is not an error for `RemoveAsync` (it is evicted from the cache, but `DocumentsRemoved`, `DocumentsChanged` and `EntityChanged` are raised only for documents the call actually deleted), and is skipped by `PatchAllAsync`.

## Retry Options

`o.Retry(n)` means **n retries after the first attempt** on every path. The default is 10. `Retry(0)` makes a single attempt and fails fast.

| Path | What a retry is |
| --- | --- |
| Script / partial patches (Update API) | `retry_on_conflict = n` (server-side), plus up to n rounds for transient failures |
| JSON / action / pipeline partial patches | Re-read and re-apply, up to n rounds (grouped per round for batches), plus transient failures |
| Pipeline script patches | Re-run update-by-query on the id, up to n rounds |
| `AddAsync` / `SaveAsync` / `RemoveAsync` | Up to n retries of transient failures. Version conflicts are not retried |
| Delete-by-query | Up to n more passes while conflicts remain |

Writes use these retries instead of the repository's `IResiliencePolicy`, because a policy can't tell a write that was never sent from one that may already be applied. Configuring a custom policy is still honored for reads and other operations; the repository logs a warning that it isn't used for document writes.

Writes pass through the configured Elasticsearch transport with a bound configuration that permits one document request per repository attempt in the standard transport. Connection resources, node health, authentication, serializers, request callbacks and transparent transport decorators remain shared, while reads retain their configured retries. This prevents standard transport failover from replaying an ambiguous write before the repository can classify its outcome.

No repository override is required for a custom transport. Transparent decorators forward the supplied request configuration unchanged. A custom transport that implements additional retries or reconstructs that configuration controls its own dispatch behavior and must preserve the write attempt boundary. The pinned Elastic.Transport implementation can retry an invalid-status-line failure on its single-node fast path even with `MaximumRetries(0)`; the internal write adapter avoids that path. Copying a bound configuration into an ordinary `RequestConfiguration` discards the adapter's node-pool settings and can reintroduce that dependency behavior. Validate custom transports with actual request counts for ambiguous failures.

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
3. One exception is thrown after the successes are processed (see [Failure handling](#failure-handling)). The message includes the counts and ids.

## Indexes Without Sequence Numbers (Elasticsearch 9.4+)

Elasticsearch 9.4 disables sequence numbers by default (`index.disable_sequence_numbers=true`, a final setting) for new **time_series data stream backing indexes**. Indexes can also be created with the setting. On those indexes:

- Responses carry the sentinel values `_seq_no: -2, _primary_term: 0`. The repository treats them as an empty version, so `Version` is `null`, and never sends them back as `if_seq_no`.
- Writes with `if_seq_no` fail with HTTP 400 (`o_c_c_not_supported_exception`) and the Update API with HTTP 400 (`update_not_supported_exception`). Both are reported as `DocumentVersioningNotSupportedException` with an explanation, for single and batch writes alike. A client-side patch that reads a document without a version reports the same exception instead of writing unchecked.
- Saving a document read from such an index tries to create it (its version is empty) and fails with `DuplicateDocumentException`; removing it throws. Use `SkipVersionCheck()`.
- Update-by-query and delete-by-query run with no conflict detection.

**`IVersioned` repositories aren't supported on such indexes.** Either don't implement `IVersioned` for time-series data, or set `index.disable_sequence_numbers: false` in the index template. `SkipVersionCheck()` saves and removes, and JSON/action patches with `SkipVersionCheck()`, still work. Standard and logsdb indexes keep sequence numbers, and may use `index.seq_no.index_options=doc_values_only` (9.1+), where OCC still works but `_seq_no` range queries are slower.

## Logging

Conflicts and retries are logged with structured fields and never include document bodies:

- **Warning** per conflict: operation, index, document id, attempt and budget, expected version, current version (parsed from the 409 reason) and outcome (`Retrying`/`Failed`).
- **Information** summary per retry round.
- **Error** when a retry budget is exhausted.
- **Warning** once per repository when a custom `IResiliencePolicy` is configured, because writes don't use it.

## Upgrading (Breaking Changes)

- **`IVersioned.Version` is `string?`.** It's `null` when a document has no version (never stored, or stored in an index with sequence numbers disabled). Declare the property as nullable, whether it is public or explicitly implemented; a nonnullable implementation produces a nullability warning.
- **`ElasticDocumentVersion` is a `readonly struct` with a `static readonly Empty`.** The implicit `string` conversion now returns `null` for an empty version, the same as `ToVersionString()`; it used to return `"0:0"`. Two empty versions are equal.
- **`SaveAsync` without a version creates.** Saving an `IVersioned` document whose `Version` is empty uses `op_type=create` and throws `DuplicateDocumentException` if the id exists. Before, it overwrote the document. Use `SkipVersionCheck()` to overwrite.
- **`RemoveAsync(T)` checks the version.** Removing an `IVersioned` document whose copy is stale throws `VersionConflictDocumentException`, and removing one without a version throws `DocumentException`. Use `SkipVersionCheck()` to delete regardless, or `RemoveAsync(id)`, which loads the document and deletes it without a check.
- **`RemoveAsync` no longer reports already-deleted documents as removed.** A document that is already gone is still not an error and its cache entry is still evicted, but `DocumentsRemoved`, `DocumentsChanged` and the `EntityChanged` (`Removed`) message are raised only for the documents the call actually deleted.
- **`VersionCheck(bool)` is fixed.** `VersionCheck(false)` now skips the check and `VersionCheck(true)` checks; they were inverted. Code that called `VersionCheck(true)` to skip the check must change to `SkipVersionCheck()`.
- **`IfVersion`** is new. It's rejected by `PatchAsync(ids)` for any number of ids, by `PatchAllAsync`, and by `AddAsync`, `SaveAsync`, `RemoveAsync` and `RemoveAllAsync` (which check the version of the document you pass in), and can't be combined with `SkipVersionCheck()`.
- **`PatchAllAsync` with `ScriptPatch` uses the bulk Update API by default** instead of update-by-query when caching is disabled. Results are the same, but every matching document is patched even under concurrent writes. Opt back in with `o.UseUpdateByQuery()`; it now throws `VersionConflictDocumentException` when documents were skipped unless `o.ProceedOnQueryConflicts()` is set.
- **`RemoveAllAsync` delete-by-query throws on leftover conflicts** after the retry budget, instead of only logging. Use `o.ProceedOnQueryConflicts()` to restore logging only.
- **Model serialization remains model-owned.** Public properties retain their configured JSON behavior. Explicit `IVersioned.Version` implementations and `[JsonIgnore]` are supported without losing cached metadata. Repository reads use Elasticsearch or cache-envelope metadata, including clearing stale tokens when metadata is absent.
- **`Retry(n)` means n retries everywhere.** Patch paths that used the resilience policy made n attempts before; they now make n + 1. This includes transient failures when submitting an asynchronous update-by-query task. `AddAsync`/`SaveAsync`/`RemoveAsync` retry 429, structured Elasticsearch 503 errors and unsent requests. Document writes are not retried after 502, 504, unstructured gateway 503 responses or timeouts.
- **Writes don't use the repository's `IResiliencePolicy`.** Retries are governed by `Retry(n)` and the transient classification above.
- **Write attempts use the configured transport.** Standard transports and transparent decorators receive a bound configuration that prevents automatic write replay. Read retries and shared connection resources are preserved; no new repository override is required. Custom transports that discard this configuration or add retries remain responsible for their dispatch behavior.
- **Failures and cancellation are no longer silent.** Batch operations process every batch and throw at the end; disposing the repository during a write throws `OperationCanceledException` after completed documents are notified, instead of returning a partial count; an update-by-query task that disappears throws instead of reporting zero; a multi-get item error throws instead of being treated as a missing document.

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
