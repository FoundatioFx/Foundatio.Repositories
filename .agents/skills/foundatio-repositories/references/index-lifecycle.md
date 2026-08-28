# Foundatio.Repositories -- Index Lifecycle

> Applies to: All versions. Code examples use v8+ syntax. For v7 mapping syntax see [patterns-v7.md](patterns-v7.md).

Full documentation: https://github.com/FoundatioFx/Foundatio.Repositories/blob/main/docs/guide/index-management.md

## Index Types

| Type | Use Case | Naming | Key Properties |
| --- | --- | --- | --- |
| `Index<T>` | Simple entities, single index | `employees` | `BulkBatchSize`, `ReindexBatchSize`, `ReindexRequestsPerSecond` |
| `VersionedIndex<T>` | Evolving schemas, automatic reindex | `employees-v2` + `employees` alias | `Version`, `DiscardIndexesOnReindex` |
| `DailyIndex<T>` | High-volume time-series, short retention | `logs-v1-2024.01.15` + `logs` alias | `MaxIndexAge`, `DiscardExpiredIndexes` |
| `MonthlyIndex<T>` | Lower-volume time-series, longer retention | `audit-v1-2024.01` + `audit` alias | `MaxIndexAge`, `DiscardExpiredIndexes` |

### Index&lt;T&gt;

```csharp
public sealed class EmployeeIndex : Index<Employee>
{
    public EmployeeIndex(IElasticConfiguration configuration)
        : base(configuration, "employees") { }

    public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
    {
        map
            .Dynamic(DynamicMapping.False)
            .Properties(p => p
                .SetupDefaults()
                .Keyword(e => e.CompanyId)
                .Text(e => e.Name, t => t.AddKeywordAndSortFields())
            );
    }
}
```

### VersionedIndex&lt;T&gt;

```csharp
public sealed class EmployeeIndex : VersionedIndex<Employee>
{
    public EmployeeIndex(IElasticConfiguration configuration)
        : base(configuration, "employees", version: 2) { }

    public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
    {
        map
            .Dynamic(DynamicMapping.False)
            .Properties(p => p
                .SetupDefaults()
                .Keyword(e => e.CompanyId)
                .Text(e => e.Name, t => t.AddKeywordAndSortFields())
                .Keyword(e => e.Department)  // Added in v2
            );
    }
}
```

Index naming: v1 = `employees-v1`, v2 = `employees-v2`. Alias `employees` always points to current version.

### DailyIndex&lt;T&gt;

```csharp
public sealed class LogEventIndex : DailyIndex<LogEvent>
{
    public LogEventIndex(IElasticConfiguration configuration)
        : base(configuration, "logs", version: 1)
    {
        MaxIndexAge = TimeSpan.FromDays(90);
        DiscardExpiredIndexes = true;
    }

    public override void ConfigureIndexMapping(TypeMappingDescriptor<LogEvent> map)
    {
        map
            .Dynamic(DynamicMapping.False)
            .Properties(p => p
                .SetupDefaults()
                .Keyword(e => e.Level)
                .Text(e => e.Message)
            );
    }
}
```

Index naming: `logs-v1-2024.01.15`. Alias `logs` covers all partitions.

### MonthlyIndex&lt;T&gt;

```csharp
public sealed class AuditLogIndex : MonthlyIndex<AuditLog>
{
    public AuditLogIndex(IElasticConfiguration configuration)
        : base(configuration, "audit", version: 1)
    {
        MaxIndexAge = TimeSpan.FromDays(365);
        DiscardExpiredIndexes = true;
    }
}
```

Index naming: `audit-v1-2024.01`.

## How Time-Series Routing Works

`DailyIndex`/`MonthlyIndex` keep **one** physical index per period (day/month) — not parallel copies of the same data. Two copies of a period only coexist transiently during a version reindex (`logs-v1-2024.01.15` → `logs-v2-2024.01.15`), then the old is dropped when `DiscardIndexesOnReindex` is `true` (default). Retention deletes aged-out periods one index at a time.

### Three naming layers

| Layer | Example | Purpose |
|---|---|---|
| Physical index | `logs-v1-2024.01.15` | Actual index on disk (version encoded) |
| Dated alias | `logs-2024.01.15` | Current version's index for one period; target for single-doc routing |
| Umbrella alias | `logs` | All current, non-expired indexes; target for cross-period queries |
| Windowed alias | `logs-last-7-days` | Indexes within a rolling window (`AddAlias(name, maxAge)`) |

Single-doc read/write routing targets the **unversioned dated alias**, so the physical version can change (via reindex) without changing how documents are addressed.

### Picking the index at write time

`GetIndex(target)` derives the destination from the document's date, resolved in order (`_getDocumentDateUtc`):

1. ObjectId creation time embedded in the id (`CreateDocumentId` encodes the date into the id).
2. `CreatedUtc` if the model implements `IHaveCreatedDate`.
3. A custom `getDocumentDateUtc` delegate passed to the constructor.

`EnsureIndexAsync` then creates the physical index for that period if missing and attaches the dated, umbrella, and matching windowed aliases in one call. The `MaxIndexAge` check runs **first** — writing to a date already past `MaxIndexAge` throws `ArgumentException: Index max age exceeded`. Bulk writes are grouped by resolved index (one write per period).

### Resolving the index at read time

- **Single-doc lookups** (`GetByIdAsync`, `ExistsAsync`, id-based patch/remove) parse the ObjectId back into its date and route to **one** dated alias. If not found and `HasMultipleIndexes`, they fall back to a query across the umbrella alias.
- **Queries** (`FindAsync`, `CountAsync`, `PatchAllAsync`, `RemoveAllAsync`) resolve indexes via `GetIndexesByQuery`: explicit `.Index("name")`, or `.Index(start, end)` expanded to dated aliases (partition pruning), else the umbrella alias (also the [large-range fallback](#large-range-fallback)).

> Full explanation with lifecycle diagrams: `docs/guide/index-management.md` → "How Time-Series Indexes Work".

## Schema Versioning

### How Versioned Indexes Work

1. Each version creates a separate index (`employees-v1`, `employees-v2`)
2. An alias (`employees`) always points to the current version
3. When you increment the version, data is migrated via reindex — but that reindex must be explicitly triggered; nothing runs it automatically (see "What triggers it" below)

### Version Upgrade Process

The upgrade itself is always these 5 steps, whether triggered directly or via the queue (see "What triggers it" below for which to use):

1. New index created (`employees-v2`) with new mapping
2. Elasticsearch reindex API copies data from v1 to v2
3. Reindex scripts transform data during migration
4. Alias atomically switched from v1 to v2
5. Old index deleted (if `DiscardIndexesOnReindex = true`, no failures, and new count >= old count)

**Prefer `configuration.ReindexAsync()` / `index.ReindexAsync()` to run this directly** — see "What triggers it" below; `ConfigureIndexesAsync()`'s default only enqueues a work item and throws if no queue is configured.

### Version Upgrades for Time-Series Indexes (Daily/Monthly)

`DailyIndex`/`MonthlyIndex` migrate **one dated partition at a time**, oldest → newest. Each partition's old index is deleted as the *final step of that partition's own reindex*, before the next partition starts — it never duplicates all partitions then bulk-deletes the originals. Peak extra disk = ~one partition, not the whole dataset.

> **Trigger explicitly with `configuration.ReindexAsync()` or `index.ReindexAsync()`.** Unlike a single `VersionedIndex<T>`, time-series indexes are **not** migrated by `ConfigureIndexesAsync(beginReindexingOutdated: true)`: its queued work item targets the unversioned base name (`audit-v1`), which doesn't match dated partitions (`audit-v1-2024.01`). `DailyIndex`/`MonthlyIndex` don't override `CreateReindexWorkItem`/`VersionedName`, so the queue path is a safe no-op for them. All time-series reindex tests call `index.ReindexAsync()` directly.

Per partition (`ReindexAsync`, under a distributed lock keyed on the alias):

1. Create `audit-v2-2024.01` with the new mapping.
2. Reindex v1 → v2 (first pass).
3. Swap aliases: remove `audit-v1-2024.01`, add `audit-v2-2024.01` (reads now hit v2).
4. Second-pass catch-up for docs written during the first pass.
5. Delete `audit-v1-2024.01` — only if `DiscardIndexesOnReindex` (default true), no failures, and new count >= old count.

Partitions past `MaxIndexAge` are skipped (left for maintenance). The umbrella alias spans both migrated (v2) and not-yet-migrated (v1) partitions during the upgrade, so reads/writes keep working. Ordering: `GetIndexesAsync` → `.OrderBy(i => i.DateUtc)`; per-partition delete: `ElasticReindexer.ReindexAsync`.

**Concurrency:** *Within an index* a reindex is strictly sequential — one partition at a time (`await`ed `foreach` in `DailyIndex.ReindexAsync`), and each ES `_reindex` is a single **unsliced** task (library never sets `slices`). *Across indexes* it depends on the trigger: `configuration.ReindexAsync()` runs them sequentially (`foreach`), but `ElasticMigrationJob` runs them **in parallel** (`Task.WhenAll`, one task per outdated index). A distributed lock keyed on the alias (`reindex:<alias>`, held 20 min, auto-renewed on progress) still caps a given index to one reindex cluster-wide across pods. So a single-index upgrade needs ~one partition of disk headroom; parallel multi-index runs need ~one in-flight partition per concurrent index.

**What triggers it:** Nothing runs a reindex automatically — no background timer, hosted service, or auto-discovered job. Three ways to trigger one: (1) **call `configuration.ReindexAsync()` / `index.ReindexAsync()` directly** — deterministic, what every reindex test uses, the recommended path. (2) **`ConfigureIndexesAsync(beginReindexingOutdated: true)`** (the default) only *enqueues* a `ReindexWorkItem`; it doesn't run it. Running it requires an `IQueue<WorkItemData>` on `ElasticConfiguration` AND a worker with `ReindexWorkItemHandler` registered to dequeue and process it — neither is wired up by the library. **If no queue is configured, `ConfigureIndexesAsync()` throws `InvalidOperationException` the moment an index is outdated** — this repo's own sample app (`samples/Foundatio.SampleApp/.../ElasticExtensions.cs`) avoids this by passing `beginReindexingOutdated: false`. Even fully wired up, this path is a no-op for time-series (see above). (3) **`ElasticMigrationJobBase`** is an abstract, opt-in helper to derive from and register in your own job runner (it correctly calls `ConfigureIndexesAsync(null, false)` then reindexes outdated indexes in parallel) — it is **not auto-registered, auto-discovered, or referenced anywhere** in this repo or its sample app; treat it as scaffolding, not a default mechanism. `MaintainIndexesJob` never reindexes (aliases/retention only).

**Recovering from a rolling restart mid-upgrade:** the lock (`reindex:audit`, 20 min TTL, renewed on progress) expires on its own if the holder dies — no explicit release needed, a new instance's `AcquireAsync` just waits then proceeds. The ES-side `_reindex` task runs server-side (`wait_for_completion=false`) and is unaffected by the client dying. A retried first pass resumes from the newest doc already in the new partition (`GetResumeStartingPointAsync`, `>=` range query) instead of recopying the whole period. Partitions are discovered by physical index name, not alias membership, so a partition whose alias was swapped but not yet deleted before a crash is still found and finished (resume copy → swap → delete) on the next run. Two instances racing for the same index never double-migrate it: the lock serializes them, and the loser's post-lock version check finds the version already advanced and returns.

**Write flip / no gap:** writes target the unversioned dated alias (`audit-2024.01`). After the first pass, all aliases on the old partition (dated + umbrella + windowed) are repointed to v2 in a **single atomic `UpdateAliasesAsync`** → no aliasing gap. Docs written to v1 during the first pass are copied by the second-pass catch-up (timestamp/ObjectId `>= now-1s`, `Conflicts=proceed`), so no lost-write gap for append-only data.

**Why oldest → newest:** time-series writes land in the current period, so old partitions are effectively immutable — migrating them first means near-empty second-pass catch-up and no write contention, while the one volatile (current) partition is done last with the smallest catch-up window. Also frees disk progressively from oldest data, and is deterministic/resumable (each run lists only still-old-version partitions via the min-version `GetCurrentVersionAsync`, so an interrupted run resumes with the remainder in the same order). Introduced as an index-management stability fix (2017).

### Reindex Scripts

#### Rename a Field

```csharp
public EmployeeIndex(IElasticConfiguration configuration)
    : base(configuration, "employees", version: 2)
{
    RenameFieldScript(2, "dept", "department");

    // Keep both fields (don't remove original):
    RenameFieldScript(2, "oldName", "newName", removeOriginal: false);
}
```

Supports dotted paths for nested fields:

```csharp
RenameFieldScript(2, "data.oldField", "data.newField");
RenameFieldScript(2, "metadata.author.name", "metadata.author.displayName");
```

#### Remove a Field

```csharp
RemoveFieldScript(3, "deprecatedField");
RemoveFieldScript(3, "data.legacyField");  // nested path
```

#### Custom Transformation

```csharp
AddReindexScript(4, """
    if (ctx._source.containsKey('firstName') && ctx._source.containsKey('lastName')) {
        ctx._source.fullName = ctx._source.firstName + ' ' + ctx._source.lastName;
    }
    if (ctx._source.containsKey('status')) {
        ctx._source.isActive = ctx._source.status == 'active';
        ctx._source.remove('status');
    }
    """);
```

### Multi-Version Migration

Scripts are applied incrementally. Upgrading from v1 to v3 applies both v2 and v3 scripts:

```csharp
public EmployeeIndex(IElasticConfiguration configuration)
    : base(configuration, "employees", version: 3)
{
    RenameFieldScript(2, "dept", "department");           // v2
    AddReindexScript(3, "ctx._source.version = 3;");     // v3
}
```

When multiple scripts apply, they're wrapped in named functions and called sequentially.

### Moving Fields Between Objects

```csharp
RenameFieldScript(2, "data.oldField", "meta.newField");  // between parents
RenameFieldScript(3, "data.name", "displayName");         // promote to top-level
RenameFieldScript(4, "companyName", "data.company");      // demote to nested
```

## Retention Policy

### Configuring Retention

```csharp
public LogEventIndex(IElasticConfiguration configuration)
    : base(configuration, "logs", version: 1)
{
    MaxIndexAge = TimeSpan.FromDays(90);
    DiscardExpiredIndexes = true;

    // Time-based aliases for recent data windows
    AddAlias("logs-last-7-days", TimeSpan.FromDays(7));
    AddAlias("logs-last-30-days", TimeSpan.FromDays(30));
}
```

### How Retention Works

1. Each index has an expiration date based on its date + `MaxIndexAge`
2. `MaintainIndexesAsync()` checks for expired indexes
3. If `DiscardExpiredIndexes = true`, expired indexes are deleted
4. Writes to dates older than `MaxIndexAge` throw `ArgumentException`

### Running Maintenance

```csharp
// Use the built-in job
services.AddJob<MaintainIndexesJob>(o => o.ApplyDefaults<MaintainIndexesJob>());

// Or call directly
await _configuration.MaintainIndexesAsync();
```

## Querying Time-Series Indexes

### .Index() vs .DateRange()

- **`.Index(start, end)`** selects which physical partitions to query (index-level routing)
- **`.DateRange(start, end, field)`** filters documents within targeted indexes

These must be set independently. `DateRange` alone does NOT narrow index selection.

```csharp
var results = await repository.FindAsync(q => q
    .Index(start, end)                         // target partitions
    .DateRange(start, end, e => e.CreatedUtc)  // filter documents
);
```

### Large Range Fallback

| Index type | Threshold | Behavior |
|---|---|---|
| `DailyIndex` | Range >= 3 months, or exceeds `MaxIndexAge` | Falls back to alias (all partitions) |
| `MonthlyIndex` | Range > 1 year, or exceeds `MaxIndexAge` | Falls back to alias (all partitions) |

In the fallback case, the query targets the alias. `.DateRange()` still filters documents correctly.

### Time-Based Alias Queries

```csharp
// Query using a time-based alias instead of computing a range
var results = await repository.FindAsync(q => q.Index("logs-last-7-days"));
```

## Mapping Lifecycle

### How Mappings Are Applied by Index Type

| Index type | `ConfigureIndexesAsync` behavior | First write (no explicit configure) | New field mapping on existing data |
|---|---|---|---|
| `Index<T>` | Creates index or PUT Mapping | `EnsureIndexAsync` triggers create-or-update (one-time) | Automatic |
| `VersionedIndex<T>` | Same as `Index<T>` | Same | Automatic |
| `DailyIndex<T>` | **No-op** -- existing partitions never updated | Creates partition with full mapping only if missing | **Manual** |
| `MonthlyIndex<T>` | Same as `DailyIndex<T>` | Same | **Manual** |

### Updating Existing Daily/Monthly Partitions

| Strategy | Cost | When to use |
|----------|------|-------------|
| **Roll forward** (do nothing) | Zero | Feature can wait until enough data accumulates via retention |
| **PutMapping + update-by-query on all partitions** | High I/O | Need field searchable across all historical data immediately |
| **Targeted backfill** (recent partitions only) | Moderate | Need field on recent data; older data will age out |
| **Bump version** (full reindex) | High + disk doubling | Need type change on existing field |

**Practical recommendation**: Roll forward by default. For a `DailyIndex` with `MaxIndexAge` of 30 days, waiting 30 days gives full coverage for free.

### Mapping Resolver Cache

| Cache layer | Lifetime | How to invalidate |
|---|---|---|
| `ElasticMappingResolver` field cache | Auto-refreshes ~60 seconds | `index.MappingResolver.RefreshMapping()` |
| `_isEnsured` flag (Index/VersionedIndex) | Process lifetime | App restart or index deletion |
| `_ensuredDates` (DailyIndex) | Process lifetime per-date | `DeleteAsync(name)` or `Dispose()` |
| `ConfigureIndexesAsync` cache marker | 5 minutes (distributed) | Expires automatically; or `ConfigureIndexesAsync(force: true)` |

## Index Operations

### ConfigureIndexesAsync

Creates indexes and updates mappings. Protected by distributed lock + cache marker:

```csharp
await configuration.ConfigureIndexesAsync();

// Bypass cache marker (after structural changes)
await configuration.ConfigureIndexesAsync(force: true);

// Configure specific indexes (bypasses lock and cache)
await configuration.ConfigureIndexesAsync([myIndex]);
```

### MaintainIndexesAsync

Updates aliases for time-series indexes, deletes expired indexes:

```csharp
await configuration.MaintainIndexesAsync();
```

### ReindexAsync

```csharp
await configuration.ReindexAsync(async (progress, message) =>
{
    _logger.LogInformation("Reindex {Progress}%: {Message}", progress, message);
});
```

Reindexing is throttled via `ReindexBatchSize` (documents per internal ES bulk batch, default 1000) and `ReindexRequestsPerSecond` (default unlimited), set on the index before calling `ReindexAsync`. Both are `null` by default (ES defaults apply) and flow through `Index<T>` → `ReindexWorkItem` → the `_reindex` API's `Source.Size`/`RequestsPerSecond`. Lower `ReindexBatchSize` if large documents trigger `es_rejected_execution_exception` ("rejected execution of coordinating operation") from Elasticsearch's [indexing pressure](https://www.elastic.co/docs/reference/elasticsearch/configuration-reference/indexing-pressure-settings) limit (10% of heap by default). Both throw `ArgumentOutOfRangeException` from `ReindexAsync` (which also throws `ArgumentNullException` for a `null` work item) if set to zero, negative, or (for `ReindexRequestsPerSecond`) `NaN`/infinite - checked explicitly with `float.IsNaN`/`float.IsInfinity` rather than relying on `<= 0`, since `+Infinity` isn't `<= 0`.

Task-status polling during reindex (`ElasticReindexer.InternalReindexAsync`) also backs off exponentially on failure — 1 second, doubling up to a 30 second cap, with +/-25% jitter (`ElasticReindexer.GetStatusRetryDelay`) — instead of retrying immediately, up to `MAX_STATUS_FAILS` (10) consecutive failures before giving up. The jitter prevents multiple reindex operations that fail from the same cluster-wide condition (e.g. indexing pressure) from retrying in lockstep.

A reindex reporting no progress for too long is treated as stalled and abandoned (`ElasticReindexer.GetNoProgressTimeout`). This defaults to 10 minutes, but scales up when `ReindexRequestsPerSecond` is set low enough that Elasticsearch's own inter-batch pause (`ReindexBatchSize ?? 1000` ÷ `ReindexRequestsPerSecond`, ×3 safety margin) would otherwise exceed 10 minutes - so a healthy, intentionally throttled reindex isn't mistaken for a stalled one and cancelled. The scaled timeout is computed in `double` (seconds) space and clamped to `TimeSpan.MaxValue` before constructing the result - an extreme `ReindexBatchSize`/`ReindexRequestsPerSecond` combination (e.g. a huge batch size with a near-zero rate) would otherwise overflow `TimeSpan`'s ~29,247 year range and throw `OverflowException` mid-reindex, abandoning an already-started, untracked server-side task.

### Explicit Index Compatibility Upgrades (ES Major-Version Upgrades)

Independent of schema versioning (`VersionedIndex.Version`), built-in indexes implement `IIndexCompatibility` to report physical indexes created under an **older Elasticsearch major version** than the connected server. Compatibility remediation is intentionally explicit: neither `ConfigureIndexesAsync` nor `ElasticMigrationJobBase` checks or reindexes these indexes.

- **Zero default overhead**: normal configuration performs no server-info or compatibility-metadata requests. `GetIndexCompatibilityAsync(cancellationToken)` is an operator-invoked preflight using one fresh `Client.InfoAsync()` plus one settings-and-aliases `Indices.GetAsync`; versioned and time-series patterns cover every structurally valid physical index in one request, including retained/expired indexes, hidden partitions, and generated `{name}-error` partitions. The reindexer marks generated error partitions with the exact hidden `.foundatio-reindex-error` alias; suffix-only legacy or foreign collisions fail closed and are never upgraded or wildcard-deleted automatically. The server version is not cached because the process may survive a rolling cluster upgrade. A multi-index upgrade completes all ownership and compatibility preflight reads before its first mutation, then recomputes each index under `reindex:{name}`, rejects candidates that map to the same deterministic destination, and verifies once more after cutover.
- **Exclusive ownership across configurations**: a physical index belongs to a configured index only when no other registered configuration index claims it more strongly — native containment (no generated-prefix stripping) outranks stripped matching, and among native families the longest configured name wins. This protects legitimately named indexes such as `reindexed-v8-events` from being claimed, reindexed, or wildcard-deleted by a configuration named `events`; ambiguous names are excluded from preflight, rejected as upgrade/recovery sources, and skipped by `DeleteAsync()`.
- **Typed sequential-major state**: `IndexCompatibilityInfo.State` is derived as `Current`, `RequiresReindex` (exactly one major behind), or `Unsupported`. Never represent an ES7-created index on ES9 as an ordinary supported reindex; restore a supported snapshot and upgrade 7→8→9 sequentially. Foundatio preflight covers configured Foundatio patterns only and never replaces Elastic Upgrade Assistant for cluster-wide readiness.
- **Offline contract and rollback boundary**: stop application writers, queue consumers, maintenance jobs, and alias managers. Take a verified snapshot and wait until the old-server rollback window closes. The source write block cannot invalidate document sequence-number/primary-term values held in application memory or caches, so restart or drain clients before resuming writes.
- **Dedicated implementation**: compatibility uses `ElasticIndexCompatibilityUpgrader` and `ElasticReindexTaskRunner`; no compatibility flags leak into `ReindexWorkItem`. The shared schema reindexer rejects filtered/routed source aliases, fails closed when aliases cannot be read, re-reads complete alias metadata at cutover, and catches up from the resolved concrete source so the moved alias cannot redirect the second pass. An initially empty source receives a full post-cutover pass to close the sample-to-alias-swap race.
- **Kibana lifecycle plus Foundatio hardening**: validate source and destination, always call the dedicated write-block API, require cluster/shard/exact-index block acknowledgement, reject partial refresh/count shards, `_create_from`, async `_reindex`, restore originally absent settings as unset, block the target through cutover, and wait for primary availability. Reject destination-template aliases, mappings, or extra explicit settings; re-read alias definitions before atomic `remove_index`, reconcile with an independent token afterward, and compare full alias metadata. Exact task results and source/task/target counts are required.
- **Physical names change; canonical names continue resolving**: `employees` becomes `reindexed-v9-employees`; `employees-v2` becomes `reindexed-v9-employees-v2`; `logs-v1-2024.01.15` becomes `reindexed-v9-logs-v1-2024.01.15`. One canonical old physical-name alias is preserved. A later major replaces the generated prefix and does not retain each generated name as another alias. A configured name that naturally starts with `reindexed-v8-` receives an outer prefix and is never stripped as generated state.
- **Schema-version precedence**: a compatibility candidate whose schema version differs from the configured `Version` and still owns the logical alias fails before any write block and instructs the operator to run the schema reindex first. Ownership and schema precedence are revalidated at execution time immediately before blocking writes. An unaliased old version intentionally retained with `DiscardIndexesOnReindex = false` is inactive and can be compatibility-upgraded independently.
- **Alias and index-time fidelity**: complete alias definitions are copied (filter, routing, hidden, and write-index metadata). `_create_from` preserves source mappings/settings; matching destination templates are rejected if they add aliases, mappings, or explicit settings. Default and final ingest pipelines are disabled only for the existing-document copy and then restored, preventing non-idempotent processors from running twice.
- **Failure and recovery semantics**: task cancellation uses `wait_for_completion` followed by an authoritative task-status read; a partial or malformed task listing never authorizes cleanup and leaves the active-task count unknown. Target deletion/unblocking is forbidden when task termination is unknown. Cleanup independently confirms target absence before removing the source write block, so a failed or ambiguous delete keeps the source fenced. A transport failure or accepted start response without a task ID is also uncertain: retain the target and source write block until matching Elasticsearch tasks are inspected. A failed `_create_from` is equally uncertain — even an error response cannot prove the destination was left untouched, so no automatic cleanup follows it. A private hidden alias marks confirmed workflow-owned targets and is atomically removed at cutover. `InspectIndexCompatibilityUpgradeAsync` is read-only and reports topology, aliases, blocks, ownership, and matching tasks. Inspect/recover require the original pre-upgrade concrete source, reject expressions and names not structurally owned by the supplied index before any request or lock, and reject a source equal to the deterministic current-major destination. A completed topology requires the canonical source alias and no ownership marker. `RecoverIndexCompatibilityUpgradeAsync` holds `reindex:{name}` and only deletes a marker-owned pre-cutover destination after zero matching tasks. Write-block removal is a separate explicit opt-in. Unmarked destinations and ambiguous cutovers never auto-heal or choose a “newer” index.
- **Unsupported sources**: reject Elasticsearch before 8.18, closed indexes, system/dot indexes, data-stream backing indexes, `_source` disabled, non-standard index modes, ILM-managed indexes, CCR followers, and read/metadata/read-only blocks. Closed indexes must be opened explicitly before the upgrade. A pre-existing write-only block is preserved on the replacement index.
- **Testing**: unit/request-budget tests cover derived compatibility state, version parsing, cancellation, lock recomputation, zero startup compatibility I/O, one preflight settings request across all partitions, and natural-prefix parsing. Live tests cover plain/versioned/daily cutover, retained inactive versions, canonical aliases, full alias metadata, destination-template drift, exact unset setting restoration, write fences, confirmed task cancellation, ownership-safe recovery, destination collisions, schema precedence, and the Elasticsearch 8.18 minimum. CI also carries one persistent data directory through ES7→ES8→ES9 without changing `VersionedIndex.Version`.

## Concurrency Safety

### Reindex Locking

- Lock key: `reindex:{alias}` (e.g., `reindex:employees`)
- Lock TTL: 20 minutes, auto-renewed during progress callbacks
- Only one reindex per logical index can run at a time
- Sequential version transitions (v1→v2, then v2→v3) cannot overlap

### Crash Recovery

If an instance crashes mid-reindex, the lock is never explicitly released — it expires 20 minutes after the last renewal, and another instance's `AcquireAsync` then proceeds. The Elasticsearch-side `_reindex` task runs server-side (`wait_for_completion=false`) and is unaffected by the client dying. A retried first pass resumes from the newest document already in the new index (`>=` range query) rather than recopying everything. `VersionedIndex.ReindexAsync()` — and the per-partition loop in `DailyIndex`/`MonthlyIndex` — are resume-safe for this reason. See "Recovering from a rolling restart mid-upgrade" above for the time-series specifics (orphaned already-swapped partitions, racing instances).

### Second-Pass Catch-Up

After the first reindex pass completes, a second pass catches documents written during the first:

1. **TimestampField available** (e.g., `IHaveDates` models): timestamp-based range query (preferred)
2. **No TimestampField, ObjectId-format IDs**: ObjectId-based range queries
3. **No TimestampField, non-ObjectId IDs**: Cannot perform second pass (logs a Warning)

### ConfigureIndexesAsync Concurrency

Multiple distributed processes calling `ConfigureIndexesAsync` on startup:

1. Cache check → return immediately if marker exists
2. Distributed lock → serialize concurrent callers
3. Double-check cache after acquiring lock
4. Full configure pass on all indexes in parallel
5. Set 5-minute TTL cache marker

Cache marker key includes a hash of all index names/versions. New deployments automatically bypass stale markers.

## Index Configuration

### Index Settings

```csharp
public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
{
    base.ConfigureIndex(idx.Settings(s => s
        .NumberOfShards(3)
        .NumberOfReplicas(1)
        .RefreshInterval(new Duration(TimeSpan.FromSeconds(5)))
        .Analysis(a => a
            .AddSortNormalizer()
        )));
}
```

### In-Place Analysis Upgrades (analyzers/tokenizers/filters)

When a `VersionedIndex` already exists and `ConfigureAsync` runs again, dynamic settings (including the
`Analysis` block: analyzers, tokenizers, token filters, normalizers, char filters) are applied in place via
`PutSettings(.Reopen())` rather than requiring a new index version. The reopen briefly closes and reopens the
index so newly added analysis components take effect; existing documents are not reindexed, so the new
component only applies to writes/queries after the upgrade.

Before applying, `UpdateIndexAsync` diffs the desired analysis components against the live index and logs a
`requires close/reopen` warning for each genuinely new component.

**Analysis settings location — `Settings.Index.Analysis` vs root `Settings.Analysis`:** these are two
different shapes of the same data:

- **Read path (`GetSettingsAsync`)** returns analysis nested under the `index` key, i.e.
  `response.Settings[name].Settings.Index.Analysis`. This is the canonical location for the *current* live
  state and is what the diff reads. The root `Settings.Analysis` is **not** populated on reads.
- **Write path (create/update request)** uses the root `Settings.Analysis` shape (what your
  `ConfigureIndex(...).Analysis(...)` builds). This is the *desired* state sent to Elasticsearch.

So the in-place diff compares desired root `Settings.Analysis` (from `ConfigureIndex`) against current
`Settings.Index.Analysis` (from `GetSettingsAsync`). Reading the current set from the root `Settings.Analysis`
returns nothing, which makes every existing component look new and falsely warns on every upgrade — always
read current analysis from `Settings.Index.Analysis`.

### Query Field Restrictions

```csharp
public class EmployeeIndex : VersionedIndex<Employee>
{
    // Restrict which fields can be queried via FilterExpression
    public override ISet<string> AllowedQueryFields { get; } = new HashSet<string> { "company_id", "name", "age" };
    public override ISet<string> AllowedAggregationFields { get; } = new HashSet<string> { "company_id", "age" };
    public override ISet<string> AllowedSortFields { get; } = new HashSet<string> { "created_utc", "name" };
}
```

## Failure Log Messages

| Level | Message | Meaning |
|-------|---------|---------|
| Error | `Error updating index ({name}) settings` | Index settings PUT failed |
| Error | `Error updating index ({name}) mappings.` | PUT Mapping failed |
| Error | `Error updating index ({name}) mappings. Changing existing fields requires a new index version.` | Tried to change existing field type on VersionedIndex |
| Warning | `Adding new analyzer/tokenizer/filter to existing index (requires close/reopen)` | New analysis component needs index close/reopen |
| Error | `Error getting task status while reindexing: {OldIndex} -> {NewIndex}` | Task status poll failed (e.g. `es_rejected_execution_exception` from indexing pressure); retried with exponential backoff (1s, doubling, capped at 30s) |
| Error | `Failed to get the status {N} times in a row for reindex task ... reindexing {OldIndex} -> {NewIndex}` | Status polling gave up after `MAX_STATUS_FAILS` (10) consecutive failures; reindex progress can no longer be tracked, but the server-side `_reindex` task keeps running |

DailyIndex never emits mapping errors from the built-in configuration path (since `ConfigureAsync` is a no-op).
