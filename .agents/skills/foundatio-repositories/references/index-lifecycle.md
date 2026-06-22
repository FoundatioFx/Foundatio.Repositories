# Foundatio.Repositories -- Index Lifecycle

> Applies to: All versions. Code examples use v8+ syntax. For v7 mapping syntax see [patterns-v7.md](patterns-v7.md).

Full documentation: https://github.com/FoundatioFx/Foundatio.Repositories/blob/main/docs/guide/index-management.md

## Index Types

| Type | Use Case | Naming | Key Properties |
| --- | --- | --- | --- |
| `Index<T>` | Simple entities, single index | `employees` | `BulkBatchSize` |
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

## Schema Versioning

### How Versioned Indexes Work

1. Each version creates a separate index (`employees-v1`, `employees-v2`)
2. An alias (`employees`) always points to the current version
3. When you increment the version, data is automatically migrated via reindex

### Version Upgrade Process

When you increment the version and call `ConfigureIndexesAsync()`:

1. New index created (`employees-v2`) with new mapping
2. Elasticsearch reindex API copies data from v1 to v2
3. Reindex scripts transform data during migration
4. Alias atomically switched from v1 to v2
5. Old index deleted (if `DiscardIndexesOnReindex = true`)

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

## Concurrency Safety

### Reindex Locking

- Lock key: `reindex:{alias}` (e.g., `reindex:employees`)
- Lock TTL: 20 minutes, auto-renewed during progress callbacks
- Only one reindex per logical index can run at a time
- Sequential version transitions (v1→v2, then v2→v3) cannot overlap

### Crash Recovery

If an instance crashes mid-reindex, the lock expires after 20 minutes. Another instance can retry. `VersionedIndex.ReindexAsync()` is resume-safe.

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

DailyIndex never emits mapping errors from the built-in configuration path (since `ConfigureAsync` is a no-op).
