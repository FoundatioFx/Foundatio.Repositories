# Configuration Options

This guide covers all configuration options available in Foundatio.Repositories, including repository-level settings and per-operation options.

## Repository-Level Configuration

These settings are configured in your repository constructor and apply to all operations by default.

### DefaultConsistency

Controls the default refresh behavior for write operations:

```csharp
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeRepository(EmployeeIndex index) : base(index)
    {
        DefaultConsistency = Consistency.Immediate;
    }
}
```

| Value | Description |
|-------|-------------|
| `Consistency.Eventual` | No refresh after write (default, fastest) |
| `Consistency.Immediate` | Refresh immediately after write |
| `Consistency.Wait` | Wait for refresh to complete |

### DefaultCacheExpiration

Default cache TTL for cached operations:

```csharp
DefaultCacheExpiration = TimeSpan.FromMinutes(10);
```

Default: 5 minutes

### DefaultPageLimit / MaxPageLimit

Pagination limits:

```csharp
DefaultPageLimit = 25;   // Default page size
MaxPageLimit = 1000;     // Maximum allowed page size
```

Defaults: 10 / 10000

### NotificationsEnabled

Enable/disable entity change notifications via message bus:

```csharp
NotificationsEnabled = true;
```

Default: `true` if a message bus is configured

### OriginalsEnabled

Track original document state during save operations for change detection:

```csharp
OriginalsEnabled = true;
```

When enabled:
- Original document is fetched before save
- Enables soft delete transition detection (`IsDeleted: false → true` sends `ChangeType.Removed`)
- Enables change tracking in `DocumentsSaving`/`DocumentsSaved` events

Default: `false`

### BatchNotifications

Batch multiple notifications together:

```csharp
BatchNotifications = true;
```

Default: `false`

### NotificationDeliveryDelay

Delay notification delivery to allow Elasticsearch indexing to complete:

```csharp
NotificationDeliveryDelay = TimeSpan.FromSeconds(2);
```

::: warning
Only set a delay if your message bus implementation supports delayed delivery. Message buses that don't support delayed delivery may silently drop messages.
:::

Default: `null` (immediate delivery)

### DefaultPipeline

Elasticsearch ingest pipeline for document processing:

```csharp
DefaultPipeline = "my-ingest-pipeline";
```

Default: `null`

### AutoCreateCustomFields

Automatically create custom field definitions for unmapped fields:

```csharp
AutoCreateCustomFields = true;
```

Default: `false`

### DefaultQueryLogLevel

Log level for query logging:

```csharp
DefaultQueryLogLevel = LogLevel.Debug;
```

Default: `LogLevel.Trace`

### Complete Repository Configuration Example

```csharp
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeRepository(EmployeeIndex index) : base(index)
    {
        // Consistency
        DefaultConsistency = Consistency.Immediate;
        
        // Caching
        DefaultCacheExpiration = TimeSpan.FromMinutes(10);
        
        // Pagination
        DefaultPageLimit = 25;
        MaxPageLimit = 1000;
        
        // Notifications
        NotificationsEnabled = true;
        BatchNotifications = false;
        NotificationDeliveryDelay = TimeSpan.FromSeconds(1);
        
        // Change tracking
        OriginalsEnabled = true;
        
        // Elasticsearch
        DefaultPipeline = null;
        AutoCreateCustomFields = false;
        
        // Logging
        DefaultQueryLogLevel = LogLevel.Debug;
    }
}
```

## Per-Operation Options

Override default settings for specific operations using `ICommandOptions`.

### Consistency Options

```csharp
// Set consistency mode
await repository.AddAsync(entity, o => o.Consistency(Consistency.Immediate));

// Shorthand for immediate consistency
await repository.AddAsync(entity, o => o.ImmediateConsistency());

// Wait for refresh
await repository.AddAsync(entity, o => o.ImmediateConsistency(shouldWait: true));
```

### Cache Options

```csharp
// Enable caching
await repository.GetByIdAsync(id, o => o.Cache());

// Enable with specific key
await repository.FindOneAsync(query, o => o.Cache("my-cache-key"));

// Enable with key and expiration
await repository.FindOneAsync(query, o => o.Cache("my-key", TimeSpan.FromMinutes(5)));

// Set cache key separately
await repository.FindAsync(query, o => o.CacheKey("employees-active"));

// Set expiration
await repository.GetByIdAsync(id, o => o.CacheExpiresIn(TimeSpan.FromMinutes(30)));
await repository.GetByIdAsync(id, o => o.CacheExpiresAt(DateTime.UtcNow.AddHours(1)));

// Read from cache only (don't write)
await repository.GetByIdAsync(id, o => o.Cache(false).ReadCache());

// Disable both document/result cache reads and writes, including explicit ReadCache()
await repository.GetByIdAsync(id, o => o.Cache(false).ReadCache(false));
```

`ReadCache(bool)` explicitly controls reads independently of cache writes and takes precedence over `Cache()`. The parameterless `ReadCache()` is equivalent to `ReadCache(true)`; it does not disable writes that were already enabled.

### Validation Options

```csharp
// Skip validation
await repository.AddAsync(entity, o => o.SkipValidation());

// Explicitly control validation
await repository.SaveAsync(entity, o => o.Validation(false));
```

### Notification Options

```csharp
// Disable notifications for this operation
await repository.AddAsync(entity, o => o.Notifications(false));

// Enable notifications (override if disabled at repository level)
await repository.SaveAsync(entity, o => o.Notifications(true));
```

### Pagination Options

```csharp
// Set page number and limit
await repository.FindAsync(query, o => o.PageNumber(2).PageLimit(50));

// Snapshot paging (scroll API)
await repository.FindAsync(query, o => o.SnapshotPaging());
await repository.FindAsync(query, o => o.SnapshotPagingLifetime(TimeSpan.FromMinutes(5)));

// Search-after paging
await repository.FindAsync(query, o => o.SearchAfterPaging());
await repository.FindAsync(query, o => o.SearchAfterToken("token"));
```

### Soft Delete Options

```csharp
// Include soft-deleted documents
await repository.FindAsync(query, o => o.IncludeSoftDeletes());

// Set soft delete mode
await repository.FindAsync(query, o => o.SoftDeleteMode(SoftDeleteQueryMode.All));
await repository.FindAsync(query, o => o.SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly));
await repository.FindAsync(query, o => o.SoftDeleteMode(SoftDeleteQueryMode.ActiveOnly));
```

### Version Options

```csharp
// Skip optimistic concurrency check
await repository.SaveAsync(entity, o => o.SkipVersionCheck());

// Explicitly control version checking
await repository.SaveAsync(entity, o => o.VersionCheck(false));
```

### Originals Options

```csharp
// Enable original document tracking for this operation
await repository.SaveAsync(entity, o => o.Originals(true));

// Provide original documents manually
await repository.SaveAsync(entity, o => o.AddOriginals(originalEntity));
await repository.SaveAsync(entities, o => o.AddOriginals(originalEntities));
```

### Field Selection Options

```csharp
// Include specific fields
await repository.FindAsync(query, o => o
    .Include(e => e.Id)
    .Include(e => e.Name)
    .Include(e => e.Email));

// Include using mask pattern
await repository.FindAsync(query, o => o.IncludeMask("id,name,email,address.*"));

// Exclude specific fields
await repository.FindAsync(query, o => o
    .Exclude(e => e.LargeContent)
    .Exclude(e => e.InternalData));

// Exclude using mask pattern
await repository.FindAsync(query, o => o.ExcludeMask("largeContent,internal*"));
```

### Timeout and Retry Options

```csharp
// Set query timeout
await repository.FindAsync(query, o => o.Timeout(TimeSpan.FromSeconds(30)));

// Set retry count
await repository.SaveAsync(entity, o => o.Retry(5));
```

### Query Logging Options

```csharp
// Set log level for this query
await repository.FindAsync(query, o => o.QueryLogLevel(LogLevel.Debug));
```

### Async Query Options

```csharp
// Enable async query
await repository.FindAsync(query, o => o.AsyncQuery());
await repository.FindAsync(query, o => o.AsyncQuery(
    waitTime: TimeSpan.FromSeconds(5), 
    ttl: TimeSpan.FromHours(1)));

// Get async query results by ID
await repository.FindAsync(query, o => o.AsyncQueryId(
    "query-id-123", 
    waitTime: TimeSpan.FromSeconds(30), 
    autoDelete: true));
```

### Multi-Get Error Options

```csharp
// Throw for MGET item errors that remain unresolved after any applicable fallback
await repository.GetByIdsAsync(ids, o => o.ThrowOnMultiGetErrors());

// Also bypass existing document/result cache entries for this read
await repository.GetByIdsAsync(ids, o => o
    .ThrowOnMultiGetErrors()
    .Cache(false)
    .ReadCache(false));
```

The option defaults to `false`; `ThrowOnMultiGetErrors(false)` restores the default behavior. Ordinary `found: false` results are not errors. For time-series and parent/child repositories, any applicable search fallback runs first. A document recovered by that fallback does not cause a strict-mode exception. Remaining item errors are aggregated into one `DocumentException` with ID, index, type, and reason details.

Strict reads also require one MGET response item per requested operation, in request order, with matching response IDs. Null items, mismatched or duplicate response IDs, found documents without a source, and missing documents with an unexpected source throw `DocumentException` before fallback or cache writes. Projected sources may omit their ID property; correlation uses Elasticsearch response metadata. This validation does not run for the default non-strict path.

The internal fallback does not read or write query-result cache entries. Document results and not-found markers are cached by the outer operation only after unresolved-error validation succeeds, using the caller's cache policy. A strict-mode item-error exception does not write those entries or evict pre-existing cache entries.

Strict mode is error handling, not a freshness or transaction guarantee. Existing positive document cache hits can satisfy a read without contacting Elasticsearch. Search fallback still obeys consistency and soft-delete options and is not a real-time multi-get. For cleanup or other irreversible decisions, handle `DocumentException` without treating the batch as missing, and separately account for filters, refresh visibility, and concurrent changes between validation and mutation.

### Combining Options

```csharp
await repository.FindAsync(
    q => q.FilterExpression("status:active"),
    o => o
        .ImmediateConsistency()
        .Cache("active-employees", TimeSpan.FromMinutes(5))
        .PageLimit(100)
        .Include(e => e.Id)
        .Include(e => e.Name)
        .QueryLogLevel(LogLevel.Debug));
```

## ConfigureOptions Override

Override `ConfigureOptions` to set custom defaults for all operations:

```csharp
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    private readonly string _tenantId;

    public EmployeeRepository(EmployeeIndex index, ITenantContext tenant) : base(index)
    {
        _tenantId = tenant.TenantId;
    }

    protected override ICommandOptions<Employee> ConfigureOptions(ICommandOptions<Employee> options)
    {
        options = base.ConfigureOptions(options);
        
        // Add custom defaults
        options.DefaultCacheKey($"tenant:{_tenantId}");
        
        return options;
    }
}
```

## Configuration Summary Table

| Setting | Default | Repository Level | Per-Operation |
|---------|---------|------------------|---------------|
| Consistency | `Eventual` | `DefaultConsistency` | `.Consistency()`, `.ImmediateConsistency()` |
| Cache Expiration | 5 minutes | `DefaultCacheExpiration` | `.CacheExpiresIn()` |
| Page Limit | 10 | `DefaultPageLimit` | `.PageLimit()` |
| Max Page Limit | 10000 | `MaxPageLimit` | N/A |
| Notifications | true (if bus) | `NotificationsEnabled` | `.Notifications()` |
| Originals | false | `OriginalsEnabled` | `.Originals()`, `.AddOriginals()` |
| Batch Notifications | false | `BatchNotifications` | N/A |
| Notification Delay | null | `NotificationDeliveryDelay` | N/A |
| Pipeline | null | `DefaultPipeline` | N/A |
| Auto Custom Fields | false | `AutoCreateCustomFields` | N/A |
| Query Log Level | Trace | `DefaultQueryLogLevel` | `.QueryLogLevel()` |
| Validation | true | N/A | `.SkipValidation()`, `.Validation()` |
| Soft Deletes | ActiveOnly | N/A | `.IncludeSoftDeletes()`, `.SoftDeleteMode()` |
| Version Check | true | N/A | `.SkipVersionCheck()`, `.VersionCheck()` |
| Multi-Get Errors | false | N/A | `.ThrowOnMultiGetErrors()` |

## Next Steps

- [Validation](/guide/validation) - Document validation
- [Caching](/guide/caching) - Cache configuration details
- [Message Bus](/guide/message-bus) - Notification configuration
- [Soft Deletes](/guide/soft-deletes) - Soft delete configuration
