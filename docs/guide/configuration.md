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
await repository.GetByIdAsync(id, o => o.ReadCache());

// Disable caching for this operation
await repository.GetByIdAsync(id, o => o.Cache(false));
```

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

## Next Steps

- [Validation](/guide/validation) - Document validation
- [Caching](/guide/caching) - Cache configuration details
- [Message Bus](/guide/message-bus) - Notification configuration
- [Soft Deletes](/guide/soft-deletes) - Soft delete configuration
