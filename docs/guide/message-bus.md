# Message Bus

Foundatio.Repositories integrates with Foundatio's message bus to publish entity change notifications. This enables real-time updates, event-driven architectures, and distributed cache invalidation.

## Overview

When documents are added, saved, or removed, the repository publishes `EntityChanged` messages to the message bus. Other parts of your system can subscribe to these messages to react to changes.

## Configuration

### Enable Message Bus

Provide an `IMessageBus` to your Elasticsearch configuration:

```csharp
using Foundatio.Messaging;

public class MyElasticConfiguration : ElasticConfiguration
{
    public MyElasticConfiguration(
        ICacheClient cache,
        IMessageBus messageBus,
        ILoggerFactory loggerFactory)
        : base(cache: cache, messageBus: messageBus, loggerFactory: loggerFactory)
    {
        AddIndex(Employees = new EmployeeIndex(this));
    }

    public EmployeeIndex Employees { get; }
}
```

### Message Bus Implementations

```csharp
// In-memory (for development/testing)
services.AddSingleton<IMessageBus>(new InMemoryMessageBus());

// Redis
services.AddSingleton<IMessageBus>(sp =>
    new RedisMessageBus(new RedisConnection("localhost:6379")));

// RabbitMQ
services.AddSingleton<IMessageBus>(sp =>
    new RabbitMQMessageBus(new RabbitMQOptions { ConnectionString = "amqp://localhost" }));
```

## EntityChanged Message

### Message Structure

```csharp
public class EntityChanged : IHaveData
{
    public string? Type { get; set; }      // Entity type name (e.g., "Employee"); null for non-entity-specific notifications
    public string? Id { get; set; }        // Document ID; null for bulk/type-level notifications
    public ChangeType ChangeType { get; set; }  // Added, Saved, or Removed
    public IDictionary<string, object?> Data { get; set; }  // Custom data
}

public enum ChangeType : byte
{
    Added = 0,
    Saved = 1,
    Removed = 2
}
```

### When Each ChangeType Is Used

| ChangeType | Triggered By |
|------------|--------------|
| `Added` | `AddAsync` - New document created |
| `Saved` | `SaveAsync` - Document updated |
| `Saved` | `PatchAsync` - Document patched (with document ID) |
| `Saved` | `PatchAsync(Ids)` - Documents patched (one message per modified ID) |
| `Saved` | `PatchAllAsync` - Documents patched (per ID or type-level) |
| `Removed` | `RemoveAsync` - Document deleted by this call (documents that were already gone raise no removal event) |
| `Removed` | `RemoveAllAsync` - Documents deleted (per document, or one type-level message with `Id = null` when a server-side delete-by-query runs without an id filter) |
| `Removed` | Soft delete transition (`IsDeleted: false → true`) via `SaveAsync` |

### Patch Notification Behavior

Patch operations always use `ChangeType.Saved`. The `Id` field in the `EntityChanged` message depends on how the patch is invoked:

| Method | Id Field | Notes |
|--------|----------|-------|
| `PatchAsync(id, ...)` | Document ID | One message, only when the patch modified the document (a noop patch or a missing document sends none) |
| `PatchAsync(Ids, ...)` (any patch type) | Document ID | One message **per modified ID** (noop and missing IDs excluded). Documents that were patched are notified before the exception for the failed ones is thrown. A request containing one ID behaves like `PatchAsync(id, ...)` |
| `PatchAllAsync` (default) | Document ID | One message **per modified ID** (sent per-batch) |
| `PatchAllAsync` with `UseUpdateByQuery()` and explicit IDs | Document ID | One message **per ID** in the query |
| `PatchAllAsync` with `UseUpdateByQuery()` and a filter-only query | `null` | Single type-level notification |

::: info
`PatchAllAsync` sends notifications incrementally per batch as documents are processed. Subscribers may receive messages while the operation is still running. Design handlers to be idempotent.
:::

If an opt-in update-by-query task has an unknown outcome (for example, its status disappears or submission times out), the repository sends query-level notifications before throwing. These notifications tell subscribers to refresh; they do not confirm that any particular document changed.

::: warning
Only `PatchAllAsync` with the opt-in `o.UseUpdateByQuery()` and a filter-only query (no explicit IDs) sends an `EntityChanged` message with `Id = null`. Every other `PatchAllAsync` path sends per-ID notifications for each modified document. Subscribers that depend on `msg.Id` should handle the `null` case if your code opts into update-by-query, for example by re-querying the affected documents.
:::

**In-process events:** The `DocumentsChanged` event fires for every patch that modified at least one document. `args.Documents` holds the patched documents for patches applied client-side (`JsonPatch`, `ActionPatch` and `PartialPatch` with a default pipeline) in `PatchAsync(id)`, `PatchAsync(Ids)` and each `PatchAllAsync` batch. It is **empty** for patches Elasticsearch applies server-side (`ScriptPatch`, and `PartialPatch` without a default pipeline), because the modified document is not available client-side, and for `PatchAllAsync` with `UseUpdateByQuery()`. The `DocumentsSaving` and `DocumentsSaved` events do **not** fire for patch operations.

**`UpdateCallback`:** The `UpdateCallback` command option receives successful IDs together after each multi-ID `PatchAsync(Ids)` call or `PatchAllAsync` batch, before an exception for failed documents is thrown. If a pipeline script response reports changes together with a failure, those IDs are reported separately, including on single-ID calls. Ordinary successful `PatchAsync(id)` calls and inputs containing exactly one ID do not invoke the callback. Neither does `PatchAllAsync` with `UseUpdateByQuery()`, or a call that modified nothing.

**Custom publishing:** Multi-ID patches and patch batches use the virtual `PublishChangeTypeMessageAsync` hooks. Client-side patches pass the patched document; server-side patches pass its ID. Overrides that enrich or suppress messages continue to apply, together with `Notifications(false)` and the configured notification delay.

**Soft-delete detection:** Patch operations do not detect `IsDeleted` transitions. The `ChangeType` is always `Saved`, even if a patch sets `IsDeleted = true`. Use `SaveAsync` with `OriginalsEnabled = true` for soft-delete transition detection.

### Soft Delete Notification Logic

`SaveAsync` pairs each document with its original by concrete index, routing and ID. Documents that share an ID across indexes or routing values therefore retain their own change events and soft-delete transitions. With `Originals()` enabled, routed and multi-index repositories read those exact locations; supplied originals avoid the corresponding reads. After a partial write failure, saved events and notifications include only the locations that succeeded.

When a document supports soft deletes (`ISupportSoftDeletes`), the notification system intelligently determines the `ChangeType`:

```csharp
// If IsDeleted transitions from false to true:
// ChangeType = Removed (not Saved)

// This requires OriginalsEnabled = true to detect the transition
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeRepository(EmployeeIndex index) : base(index)
    {
        OriginalsEnabled = true;  // Required for soft delete detection
    }
}
```

**Example:**

```csharp
var employee = await repository.GetByIdAsync(id);
employee.IsDeleted = true;
await repository.SaveAsync(employee);

// EntityChanged message:
// - Type: "Employee"
// - Id: employee.Id
// - ChangeType: Removed (not Saved!)
```

## Subscribing to Notifications

### Basic Subscription

```csharp
await messageBus.SubscribeAsync<EntityChanged>(async (msg, ct) =>
{
    Console.WriteLine($"{msg.Type} {msg.ChangeType}: {msg.Id}");
});
```

### Filter by Entity Type

```csharp
await messageBus.SubscribeAsync<EntityChanged>(async (msg, ct) =>
{
    if (msg.Type == nameof(Employee))
    {
        switch (msg.ChangeType)
        {
            case ChangeType.Added:
                await OnEmployeeAdded(msg.Id);
                break;
            case ChangeType.Saved:
                await OnEmployeeUpdated(msg.Id);
                break;
            case ChangeType.Removed:
                await OnEmployeeRemoved(msg.Id);
                break;
        }
    }
});
```

### Real-Time UI Updates

```csharp
public class EmployeeHub : Hub
{
    private readonly IMessageBus _messageBus;

    public EmployeeHub(IMessageBus messageBus)
    {
        _messageBus = messageBus;
    }

    public override async Task OnConnectedAsync()
    {
        await _messageBus.SubscribeAsync<EntityChanged>(async (msg, ct) =>
        {
            if (msg.Type == nameof(Employee))
            {
                await Clients.All.SendAsync("EmployeeChanged", new
                {
                    Id = msg.Id,
                    ChangeType = msg.ChangeType.ToString()
                });
            }
        });

        await base.OnConnectedAsync();
    }
}
```

## Repository Configuration

### NotificationsEnabled

Enable or disable notifications at the repository level:

```csharp
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeRepository(EmployeeIndex index) : base(index)
    {
        NotificationsEnabled = true;  // Default: true if message bus configured
    }
}
```

### NotificationDeliveryDelay

Delay notification delivery to allow Elasticsearch indexing to complete:

```csharp
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeRepository(EmployeeIndex index) : base(index)
    {
        NotificationDeliveryDelay = TimeSpan.FromSeconds(2);
    }
}
```

::: warning
Only set a delay if your message bus implementation supports delayed delivery. Message buses that don't support delayed delivery may silently drop messages. The in-memory message bus supports delayed delivery.
:::

### BatchNotifications

Batch multiple notifications together:

```csharp
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeRepository(EmployeeIndex index) : base(index)
    {
        BatchNotifications = true;
    }
}
```

### Per-Operation Control

Disable notifications for specific operations:

```csharp
// Disable notifications for bulk import
await repository.AddAsync(employees, o => o.Notifications(false));

// Disable notifications for internal updates
await repository.SaveAsync(employee, o => o.Notifications(false));
```

## BeforePublishEntityChanged Event

Intercept and modify notifications before they're published.

### Event Arguments

```csharp
public class BeforePublishEntityChangedEventArgs<T> : CancelEventArgs
{
    public EntityChanged Message { get; }
    public IReadOnlyRepository<T> Repository { get; }
    // Inherited: bool Cancel { get; set; }
}
```

### Intercepting Notifications

```csharp
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeRepository(EmployeeIndex index) : base(index)
    {
        BeforePublishEntityChanged.AddHandler(OnBeforePublish);
    }

    private Task OnBeforePublish(object sender, BeforePublishEntityChangedEventArgs<Employee> args)
    {
        // Add custom data to the notification
        args.Message.Data["TenantId"] = _tenantId;
        args.Message.Data["ModifiedBy"] = _currentUserId;

        return Task.CompletedTask;
    }
}
```

### Canceling Notifications

```csharp
BeforePublishEntityChanged.AddHandler((sender, args) =>
{
    // Don't publish notifications for internal system changes
    if (args.Message.Data.ContainsKey("internal"))
    {
        args.Cancel = true;
    }

    // Don't publish for certain entity states
    // (Note: You'd need to fetch the document to check this)

    return Task.CompletedTask;
});
```

### Adding Custom Data

```csharp
BeforePublishEntityChanged.AddHandler((sender, args) =>
{
    // Add metadata to all notifications
    args.Message.Data["Timestamp"] = DateTime.UtcNow;
    args.Message.Data["Source"] = Environment.MachineName;

    return Task.CompletedTask;
});
```

## In-Process Events vs Message Bus

Foundatio.Repositories provides two notification mechanisms:

### In-Process Events

Synchronous events fired within the same process:

```csharp
repository.DocumentsChanged.AddHandler(async (sender, args) =>
{
    // Runs in the same process, same transaction context
    foreach (var doc in args.Documents)
    {
        await UpdateRelatedDataAsync(doc.Value);
    }
});
```

**Use for:**
- Cache invalidation
- Validation
- Local side effects
- Audit logging within the same service

### Message Bus Notifications

Distributed messages published to `IMessageBus`:

```csharp
await messageBus.SubscribeAsync<EntityChanged>(async (msg, ct) =>
{
    // Runs in any subscribed process
    await RefreshCacheAsync(msg.Id);
});
```

**Use for:**
- Cross-service communication
- Real-time UI updates
- Event-driven architectures
- Distributed cache invalidation

### Comparison

| Aspect | In-Process Events | Message Bus |
|--------|-------------------|-------------|
| Scope | Same process | Distributed |
| Timing | Synchronous | Asynchronous |
| Reliability | Guaranteed | Depends on bus |
| Use Case | Local side effects | Cross-service |
| Access to Document | Full document for `SaveAsync` and client-side patches (`JsonPatch`, `ActionPatch`, pipeline `PartialPatch`); empty list for server-side patches | ID only (or `null` for type-level) |

## Distributed Cache Invalidation

Use message bus notifications to invalidate cache across instances:

```csharp
public class CacheInvalidationService : IHostedService
{
    private readonly IMessageBus _messageBus;
    private readonly IEmployeeRepository _repository;

    public CacheInvalidationService(IMessageBus messageBus, IEmployeeRepository repository)
    {
        _messageBus = messageBus;
        _repository = repository;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _messageBus.SubscribeAsync<EntityChanged>(async (msg, ct) =>
        {
            if (msg.Type == nameof(Employee))
            {
                // Invalidate local cache when other instances make changes
                await _repository.InvalidateCacheAsync(msg.Id);
            }
        }, cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
```

## Event Sourcing Pattern

Use notifications to build event-sourced systems:

```csharp
public class EmployeeEventStore
{
    private readonly IMessageBus _messageBus;
    private readonly IEventRepository _eventRepository;

    public EmployeeEventStore(IMessageBus messageBus, IEventRepository eventRepository)
    {
        _messageBus = messageBus;
        _eventRepository = eventRepository;
    }

    public async Task StartAsync()
    {
        await _messageBus.SubscribeAsync<EntityChanged>(async (msg, ct) =>
        {
            if (msg.Type == nameof(Employee))
            {
                await _eventRepository.AddAsync(new EmployeeEvent
                {
                    EmployeeId = msg.Id,
                    EventType = msg.ChangeType.ToString(),
                    Timestamp = DateTime.UtcNow,
                    Data = msg.Data
                });
            }
        });
    }
}
```

## Notification Flow Diagram

```mermaid
sequenceDiagram
    participant App as Application
    participant Repo as Repository
    participant ES as Elasticsearch
    participant Bus as Message Bus
    participant Sub as Subscribers

    App->>Repo: SaveAsync(employee)
    Repo->>ES: Index document
    ES-->>Repo: Success
    Repo->>Repo: BeforePublishEntityChanged
    alt Not Cancelled
        Repo->>Bus: Publish EntityChanged
        Bus->>Sub: Deliver to subscribers
        Sub->>Sub: Process notification
    end
    Repo-->>App: Return result
```

## Best Practices

### 1. Use OriginalsEnabled for Soft Delete Detection

```csharp
OriginalsEnabled = true;  // Detect IsDeleted transitions
```

### 2. Add Custom Data for Context

```csharp
BeforePublishEntityChanged.AddHandler((sender, args) =>
{
    args.Message.Data["UserId"] = _currentUserId;
    args.Message.Data["TenantId"] = _tenantId;
    return Task.CompletedTask;
});
```

### 3. Handle Notification Failures Gracefully

```csharp
await messageBus.SubscribeAsync<EntityChanged>(async (msg, ct) =>
{
    try
    {
        await ProcessNotificationAsync(msg);
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "Failed to process notification for {Type} {Id}",
            msg.Type, msg.Id);
        // Don't rethrow - allow other subscribers to process
    }
});
```

### 4. Use NotificationDeliveryDelay for Eventual Consistency

```csharp
NotificationDeliveryDelay = TimeSpan.FromSeconds(1);
```

## Next Steps

- [Caching](/guide/caching) - Distributed cache invalidation
- [Configuration](/guide/configuration) - Notification configuration
- [Repository Pattern](/guide/repository-pattern) - In-process events
