# Soft Deletes

Foundatio.Repositories provides built-in soft delete support, allowing you to mark documents as deleted without physically removing them. This guide covers the soft delete behavior across all repository APIs.

## Overview

Soft deletes allow you to:
- Mark documents as deleted without permanent removal
- Restore deleted documents
- Query deleted documents when needed
- Maintain audit trails and data recovery options

## Enabling Soft Deletes

Implement `ISupportSoftDeletes` on your entity:

```csharp
using Foundatio.Repositories.Models;

public class Employee : IIdentity, IHaveDates, ISupportSoftDeletes
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public DateTime CreatedUtc { get; set; }
    public DateTime UpdatedUtc { get; set; }
    public bool IsDeleted { get; set; }  // Required by ISupportSoftDeletes
}
```

The repository automatically:
- Detects `ISupportSoftDeletes` implementation
- Adds `IsDeleted` to the index mapping
- Filters queries based on `SoftDeleteQueryMode`

## SoftDeleteQueryMode

Control how soft-deleted documents are handled in queries:

```csharp
public enum SoftDeleteQueryMode
{
    ActiveOnly,   // Only IsDeleted = false (default)
    DeletedOnly,  // Only IsDeleted = true
    All           // All documents regardless of IsDeleted
}
```

### Setting the Mode

```csharp
// Include soft-deleted documents
var results = await repository.FindAsync(query, o => o.IncludeSoftDeletes());

// Only deleted documents
var results = await repository.FindAsync(query, o => o.SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly));

// Explicitly active only (default)
var results = await repository.FindAsync(query, o => o.SoftDeleteMode(SoftDeleteQueryMode.ActiveOnly));
```

## Soft Delete vs Hard Delete

### Soft Delete

Mark a document as deleted (recoverable):

```csharp
var employee = await repository.GetByIdAsync(id);
employee.IsDeleted = true;
await repository.SaveAsync(employee);
```

### Hard Delete

Permanently remove a document (not recoverable):

```csharp
await repository.RemoveAsync(id);
// or
await repository.RemoveAsync(employee);
```

## API Behavior Reference

### Read Operations

| API | Default Behavior | Respects SoftDeleteMode | Notes |
|-----|------------------|------------------------|-------|
| `GetByIdAsync` | Filters deleted | Yes | Returns `null` for soft-deleted |
| `GetByIdsAsync` | Filters deleted | Yes | Excludes soft-deleted from results |
| `GetAllAsync` | Filters deleted | Yes | Delegates to `FindAsync` |
| `FindAsync` | Filters deleted | Yes | Uses `SoftDeletesQueryBuilder` |
| `FindOneAsync` | Filters deleted | Yes | Uses `SoftDeletesQueryBuilder` |
| `CountAsync` | Filters deleted | Yes | Only counts active documents |
| `ExistsAsync` | Filters deleted | Yes | Uses search for soft-delete models |

### Write Operations

| API | Behavior | Notes |
|-----|----------|-------|
| `AddAsync` | N/A | New documents typically have `IsDeleted = false` |
| `SaveAsync` | Use for soft delete | Set `IsDeleted = true` and save |
| `RemoveAsync` | **HARD DELETE** | Permanently removes document |
| `RemoveAllAsync` | **HARD DELETE** | Permanently removes matching documents |
| `PatchAsync` | No filtering | Operates directly by ID |
| `PatchAllAsync` | Filters deleted | Query respects `SoftDeleteMode` |

## Detailed API Behavior

### GetByIdAsync

```csharp
// Default: Returns null for soft-deleted documents
var employee = await repository.GetByIdAsync(id);

// Include soft-deleted
var employee = await repository.GetByIdAsync(id, o => o.IncludeSoftDeletes());

// Only if deleted
var employee = await repository.GetByIdAsync(id, o => o.SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly));
```

### GetByIdsAsync

```csharp
var ids = new[] { "emp-1", "emp-2", "emp-3" };

// Default: Excludes soft-deleted
var employees = await repository.GetByIdsAsync(ids);

// Include all
var employees = await repository.GetByIdsAsync(ids, o => o.IncludeSoftDeletes());
```

### FindAsync

```csharp
// Default: Only active documents
var results = await repository.FindAsync(q => q.FilterExpression("department:Engineering"));

// Include soft-deleted
var results = await repository.FindAsync(
    q => q.FilterExpression("department:Engineering"),
    o => o.IncludeSoftDeletes());

// Only deleted
var results = await repository.FindAsync(
    q => q.FilterExpression("department:Engineering"),
    o => o.SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly));
```

### CountAsync

```csharp
// Count active only
long activeCount = await repository.CountAsync();

// Count all including deleted
var result = await repository.CountAsync(null, o => o.IncludeSoftDeletes());
long totalCount = result.Total;

// Count deleted only
var result = await repository.CountAsync(null, o => o.SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly));
long deletedCount = result.Total;
```

### ExistsAsync

```csharp
// Check if active document exists
bool exists = await repository.ExistsAsync(id);

// Check if document exists (including deleted)
bool exists = await repository.ExistsAsync(id, o => o.IncludeSoftDeletes());
```

### RemoveAsync (Hard Delete)

::: warning
`RemoveAsync` performs a **hard delete** - the document is permanently removed from Elasticsearch.
:::

```csharp
// Permanently delete
await repository.RemoveAsync(id);
await repository.RemoveAsync(employee);
await repository.RemoveAsync(employees);
```

### RemoveAllAsync (Hard Delete)

```csharp
// Permanently delete all matching (respects SoftDeleteMode for finding)
long deleted = await repository.RemoveAllAsync(
    q => q.FieldEquals(e => e.Status, "inactive"));

// Delete including soft-deleted
long deleted = await repository.RemoveAllAsync(
    q => q.FieldEquals(e => e.Status, "inactive"),
    o => o.IncludeSoftDeletes());
```

### PatchAsync

Patch operations work on documents regardless of soft delete status:

```csharp
// This will patch even if document is soft-deleted
await repository.PatchAsync(id, new PartialPatch(new { Name = "Updated" }));
```

### PatchAllAsync

Query respects `SoftDeleteMode`:

```csharp
// Only patches active documents
await repository.PatchAllAsync(
    q => q.FieldEquals(e => e.Department, "Sales"),
    new PartialPatch(new { Region = "West" }));

// Patch including soft-deleted
await repository.PatchAllAsync(
    q => q.FieldEquals(e => e.Department, "Sales"),
    new PartialPatch(new { Region = "West" }),
    o => o.IncludeSoftDeletes());
```

## Soft Delete Operations

### Soft Delete a Document

```csharp
var employee = await repository.GetByIdAsync(id);
employee.IsDeleted = true;
await repository.SaveAsync(employee);
```

### Restore a Soft-Deleted Document

```csharp
// Get the deleted document
var employee = await repository.GetByIdAsync(id, o => o.SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly));

// Restore it
employee.IsDeleted = false;
await repository.SaveAsync(employee);
```

### Bulk Soft Delete

```csharp
// Using PatchAllAsync
await repository.PatchAllAsync(
    q => q.FieldEquals(e => e.Department, "Closed"),
    new PartialPatch(new { IsDeleted = true }));

// Or using BatchProcessAsync
await repository.BatchProcessAsync(
    q => q.FieldEquals(e => e.Department, "Closed"),
    async batch =>
    {
        foreach (var emp in batch.Documents)
        {
            emp.IsDeleted = true;
        }
        await repository.SaveAsync(batch.Documents);
        return true;
    });
```

### Bulk Restore

```csharp
await repository.PatchAllAsync(
    q => q.FieldEquals(e => e.Department, "Reopened"),
    new PartialPatch(new { IsDeleted = false }),
    o => o.SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly));
```

## Parent-Child Soft Delete Filtering

When using parent-child relationships, children are automatically filtered when their parent is soft-deleted:

```csharp
// Parent is soft-deleted
parent.IsDeleted = true;
await parentRepository.SaveAsync(parent);

// Children are now filtered out (even though they're not deleted)
var children = await childRepository.FindAsync(q => q.ParentId(parent.Id));
// Returns empty - children are filtered because parent is deleted

// Restore parent
parent.IsDeleted = false;
await parentRepository.SaveAsync(parent);

// Children are now visible again
var children = await childRepository.FindAsync(q => q.ParentId(parent.Id));
// Returns children
```

## EntityChanged Notifications

When a document is soft-deleted, the notification system sends `ChangeType.Removed`:

```csharp
// Enable originals tracking (required for soft delete detection)
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeRepository(EmployeeIndex index) : base(index)
    {
        OriginalsEnabled = true;
    }
}

// When soft-deleting:
employee.IsDeleted = true;
await repository.SaveAsync(employee);

// EntityChanged message:
// - ChangeType: Removed (not Saved!)
// - Id: employee.Id
// - Type: "Employee"
```

::: tip
Set `OriginalsEnabled = true` in your repository to enable soft delete transition detection. Without this, soft deletes will send `ChangeType.Saved` instead of `ChangeType.Removed`.
:::

## Cache Behavior

The repository maintains a special cache list to handle eventual consistency:

### How It Works

1. When a document is soft-deleted, its ID is added to a `"deleted"` list in cache
2. The list has a 30-second TTL
3. Queries automatically exclude IDs in the `"deleted"` list

```csharp
// When you soft-delete:
employee.IsDeleted = true;
await repository.SaveAsync(employee);

// Internally:
// 1. Document is indexed to Elasticsearch
// 2. ID is added to "deleted" cache list (30s TTL)
// 3. Subsequent queries exclude this ID from results
```

### Purpose

This handles the eventual consistency window where:
1. Document is soft-deleted
2. Elasticsearch hasn't indexed the change yet
3. Cache knows about the deletion
4. Queries correctly exclude the document

After 30 seconds, Elasticsearch should have indexed the change, and the cache entry expires.

## Query Filtering Implementation

The `SoftDeletesQueryBuilder` automatically adds filters to queries:

```csharp
// For ActiveOnly mode:
// Adds: { "term": { "isDeleted": false } }

// For DeletedOnly mode:
// Adds: { "term": { "isDeleted": true } }

// For All mode:
// No filter added
```

## Common Patterns

### Audit Trail

```csharp
public class Employee : IIdentity, IHaveDates, ISupportSoftDeletes
{
    public string Id { get; set; }
    public bool IsDeleted { get; set; }
    public DateTime? DeletedUtc { get; set; }
    public string DeletedBy { get; set; }
    // ... other properties
}

// When soft-deleting:
employee.IsDeleted = true;
employee.DeletedUtc = DateTime.UtcNow;
employee.DeletedBy = currentUserId;
await repository.SaveAsync(employee);
```

### Scheduled Hard Delete

```csharp
// Delete documents that have been soft-deleted for more than 30 days
var cutoffDate = DateTime.UtcNow.AddDays(-30);

await repository.RemoveAllAsync(
    q => q
        .FieldEquals(e => e.IsDeleted, true)
        .DateRange(null, cutoffDate, e => e.DeletedUtc),
    o => o.IncludeSoftDeletes());
```

### Recycle Bin UI

```csharp
// Get deleted items for recycle bin
var deletedItems = await repository.FindAsync(
    q => q.SortExpression("-deletedUtc"),
    o => o.SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly).PageLimit(50));

// Restore selected items
foreach (var id in selectedIds)
{
    await repository.PatchAsync(id, new PartialPatch(new { IsDeleted = false }));
}

// Permanently delete selected items
foreach (var id in selectedIds)
{
    var item = await repository.GetByIdAsync(id, o => o.SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly));
    if (item != null)
        await repository.RemoveAsync(item);
}
```

## Summary Table

| Operation | Soft-Deleted Documents | Notes |
|-----------|----------------------|-------|
| `GetByIdAsync` | Filtered by default | Use `IncludeSoftDeletes()` to include |
| `GetByIdsAsync` | Filtered by default | Use `IncludeSoftDeletes()` to include |
| `FindAsync` | Filtered by default | Use `SoftDeleteMode()` to control |
| `CountAsync` | Filtered by default | Use `SoftDeleteMode()` to control |
| `ExistsAsync` | Filtered by default | Use `IncludeSoftDeletes()` to include |
| `SaveAsync` | Use to soft delete | Set `IsDeleted = true` |
| `RemoveAsync` | **Hard deletes** | Permanently removes |
| `RemoveAllAsync` | **Hard deletes** | Query respects mode |
| `PatchAsync` | No filtering | Works on any document |
| `PatchAllAsync` | Query filtered | Use `IncludeSoftDeletes()` |

## Next Steps

- [Message Bus](/guide/message-bus) - Soft delete notifications
- [Caching](/guide/caching) - Soft delete cache behavior
- [Configuration](/guide/configuration) - Soft delete configuration
