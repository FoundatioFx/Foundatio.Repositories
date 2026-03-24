# Patch Operations

Foundatio.Repositories provides flexible patch operations for partial document updates without fetching the full document. This guide covers all patch types and their use cases.

## Overview

Patch operations allow you to:
- Update specific fields without loading the entire document
- Execute atomic updates (counters, arrays)
- Apply bulk updates efficiently
- Reduce network traffic and conflicts

## Automatic Date Tracking

For models implementing `IHaveDates`, all patch operations automatically set `UpdatedUtc` to the current time. This is consistent with how `AddAsync` and `SaveAsync` handle date tracking.

- **`UpdatedUtc`** is set automatically on every patch operation — no manual intervention needed
- **`CreatedUtc`** is not overwritten by patch operations under normal circumstances; it may be corrected if missing or invalid (for example, `DateTime.MinValue` or a future value), consistent with `SetDates` behavior
- Works across all patch types: `PartialPatch`, `ScriptPatch`, `JsonPatch`, and `ActionPatch`

### Caller-provided UpdatedUtc

For `ScriptPatch` and `PartialPatch`, if you explicitly provide `updatedUtc` in your script parameters or partial document, the repository respects your value and skips auto-injection. This is logged at `Debug` level.

For `JsonPatch` and `ActionPatch`, `UpdatedUtc` is always set by the framework after applying your changes — matching `SaveAsync` semantics. If you need explicit control over the timestamp, use `ScriptPatch` or `PartialPatch`.

::: warning Stale timestamps on server-side retries
For `ScriptPatch` and `PartialPatch`, the `UpdatedUtc` timestamp is captured once when the request is built and is fixed for any server-side retries (`RetryOnConflict`). If a version conflict causes Elasticsearch to retry the update, the retried write carries the **original timestamp**, not a fresh one. This means a document updated after several retries will have an `UpdatedUtc` that is slightly older than the actual commit time. For `JsonPatch` and `ActionPatch`, the timestamp is refreshed on each client-side retry since the entire operation (fetch + mutate + index) re-executes.

This matches `SaveAsync` behavior, where `SetDates` is called once before the Elasticsearch request. In most scenarios the retry window is milliseconds, but callers relying on `UpdatedUtc` for strict ordering or audit trails should be aware of this.
:::

### Custom Date Fields

If your model uses custom date fields instead of `IHaveDates` (e.g., a nested `MetaData.DateUpdatedUtc` property), you can opt into automatic date tracking by overriding three virtual hooks on your repository:

```csharp
public class MyRepository : ElasticRepositoryBase<MyEntity>
{
    protected override bool HasDateTracking => true;

    protected override string GetUpdatedUtcFieldPath()
    {
        return InferField(d => ((IHaveDateMetaData)d).MetaData.DateUpdatedUtc);
    }

    protected override void SetDocumentDates(MyEntity document, TimeProvider timeProvider)
    {
        base.SetDocumentDates(document, timeProvider);

        if (document is IHaveDateMetaData metaDoc)
        {
            var utcNow = timeProvider.GetUtcNow().UtcDateTime;
            metaDoc.MetaData ??= new DateMetaData();

            if (metaDoc.MetaData.DateCreatedUtc is null
                || metaDoc.MetaData.DateCreatedUtc == DateTime.MinValue
                || metaDoc.MetaData.DateCreatedUtc > utcNow)
                metaDoc.MetaData.DateCreatedUtc = utcNow;

            metaDoc.MetaData.DateUpdatedUtc = utcNow;
        }
    }
}
```

| Hook | Purpose | Default Behavior |
|------|---------|-----------------|
| `HasDateTracking` | Gate for all date tracking logic | `true` when `T` implements `IHaveDates` |
| `GetUpdatedUtcFieldPath()` | Returns the Elasticsearch field path for the updated timestamp | Returns the inferred `UpdatedUtc` field name. Throws `RepositoryException` if `HasDateTracking` is `true` but no field path is available |
| `SetDocumentDates(T, TimeProvider)` | Sets date properties on the C# object (used by Add, Save, ActionPatch, JsonPatch bulk) | Sets `CreatedUtc` and `UpdatedUtc` on `IHaveDates` models |

The `ApplyDateTracking` overloads for `ScriptPatch`, `PartialPatch`, and `JsonNode` are also virtual and can be overridden for full control over how dates are injected into each patch type. For nested fields, the script parameter key uses the last segment of the field path (e.g., `dateUpdatedUtc` for `metaData.dateUpdatedUtc`).

## Patch Types

### PartialPatch

Update specific fields with new values:

```csharp
// Update single field
await repository.PatchAsync(id, new PartialPatch(new { Name = "John Smith" }));

// Update multiple fields
await repository.PatchAsync(id, new PartialPatch(new 
{ 
    Name = "John Smith",
    Age = 32,
    Department = "Engineering"
}));

// Update nested object
await repository.PatchAsync(id, new PartialPatch(new 
{ 
    Address = new { City = "Seattle", State = "WA" }
}));
```

### ScriptPatch

Use Elasticsearch Painless scripts for complex updates:

```csharp
// Increment a counter
await repository.PatchAsync(id, new ScriptPatch("ctx._source.viewCount++"));

// Increment with parameter
await repository.PatchAsync(id, new ScriptPatch("ctx._source.counter += params.amount")
{
    Params = new Dictionary<string, object> { ["amount"] = 5 }
});

// Conditional update
await repository.PatchAsync(id, new ScriptPatch(@"
    if (ctx._source.status == 'pending') {
        ctx._source.status = 'approved';
        ctx._source.approvedAt = params.now;
    }
")
{
    Params = new Dictionary<string, object> { ["now"] = DateTime.UtcNow }
});

// Array operations
await repository.PatchAsync(id, new ScriptPatch("ctx._source.tags.add(params.tag)")
{
    Params = new Dictionary<string, object> { ["tag"] = "featured" }
});

// Remove from array
await repository.PatchAsync(id, new ScriptPatch("ctx._source.tags.remove(ctx._source.tags.indexOf(params.tag))")
{
    Params = new Dictionary<string, object> { ["tag"] = "draft" }
});
```

### JsonPatch

RFC 6902 JSON Patch operations:

```csharp
using Foundatio.Repositories.JsonPatch;

// Replace operation
var patch = new PatchDocument(
    new ReplaceOperation { Path = "name", Value = "John Smith" }
);
await repository.PatchAsync(id, new JsonPatch(patch));

// Multiple operations
var patch = new PatchDocument(
    new ReplaceOperation { Path = "name", Value = "John Smith" },
    new AddOperation { Path = "tags/-", Value = "senior" },
    new RemoveOperation { Path = "tempField" }
);
await repository.PatchAsync(id, new JsonPatch(patch));
```

#### JSON Patch Operations

| Operation | Description | Example |
|-----------|-------------|---------|
| `AddOperation` | Add value at path | `{ Path = "tags/-", Value = "new" }` |
| `RemoveOperation` | Remove value at path | `{ Path = "tempField" }` |
| `ReplaceOperation` | Replace value at path | `{ Path = "name", Value = "New Name" }` |
| `MoveOperation` | Move value from one path to another | `{ From = "oldPath", Path = "newPath" }` |
| `CopyOperation` | Copy value from one path to another | `{ From = "source", Path = "dest" }` |
| `TestOperation` | Test value at path (fails if not equal) | `{ Path = "status", Value = "active" }` |

### ActionPatch

Lambda-based patching for strongly-typed updates. Supports both `Action<T>` (always writes) and `Func<T, bool>` (conditional write based on return value):

```csharp
// Action<T> — always treated as a modification
await repository.PatchAsync(id, new ActionPatch<Employee>(e =>
{
    e.Name = "John Smith";
    e.Age = 32;
}));

// Func<T, bool> — return false to skip the write
bool modified = await repository.PatchAsync(id, new ActionPatch<Employee>(e =>
{
    if (e.Status == EmployeeStatus.Active)
        return false;

    e.Status = EmployeeStatus.Active;
    e.UpdatedBy = currentUserId;
    return true;
}));
```

::: tip
`ActionPatch` fetches the document, applies the lambda, and saves it. For true partial updates without fetching, use `PartialPatch` or `ScriptPatch`.
:::

## Return Values

All `PatchAsync` overloads return status information:

- **`PatchAsync(Id, ...)`** returns `Task<bool>` -- `true` if the document was modified, `false` if the operation was treated as a no-op.
  - For `PartialPatch`, Elasticsearch's automatic noop detection reports `false` when the update does not change any field values (e.g., setting a field to its current value). However, models implementing `IHaveDates` have `UpdatedUtc` injected automatically on partial updates, so most `PartialPatch` calls will return `true` unless date tracking is disabled or the caller explicitly supplies an `UpdatedUtc` value that does not change.
  - For `ScriptPatch`, the operation is only a no-op when the script explicitly sets `ctx.op = 'none'`; simply reassigning the same value in a script is treated as a modification by Elasticsearch. The automatic date tracking script is appended after your script, but Elasticsearch evaluates `ctx.op` at the end of execution — so `ctx.op = 'none'` correctly prevents the write even with the appended timestamp assignment.
  - For `ActionPatch`, noop detection depends on the overload used. The `Func<T, bool>` overload respects the return value — `false` skips the Elasticsearch write entirely (no Index API call, no date tracking, no cache invalidation). The document is still fetched and the delegate executes, but the reindex step is avoided. The `Action<T>` overload always assumes the document was modified. Empty actions (no callbacks) return `false`.
  - For `JsonPatch`, operations always return `true` (the get-modify-reindex pattern always writes). Empty operations (no patches) return `false`.
- **`PatchAsync(Ids, ...)`** returns `Task<long>` -- the number of documents actually modified (excludes no-ops as reported by the backend).
- **`PatchAllAsync(...)`** returns `Task<long>` — the number of documents modified by the query.

Errors (document not found, version conflicts) throw exceptions rather than returning a status value. See [Error Handling](#error-handling) for details.

::: warning Noop Detection Limitations
- **Date tracking**: Models implementing `IHaveDates` have `UpdatedUtc` set automatically on partial updates. This injected timestamp change means `PartialPatch` will almost always report `true` even when no other field values changed.
- **Script patches**: Elasticsearch does not detect noops for script-based updates automatically. Your script must explicitly set `ctx.op = 'none'` to signal a no-op. See [Script Noop Example](#script-noop-example) below.
- **JsonPatch**: Uses a get-modify-reindex pattern (Index API), so a write always occurs and `true` is always returned.
- **ActionPatch (`Action<T>`)**: The `Action<T>` overload always assumes the document was modified. Use the `Func<T, bool>` overload and return `false` from your function to skip the reindex step for unchanged documents.
:::

### Script Noop Example

Use `ctx.op = 'none'` in your Painless script to conditionally skip the update. Elasticsearch evaluates `ctx.op` after the entire script executes, so placing it anywhere in the script works — even when automatic date tracking appends a timestamp assignment.

> **Note:** Painless uses `==` for equality comparison (Java-style), not `===` (JavaScript-style). The `===` operator is not valid in Painless.

```csharp
// Only update if the value actually changes
bool modified = await repository.PatchAsync(id, new ScriptPatch(
    """
    if (ctx._source.status == params.newStatus) {
        ctx.op = 'none';
    } else {
        ctx._source.status = params.newStatus;
    }
    """)
{
    Params = new Dictionary<string, object> { ["newStatus"] = "active" }
});

// modified == false when the document already had status == "active"
```

For bulk operations, noop documents are excluded from the modified count:

```csharp
long modifiedCount = await repository.PatchAsync(
    new Ids(id1, id2, id3),
    new ScriptPatch(
        """
        if (ctx._source.status == params.newStatus) {
            ctx.op = 'none';
        } else {
            ctx._source.status = params.newStatus;
        }
        """)
    {
        Params = new Dictionary<string, object> { ["newStatus"] = "active" }
    });

// modifiedCount excludes documents that were already "active"
```

### ActionPatch Noop Detection

`ActionPatch` supports two modes — fire-and-forget with `Action<T>`, or explicit noop signaling with `Func<T, bool>`. The `Func<T, bool>` overload lets your action return `false` to signal a no-op. When all actions return `false`, the write to Elasticsearch is skipped entirely — no Index API call, no date tracking update, no cache invalidation. The document is still fetched and the delegate executes, but the expensive reindex step is avoided.

The `Action<T>` overload always assumes the document was modified (returns `true` internally). Use `Func<T, bool>` when you want to conditionally skip writes:

```csharp
// Action<T> — always treated as modified (backward compatible)
await repository.PatchAsync(id,
    new ActionPatch<Employee>(e => e.Name = "Alice"));

// Func<T, bool> — return false to skip the write when nothing changed
bool modified = await repository.PatchAsync(id,
    new ActionPatch<Employee>(e =>
    {
        if (String.Equals(e.Name, "Alice", StringComparison.Ordinal))
            return false;

        e.Name = "Alice";
        return true;
    }));

// modified == false when the employee's name was already "Alice"
```

For bulk operations (`PatchAsync(Ids, ...)` and `PatchAllAsync`), the same pattern applies — documents where all actions return `false` are skipped entirely and not sent to Elasticsearch.

## Patching Multiple Documents

### Patch by IDs

```csharp
var ids = new[] { "emp-1", "emp-2", "emp-3" };

// Partial patch
await repository.PatchAsync(ids, new PartialPatch(new { Department = "Engineering" }));

// Script patch
await repository.PatchAsync(ids, new ScriptPatch("ctx._source.reviewCount++"));
```

### Patch by Query (PatchAllAsync)

For `ISearchableRepository<T>`:

```csharp
// Update all matching documents
long updated = await repository.PatchAllAsync(
    q => q.FieldEquals(e => e.Department, "Sales"),
    new PartialPatch(new { Region = "West" }));

Console.WriteLine($"Updated {updated} documents");

// Increment counter for all matching
await repository.PatchAllAsync(
    q => q.FieldEquals(e => e.Status, "active"),
    new ScriptPatch("ctx._source.loginCount++"));

// Conditional bulk update
await repository.PatchAllAsync(
    q => q.DateRange(null, DateTime.UtcNow.AddDays(-30), e => e.LastLoginUtc),
    new ScriptPatch(@"
        ctx._source.status = 'inactive';
        ctx._source.deactivatedAt = params.now;
    ")
    {
        Params = new Dictionary<string, object> { ["now"] = DateTime.UtcNow }
    });
```

## Patch Options

```csharp
// Immediate consistency
await repository.PatchAsync(id, patch, o => o.ImmediateConsistency());

// Skip version check
await repository.PatchAsync(id, patch, o => o.SkipVersionCheck());

// Disable notifications
await repository.PatchAsync(id, patch, o => o.Notifications(false));
```

## Common Patterns

### Counter Increment

```csharp
// Atomic increment
await repository.PatchAsync(id, new ScriptPatch("ctx._source.viewCount++"));

// Increment by amount
await repository.PatchAsync(id, new ScriptPatch("ctx._source.balance += params.amount")
{
    Params = new Dictionary<string, object> { ["amount"] = 100.00m }
});

// Decrement with floor
await repository.PatchAsync(id, new ScriptPatch(@"
    ctx._source.stock = Math.max(0, ctx._source.stock - params.quantity)
")
{
    Params = new Dictionary<string, object> { ["quantity"] = 5 }
});
```

### Array Manipulation

```csharp
// Add to array
await repository.PatchAsync(id, new ScriptPatch("ctx._source.tags.add(params.tag)")
{
    Params = new Dictionary<string, object> { ["tag"] = "featured" }
});

// Add if not exists
await repository.PatchAsync(id, new ScriptPatch(@"
    if (!ctx._source.tags.contains(params.tag)) {
        ctx._source.tags.add(params.tag);
    }
")
{
    Params = new Dictionary<string, object> { ["tag"] = "featured" }
});

// Remove from array
await repository.PatchAsync(id, new ScriptPatch(@"
    if (ctx._source.tags.contains(params.tag)) {
        ctx._source.tags.remove(ctx._source.tags.indexOf(params.tag));
    }
")
{
    Params = new Dictionary<string, object> { ["tag"] = "draft" }
});

// Clear array
await repository.PatchAsync(id, new ScriptPatch("ctx._source.tags.clear()"));
```

### Conditional Updates

```csharp
// Update only if condition is met
await repository.PatchAsync(id, new ScriptPatch(@"
    if (ctx._source.status == params.expectedStatus) {
        ctx._source.status = params.newStatus;
        ctx._source.statusChangedAt = params.now;
    }
")
{
    Params = new Dictionary<string, object>
    {
        ["expectedStatus"] = "pending",
        ["newStatus"] = "approved",
        ["now"] = DateTime.UtcNow
    }
});

// Increment only if below threshold
await repository.PatchAsync(id, new ScriptPatch(@"
    if (ctx._source.failureCount < params.maxFailures) {
        ctx._source.failureCount++;
    } else {
        ctx._source.status = 'blocked';
    }
")
{
    Params = new Dictionary<string, object> { ["maxFailures"] = 5 }
});
```

### Timestamp Updates

`UpdatedUtc` is set automatically for models implementing `IHaveDates` (see [Automatic Date Tracking](#automatic-date-tracking) above). For custom timestamp fields that are not part of `IHaveDates`, you can set them manually:

```csharp
// Update a custom timestamp field
await repository.PatchAsync(id, new ScriptPatch("ctx._source.lastAccessedAt = params.now")
{
    Params = new Dictionary<string, object> { ["now"] = DateTime.UtcNow }
});
```

### Nested Object Updates

```csharp
// Update nested field
await repository.PatchAsync(id, new ScriptPatch("ctx._source.address.city = params.city")
{
    Params = new Dictionary<string, object> { ["city"] = "Seattle" }
});

// Update entire nested object
await repository.PatchAsync(id, new PartialPatch(new
{
    Address = new
    {
        Street = "123 Main St",
        City = "Seattle",
        State = "WA",
        Zip = "98101"
    }
}));
```

## Cache Behavior

::: warning Important
Patch operations invalidate cache by document ID, but **custom cache keys are NOT automatically invalidated**.
:::

```csharp
// This invalidates cache by ID
await repository.PatchAsync(id, patch);

// But if you cached by email:
var employee = await repository.FindOneAsync(
    q => q.FieldEquals(e => e.Email, email),
    o => o.Cache($"employee:email:{email}"));

// And then patch the email:
await repository.PatchAsync(id, new PartialPatch(new { Email = "new@example.com" }));

// The cache key "employee:email:old@example.com" is NOT invalidated
```

**Solution:** Override `InvalidateCacheAsync` or manually invalidate:

```csharp
await repository.PatchAsync(id, patch);
await repository.InvalidateCacheAsync($"employee:email:{oldEmail}");
```

## Soft Delete Behavior

Patch operations work on documents regardless of soft delete status:

```csharp
// This will patch even if document is soft-deleted
await repository.PatchAsync(id, patch);

// PatchAllAsync respects SoftDeleteQueryMode for finding documents
await repository.PatchAllAsync(
    q => q.FieldEquals(e => e.Status, "pending"),
    patch);  // Only patches non-deleted documents by default

// Include soft-deleted in bulk patch
await repository.PatchAllAsync(
    q => q.FieldEquals(e => e.Status, "pending"),
    patch,
    o => o.IncludeSoftDeletes());
```

## Error Handling

```csharp
try
{
    await repository.PatchAsync(id, patch);
}
catch (DocumentNotFoundException ex)
{
    // Document doesn't exist
    Console.WriteLine($"Document {ex.Id} not found");
}
catch (VersionConflictDocumentException ex)
{
    // Concurrent modification (if version checking enabled)
    Console.WriteLine($"Version conflict: {ex.Message}");
}
```

### Exception Types by Patch Type

All patch types can throw `DocumentNotFoundException` (HTTP 404) or `VersionConflictDocumentException` (HTTP 409). Other Elasticsearch errors produce a `DocumentException`.

| Patch Type | 404 (Not Found) | 409 (Version Conflict) | Notes |
|------------|-----------------|------------------------|-------|
| `PartialPatch` | `DocumentNotFoundException` | `VersionConflictDocumentException` | Uses Elasticsearch Update API |
| `ScriptPatch` | `DocumentNotFoundException` | `VersionConflictDocumentException` | Use `RetryOnConflict` for server-side retry |
| `JsonPatch` | `DocumentNotFoundException` | `VersionConflictDocumentException` | Fetches document, applies patch, then indexes |
| `ActionPatch` | `DocumentNotFoundException` | `VersionConflictDocumentException` | Fetches document, applies lambda, then indexes |

::: tip
`ScriptPatch` and `PartialPatch` execute on the Elasticsearch node via the Update API, so you can set `RetryOnConflict` for server-side retries without a round trip. `JsonPatch` and `ActionPatch` perform a client-side fetch-mutate-index cycle, so version conflicts require the caller to retry the full operation.
:::

### Automatic Retry Behavior

Transient errors (HTTP 429/503) are automatically retried with exponential backoff (up to 3 retries). Version conflicts (409) are **not** retried by the resilience policy — they indicate a genuine concurrency issue the caller should handle.

## Performance Considerations

### Use ScriptPatch for Atomic Operations

```csharp
// Good: Atomic increment
await repository.PatchAsync(id, new ScriptPatch("ctx._source.counter++"));

// Less efficient: Fetch, modify, save
var doc = await repository.GetByIdAsync(id);
doc.Counter++;
await repository.SaveAsync(doc);  // Risk of lost updates
```

### Batch Updates with PatchAllAsync

```csharp
// Good: Single bulk operation
await repository.PatchAllAsync(
    q => q.FieldEquals(e => e.Department, "Sales"),
    new PartialPatch(new { Region = "West" }));

// Less efficient: Individual patches
var employees = await repository.FindAsync(q => q.FieldEquals(e => e.Department, "Sales"));
foreach (var emp in employees.Documents)
{
    await repository.PatchAsync(emp.Id, new PartialPatch(new { Region = "West" }));
}
```

### Cache Invalidation

Patch operations handle cache invalidation differently depending on whether the full document is available:

**Document-based invalidation** (supports custom `InvalidateCacheAsync` overrides):
- `ActionPatch` — single-doc and bulk: the modified `T` is passed to `InvalidateCacheAsync(IEnumerable<T>)` and `OnDocumentsChangedAsync`, enabling custom cache key strategies based on document properties.
- `JsonPatch` — bulk (`PatchAllAsync`): deserialized documents are tracked and used for document-based invalidation after successful indexing.

**ID-based invalidation** (removes cache entries by document ID only):
- `ScriptPatch` — all paths: executes server-side on Elasticsearch; the modified document is never returned to the client.
- `PartialPatch` — all paths: same as `ScriptPatch`; the Update API does not return the full document.
- `JsonPatch` — single-doc (`PatchAsync(Id)`): uses the low-level client with raw `JToken`, so a typed `T` is not available.

::: warning ID-Based Invalidation Limitation
If your repository overrides `InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<T>>)` to compute custom cache keys from document properties (e.g., composite keys, secondary lookups), those overrides will **not** fire for `ScriptPatch`, `PartialPatch`, or single-doc `JsonPatch`. Only the standard ID-based cache key is removed. Consider using `ActionPatch` with `Func<T, bool>` if you need full document-based cache invalidation for conditional updates.
:::

## Next Steps

- [CRUD Operations](/guide/crud-operations) - Full document operations
- [Caching](/guide/caching) - Cache behavior with patches
- [Soft Deletes](/guide/soft-deletes) - Patch behavior with soft deletes
