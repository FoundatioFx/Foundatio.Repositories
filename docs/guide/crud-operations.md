# CRUD Operations

This guide covers the core Create, Read, Update, and Delete operations in Foundatio.Repositories.

## Adding Documents

### Add Single Document

```csharp
var employee = new Employee
{
    Name = "John Doe",
    Email = "john@example.com",
    Age = 30
};

var result = await repository.AddAsync(employee);
Console.WriteLine($"Created with ID: {result.Id}");
```

The repository automatically:
- Generates an ID if not provided
- Sets `CreatedUtc` and `UpdatedUtc` (if `IHaveDates`)
- Validates the document (if validation is configured)
- Publishes `EntityChanged` notification (if message bus is configured)

### Add Multiple Documents

```csharp
var employees = new List<Employee>
{
    new Employee { Name = "John Doe", Age = 30 },
    new Employee { Name = "Jane Smith", Age = 28 }
};

await repository.AddAsync(employees);
```

### Add with Options

```csharp
// Immediate consistency - wait for index refresh
var employee = await repository.AddAsync(entity, o => o.ImmediateConsistency());

// Disable notifications
await repository.AddAsync(entity, o => o.Notifications(false));

// Enable caching
await repository.AddAsync(entity, o => o.Cache());

// Combine options
await repository.AddAsync(entity, o => o
    .ImmediateConsistency()
    .Cache()
    .Notifications(false));
```

## Reading Documents

### Get by ID

```csharp
var employee = await repository.GetByIdAsync("employee-123");
if (employee == null)
{
    Console.WriteLine("Employee not found");
}
```

### Get Multiple by IDs

```csharp
var ids = new[] { "emp-1", "emp-2", "emp-3" };
var employees = await repository.GetByIdsAsync(ids);
Console.WriteLine($"Found {employees.Count} employees");
```

### Get All Documents

```csharp
var results = await repository.GetAllAsync();
Console.WriteLine($"Total: {results.Total}");

foreach (var employee in results.Documents)
{
    Console.WriteLine($"- {employee.Name}");
}
```

### Check Existence

```csharp
bool exists = await repository.ExistsAsync("employee-123");
```

### Count Documents

```csharp
long count = await repository.CountAsync();
```

### Read with Options

```csharp
// With caching
var employee = await repository.GetByIdAsync(id, o => o.Cache());

// Include soft-deleted documents
var employee = await repository.GetByIdAsync(id, o => o.IncludeSoftDeletes());

// Select specific fields
var employee = await repository.GetByIdAsync(id, o => o
    .Include(e => e.Name)
    .Include(e => e.Email));
```

## Updating Documents

### Save (Full Update)

```csharp
var employee = await repository.GetByIdAsync(id);
employee.Name = "John Smith";
employee.Age = 31;

await repository.SaveAsync(employee);
```

The repository automatically:
- Updates `UpdatedUtc` (if `IHaveDates`)
- Checks the document's `Version` against Elasticsearch and throws `VersionConflictDocumentException` if it changed since you read it (if `IVersioned`). See [Versioning](/guide/versioning).
- Invalidates cache
- Publishes `EntityChanged` notification

::: tip Consistent Date Tracking
All write operations — `AddAsync`, `SaveAsync`, and all patch types (`PatchAsync`, `PatchAllAsync`) — automatically set `UpdatedUtc` for models implementing `IHaveDates`. `CreatedUtc` is set on initial creation and is not changed by later operations unless the existing value is missing or invalid (for example, `DateTime.MinValue` or a timestamp in the future). For `ScriptPatch` and `PartialPatch`, if you explicitly provide the `updatedUtc` field, the framework respects your value. `JsonPatch` and `ActionPatch` always overwrite `UpdatedUtc`, matching `SaveAsync` semantics.
:::

### Save Multiple Documents

```csharp
var employees = await repository.GetByIdsAsync(ids);
foreach (var emp in employees)
{
    emp.Department = "Engineering";
}

await repository.SaveAsync(employees);
```

### Save with Options

```csharp
// Skip version check (last write wins; can overwrite concurrent changes)
await repository.SaveAsync(employee, o => o.SkipVersionCheck());

// Immediate consistency
await repository.SaveAsync(employee, o => o.ImmediateConsistency());

// Provide original for change detection
await repository.SaveAsync(employee, o => o.AddOriginals(originalEmployee));
```

`o.VersionCheck(false)` is equivalent to `SkipVersionCheck()`. Saving an `IVersioned` document whose `Version` is empty (never read, or read from an index with sequence numbers disabled) creates it with `op_type=create`: it throws `DuplicateDocumentException` if a document with that id already exists, unless the check is skipped. See [Documents without a version](/guide/versioning#documents-without-a-version).

## Deleting Documents

### Remove by ID

```csharp
await repository.RemoveAsync("employee-123");
```

### Remove Document

```csharp
var employee = await repository.GetByIdAsync(id);
await repository.RemoveAsync(employee);
```

For `IVersioned` documents, `RemoveAsync(T)` and `RemoveAsync(IEnumerable<T>)` send the document's version, like `SaveAsync`. If the document changed since you read it, `VersionConflictDocumentException` is thrown and nothing is deleted. A document with no version can't be checked (a delete has no create fallback), so it throws `DocumentException` before anything is deleted. Use `o.SkipVersionCheck()` to delete regardless. `RemoveAsync(id)` and `RemoveAsync(ids)` load the documents themselves and delete without a check. A document that was already deleted is treated as removed, for single and bulk removes alike.

### Remove Multiple Documents

```csharp
var employees = await repository.GetByIdsAsync(ids);
await repository.RemoveAsync(employees);
```

### Remove All Documents

::: warning
This permanently deletes ALL documents in the index.
:::

```csharp
long deleted = await repository.RemoveAllAsync();
Console.WriteLine($"Deleted {deleted} documents");
```

### Remove with Query

For `ISearchableRepository<T>`:

```csharp
// Remove all employees in a department
long deleted = await repository.RemoveAllAsync(
    q => q.FieldEquals(e => e.Department, "Sales"));
```

::: tip Version Conflicts and Retries
When no event listeners are registered and caching is disabled, `RemoveAllAsync` uses Elasticsearch's `delete_by_query`. This API snapshots document versions when it begins and skips any document modified before the delete executes, counting it as a version conflict. Because `delete_by_query` does not support `retry_on_conflict`, the repository re-runs the query up to `Retry(n)` more times (default `10`, so up to 11 passes), with exponential backoff and a refresh between passes, until no conflicts remain.

The returned count is the cumulative number of documents deleted across all passes. If conflicts persist after the retry budget, a `VersionConflictDocumentException` with the counts is thrown. Set `o.ProceedOnQueryConflicts()` to log a warning and return the partial count instead.
:::

### Soft Delete vs Hard Delete

If your entity implements `ISupportSoftDeletes`:

```csharp
// Soft delete - sets IsDeleted = true
employee.IsDeleted = true;
await repository.SaveAsync(employee);

// Hard delete - permanently removes
await repository.RemoveAsync(employee);
```

See [Soft Deletes](/guide/soft-deletes) for more details.

## Patch Operations

Patch operations allow partial updates without fetching the full document.

### Partial Patch

Update specific fields:

```csharp
await repository.PatchAsync(id, new PartialPatch(new { Age = 32 }));
```

### Script Patch

Use Elasticsearch Painless scripts:

```csharp
await repository.PatchAsync(id, new ScriptPatch("ctx._source.counter += params.increment")
{
    Params = new Dictionary<string, object> { ["increment"] = 1 }
});
```

### JSON Patch

RFC 6902 JSON Patch operations:

```csharp
var patch = new PatchDocument(
    new ReplaceOperation { Path = "name", Value = "John Smith" },
    new AddOperation { Path = "tags/-", Value = "senior" }
);
await repository.PatchAsync(id, new JsonPatch(patch));
```

### Action Patch

Lambda-based patching:

```csharp
await repository.PatchAsync(id, new ActionPatch<Employee>(e => 
{
    e.Name = "John Smith";
    e.Age = 32;
}));
```

### Bulk Patch

Patch multiple documents by query:

```csharp
// Increment counter for all employees in department
await repository.PatchAllAsync(
    q => q.FieldEquals(e => e.Department, "Engineering"),
    new ScriptPatch("ctx._source.reviewCount++"));
```

See [Patch Operations](/guide/patch-operations) for more details.

## Batch Processing

Process large datasets in batches:

```csharp
long processed = await repository.BatchProcessAsync(
    q => q.FieldEquals(e => e.Status, "pending"),
    async batch =>
    {
        foreach (var employee in batch.Documents)
        {
            // Process each employee
            await ProcessEmployeeAsync(employee);
        }
        return true; // Continue processing
    },
    o => o.PageLimit(100));

Console.WriteLine($"Processed {processed} employees");
```

Return `false` from the callback to stop processing early:

```csharp
int count = 0;
await repository.BatchProcessAsync(query, async batch =>
{
    count += batch.Documents.Count;
    return count < 1000; // Stop after 1000 documents
});
```

## Find Results

Query operations return `FindResults<T>`:

```csharp
public class FindResults<T>
{
    public IReadOnlyCollection<T> Documents { get; }
    public IReadOnlyCollection<FindHit<T>> Hits { get; }
    public long Total { get; }
    public int Page { get; }
    public bool HasMore { get; }
    
    // Automatic pagination
    public Task<bool> NextPageAsync();
}
```

### Iterating Results

```csharp
var results = await repository.FindAsync(query);

// Access documents directly
foreach (var employee in results.Documents)
{
    Console.WriteLine(employee.Name);
}

// Access hits for scores and metadata
foreach (var hit in results.Hits)
{
    Console.WriteLine($"{hit.Document.Name} (score: {hit.Score})");
}
```

### Automatic Pagination

```csharp
var results = await repository.FindAsync(query, o => o.PageLimit(100));

do
{
    foreach (var employee in results.Documents)
    {
        await ProcessAsync(employee);
    }
} while (await results.NextPageAsync());
```

## Command Options Reference

| Option | Description |
|--------|-------------|
| `.ImmediateConsistency()` | Wait for index refresh |
| `.Consistency(mode)` | Set consistency mode |
| `.Cache()` | Enable caching |
| `.CacheKey(key)` | Set cache key |
| `.CacheExpiresIn(duration)` | Set cache expiration |
| `.Notifications(bool)` | Enable/disable notifications |
| `.SkipValidation()` | Skip document validation |
| `.SkipVersionCheck()` | Skip optimistic concurrency |
| `.PageLimit(limit)` | Set page size |
| `.PageNumber(page)` | Set page number |
| `.Include(field)` | Include specific field |
| `.Exclude(field)` | Exclude specific field |
| `.IncludeSoftDeletes()` | Include soft-deleted documents |
| `.SoftDeleteMode(mode)` | Set soft delete query mode |

## Error Handling

### Common Exceptions

```csharp
try
{
    await repository.SaveAsync(employee);
}
catch (DocumentNotFoundException ex)
{
    // Document doesn't exist
    Console.WriteLine($"Document {ex.Id} not found");
}
catch (VersionConflictDocumentException ex)
{
    // Optimistic concurrency conflict
    Console.WriteLine($"Version conflict: {ex.Message}");
}
catch (DocumentValidationException ex)
{
    // Validation failed
    Console.WriteLine($"Validation error: {ex.Message}");
}
catch (DuplicateDocumentException ex)
{
    // Duplicate document
    Console.WriteLine($"Duplicate document: {ex.Message}");
}
```

### Retry Pattern

A version conflict means someone else changed the document. Re-read it (bypassing the cache, which may hold the old version), re-apply your change, and save again:

```csharp
for (int attempt = 1; ; attempt++)
{
    var employee = await repository.GetByIdAsync(id, o => o.Cache(false));
    employee.Counter++;

    try
    {
        await repository.SaveAsync(employee);
        break;
    }
    catch (VersionConflictDocumentException) when (attempt < 3)
    {
    }
}
```

For simple changes like counters, a `ScriptPatch` avoids the round trip entirely.

## Partial Failure Behavior

When adding or saving multiple documents in a single call, some documents may succeed while others fail. The repository processes successes before throwing an exception for failures.

### How It Works

1. **Successful documents are fully processed** — events are fired, cache is populated, and change notifications are sent.
2. **Failed documents leave cache unchanged** — failed writes don't mutate Elasticsearch, so existing cache entries remain valid. Cache consistency for concurrent writes is handled by message bus notifications.
3. **A typed exception is thrown** after all successes are processed.

### Exception Types by Operation

| Operation | Failure Cause | Exception |
|-----------|---------------|-----------|
| `AddAsync` | Document ID already exists | `DuplicateDocumentException` |
| `SaveAsync` | Version conflict | `VersionConflictDocumentException` |
| `PatchAsync` | Version conflict | `VersionConflictDocumentException` |
| `PatchAsync` | Document not found | `DocumentNotFoundException` |
| Any | Other Elasticsearch error | `DocumentException` |

### Example

```csharp
try
{
    await repository.AddAsync(employees);
}
catch (DuplicateDocumentException ex)
{
    // Some documents were added successfully (events fired, cached, notified).
    // Duplicate documents preserve their existing cache entries (nothing was mutated).
    // ex.Message contains details about which documents failed.
    _logger.LogWarning(ex, "Some documents already existed");
}
```

### Automatic Retry Behavior

- **`AddAsync`, `SaveAsync` and `RemoveAsync`** (single and bulk) retry transient failures up to `o.Retry(n)` times (default 10) with exponential backoff (50 ms doubling, capped at 2 s) and jitter: HTTP **429** and **503**, and connection failures where the request was never sent (refused connection, DNS or TLS failure). Bulk requests retry only the failed items.
- **HTTP 502, 504, timeouts** and failures without a status after the request was sent are **not** retried, because the write may already have been applied. They're reported as `DocumentException`; re-read before retrying.
- **Version conflicts (409)** on `AddAsync`/`SaveAsync`/`RemoveAsync` are **never** retried; the caller has to re-read and re-apply.
- `DuplicateDocumentException` is **not** retried.
- Successful documents are processed (events, cache, notifications) even if other documents fail. One exception is thrown at the end, chosen as described in [Failure handling](/guide/versioning#failure-handling).
- Writes don't use the repository's `IResiliencePolicy`; see [Retry Options](/guide/versioning#retry-options).

::: tip
For operations where version conflicts are expected (e.g., high-contention counters), use a `ScriptPatch` instead of `SaveAsync`. Script patches run on the Elasticsearch node and conflicts are retried server-side (`retry_on_conflict`, set with `o.Retry(n)`, default 10).
:::

## Next Steps

- [Querying](/guide/querying) - Build dynamic queries
- [Patch Operations](/guide/patch-operations) - Advanced patching
- [Caching](/guide/caching) - Cache configuration
- [Soft Deletes](/guide/soft-deletes) - Soft delete behavior
