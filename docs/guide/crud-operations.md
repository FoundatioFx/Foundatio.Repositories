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
- Checks version for conflicts (if `IVersioned`)
- Invalidates cache
- Publishes `EntityChanged` notification

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
// Skip version check
await repository.SaveAsync(employee, o => o.SkipVersionCheck());

// Immediate consistency
await repository.SaveAsync(employee, o => o.ImmediateConsistency());

// Provide original for change detection
await repository.SaveAsync(employee, o => o.AddOriginals(originalEmployee));
```

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

```csharp
var employee = await repository.GetByIdAsync(id);
int retries = 3;

while (retries > 0)
{
    try
    {
        employee.Counter++;
        await repository.SaveAsync(employee);
        break;
    }
    catch (VersionConflictDocumentException)
    {
        retries--;
        if (retries == 0) throw;
        
        // Refresh and retry
        employee = await repository.GetByIdAsync(id);
    }
}
```

## Next Steps

- [Querying](/guide/querying) - Build dynamic queries
- [Patch Operations](/guide/patch-operations) - Advanced patching
- [Caching](/guide/caching) - Cache configuration
- [Soft Deletes](/guide/soft-deletes) - Soft delete behavior
