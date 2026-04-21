---
name: foundatio-repositories
description: >
  Use when querying, counting, patching, or paginating data through
  Foundatio.Repositories Elasticsearch abstractions. Covers filter expressions,
  aggregation queries, partial and script patches, and search-after pagination.
  Apply when working with any IRepository, ISearchableRepository, FindAsync,
  CountAsync, PatchAsync, PatchAllAsync, or RemoveAllAsync method. Never use
  raw IElasticClient directly -- always use repository methods. Use context7
  MCP to fetch current API docs and examples.
---

# Foundatio Repositories

High-level Elasticsearch repository pattern for .NET. Interface-first, with built-in caching, messaging, patch operations, and soft deletes. **Never use raw `IElasticClient` directly** -- always use repository methods.

## Documentation via context7

Use context7 MCP for complete, up-to-date API docs and examples:

```text
query-docs(libraryId="/foundatiofx/foundatio.repositories", query="How to use PatchAllAsync with ScriptPatch to update documents by query")
```

Related libraries:

```text
query-docs(libraryId="/foundatiofx/foundatio.parsers", query="How to build aggregation expressions with nested terms and date histograms")
```

Query with specific questions, not single keywords. Both libraries are indexed with full guide content.

## Repository Hierarchy

```text
IReadOnlyRepository<T>
  ├─ ISearchableReadOnlyRepository<T>
  └─ IRepository<T>  (T : IIdentity)
       └─ ISearchableRepository<T>  (also extends ISearchableReadOnlyRepository<T>)
```

### IReadOnlyRepository&lt;T&gt;

Basic CRUD reads. No query builder -- operates by ID or returns all.

| Method | Returns |
| --- | --- |
| `GetByIdAsync(id, options?)` | `Task<T>` |
| `GetByIdsAsync(ids, options?)` | `Task<IReadOnlyCollection<T>>` |
| `GetAllAsync(options?)` | `Task<FindResults<T>>` (pageable) |
| `ExistsAsync(id, options?)` | `Task<bool>` |
| `CountAsync(options?)` | `Task<CountResult>` |
| `InvalidateCacheAsync(...)` | Invalidate by document, documents, key, or keys |

**Events:** `BeforeQuery`, `AfterQuery`

### ISearchableReadOnlyRepository&lt;T&gt; : IReadOnlyRepository&lt;T&gt;

Adds query-based search with filter expressions, aggregations, and projections.

| Method | Returns |
| --- | --- |
| `FindAsync(query, options?)` | `Task<FindResults<T>>` (pageable) |
| `FindAsAsync<TResult>(query, options?)` | `Task<FindResults<TResult>>` (projection) |
| `FindOneAsync(query, options?)` | `Task<FindHit<T>>` |
| `CountAsync(query, options?)` | `Task<CountResult>` (with aggregations) |
| `ExistsAsync(query, options?)` | `Task<bool>` |

### IRepository&lt;T&gt; : IReadOnlyRepository&lt;T&gt; where T : IIdentity

Adds write operations. Does NOT include query-based search.

| Method | Returns |
| --- | --- |
| `AddAsync(doc/docs, options?)` | `Task<T>` or `Task` (bulk) |
| `SaveAsync(doc/docs, options?)` | `Task<T>` or `Task` (bulk) |
| `PatchAsync(id, operation, options?)` | `Task<bool>` (true if modified) |
| `PatchAsync(ids, operation, options?)` | `Task<long>` (count modified) |
| `RemoveAsync(id/ids/doc/docs, options?)` | `Task` |
| `RemoveAllAsync(options?)` | `Task<long>` |

**Events:** `DocumentsAdding`, `DocumentsAdded`, `DocumentsSaving`, `DocumentsSaved`, `DocumentsRemoving`, `DocumentsRemoved`, `DocumentsChanging`, `DocumentsChanged`

### ISearchableRepository&lt;T&gt; : IRepository&lt;T&gt;, ISearchableReadOnlyRepository&lt;T&gt;

Full interface -- combines all reads, writes, and query-based operations.

| Method | Returns |
| --- | --- |
| `PatchAllAsync(query, operation, options?)` | `Task<long>` (count modified) |
| `RemoveAllAsync(query, options?)` | `Task<long>` (count removed) |
| `BatchProcessAsync(query, processFunc, options?)` | `Task<long>` (count processed) |
| `BatchProcessAsAsync<TResult>(query, processFunc, options?)` | `Task<long>` (projected batches) |

### Model Interfaces

| Interface | Provides | Automatic Behavior |
| --- | --- | --- |
| `IIdentity` | `string Id` | Required for `IRepository<T>` |
| `IHaveCreatedDate` | `DateTime CreatedUtc` | Auto-set on Add |
| `IHaveDates` | `CreatedUtc` + `DateTime UpdatedUtc` | Auto-set on Add, Save, and all Patches |
| `ISupportSoftDeletes` | `bool IsDeleted` | Filtered by default, Remove sets flag |
| `IVersioned` | `string Version` | Optimistic concurrency on Save |

## Querying

### Strongly-Typed Queries (preferred)

```csharp
var results = await repository.FindAsync(q => q
    .FieldEquals(e => e.CompanyId, companyId)
    .DateRange(start, end, e => e.CreatedUtc)
    .SortDescending(e => e.CreatedUtc));

var hit = await repository.FindOneAsync(q => q
    .FieldEquals(e => e.EmailAddress, "jane@example.com"));
var employee = hit?.Document;
```

**Available query methods:**

| Method | Purpose |
| --- | --- |
| `.FieldEquals(e => e.Field, value)` | Exact match (multiple values = OR) |
| `.FieldNotEquals(e => e.Field, value)` | Negated exact match |
| `.FieldCondition(e => e.Field, op, value)` | Comparison (any operator) |
| `.FieldHasValue(e => e.Field)` | Field is not null/empty |
| `.FieldEmpty(e => e.Field)` | Field is null/empty |
| `.FieldContains(e => e.Field, "token")` | Full-text token match (analyzed fields only) |
| `.FieldNotContains(e => e.Field, "token")` | Negated full-text token match |
| `.FieldGreaterThan(e => e.Field, value)` | Strictly greater than |
| `.FieldGreaterThanOrEqual(e => e.Field, value)` | Greater than or equal |
| `.FieldLessThan(e => e.Field, value)` | Strictly less than |
| `.FieldLessThanOrEqual(e => e.Field, value)` | Less than or equal |
| `.FieldOr(g => g.FieldEquals(...).FieldEquals(...))` | OR grouping |
| `.FieldAnd(g => g.FieldEquals(...).FieldEquals(...))` | AND grouping (explicit) |
| `.FieldNot(g => g.FieldEquals(...))` | NOT grouping (AND-NOT semantics) |
| `.DateRange(start, end, e => e.DateField)` | Date range filter |
| `.SortAscending(e => e.Field)` | Sort ascending |
| `.SortDescending(e => e.Field)` | Sort descending |
| `.Include(e => e.Field)` | Return only specific fields |
| `.Exclude(e => e.Field)` | Exclude fields from response |

Most single-field `Field*` predicate methods (such as `FieldEquals`, `FieldNotEquals`, `FieldContains`, `FieldNotContains`, `FieldGreaterThan`, `FieldGreaterThanOrEqual`, `FieldLessThan`, `FieldLessThanOrEqual`, `FieldHasValue`, and `FieldEmpty`) have `*If` variants for conditional application (e.g., `.FieldEqualsIf(e => e.Field, value, condition)`).
Range operators support DateTime, DateTimeOffset, numeric (int/long/double/float/decimal), and string types.

### FilterExpression (Lucene-style)

Use for dynamic or user-provided queries. Parsed by Foundatio.Parsers.

```csharp
var results = await repository.FindAsync(q => q
    .FilterExpression($"company_id:{companyId} employment_type:FullTime age:>=25"));

// OR logic
var results = await repository.FindAsync(q => q
    .FilterExpression("employment_type:FullTime OR employment_type:Contract"));

// Build from collection
string filter = String.Join(" OR ", companyIds.Select(id => $"company_id:{id}"));
```

### Date Range with Daily Index Routing

For `DailyIndex` / `MonthlyIndex`, `.Index()` limits which physical Elasticsearch indexes are queried. `.DateRange()` filters documents within those indexes.

```csharp
var results = await repository.FindAsync(q => q
    .Index(start, end)
    .DateRange(start, end, e => e.CreatedUtc)
    .FieldEquals(e => e.CompanyId, companyId));
```

## Aggregations

`CountAsync` returns a `CountResult` with `.Total` (long) and `.Aggregations`.

### AggregationsExpression DSL

Multiple aggregations are space-separated. Nested sub-aggregations use parentheses.

| Expression                             | Meaning                            |
| -------------------------------------- | ---------------------------------- |
| `terms:field`                          | Terms aggregation                  |
| `terms:(field~SIZE)`                   | Terms with bucket size limit       |
| `terms:(field~SIZE sub_agg)`           | Terms with nested sub-aggregation  |
| `terms:(field @include:VALUE)`         | Terms with include filter          |
| `date:field` / `date:field~1d`         | Date histogram (auto or fixed)     |
| `date:(field sub_agg)`                 | Date histogram with nested agg     |
| `cardinality:field`                    | Distinct count                     |
| `min:field` / `max:field`              | Min/Max                            |
| `avg:field` / `sum:field`              | Average / Sum                      |
| `sum:field~DEFAULT`                    | Sum with default value             |
| `missing:field`                        | Count where field is missing       |
| `percentiles:field`                    | Percentile distribution            |
| Prefix `-` (e.g. `-sum:field~1`)       | Sort descending by this agg        |

Also supported: `geogrid:`, `tophits:` (sub-agg only). See context7 for full syntax.

### Accessing Aggregation Results

Result key naming convention: `{type}_{field}`.

```csharp
var result = await repository.CountAsync(q => q
    .FieldEquals(e => e.CompanyId, companyId)
    .AggregationsExpression("terms:employment_type avg:age cardinality:company_id"));

double? avgAge = result.Aggregations.Average("avg_age")?.Value;
double? unique = result.Aggregations.Cardinality("cardinality_company_id")?.Value;

foreach (var bucket in result.Aggregations.Terms("terms_employment_type").Buckets)
{
    // bucket.Key = "FullTime", bucket.Total = 42
}
```

### Terms with Nested Min/Max

```csharp
var result = await repository.CountAsync(q => q
    .AggregationsExpression("terms:(company_id~50 min:created_utc max:created_utc)"));

foreach (var b in result.Aggregations.Terms("terms_company_id").Buckets)
{
    DateTime earliest = b.Aggregations.Min<DateTime>("min_created_utc").Value;
    DateTime latest = b.Aggregations.Max<DateTime>("max_created_utc").Value;
}
```

### Date Histogram

```csharp
var result = await repository.CountAsync(q => q
    .FieldEquals(e => e.CompanyId, companyId)
    .AggregationsExpression("date:(created_utc~1M cardinality:company_id)"));

foreach (var bucket in result.Aggregations.DateHistogram("date_created_utc").Buckets)
{
    // bucket.Date, bucket.Total
    double? uniqueInMonth = bucket.Aggregations.Cardinality("cardinality_company_id")?.Value;
}
```

## Pagination

Always use `SearchAfterPaging()` for deep pagination. Never use offset-based `.PageNumber()` for large result sets.

```csharp
var results = await repository.GetAllAsync(o => o.SearchAfterPaging().PageLimit(500));
do
{
    foreach (var doc in results.Documents)
    {
        await ProcessAsync(doc);
    }
} while (!cancellationToken.IsCancellationRequested && await results.NextPageAsync());
```

Works the same with `FindAsync`:

```csharp
var results = await repository.FindAsync(
    q => q.FieldEquals(e => e.CompanyId, companyId),
    o => o.SearchAfterPaging().PageLimit(1000));

do
{
    foreach (var doc in results.Documents)
    {
        await ProcessAsync(doc);
    }
} while (!cancellationToken.IsCancellationRequested && await results.NextPageAsync());
```

## Patch Operations

Four patch types are available. All automatically set `UpdatedUtc` on models implementing `IHaveDates`.

| Patch Type | How It Works | When to Use |
| --- | --- | --- |
| `PartialPatch` | Server-side field update via ES Update API | Simple field value changes |
| `ScriptPatch` | Server-side Painless script via ES Update API | Atomic increments, conditional logic |
| `ActionPatch` | Client-side fetch-mutate-reindex | Complex logic, full document-based cache invalidation |
| `JsonPatch` | Client-side RFC 6902 JSON Patch | Standard patch operations |

```csharp
// PartialPatch -- update fields by value
await repository.PatchAllAsync(
    q => q.FieldEquals(e => e.CompanyId, companyId),
    new PartialPatch(new { company_name = "New Corp" }));

// ScriptPatch -- atomic server-side update
await repository.PatchAsync(id, new ScriptPatch("ctx._source.years_employed++"));

// ScriptPatch with params
await repository.PatchAsync(id, new ScriptPatch(
    """
    ctx._source.years_employed += params.years;
    ctx._source.last_review = params.reviewDate;
    """)
{
    Params = new Dictionary<string, object>
    {
        { "years", 1 },
        { "reviewDate", DateTime.UtcNow }
    }
});

// ActionPatch -- return false to skip the write (noop)
bool modified = await repository.PatchAsync(id, new ActionPatch<Employee>(e =>
{
    if (e.YearsEmployed >= 10)
        return false;

    e.YearsEmployed = 10;
    return true;
}));

// Patch multiple by IDs
await repository.PatchAsync(new Ids(id1, id2, id3),
    new ScriptPatch("ctx._source.years_employed++"));
```

**Return values:** `PatchAsync(Id)` returns `Task<bool>`, `PatchAsync(Ids)` and `PatchAllAsync` return `Task<long>`.

## Remove and Fetch

```csharp
// Remove by query
await repository.RemoveAllAsync(q => q
    .FieldEquals(e => e.CompanyId, companyId));

// Batch fetch (returns IReadOnlyCollection<T>)
var employees = await repository.GetByIdsAsync(ids, o => o.Cache());

// Existence check
bool exists = await repository.ExistsAsync(id);
```

## Command Options

| Option                          | Purpose                                      |
| ------------------------------- | -------------------------------------------- |
| `o => o.Cache()`                | Enable cache read/write                      |
| `o => o.Cache("key")`           | Cache with specific key                      |
| `o => o.ImmediateConsistency()` | ES refresh after write (use in tests only)   |
| `o => o.SearchAfterPaging()`    | Deep pagination with search_after            |
| `o => o.PageLimit(N)`           | Page size                                    |
| `o => o.SoftDeleteMode(mode)`   | `ActiveOnly` (default), `All`, `DeletedOnly` |
| `o => o.Notifications(false)`   | Suppress change notifications                |
| `o => o.Originals()`            | Track original values for change detection   |
| `o => o.IncludeSoftDeletes()`   | Include soft-deleted docs in queries         |

## Gotchas

- **`.Index(start, end)` is only for DailyIndex/MonthlyIndex**: It routes to physical daily/monthly shards. On `VersionedIndex` (single index) it is a no-op. Always pair with `.DateRange()` which filters documents within whatever indexes are targeted.
- **Painless uses `==` not `===`**: The `===` operator does not exist in Painless. Always use `==` for equality in ScriptPatch scripts.
- **`NextPageAsync()` mutates in-place**: It returns `Task<bool>` and replaces `.Documents`/`.Hits` on the same result object. Do not hold references to the previous page.
- **Cache invalidation limits on ScriptPatch/PartialPatch**: These execute server-side and only invalidate cache by document ID. Custom `InvalidateCacheAsync` overrides based on document properties will NOT fire. Use `ActionPatch` if you need full document-based cache invalidation.
- **`ctx.op = 'none'` for script noops**: Elasticsearch does not auto-detect noops for script updates. Your script must explicitly set `ctx.op = 'none'` to skip the write. Works correctly with automatic `UpdatedUtc` date tracking.
- **Automatic `UpdatedUtc` on patches**: Models implementing `IHaveDates` get `UpdatedUtc` set automatically on every patch. This means `PartialPatch` almost always reports `modified = true` even if no other field changed.
- **`ImmediateConsistency()` is for tests only**: It triggers an Elasticsearch index refresh after writes. Never use in production -- it degrades cluster performance.
- **Register repositories as singletons**: Repository instances maintain internal state (index configuration, cache references). Always register via DI as singletons.
- **`FieldEquals` with multiple values is OR**: `.FieldEquals(e => e.EmploymentType, "FullTime", "Contract")` produces an OR filter, not AND.
- **`FieldContains` is token matching, NOT wildcard**: `FieldContains(f => f.Name, "Er")` will NOT match "Eric". Use `FilterExpression("field:pattern*")` for prefix/wildcard matching.
- **`FieldNot` is AND-NOT**: Multiple conditions inside `FieldNot` mean NOT A AND NOT B. For NOT (A AND B), nest `FieldAnd` inside `FieldNot`.
- **Range operators + time-series indexes**: `FieldLessThanOrEqual(f => f.CreatedUtc, now)` does NOT narrow which daily/monthly indexes are queried. Always pair with `.Index(start, end)`.
- **`FieldEquals` on analyzed text fields throws**: If the field has no `.keyword` sub-field, `FieldEquals` throws `QueryValidationException`. Use `FieldContains` for full-text search.
- **`PatchAsync(Ids, ...)` requires `Ids` type**: Use `new Ids(id1, id2)` -- `string[]` does not implicitly convert to `Ids`.
- **`PatchAsync(Ids, ...)` delegates `JsonPatch`/`ActionPatch` to `PatchAllAsync`**: For `ScriptPatch`/`PartialPatch` multi-ID sends per-ID notifications. For `JsonPatch`/`ActionPatch`, the call routes through `PatchAllAsync` with explicit IDs, so query-based notification rules apply.
- **`PatchAllAsync` with filter-only queries sends type-level notifications**: When `PatchAllAsync` is called without explicit IDs, the `EntityChanged` message has `Id = null`. Subscribers that need specific document IDs should restructure the query to use explicit IDs or re-query the affected documents.
- **Patch `DocumentsChanged` event has empty document list**: For `ScriptPatch`, `PartialPatch`, and single-doc `JsonPatch`, `args.Documents` is empty because the modified document is not available client-side. Only `ActionPatch` populates the documents list.
- **Patches do not fire `DocumentsSaving`/`DocumentsSaved`**: Unlike `SaveAsync`, patch operations only fire `DocumentsChanged`. Handlers on `DocumentsSaving` or `DocumentsSaved` will not see patched documents.
- **Patches do not detect soft-delete transitions**: Even if a patch sets `IsDeleted = true`, the `ChangeType` is always `Saved`. Soft-delete detection (`ChangeType.Removed`) requires `SaveAsync` with `OriginalsEnabled = true`.
