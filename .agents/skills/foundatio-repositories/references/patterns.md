# Foundatio.Repositories -- v8 API Patterns

> Applies to: v8+ (Elastic.Clients.Elasticsearch). For v7/NEST see [patterns-v7.md](patterns-v7.md).

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

Most single-field `Field*` predicate methods have `*If` variants for conditional application (e.g., `.FieldEqualsIf(e => e.Field, value, condition)`).
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

## Index Mapping

All index configurations use `.Dynamic(false)`, which disables Elasticsearch's automatic field mapping. **Every field you query, filter, sort, or aggregate on MUST have an explicit mapping** in `ConfigureIndexMapping`. Unmapped fields are stored in `_source` but never indexed -- queries against them silently return zero results.

### Field Type Reference

| Mapping Call | When to Use | ES Type |
| --- | --- | --- |
| `.Keyword(e => e.Field)` | Exact match, filtering, aggregations | `keyword` |
| `.Text(e => e.Field, t => t.AddKeywordAndSortFields())` | Full-text search + exact match + sorting | `text` + `.keyword` + `.sort` sub-fields |
| `.IntegerNumber(e => e.Field)` | Integer numeric fields | `integer` |
| `.LongNumber(e => e.Field)` | Long numeric fields | `long` |
| `.DoubleNumber(e => e.Field)` | Double numeric fields | `double` |
| `.FloatNumber(e => e.Field)` | Float numeric fields | `float` |
| `.Date(e => e.Field)` | DateTime / DateTimeOffset | `date` |
| `.Boolean(e => e.Field)` | Boolean flags | `boolean` |
| `.GeoPoint(e => e.Field)` | Latitude/longitude | `geo_point` |
| `.Nested(e => e.Field, n => n.Properties(...))` | Nested objects needing independent querying | `nested` |
| `.Object(e => e.Field, o => o.Properties(...))` | Embedded objects (flattened) | `object` |

`.SetupDefaults()` automatically maps `Id` (keyword), `CreatedUtc` (date), `UpdatedUtc` (date), `IsDeleted` (boolean), and `Version` (keyword) based on which model interfaces are implemented.

### When to Bump the Index Version

**No version bump needed** -- adding a mapping for a brand-new field:

- For `Index<T>` / `VersionedIndex<T>`: `ConfigureIndexesAsync` (or the lazy first-write path) calls the PUT Mapping API on the existing index, adding the new field mapping additively.
- For `DailyIndex<T>` / `MonthlyIndex<T>`: new daily/monthly physical indexes are created on-demand with the full current mapping. **Already-created daily/monthly indexes are NOT updated** -- `DailyIndex.ConfigureAsync` is a no-op.

**Version bump IS required** -- changing an existing field's type or analyzer (e.g., `Text` to `Keyword`). Elasticsearch does not allow in-place type changes. Bump the `VersionedIndex` version to trigger creation of a new index with the correct mapping and automatic reindexing.

### Existing Documents After Adding a New Mapping

After adding a mapping for a previously unmapped field, only **newly saved/indexed documents** will be searchable on that field. To make existing documents searchable:

- **Foundatio migration** -- create a `MigrationBase` subclass that uses `PatchAllAsync` or `BatchProcessAsync` to touch all affected documents (recommended for production).
- **`PatchAllAsync`** with a no-op `ScriptPatch` (e.g., `ctx.op = 'none'` -- still triggers re-index of `_source`).
- **Elasticsearch Update By Query API** with no script: `POST /{index}/_update_by_query` re-indexes every document in place.

For `DailyIndex`/`MonthlyIndex`, you must also apply the mapping to existing physical indexes before the update-by-query will help.

**Trade-off for Daily/Monthly indexes**: Rolling forward (doing nothing to old partitions and waiting for retention to cycle out old data) is often the cheapest strategy.

**Mapping resolver cache**: After applying a manual PUT Mapping, the in-process `ElasticMappingResolver` auto-refreshes from the server within ~60 seconds. To force immediate recognition, call `index.MappingResolver.RefreshMapping()`.

### Checklist: Adding a Queryable Model Field

1. Add the property to the model class.
2. Add the mapping in `ConfigureIndexMapping` (choose the correct field type from the table above).
3. If changing an existing field's mapping, bump the index version and add a reindex script.
4. If brand-new field, no version bump is needed.
5. Add to `AllowedQueryFields` / `AllowedAggregationFields` / `AllowedSortFields` if the index restricts those sets.
6. Decide whether existing documents need to be searchable on the new field immediately. If yes, create a Foundatio migration to backfill.
