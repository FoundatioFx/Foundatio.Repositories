# Foundatio.Repositories v7.x (NEST) -- Maintenance Mode

> Applies to: v7.x (NEST / IElasticClient). For the current version (v8+) see [patterns.md](patterns.md).

**This version is in maintenance mode.** New features are developed on v8+ only. Consider upgrading -- see [upgrading-from-nest.md](upgrading-from-nest.md).

Full v7 documentation: https://github.com/FoundatioFx/Foundatio.Repositories/tree/v7.18.3/docs/guide/

Use context7 MCP for v7 docs:

```text
query-docs(libraryId="/foundatiofx/foundatio.repositories", query="How to configure index mapping with NEST TypeMappingDescriptor")
```

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

```csharp
var results = await repository.FindAsync(q => q
    .Index(start, end)
    .DateRange(start, end, e => e.CreatedUtc)
    .FieldEquals(e => e.CompanyId, companyId));
```

## Aggregations

`CountAsync` returns a `CountResult` with `.Total` (long) and `.Aggregations`.

### AggregationsExpression DSL

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

### Accessing Aggregation Results

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

## Pagination

Always use `SearchAfterPaging()` for deep pagination.

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

## Patch Operations

| Patch Type | How It Works | When to Use |
| --- | --- | --- |
| `PartialPatch` | Server-side field update via ES Update API | Simple field value changes |
| `ScriptPatch` | Server-side Painless script via ES Update API | Atomic increments, conditional logic |
| `ActionPatch` | Client-side fetch-mutate-reindex | Complex logic, full document-based cache invalidation |
| `JsonPatch` | Client-side RFC 6902 JSON Patch | Standard patch operations |

```csharp
// PartialPatch
await repository.PatchAllAsync(
    q => q.FieldEquals(e => e.CompanyId, companyId),
    new PartialPatch(new { company_name = "New Corp" }));

// ScriptPatch
await repository.PatchAsync(id, new ScriptPatch("ctx._source.years_employed++"));

// ScriptPatch with params
await repository.PatchAsync(id, new ScriptPatch(
    "ctx._source.years_employed += params.years; ctx._source.last_review = params.reviewDate;")
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

## Remove and Fetch

```csharp
await repository.RemoveAllAsync(q => q
    .FieldEquals(e => e.CompanyId, companyId));

var employees = await repository.GetByIdsAsync(ids, o => o.Cache());

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

## Index Mapping (NEST Syntax)

All index configurations use `.Dynamic(false)`. Every field you query MUST have an explicit mapping.

### Field Type Reference (NEST)

| C# / NEST Mapping | When to Use | ES Type |
| --- | --- | --- |
| `.Keyword(f => f.Name(e => e.Field))` | Exact match, filtering, aggregations | `keyword` |
| `.Text(f => f.Name(e => e.Field).AddKeywordAndSortFields())` | Full-text search + exact match + sorting | `text` + `.keyword` + `.sort` sub-fields |
| `.Scalar(f => f.Field, f => f.Name(e => e.Field))` | Numeric fields (int, long, double, decimal) | `integer` / `long` / `double` etc. |
| `.Number(f => f.Name(e => e.Field).Type(NumberType.Integer))` | Numeric with explicit type | specified number type |
| `.Date(f => f.Name(e => e.Field))` | DateTime / DateTimeOffset | `date` |
| `.Boolean(f => f.Name(e => e.Field))` | Boolean flags | `boolean` |
| `.GeoPoint(f => f.Name(e => e.Field))` | Latitude/longitude | `geo_point` |
| `.Nested<T>(f => f.Name(e => e.Field).Properties(...))` | Nested objects needing independent querying | `nested` |
| `.Object<T>(f => f.Name(e => e.Field).Properties(...))` | Embedded objects (flattened) | `object` |

`.SetupDefaults()` automatically maps `Id` (keyword), `CreatedUtc` (date), `UpdatedUtc` (date), `IsDeleted` (boolean), and `Version` (keyword) based on which model interfaces are implemented.

### ConfigureIndexMapping Example (NEST)

```csharp
public override TypeMappingDescriptor<Employee> ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
{
    return map
        .Dynamic(false)
        .Properties(p => p
            .SetupDefaults()
            .Keyword(f => f.Name(e => e.CompanyId))
            .Text(f => f.Name(e => e.Name).AddKeywordAndSortFields())
            .Number(f => f.Name(e => e.Age).Type(NumberType.Integer))
            .Date(f => f.Name(e => e.HireDate))
        );
}
```

### When to Bump the Index Version

**No version bump needed** -- adding a mapping for a brand-new field:

- For `VersionedIndex<T>`: `ConfigureIndexesAsync` calls the PUT Mapping API on the existing index.
- For `DailyIndex<T>` / `MonthlyIndex<T>`: new daily/monthly physical indexes are created on-demand. Already-created indexes are NOT updated.

**Version bump IS required** -- changing an existing field's type or analyzer.

### Checklist: Adding a Queryable Model Field

1. Add the property to the model class.
2. Add the mapping in `ConfigureIndexMapping` (choose the correct field type from the table above).
3. If changing an existing field's mapping, bump the index version and add a reindex script.
4. If brand-new field, no version bump is needed.
5. Add to `AllowedQueryFields` / `AllowedAggregationFields` / `AllowedSortFields` if the index restricts those sets.
6. Decide whether existing documents need to be searchable on the new field immediately.

---

**Ready to upgrade?** Read [upgrading-from-nest.md](upgrading-from-nest.md) for the full migration guide.
