# Querying

Foundatio.Repositories provides powerful querying capabilities through `ISearchableRepository<T>`. The query system is built on [Foundatio.Parsers](https://github.com/FoundatioFx/Foundatio.Parsers), which provides Lucene-style query parsing with support for filtering, sorting, and aggregations.

## Query Parser (Foundatio.Parsers)

The query expressions used throughout this library are powered by Foundatio.Parsers, which translates human-readable query strings into Elasticsearch queries. Key features include:

- **Lucene-style syntax** - Familiar query syntax for developers
- **Field aliasing** - Map user-friendly names to actual field names
- **Type coercion** - Automatic type conversion for dates, numbers, etc.
- **Validation** - Query validation against index mappings
- **Extensibility** - Custom query visitors and field resolvers

## Basic Queries

### Find with Filter Expression

Use Lucene-style filter expressions:

```csharp
// Simple field match
var results = await repository.FindAsync(q => q.FilterExpression("age:30"));

// Range queries
var results = await repository.FindAsync(q => q.FilterExpression("age:>=25"));
var results = await repository.FindAsync(q => q.FilterExpression("age:[25 TO 35]"));

// Multiple conditions (AND)
var results = await repository.FindAsync(q => q.FilterExpression("age:>=25 AND department:Engineering"));

// OR conditions
var results = await repository.FindAsync(q => q.FilterExpression("department:Engineering OR department:Sales"));

// NOT conditions
var results = await repository.FindAsync(q => q.FilterExpression("NOT status:inactive"));

// Wildcards
var results = await repository.FindAsync(q => q.FilterExpression("name:John*"));

// Exists check
var results = await repository.FindAsync(q => q.FilterExpression("_exists_:email"));
```

### Find with Search Expression

Full-text search across analyzed fields:

```csharp
var results = await repository.FindAsync(q => q.SearchExpression("john developer"));
```

### Find One

Get a single matching document:

```csharp
var hit = await repository.FindOneAsync(q => q.FieldEquals(e => e.Email, "john@example.com"));
var employee = hit?.Document;
```

## Strongly-Typed Queries

### Field Equals

```csharp
// Single value
var results = await repository.FindAsync(q => q.FieldEquals(e => e.Department, "Engineering"));

// Multiple values (OR)
var results = await repository.FindAsync(q => q.FieldEquals(e => e.Status, "active", "pending"));

// Enum values
var results = await repository.FindAsync(q => q.FieldEquals(e => e.Type, EmployeeType.FullTime));
```

### Field Conditions

`FieldCondition` supports equality, text matching, and existence checks:

```csharp
// Equality check
var results = await repository.FindAsync(q => q
    .FieldCondition(e => e.Name, ComparisonOperator.Equals, "John Smith"));

// Contains (for text fields)
var results = await repository.FindAsync(q => q
    .FieldCondition(e => e.Name, ComparisonOperator.Contains, "John"));
```

Available operators: `Equals`, `NotEquals`, `IsEmpty`, `HasValue`, `Contains`, `NotContains`.

> **Note:** For numeric range comparisons (e.g., age >= 25), use `FilterExpression` with Lucene syntax: `q.FilterExpression("age:[25 TO *]")`

### Field Empty/Has Value

```csharp
// Find documents where field is null or empty
var results = await repository.FindAsync(q => q.FieldEmpty(e => e.ManagerId));

// Find documents where field has a value
var results = await repository.FindAsync(q => q.FieldHasValue(e => e.ManagerId));
```

### Date Range

`.DateRange()` adds an Elasticsearch filter clause that restricts documents by a date field value. It does **not** select which physical indexes are queried — it only filters documents within whatever indexes are targeted.

```csharp
var results = await repository.FindAsync(q => q
    .DateRange(
        start: DateTime.UtcNow.AddDays(-30),
        end: DateTime.UtcNow,
        field: e => e.CreatedUtc));
```

#### Time-Series Indexes (DailyIndex / MonthlyIndex)

When querying a `DailyIndex` or `MonthlyIndex`, each partition (day or month) is a separate physical Elasticsearch index. Without specifying which indexes to target, the query runs against the umbrella alias and scans all partitions regardless of the date range filter.

To limit which physical indexes are queried, use `.Index(start, end)` alongside `.DateRange()`:

```csharp
var start = DateTime.UtcNow.AddDays(-7);
var end = DateTime.UtcNow;

var results = await repository.FindAsync(q => q
    .Index(start, end)                         // target only the relevant daily index partitions
    .DateRange(start, end, e => e.CreatedUtc)  // filter documents within those indexes
);
```

> **Note:** `.Index(start, end)` and `.DateRange()` serve different purposes and must be set independently. `DateRange` without `.Index()` is still valid — it will filter documents correctly — but it queries all partitions, which is less efficient for large time-series datasets.

#### Large Range Fallback

To prevent generating an excessively long list of individual index names, index selection falls back to the alias (which covers all partitions) when the requested range is too broad:

| Index type | Threshold | Behavior |
|---|---|---|
| `DailyIndex` | Range >= 3 months, or exceeds `MaxIndexAge` | Falls back to alias |
| `MonthlyIndex` | Range > 1 year, or exceeds `MaxIndexAge` | Falls back to alias |

The query is still executed correctly in the fallback case — Elasticsearch simply searches all partitions — but there is no index pruning optimization. The `.DateRange()` filter still narrows the returned documents.

### ID Queries

```csharp
// Find by IDs
var results = await repository.FindAsync(q => q.Id("emp-1", "emp-2", "emp-3"));

// Exclude IDs
var results = await repository.FindAsync(q => q.ExcludedId("emp-1"));
```

## Sorting

### Sort Expression

```csharp
// Single field ascending
var results = await repository.FindAsync(q => q.SortExpression("name"));

// Descending
var results = await repository.FindAsync(q => q.SortExpression("-createdUtc"));

// Multiple fields
var results = await repository.FindAsync(q => q.SortExpression("department -salary"));
```

### Strongly-Typed Sort

```csharp
var results = await repository.FindAsync(q => q
    .SortAscending(e => e.Name)
    .SortDescending(e => e.CreatedUtc));
```

## Pagination

### Basic Pagination

```csharp
var results = await repository.FindAsync(
    q => q.FieldEquals(e => e.Department, "Engineering"),
    o => o.PageNumber(1).PageLimit(25));

Console.WriteLine($"Page {results.Page}, Total: {results.Total}, HasMore: {results.HasMore}");
```

### Automatic Pagination

```csharp
var results = await repository.FindAsync(query, o => o.PageLimit(100));

do
{
    foreach (var doc in results.Documents)
    {
        await ProcessAsync(doc);
    }
} while (await results.NextPageAsync());
```

### Snapshot Paging (Scroll API)

For large result sets, use snapshot paging:

```csharp
var results = await repository.FindAsync(
    query,
    o => o.SnapshotPaging().SnapshotPagingLifetime(TimeSpan.FromMinutes(5)));

do
{
    foreach (var doc in results.Documents)
    {
        await ProcessAsync(doc);
    }
} while (await results.NextPageAsync());
```

### Search After Paging

More efficient for deep pagination:

```csharp
var results = await repository.FindAsync(
    q => q.SortExpression("createdUtc"),
    o => o.SearchAfterPaging());

// For subsequent pages, use the token
var nextResults = await repository.FindAsync(
    q => q.SortExpression("createdUtc"),
    o => o.SearchAfterToken(results.GetSearchAfterToken()));
```

## Aggregations

### Aggregation Expression

```csharp
var results = await repository.CountAsync(q => q
    .AggregationsExpression("terms:department terms:status"));

// Access aggregation results
var departmentAgg = results.Aggregations.Terms("terms_department");
foreach (var bucket in departmentAgg.Buckets)
{
    Console.WriteLine($"{bucket.Key}: {bucket.Total}");
}
```

### Common Aggregations

```csharp
// Terms aggregation
var results = await repository.CountAsync(q => q
    .AggregationsExpression("terms:department"));

// Date histogram
var results = await repository.CountAsync(q => q
    .AggregationsExpression("date:createdUtc"));

// Cardinality (unique count)
var results = await repository.CountAsync(q => q
    .AggregationsExpression("cardinality:userId"));

// Statistics
var results = await repository.CountAsync(q => q
    .AggregationsExpression("avg:salary min:salary max:salary"));

// Multiple aggregations
var results = await repository.CountAsync(q => q
    .AggregationsExpression("terms:department avg:salary cardinality:userId"));
```

### Accessing Aggregation Results

```csharp
var results = await repository.CountAsync(q => q
    .AggregationsExpression("terms:department avg:salary max:createdUtc"));

// Terms aggregation
var deptAgg = results.Aggregations.Terms("terms_department");
foreach (var bucket in deptAgg.Buckets)
{
        Console.WriteLine($"Department: {bucket.Key}, Count: {bucket.Total}");
}

// Value aggregations
var avgSalary = results.Aggregations.Average("avg_salary")?.Value;
var maxDate = results.Aggregations.Max<DateTime>("max_createdUtc")?.Value;

Console.WriteLine($"Average Salary: {avgSalary}");
Console.WriteLine($"Latest Created: {maxDate}");
```

### Nested Field Aggregations

When aggregating on fields that are mapped as `nested` in Elasticsearch, the framework automatically wraps the aggregation in a nested aggregation context. Use the same `parentObject.childField` syntax you use for flat fields:

```csharp
// Terms aggregation on a nested field
var results = await repository.CountAsync(q => q
    .AggregationsExpression("terms:peerReviews.rating"));

// Multiple aggregation types on nested fields
var results = await repository.CountAsync(q => q
    .AggregationsExpression("terms:peerReviews.reviewerEmployeeId min:peerReviews.rating max:peerReviews.rating"));
```

The framework detects nested fields via the index mapping and groups all nested aggregations under a single `SingleBucketAggregate` keyed by the nested path. Access results through that wrapper:

```csharp
var results = await repository.CountAsync(q => q
    .AggregationsExpression("terms:peerReviews.rating min:peerReviews.rating max:peerReviews.rating"));

// All nested aggregations are grouped under a single-bucket aggregate
var nestedAgg = results.Aggregations["nested_peerReviews"] as SingleBucketAggregate;

var ratingTerms = nestedAgg.Aggregations.Terms<int>("terms_peerReviews.rating");
foreach (var bucket in ratingTerms.Buckets)
{
    Console.WriteLine($"Rating {bucket.Key}: {bucket.Total}");
}

var minRating = nestedAgg.Aggregations.Min("min_peerReviews.rating")?.Value;
var maxRating = nestedAgg.Aggregations.Max("max_peerReviews.rating")?.Value;
```

Include and exclude filtering works the same way as non-nested aggregations:

```csharp
// Only include specific terms
var results = await repository.CountAsync(q => q
    .AggregationsExpression("terms:(peerReviews.reviewerEmployeeId @include:emp1 @include:emp2)"));

// Exclude specific terms
var results = await repository.CountAsync(q => q
    .AggregationsExpression("terms:(peerReviews.rating @exclude:1 @exclude:2)"));
```

### Top Hits Aggregation

The `tophits` sub-aggregation returns the top matching documents within each bucket:

```csharp
var results = await repository.CountAsync(q => q
    .AggregationsExpression("terms:(age tophits:_)"));

var bucket = results.Aggregations.Terms<int>("terms_age").Buckets.First();
var topHits = bucket.Aggregations.TopHits();
var employees = topHits.Documents<Employee>();
```

::: warning TopHitsAggregate Cannot Be Serialized
`TopHitsAggregate` holds `ILazyDocument` references that contain raw Elasticsearch document bytes and require an active serializer instance to materialize into typed objects. These references are lost during JSON serialization, which means:

- **Caching**: `CountResult` or `FindResults` containing `TopHitsAggregate` cannot be cached and restored via JSON serialization (Newtonsoft or System.Text.Json). The top hits data will be `null` after deserialization.
- **Workaround**: If you need to cache results that include top hits, materialize the documents into concrete types *before* caching, and cache those typed results separately.
:::

## Nested Queries

When querying fields inside [nested objects](https://www.elastic.co/guide/en/elasticsearch/reference/current/nested.html), the framework automatically wraps filter expressions in the required Elasticsearch `nested` query. You do not need to manually construct nested queries -- just use dotted field paths.

### Prerequisites

The field must be mapped as `nested` in your index configuration:

```csharp
public override TypeMappingDescriptor<Employee> ConfigureIndexMapping(
    TypeMappingDescriptor<Employee> map)
{
    return map
        .Dynamic(false)
        .Properties(p => p
            .SetupDefaults()
            .Keyword(f => f.Name(e => e.Id))
            .Text(f => f.Name(e => e.Name).AddKeywordAndSortFields())
            .Nested<PeerReview>(f => f.Name(e => e.PeerReviews).Properties(p1 => p1
                .Keyword(f2 => f2.Name(p2 => p2.ReviewerEmployeeId))
                .Scalar(p3 => p3.Rating, f2 => f2.Name(p3 => p3.Rating))))
        );
}
```

### Basic Nested Queries

Query nested fields with standard filter expressions using `parentObject.childField` syntax:

```csharp
// Exact match on a nested field
var results = await repository.FindAsync(q => q
    .FilterExpression("peerReviews.rating:5"));

// Range query on a nested field
var results = await repository.FindAsync(q => q
    .FilterExpression("peerReviews.rating:[4 TO 5]"));

// Match on a nested keyword field
var results = await repository.FindAsync(q => q
    .FilterExpression("peerReviews.reviewerEmployeeId:bob_456"));
```

### Combining Nested Conditions

Multiple conditions on the same nested path are combined into a single `nested` query with a `bool` clause:

```csharp
// AND: both conditions must match the SAME nested document
var results = await repository.FindAsync(q => q
    .FilterExpression("peerReviews.rating:5 AND peerReviews.reviewerEmployeeId:bob_456"));

// OR: either condition can match across different nested documents
var results = await repository.FindAsync(q => q
    .FilterExpression("peerReviews.rating:>=4 OR peerReviews.reviewerEmployeeId:bob_456"));
```

### Mixing Nested and Non-Nested Fields

Nested fields and regular fields can be used together. The framework only wraps the nested portions in a `nested` query:

```csharp
// "name" is a root-level field, "peerReviews.rating" is nested
var results = await repository.FindAsync(q => q
    .FilterExpression("name:Alice peerReviews.rating:5"));
```

### Negating Nested Conditions

```csharp
// Exclude employees who have any peer review with rating 5
var results = await repository.FindAsync(q => q
    .FilterExpression("NOT peerReviews.rating:5"));
```

### Default Fields with Nested Paths

Nested fields can be included in default search fields via `SetDefaultFields` in your query parser configuration. This allows unqualified search terms to match against nested fields:

```csharp
protected override void ConfigureQueryParser(ElasticQueryParserConfiguration config)
{
    base.ConfigureQueryParser(config);
    config.SetDefaultFields([
        nameof(Employee.Id).ToLowerInvariant(),
        nameof(Employee.Name).ToLowerInvariant(),
        "peerReviews.reviewerEmployeeId"
    ]);
}
```

With this configuration, a bare search term like `bob_456` will match against `id`, `name`, and `peerReviews.reviewerEmployeeId`:

```csharp
// Searches id, name, AND the nested peerReviews.reviewerEmployeeId field
var results = await repository.FindAsync(q => q.SearchExpression("bob_456"));
```

### Sorting on Nested Fields

Sort expressions on nested fields automatically include the required `nested` context:

```csharp
// Sort descending by a nested numeric field
var results = await repository.FindAsync(q => q
    .SortExpression("-peerReviews.rating"));
```

### Exists / Missing on Nested Fields

`_exists_` and `_missing_` queries on nested fields are automatically wrapped in a `nested` query:

```csharp
// Find employees that have at least one peer review with a reviewerEmployeeId
var results = await repository.FindAsync(q => q
    .FilterExpression("_exists_:peerReviews.reviewerEmployeeId"));
```

### Deeply Nested Types

Multi-level nesting (nested objects inside other nested objects) is supported. The framework resolves the correct nested path at each level:

```csharp
// Given a mapping: parent (nested) -> child (nested inside parent)
// Query a deeply nested field
var results = await repository.FindAsync(q => q
    .FilterExpression("parent.child.field1:value"));
```

### Known Limitations

| Limitation | Details |
|---|---|
| **TopHits round-tripping** | `TopHitsAggregate` cannot survive JSON serialization. See the [Top Hits Aggregation](#top-hits-aggregation) warning above. |

## Field Selection

Field selection controls which fields are returned from Elasticsearch via `_source` filtering. This reduces network payload and deserialization cost when you only need a subset of fields from a document.

### Including Fields

Use `.Include()` to specify individual fields to return:

```csharp
var results = await repository.FindAsync(
    query,
    o => o.Include(e => e.Id).Include(e => e.Name).Include(e => e.Email));
```

You can also pass multiple fields at once:

```csharp
var results = await repository.FindAsync(
    query,
    o => o.Include(e => e.Id, e => e.Name, e => e.Email));
```

### Excluding Fields

Use `.Exclude()` to omit specific fields while returning everything else:

```csharp
var results = await repository.FindAsync(
    query,
    o => o.Exclude(e => e.LargeContent).Exclude(e => e.Attachments));
```

### Field Mask Expressions

For complex field selections, use `.IncludeMask()` or `.ExcludeMask()` with a Google FieldMask-style expression. Nested fields are grouped with parentheses and comma-separated:

| Expression | Expanded Fields |
|---|---|
| `"id,name"` | `id`, `name` |
| `"address(street,city)"` | `address.street`, `address.city` |
| `"results(id,program(name,id))"` | `results.id`, `results.program.name`, `results.program.id` |

```csharp
var results = await repository.FindAsync(
    query,
    o => o.IncludeMask("id,name,address(street,city,state)"));
```

Masks and individual `.Include()`/`.Exclude()` calls are additive -- they are merged into a single set at query time.

### Query-Level vs Options-Level

Field includes and excludes can be set on both the query and the command options. Both sources are merged at execution time:

```csharp
var results = await repository.FindAsync(
    q => q.Include(e => e.Name),
    o => o.Include(e => e.Email));
// Both Name and Email are included
```

This is useful when a repository method sets default options-level field restrictions while callers add query-level overrides.

### Merge and Precedence Rules

At execution time, the repository merges all field selection settings:

1. **All includes are merged**: individual fields from `.Include()` and parsed fields from `.IncludeMask()` from both the query and command options combine into one include set.
2. **All excludes are merged**: same for `.Exclude()` and `.ExcludeMask()`.
3. **Includes win over excludes**: if the same field appears in both includes and excludes, it is included (the exclude is dropped).
4. **Automatic `Id` field**: when any includes are specified on an entity that implements `IIdentity`, the `Id` field is automatically added to ensure the document identity is always available.

### Default Excludes

Repositories can register fields to exclude by default by calling `AddDefaultExclude()` in the constructor:

```csharp
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeRepository(/* ... */)
    {
        AddDefaultExclude(e => e.InternalNotes);
        AddDefaultExclude(e => e.AuditLog);
    }
}
```

Default excludes are only applied when **no explicit excludes** are set on the query. As soon as the caller specifies any `.Exclude()` call, the defaults are skipped entirely. This prevents unexpected interactions between default and explicit excludes.

### Properties Required for Remove

When `RemoveAllAsync` processes batch deletions, it first queries for matching documents. The repository ensures that certain critical fields are always included in these queries (regardless of any field restrictions the caller may have set) by registering them with `AddPropertyRequiredForRemove()`:

```csharp
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeRepository(/* ... */)
    {
        AddPropertyRequiredForRemove(e => e.DepartmentId);
    }
}
```

The `Id` and `CreatedUtc` fields (when applicable) are registered automatically. These fields are needed for cache invalidation, message bus notifications, and event handlers that fire during the delete process.

### Caching Impact

When includes or excludes are active, the repository skips ID-based caching to avoid storing incomplete documents in the cache. This means:

- `GetByIdAsync` / `GetByIdsAsync` with field restrictions will always hit Elasticsearch directly.
- Queries with custom cache keys still function normally since they cache the complete filtered result as-is.

If performance is important and you frequently fetch partial documents, consider using a dedicated query with a custom cache key rather than relying on ID-based caching.

## Count and Exists

### Count with Query

```csharp
var count = await repository.CountAsync(q => q.FieldEquals(e => e.Department, "Engineering"));
Console.WriteLine($"Engineering employees: {count.Total}");
```

### Exists with Query

```csharp
bool hasActiveEmployees = await repository.ExistsAsync(
    q => q.FieldEquals(e => e.Status, "active"));
```

## Building Complex Queries

### Combining Query Methods

```csharp
var results = await repository.FindAsync(q => q
    .FieldEquals(e => e.Status, "active")
    .FieldEquals(e => e.Department, "Engineering")
    .DateRange(DateTime.UtcNow.AddYears(-1), DateTime.UtcNow, e => e.HireDate)
    .SortExpression("-salary")
    .AggregationsExpression("terms:title avg:salary"),
    o => o.PageLimit(50));
```

### Reusable Query Objects

```csharp
var query = new RepositoryQuery<Employee>()
    .FieldEquals(e => e.Department, "Engineering")
    .FieldCondition(e => e.Name, ComparisonOperator.Contains, "John");

var results = await repository.FindAsync(q => query);
var count = await repository.CountAsync(q => query);
```

### Custom Query Extensions

Create domain-specific query methods:

```csharp
public static class EmployeeQueryExtensions
{
    public static IRepositoryQuery<Employee> ActiveInDepartment(
        this IRepositoryQuery<Employee> query, string department)
    {
        return query
            .FieldEquals(e => e.Status, "active")
            .FieldEquals(e => e.Department, department);
    }

    public static IRepositoryQuery<Employee> HiredBetween(
        this IRepositoryQuery<Employee> query, DateTime start, DateTime end)
    {
        return query.DateRange(start, end, e => e.HireDate);
    }
}

// Usage
var results = await repository.FindAsync(q => q
    .ActiveInDepartment("Engineering")
    .HiredBetween(DateTime.UtcNow.AddYears(-2), DateTime.UtcNow));
```

## Query Logging

Enable query logging for debugging:

```csharp
var results = await repository.FindAsync(
    query,
    o => o.QueryLogLevel(Microsoft.Extensions.Logging.LogLevel.Debug));
```

## Async Queries

For long-running queries:

```csharp
// Start async query
var results = await repository.FindAsync(
    query,
    o => o.AsyncQuery(waitTime: TimeSpan.FromSeconds(5), ttl: TimeSpan.FromHours(1)));

if (results.Total == 0 && results.IsAsyncQueryRunning())
{
    // Query is still running, get the ID
    var queryId = results.GetAsyncQueryId();
    
    // Check later
    var laterResults = await repository.FindAsync(
        query,
        o => o.AsyncQueryId(queryId, waitTime: TimeSpan.FromSeconds(30)));
}
```

## Next Steps

- [Configuration](/guide/configuration) - Query configuration options
- [Caching](/guide/caching) - Cache query results
- [Soft Deletes](/guide/soft-deletes) - Query soft-deleted documents
- [Index Management](/guide/index-management) - Query across multiple indexes
