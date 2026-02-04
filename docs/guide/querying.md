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
var hit = await repository.FindOneAsync(q => q.FilterExpression("email:john@example.com"));
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

```csharp
// Comparison operators
var results = await repository.FindAsync(q => q
    .FieldCondition(e => e.Age, ComparisonOperator.GreaterThanOrEquals, 25));

// Contains (for text fields)
var results = await repository.FindAsync(q => q
    .FieldCondition(e => e.Name, ComparisonOperator.Contains, "John"));
```

### Field Empty/Has Value

```csharp
// Find documents where field is null or empty
var results = await repository.FindAsync(q => q.FieldEmpty(e => e.ManagerId));

// Find documents where field has a value
var results = await repository.FindAsync(q => q.FieldHasValue(e => e.ManagerId));
```

### Date Range

```csharp
var results = await repository.FindAsync(q => q
    .DateRange(
        start: DateTime.UtcNow.AddDays(-30),
        end: DateTime.UtcNow,
        field: e => e.CreatedUtc));
```

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
    q => q.FilterExpression("department:Engineering"),
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
    Console.WriteLine($"{bucket.Key}: {bucket.DocCount}");
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
    Console.WriteLine($"Department: {bucket.Key}, Count: {bucket.DocCount}");
}

// Value aggregations
var avgSalary = results.Aggs.Average("avg_salary");
var maxDate = results.Aggs.Max<DateTime>("max_createdUtc");

Console.WriteLine($"Average Salary: {avgSalary}");
Console.WriteLine($"Latest Created: {maxDate}");
```

## Field Selection

### Include Fields

```csharp
// Include specific fields only
var results = await repository.FindAsync(
    query,
    o => o.Include(e => e.Id).Include(e => e.Name).Include(e => e.Email));

// Using mask pattern
var results = await repository.FindAsync(
    query,
    o => o.IncludeMask("id,name,email,address.*"));
```

### Exclude Fields

```csharp
// Exclude large fields
var results = await repository.FindAsync(
    query,
    o => o.Exclude(e => e.LargeContent).Exclude(e => e.Attachments));

// Using mask pattern
var results = await repository.FindAsync(
    query,
    o => o.ExcludeMask("largeContent,attachments,internal*"));
```

## Count and Exists

### Count with Query

```csharp
var count = await repository.CountAsync(q => q.FilterExpression("department:Engineering"));
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
    .FilterExpression("status:active")
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
    .FieldCondition(e => e.Age, ComparisonOperator.GreaterThanOrEquals, 25);

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
    o => o.QueryLogLevel(LogLevel.Debug));
```

## Async Queries

For long-running queries:

```csharp
// Start async query
var results = await repository.FindAsync(
    query,
    o => o.AsyncQuery(waitTime: TimeSpan.FromSeconds(5), ttl: TimeSpan.FromHours(1)));

if (results.Total == 0 && results.HasAsyncQueryId())
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
