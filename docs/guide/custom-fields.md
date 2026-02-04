# Custom Fields

Foundatio.Repositories supports dynamic custom fields for tenant-specific or user-defined data. This guide covers implementing and using custom fields.

## Overview

Custom fields allow you to:
- Add tenant-specific fields without schema changes
- Support user-defined attributes
- Index dynamic data with proper field types
- Query and aggregate on custom fields

## The Elasticsearch Field Limit Problem

### Why Custom Fields Matter

Elasticsearch enforces a **default limit of 1,000 fields per index** through the `index.mapping.total_fields.limit` setting. This limit exists to prevent "mapping explosion"—uncontrolled field growth that causes:

- **High memory pressure**: Each field mapping consumes JVM heap memory
- **Slow cluster startup**: Large mappings take longer to load
- **Performance degradation**: Query planning becomes slower with more fields
- **Index corruption risk**: Extremely large mappings can cause stability issues

When you exceed this limit, Elasticsearch rejects document ingestion with:

```
Limit of total fields [1000] has been exceeded while adding new fields
```

### What Counts Toward the Limit

The limit counts **all mappers**, not just leaf fields:

| Type | Example | Count |
|------|---------|-------|
| Object mappers | `employee.address.city` | 3 (employee, address, city) |
| Field mappers | `name`, `email`, `age` | 1 each |
| Multi-fields | `name.keyword`, `name.raw` | 1 each |
| Field aliases | Any alias | 1 each |

### The Multi-Tenant Challenge

In multi-tenant applications, each tenant may want their own custom fields:

```
Tenant A: customField1, customField2, customField3
Tenant B: departmentCode, region, priority
Tenant C: projectId, clientRef, billingCode
...
```

With 100 tenants each wanting 10 custom fields, you'd need 1,000 fields just for custom data—hitting the limit immediately.

### Naive Solutions (And Why They Fail)

**Option 1: Increase the limit**
```json
PUT /my-index/_settings
{
  "index.mapping.total_fields.limit": 10000
}
```
❌ **Problem**: Causes memory issues, slow queries, and cluster instability.

**Option 2: Use dynamic mapping**
```json
{
  "mappings": {
    "dynamic": true
  }
}
```
❌ **Problem**: Creates new fields automatically, quickly hitting the limit.

**Option 3: Use flattened type**
```json
{
  "custom_data": {
    "type": "flattened"
  }
}
```
❌ **Problem**: Limited query capabilities—no range queries, no aggregations on numeric values.

### The Custom Fields Solution

Foundatio.Repositories solves this with **pooled field slots**:

```
Instead of:                    Use pooled slots:
tenant_a_field1  ─┐            idx.s0 ← All string fields
tenant_a_field2   │            idx.s1
tenant_b_field1   ├─ 1000+     idx.n0 ← All numeric fields
tenant_b_field2   │  fields    idx.n1
tenant_c_field1   │            idx.b0 ← All boolean fields
...              ─┘            idx.d0 ← All date fields
                               ────────
                               ~20 fields total
```

**How it works:**
1. Define a fixed number of typed slots in your mapping (e.g., 5 string slots, 3 numeric slots)
2. Each tenant's custom field is assigned to an available slot of the matching type
3. Field definitions map logical names to physical slots per tenant
4. Queries are automatically translated from logical names to slot names

**Benefits:**
- ✅ Unlimited logical custom fields across all tenants
- ✅ Full query and aggregation support
- ✅ Proper field types (not just strings)
- ✅ Fixed, predictable mapping size
- ✅ No risk of mapping explosion

## Enabling Custom Fields

### Implement IHaveCustomFields

```csharp
using Foundatio.Repositories.Elasticsearch.CustomFields;

public class Employee : IIdentity, IHaveDates, IHaveCustomFields
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public DateTime CreatedUtc { get; set; }
    public DateTime UpdatedUtc { get; set; }
    
    // Custom fields
    public IDictionary<string, object> Data { get; set; } = new Dictionary<string, object>();
    public IDictionary<string, object> Idx { get; set; } = new Dictionary<string, object>();
    
    public string GetTenantKey() => TenantId;
    public string TenantId { get; set; }
}
```

### Interface Requirements

```csharp
public interface IHaveCustomFields : IHaveData
{
    IDictionary<string, object> Idx { get; }  // Indexed values
    string GetTenantKey();                    // Tenant identifier
}

public interface IHaveData
{
    IDictionary<string, object> Data { get; set; }  // Non-indexed data
}
```

**Data vs Idx:**
- `Data` - Stored but not indexed (use for large or rarely queried data)
- `Idx` - Stored and indexed (use for queryable/aggregatable fields)

## Custom Field Definitions

### CustomFieldDefinition

```csharp
public record CustomFieldDefinition : IIdentity, IHaveDates, ISupportSoftDeletes
{
    public string Id { get; set; }
    public string EntityType { get; set; }     // e.g., "Employee"
    public string TenantKey { get; set; }      // Tenant identifier
    public string Name { get; set; }           // Field name
    public string IndexType { get; set; }      // e.g., "string", "number", "date"
    public int IndexSlot { get; set; }         // Pooled slot for mapping
    public CustomFieldProcessMode ProcessMode { get; set; }
    public DateTime CreatedUtc { get; set; }
    public DateTime UpdatedUtc { get; set; }
    public bool IsDeleted { get; set; }
}
```

### ProcessMode

```csharp
public enum CustomFieldProcessMode
{
    None,       // Don't process
    Index,      // Index the value
    Copy        // Copy to another field
}
```

## Built-in Field Types

| Type | IndexType | Description |
|------|-----------|-------------|
| `BooleanFieldType` | `boolean` | True/false values |
| `DateFieldType` | `date` | Date/time values |
| `DoubleFieldType` | `double` | Floating-point numbers |
| `FloatFieldType` | `float` | Single-precision floats |
| `IntegerFieldType` | `integer` | Integer numbers |
| `KeywordFieldType` | `keyword` | Exact match strings |
| `LongFieldType` | `long` | Long integers |
| `StringFieldType` | `string` | Analyzed text |

## Configuration

### Repository Configuration

```csharp
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeRepository(EmployeeIndex index) : base(index)
    {
        AutoCreateCustomFields = true;  // Auto-create field definitions
    }
}
```

### Index Configuration

```csharp
public sealed class EmployeeIndex : VersionedIndex<Employee>
{
    public EmployeeIndex(IElasticConfiguration configuration) 
        : base(configuration, "employees", version: 1)
    {
        // Register custom field types
        CustomFieldTypes.Add("string", new StringFieldType());
        CustomFieldTypes.Add("keyword", new KeywordFieldType());
        CustomFieldTypes.Add("integer", new IntegerFieldType());
        CustomFieldTypes.Add("double", new DoubleFieldType());
        CustomFieldTypes.Add("boolean", new BooleanFieldType());
        CustomFieldTypes.Add("date", new DateFieldType());
    }

    public override TypeMappingDescriptor<Employee> ConfigureIndexMapping(
        TypeMappingDescriptor<Employee> map)
    {
        return map
            .Dynamic(false)
            .Properties(p => p
                .SetupDefaults()
                // ... other fields ...
                
                // Configure custom field slots
                .Object<object>(o => o
                    .Name("idx")
                    .Properties(ip => ip
                        .Keyword(f => f.Name("s0"))  // String slot 0
                        .Keyword(f => f.Name("s1"))  // String slot 1
                        .Number(f => f.Name("n0").Type(NumberType.Double))  // Number slot 0
                        .Number(f => f.Name("n1").Type(NumberType.Double))  // Number slot 1
                        .Boolean(f => f.Name("b0"))  // Boolean slot 0
                        .Date(f => f.Name("d0"))     // Date slot 0
                    ))
            );
    }
}
```

## Using Custom Fields

### Setting Custom Fields

```csharp
var employee = new Employee
{
    Name = "John Doe",
    TenantId = "tenant-123",
    Data = new Dictionary<string, object>
    {
        ["notes"] = "Some notes that don't need indexing"
    },
    Idx = new Dictionary<string, object>
    {
        ["department"] = "Engineering",
        ["level"] = 5,
        ["isRemote"] = true
    }
};

await repository.AddAsync(employee);
```

### Querying Custom Fields

```csharp
// Query by custom field
var results = await repository.FindAsync(q => q
    .FilterExpression("idx.department:Engineering"));

// Query with aggregation
var results = await repository.CountAsync(q => q
    .AggregationsExpression("terms:idx.department"));
```

### Field Name Resolution

Custom fields are automatically resolved based on definitions:

```csharp
// If "department" is defined as a custom field for this tenant,
// this query will be translated to query the appropriate idx slot
var results = await repository.FindAsync(q => q
    .FilterExpression("department:Engineering"));
```

## Custom Field Types

### Implementing ICustomFieldType

```csharp
public interface ICustomFieldType
{
    string Type { get; }
    Task<ProcessFieldValueResult> ProcessValueAsync<T>(
        T document, 
        object value, 
        CustomFieldDefinition fieldDefinition);
    IProperty ConfigureMapping<T>(SingleMappingSelector<T> map);
}
```

### Custom Field Type Example

```csharp
public class CurrencyFieldType : ICustomFieldType
{
    public string Type => "currency";

    public Task<ProcessFieldValueResult> ProcessValueAsync<T>(
        T document, 
        object value, 
        CustomFieldDefinition fieldDefinition)
    {
        if (value is decimal decimalValue)
        {
            // Store as cents for precision
            return Task.FromResult(new ProcessFieldValueResult
            {
                Value = (long)(decimalValue * 100)
            });
        }
        
        return Task.FromResult(new ProcessFieldValueResult { Value = value });
    }

    public IProperty ConfigureMapping<T>(SingleMappingSelector<T> map)
    {
        return map.Number(n => n.Type(NumberType.Long));
    }
}
```

### Registering Custom Types

```csharp
public EmployeeIndex(IElasticConfiguration configuration) 
    : base(configuration, "employees", version: 1)
{
    CustomFieldTypes.Add("currency", new CurrencyFieldType());
}
```

## Managing Field Definitions

### Custom Field Definition Repository

```csharp
public interface ICustomFieldDefinitionRepository : ISearchableRepository<CustomFieldDefinition>
{
    Task<CustomFieldDefinition> GetByNameAsync(string entityType, string tenantKey, string name);
    Task<IReadOnlyCollection<CustomFieldDefinition>> GetByEntityTypeAsync(string entityType, string tenantKey);
}
```

### Creating Field Definitions

```csharp
var definition = new CustomFieldDefinition
{
    EntityType = nameof(Employee),
    TenantKey = "tenant-123",
    Name = "department",
    IndexType = "keyword",
    ProcessMode = CustomFieldProcessMode.Index
};

await customFieldRepository.AddAsync(definition);
```

### Auto-Creating Field Definitions

When `AutoCreateCustomFields = true`:

```csharp
// This automatically creates a field definition if one doesn't exist
var employee = new Employee
{
    Idx = new Dictionary<string, object>
    {
        ["newField"] = "value"  // Auto-creates definition
    }
};
await repository.AddAsync(employee);
```

## Slot Management

Custom fields use pooled slots in the index mapping to avoid mapping explosions. This is the core mechanism that enables unlimited custom fields while staying within Elasticsearch's field limits.

### How Slots Work

```
Logical Field Name          Slot Assignment         Physical Field
─────────────────          ────────────────         ──────────────
Tenant A: "department"  →  string slot 0       →   idx.s0
Tenant A: "region"      →  string slot 1       →   idx.s1
Tenant B: "department"  →  string slot 0       →   idx.s0  (same slot, different tenant)
Tenant B: "priority"    →  integer slot 0      →   idx.n0
Tenant C: "projectId"   →  string slot 0       →   idx.s0
```

Each tenant gets their own namespace, so "department" for Tenant A and "department" for Tenant B both map to `idx.s0` but are isolated by tenant queries.

### Slot Naming Convention

```csharp
// Mapping has fixed slots by type:
// idx.s0, idx.s1, idx.s2... - String/keyword slots (s = string)
// idx.n0, idx.n1, idx.n2... - Numeric slots (n = number)
// idx.b0, idx.b1...         - Boolean slots (b = boolean)
// idx.d0, idx.d1...         - Date slots (d = date)

// Field definitions map logical names to slots:
// "department" -> idx.s0
// "level" -> idx.n0
// "isRemote" -> idx.b0
```

### Capacity Planning

Plan your slot capacity based on expected custom field usage:

```csharp
.Object<object>(o => o
    .Name("idx")
    .Properties(ip => ip
        // String slots - most common, allocate more
        .Keyword(f => f.Name("s0"))
        .Keyword(f => f.Name("s1"))
        .Keyword(f => f.Name("s2"))
        .Keyword(f => f.Name("s3"))
        .Keyword(f => f.Name("s4"))
        .Keyword(f => f.Name("s5"))
        .Keyword(f => f.Name("s6"))
        .Keyword(f => f.Name("s7"))
        .Keyword(f => f.Name("s8"))
        .Keyword(f => f.Name("s9"))  // 10 string slots
        
        // Numeric slots
        .Number(f => f.Name("n0").Type(NumberType.Double))
        .Number(f => f.Name("n1").Type(NumberType.Double))
        .Number(f => f.Name("n2").Type(NumberType.Double))
        .Number(f => f.Name("n3").Type(NumberType.Double))
        .Number(f => f.Name("n4").Type(NumberType.Double))  // 5 numeric slots
        
        // Boolean slots - usually need fewer
        .Boolean(f => f.Name("b0"))
        .Boolean(f => f.Name("b1"))
        .Boolean(f => f.Name("b2"))  // 3 boolean slots
        
        // Date slots
        .Date(f => f.Name("d0"))
        .Date(f => f.Name("d1"))
        .Date(f => f.Name("d2"))  // 3 date slots
    ))
```

::: tip Slot Capacity Guidelines
- **String slots**: Allocate the most (10-20). Most custom fields are strings/keywords.
- **Numeric slots**: Moderate allocation (5-10). Used for counts, amounts, scores.
- **Boolean slots**: Fewer needed (3-5). Binary flags are less common.
- **Date slots**: Fewer needed (3-5). Custom dates are relatively rare.

Total: ~25-40 slots covers most use cases while using only ~25-40 of your 1,000 field budget.
:::

### Slot Exhaustion

If a tenant runs out of slots for a particular type:

```csharp
// When all string slots are used, adding a new string field fails
try
{
    await repository.AddAsync(employeeWithNewStringField);
}
catch (InvalidOperationException ex)
{
    // "No available slot for field type 'string'"
}
```

**Solutions:**
1. Add more slots (requires reindex)
2. Delete unused custom field definitions to free slots
3. Use `Data` dictionary for non-queryable fields

## Virtual Custom Fields

### IHaveVirtualCustomFields

For fields computed at query time:

```csharp
public interface IHaveVirtualCustomFields : IHaveCustomFields
{
    // Marker interface for entities that support virtual fields
}
```

## Best Practices

### 1. Use Appropriate Field Types

```csharp
// Good: Use correct types for better indexing
Idx["count"] = 42;           // Integer
Idx["price"] = 19.99;        // Double
Idx["isActive"] = true;      // Boolean
Idx["createdAt"] = DateTime.UtcNow;  // Date

// Bad: Store everything as strings
Idx["count"] = "42";         // Loses numeric capabilities
```

### 2. Index Only What You Query

```csharp
// Queryable data goes in Idx
Idx["searchableField"] = "value";

// Large or rarely queried data goes in Data
Data["notes"] = "Long text that doesn't need indexing...";
```

### 3. Plan Slot Capacity

```csharp
// Configure enough slots for expected custom fields
.Object<object>(o => o
    .Name("idx")
    .Properties(ip => ip
        .Keyword(f => f.Name("s0"))
        .Keyword(f => f.Name("s1"))
        .Keyword(f => f.Name("s2"))
        .Keyword(f => f.Name("s3"))
        .Keyword(f => f.Name("s4"))  // 5 string slots
        .Number(f => f.Name("n0"))
        .Number(f => f.Name("n1"))
        .Number(f => f.Name("n2"))   // 3 numeric slots
    ))
```

### 4. Handle Missing Fields

```csharp
// Check before accessing
if (employee.Idx.TryGetValue("department", out var dept))
{
    Console.WriteLine($"Department: {dept}");
}
```

### 5. Validate Field Values

```csharp
protected override Task ValidateAndThrowAsync(Employee document)
{
    foreach (var (key, value) in document.Idx)
    {
        var definition = GetFieldDefinition(key);
        if (definition != null && !IsValidValue(value, definition.IndexType))
        {
            throw new DocumentValidationException(
                $"Invalid value for custom field '{key}'");
        }
    }
    return Task.CompletedTask;
}
```

## Next Steps

- [Querying](/guide/querying) - Query custom fields
- [Index Management](/guide/index-management) - Configure index mappings
- [Configuration](/guide/configuration) - Custom field configuration
