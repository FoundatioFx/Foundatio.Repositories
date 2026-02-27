# Custom Fields

Foundatio.Repositories supports dynamic custom fields for tenant-specific or user-defined data. This guide covers setup, usage, lifecycle management, and best practices.

## Overview

Custom fields allow you to:
- Add tenant-specific fields without schema changes
- Support user-defined attributes
- Index dynamic data with proper field types
- Query and aggregate on custom fields

## The Elasticsearch Field Limit Problem

### Why Custom Fields Matter

Elasticsearch enforces a **default limit of 1,000 fields per index** through the `index.mapping.total_fields.limit` setting. This limit exists to prevent "mapping explosion" — uncontrolled field growth that causes:

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

With 100 tenants each wanting 10 custom fields, you'd need 1,000 fields just for custom data — hitting the limit immediately.

### Naive Solutions (And Why They Fail)

**Option 1: Increase the limit**
```json
PUT /my-index/_settings
{
  "index.mapping.total_fields.limit": 10000
}
```
::: danger
Causes memory issues, slow queries, and cluster instability.
:::

**Option 2: Use dynamic mapping**
```json
{
  "mappings": {
    "dynamic": true
  }
}
```
::: danger
Creates new fields automatically, quickly hitting the limit.
:::

**Option 3: Use flattened type**
```json
{
  "custom_data": {
    "type": "flattened"
  }
}
```
::: danger
Limited query capabilities — no range queries, no aggregations on numeric values.
:::

### The Custom Fields Solution

Foundatio.Repositories solves this with **pooled field slots** and **dynamic templates**:

```
Instead of:                    Use pooled slots:
tenant_a_field1  ─┐            idx.string-1   ← All string fields
tenant_a_field2   │            idx.string-2
tenant_b_field1   ├─ 1000+     idx.int-1      ← All integer fields
tenant_b_field2   │  fields    idx.int-2
tenant_c_field1   │            idx.bool-1     ← All boolean fields
...              ─┘            idx.date-1     ← All date fields
                               ────────
                               ~20 fields total
```

**How it works:**
1. Register typed field handlers in your index (e.g., string, int, bool, date)
2. Elasticsearch dynamic templates auto-map `idx.*` sub-fields by type pattern
3. Each tenant's custom field is assigned to an available slot of the matching type
4. Field definitions map logical names to physical slots per tenant
5. Queries are automatically translated from logical names to slot names

**Benefits:**
- Unlimited logical custom fields across all tenants
- Full query and aggregation support
- Proper field types (not just strings)
- Fixed, predictable mapping size
- No risk of mapping explosion

## Setup and Registration

### 1. Implement IHaveCustomFields on Your Entity

```csharp
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Models;

public class Employee : IIdentity, IHaveDates, IHaveCustomFields
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string CompanyId { get; set; } = string.Empty;
    public DateTime CreatedUtc { get; set; }
    public DateTime UpdatedUtc { get; set; }

    public IDictionary<string, object> Data { get; set; } = new Dictionary<string, object>();
    public IDictionary<string, object> Idx { get; set; } = new Dictionary<string, object>();

    public string GetTenantKey() => CompanyId;
}
```

### Interface Requirements

```csharp
public interface IHaveCustomFields : IHaveData
{
    IDictionary<string, object> Idx { get; }
    string GetTenantKey();
}

public interface IHaveData
{
    IDictionary<string, object> Data { get; set; }
}
```

**Data vs Idx:**
- `Data` - Stored but not indexed. Put custom field **values** here. The framework reads from `Data` during save.
- `Idx` - Stored and indexed. The framework **automatically populates** this from `Data` during save. Do not set `Idx` values directly.

### 2. Configure Your Index

Register custom field types in your index constructor. Call `AddStandardCustomFieldTypes()` to register all built-in types, or register individual types with `AddCustomFieldType()`:

```csharp
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Extensions;

public sealed class EmployeeIndex : VersionedIndex<Employee>
{
    public EmployeeIndex(IElasticConfiguration configuration)
        : base(configuration, "employees", version: 1)
    {
        AddStandardCustomFieldTypes();
    }

    public override TypeMappingDescriptor<Employee> ConfigureIndexMapping(
        TypeMappingDescriptor<Employee> map)
    {
        return map
            .Dynamic(false)
            .Properties(p => p
                .SetupDefaults()
                .Keyword(f => f.Name(e => e.Id))
                .Keyword(f => f.Name(e => e.CompanyId))
                .Text(f => f.Name(e => e.Name))
            );
    }
}
```

::: tip How It Works Under the Hood
- `SetupDefaults()` detects `IHaveCustomFields` on your entity and automatically adds `idx` as a dynamic object field.
- When custom field types are registered, Elasticsearch **dynamic templates** are created that auto-map sub-fields under `idx.*` by type pattern (e.g., `string-*`, `int-*`, `bool-*`).
- This means you do **not** need to manually define individual slot fields in your mapping.
:::

### 3. Configure Your ElasticConfiguration

Call `AddCustomFieldIndex()` in your configuration constructor to create the `CustomFieldDefinition` index and enable the `CustomFieldDefinitionRepository`:

```csharp
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.CustomFields;

public class MyAppElasticConfiguration : ElasticConfiguration
{
    public MyAppElasticConfiguration(
        IQueue<WorkItemData> workItemQueue,
        ICacheClient cacheClient,
        IMessageBus messageBus,
        ILoggerFactory loggerFactory)
        : base(workItemQueue, cacheClient, messageBus, loggerFactory: loggerFactory)
    {
        AddIndex(Employees = new EmployeeIndex(this));
        CustomFields = AddCustomFieldIndex(replicas: 1);
    }

    public EmployeeIndex Employees { get; }
    public CustomFieldDefinitionIndex CustomFields { get; }
}
```

`AddCustomFieldIndex()` creates a `CustomFieldDefinitionIndex` (a `VersionedIndex<CustomFieldDefinition>`) and registers it with the configuration. The `CustomFieldDefinitionRepository` is lazily created when first accessed via `configuration.CustomFieldDefinitionRepository`.

### 4. Register DI Services

Register the `ICustomFieldDefinitionRepository` singleton by resolving it from your configuration:

```csharp
services.AddSingleton<MyAppElasticConfiguration>();
services.AddSingleton<IElasticConfiguration>(s => s.GetRequiredService<MyAppElasticConfiguration>());
services.AddSingleton<ICustomFieldDefinitionRepository>(s =>
    s.GetRequiredService<MyAppElasticConfiguration>().CustomFieldDefinitionRepository);
```

### 5. Configure Your Repository

```csharp
using Foundatio.Repositories.Elasticsearch;

public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeRepository(EmployeeIndex index) : base(index)
    {
        AutoCreateCustomFields = true;
    }
}
```

When `AutoCreateCustomFields` is `true`, any key in `Data` that doesn't have a matching `CustomFieldDefinition` will automatically get one created as a `string` type.

## Custom Field Definitions

### CustomFieldDefinition

Each custom field is tracked by a `CustomFieldDefinition` record stored in a dedicated Elasticsearch index:

```csharp
public record CustomFieldDefinition : IIdentity, IHaveDates, ISupportSoftDeletes, IHaveData
{
    public string Id { get; set; }
    public string EntityType { get; set; }     // e.g., "Employee" (immutable after creation)
    public string TenantKey { get; set; }      // Tenant identifier (immutable after creation)
    public string Name { get; set; }           // Friendly field name
    public string Description { get; set; }    // Optional description
    public int DisplayOrder { get; set; }      // UI ordering hint
    public CustomFieldProcessMode ProcessMode { get; set; } = CustomFieldProcessMode.ProcessOnValue;
    public int ProcessOrder { get; set; }      // Processing sequence within a mode
    public string IndexType { get; set; }      // e.g., "string", "int", "date", "bool"
    public int IndexSlot { get; set; }         // Auto-assigned slot number (immutable after creation)
    public IDictionary<string, object> Data { get; set; } = new Dictionary<string, object>();
    public DateTime CreatedUtc { get; set; }
    public DateTime UpdatedUtc { get; set; }
    public bool IsDeleted { get; set; }
}
```

::: warning Immutable After Creation
`EntityType`, `TenantKey`, and `IndexSlot` cannot be changed after a definition is created. Attempting to modify these via `SaveAsync` throws a `DocumentValidationException`.
:::

### ProcessMode

```csharp
public enum CustomFieldProcessMode
{
    ProcessOnValue,  // Default: process only when Data contains a value for this field
    AlwaysProcess    // Run the field type processor even when no value is present (for calculated fields)
}
```

- `ProcessOnValue` fields are processed first, in `ProcessOrder` order
- `AlwaysProcess` fields are processed after all `ProcessOnValue` fields, in `ProcessOrder` order

### ICustomFieldDefinitionRepository

The repository provides CRUD operations plus custom field-specific methods:

```csharp
public interface ICustomFieldDefinitionRepository : ISearchableRepository<CustomFieldDefinition>
{
    Task<IDictionary<string, CustomFieldDefinition>> GetFieldMappingAsync(
        string entityType, string tenantKey);

    Task<FindResults<CustomFieldDefinition>> FindByTenantAsync(
        string entityType, string tenantKey);

    Task<CustomFieldDefinition> AddFieldAsync(
        string entityType, string tenantKey, string name, string indexType,
        string description = null, int displayOrder = 0, IDictionary<string, object> data = null);
}
```

| Method | Description |
|--------|-------------|
| `GetFieldMappingAsync` | Returns a name-keyed dictionary of all active definitions for a tenant. Cached for 15 minutes. |
| `FindByTenantAsync` | Returns paginated results of all definitions for an entity type + tenant (up to 1000 per page). |
| `AddFieldAsync` | Convenience method to create a `CustomFieldDefinition` with auto-assigned slot. |

The concrete `CustomFieldDefinitionRepository` class also exposes `RemoveByTenantAsync(entityType, tenantKey)` for bulk tenant removal (not on the interface).

Since the interface extends `ISearchableRepository<CustomFieldDefinition>`, all standard repository methods are available: `AddAsync`, `SaveAsync`, `RemoveAsync`, `RemoveAllAsync`, `GetByIdAsync`, `GetByIdsAsync`, `FindAsync`, etc.

## Built-in Field Types

| Class | IndexType | Elasticsearch Mapping | Slot Pattern |
|-------|-----------|----------------------|--------------|
| `BooleanFieldType` | `bool` | Boolean | `idx.bool-{slot}` |
| `DateFieldType` | `date` | Date | `idx.date-{slot}` |
| `DoubleFieldType` | `double` | Number (Double) | `idx.double-{slot}` |
| `FloatFieldType` | `float` | Number (Float) | `idx.float-{slot}` |
| `IntegerFieldType` | `int` | Number (Integer) | `idx.int-{slot}` |
| `KeywordFieldType` | `keyword` | Keyword | `idx.keyword-{slot}` |
| `LongFieldType` | `long` | Number (Long) | `idx.long-{slot}` |
| `StringFieldType` | `string` | Text + Keyword sub-field | `idx.string-{slot}` |

Register all standard types at once with `AddStandardCustomFieldTypes()` in your index constructor, or register individual types with `AddCustomFieldType<T>()` or `AddCustomFieldType(instance)`.

## Using Custom Fields

### Setting Custom Fields

Custom field values go in the `Data` dictionary. The framework automatically processes them into `Idx` during save:

```csharp
var employee = new Employee
{
    Name = "John Doe",
    CompanyId = "tenant-123",
    Data = new Dictionary<string, object>
    {
        ["department"] = "Engineering",
        ["level"] = 5,
        ["isRemote"] = true
    }
};

await _repository.AddAsync(employee, o => o.ImmediateConsistency());
```

::: warning
Do **not** set values directly on `Idx`. The framework clears and repopulates `Idx` from `Data` on every save, using the registered `CustomFieldDefinition` for each field to determine the correct slot.
:::

### Querying Custom Fields

Custom fields support automatic field name resolution. Use logical field names in filter expressions — the framework translates them to the correct `idx.*` slot:

```csharp
var results = await _repository.FindAsync(q => q
    .FilterExpression("department:Engineering"));

var results = await _repository.FindAsync(q => q
    .FilterExpression("level:5"));
```

Field name resolution is case-insensitive and requires the query to include a tenant key so the correct field mapping can be loaded.

### Type Mismatches

If a value in `Data` does not match the `IndexType` of its `CustomFieldDefinition`, the document will still be saved but Elasticsearch will silently reject the malformed index value. The field will appear to not exist when queried:

```csharp
// Definition expects an integer
await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
{
    EntityType = nameof(Employee),
    TenantKey = "acme",
    Name = "Level",
    IndexType = IntegerFieldType.IndexType
});

// But we store a string value
employee.Data["Level"] = "not-a-number";
await _repository.AddAsync(employee, o => o.ImmediateConsistency());

// This returns NO results — Elasticsearch ignored the malformed value
var results = await _repository.FindAsync(q => q
    .FilterExpression("_exists_:level"));
```

::: warning
The document is saved successfully and `Data["Level"]` will contain `"not-a-number"` when retrieved. However, the value is **not indexed** — it won't appear in search results, `_exists_` checks, or aggregations. Always validate values before saving to avoid silent data loss in the index.
:::

## Custom Field Types

### Implementing ICustomFieldType

```csharp
public interface ICustomFieldType
{
    string Type { get; }
    Task<ProcessFieldValueResult> ProcessValueAsync<T>(
        T document, object value, CustomFieldDefinition fieldDefinition) where T : class;
    IProperty ConfigureMapping<T>(SingleMappingSelector<T> map) where T : class;
}

public class ProcessFieldValueResult
{
    public object Value { get; set; }
    public object Idx { get; set; }
    public bool IsCustomFieldDefinitionModified { get; set; }
}
```

| Property | Description |
|----------|-------------|
| `Value` | The processed value to store back in `Data` |
| `Idx` | Optional separate value for the index (if different from `Value`). When `null`, `Value` is used for both. |
| `IsCustomFieldDefinitionModified` | Set to `true` if your processor modified the `CustomFieldDefinition` itself (triggers a save). |

### Custom Field Type Example

```csharp
public class PercentFieldType : ICustomFieldType
{
    public string Type => "percent";

    public Task<ProcessFieldValueResult> ProcessValueAsync<T>(
        T document, object value, CustomFieldDefinition fieldDefinition) where T : class
    {
        if (value is int intValue)
        {
            var clamped = Math.Clamp(intValue, 0, 100);
            return Task.FromResult(new ProcessFieldValueResult { Value = clamped });
        }

        return Task.FromResult(new ProcessFieldValueResult { Value = value });
    }

    public IProperty ConfigureMapping<T>(SingleMappingSelector<T> map) where T : class
    {
        return map.Number(n => n.Type(NumberType.Integer));
    }
}
```

### Registering Custom Types

```csharp
public sealed class EmployeeIndex : VersionedIndex<Employee>
{
    public EmployeeIndex(IElasticConfiguration configuration)
        : base(configuration, "employees", version: 1)
    {
        AddStandardCustomFieldTypes();
        AddCustomFieldType(new PercentFieldType());
    }
}
```

## Field Definition Lifecycle Management

Understanding the full lifecycle of a custom field definition is critical for capacity planning and avoiding slot exhaustion.

### Complete Lifecycle Example

This example walks through creating, soft-deleting, reusing names, hard-deleting, and reclaiming slots:

```csharp
// Step 1: Create three string fields for tenant "acme"
// Slots are assigned sequentially: 1, 2, 3
var field1 = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
{
    EntityType = nameof(Employee),
    TenantKey = "acme",
    Name = "Department",
    IndexType = StringFieldType.IndexType
});
// field1.IndexSlot == 1, physical field: idx.string-1

var field2 = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
{
    EntityType = nameof(Employee),
    TenantKey = "acme",
    Name = "Region",
    IndexType = StringFieldType.IndexType
});
// field2.IndexSlot == 2, physical field: idx.string-2

var field3 = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
{
    EntityType = nameof(Employee),
    TenantKey = "acme",
    Name = "CostCenter",
    IndexType = StringFieldType.IndexType
});
// field3.IndexSlot == 3, physical field: idx.string-3

// Step 2: Soft-delete "Region" — frees the NAME but NOT the slot
field2.IsDeleted = true;
await _customFieldDefinitionRepository.SaveAsync(field2);

var mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync(
    nameof(Employee), "acme");
// mapping contains "Department" and "CostCenter" — "Region" is excluded

// Step 3: Reuse the name "Region" — gets a NEW slot (4), not the old one (2)
var field4 = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
{
    EntityType = nameof(Employee),
    TenantKey = "acme",
    Name = "Region",
    IndexType = StringFieldType.IndexType
});
// field4.IndexSlot == 4 (slot 2 is still occupied by the soft-deleted record)

// Step 4: Hard-delete the original soft-deleted "Region" — frees slot 2
await _customFieldDefinitionRepository.RemoveAsync(field2);

// Step 5: Next new field gets the freed slot 2
var field5 = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
{
    EntityType = nameof(Employee),
    TenantKey = "acme",
    Name = "Division",
    IndexType = StringFieldType.IndexType
});
// field5.IndexSlot == 2 (recycled from the hard-deleted record)
```

::: tip Slot Recycling Summary
- **Soft delete** → name freed, slot occupied (allows graceful migration)
- **Hard delete** → name freed, slot freed (allows slot reuse)
- **To fully free a slot:** soft-delete first, then hard-delete once you're confident existing data has been migrated or is no longer needed
:::

### Creating Definitions

Create definitions explicitly via `AddAsync` or the `AddFieldAsync` convenience method:

```csharp
var definition = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
{
    EntityType = nameof(Employee),
    TenantKey = "tenant-123",
    Name = "department",
    IndexType = StringFieldType.IndexType
});
```

Or use the convenience method:

```csharp
var definition = await _customFieldDefinitionRepository.AddFieldAsync(
    entityType: nameof(Employee),
    tenantKey: "tenant-123",
    name: "department",
    indexType: "string",
    description: "Employee department");
```

**Slot assignment** is automatic. Slots start at 1 and increment per `(EntityType, TenantKey, IndexType)` scope. You cannot pre-assign a slot — `IndexSlot` must be 0 when calling `AddAsync`.

**Duplicate handling:**
- Adding a field with the **same name and same type** as an existing **active** definition silently returns the existing definition.
- Adding a field with the **same name but different type** throws `DocumentValidationException`.
- Adding a field with the **same name** as a **soft-deleted** definition creates a **new** definition with a **new slot**. The soft-deleted record's slot remains occupied until it is hard-deleted.

### Updating Definitions

Use `SaveAsync` to update mutable properties:

```csharp
definition.Description = "Updated description";
definition.DisplayOrder = 5;
await _customFieldDefinitionRepository.SaveAsync(definition);
```

**Mutable properties:** `Name`, `Description`, `DisplayOrder`, `ProcessMode`, `ProcessOrder`, `Data`, `IsDeleted`

**Immutable properties (enforced at save time):** `EntityType`, `TenantKey`, `IndexSlot`

### Soft Deleting Definitions

Soft delete frees the field **name** for reuse but the **slot remains occupied**:

```csharp
definition.IsDeleted = true;
await _customFieldDefinitionRepository.SaveAsync(definition);
```

After soft deletion:
- The field name can be reused by a new definition (assigned a new slot)
- The field is excluded from `GetFieldMappingAsync` results
- The slot is **not** freed — it cannot be reused until the definition is hard-deleted
- The definition is still queryable with `IncludeSoftDeletes()` option

### Hard Deleting Definitions

Hard delete frees **both** the name and the slot for reuse:

```csharp
await _customFieldDefinitionRepository.RemoveAsync(definition);
```

After hard deletion, the freed slot number will be reassigned to the next new field of the same type for that tenant.

### Bulk Operations

```csharp
// Find all definitions for a tenant
var tenantFields = await _customFieldDefinitionRepository.FindByTenantAsync(
    nameof(Employee), "tenant-123");
var allFields = new List<CustomFieldDefinition>();
do
{
    allFields.AddRange(tenantFields.Documents);
} while (await tenantFields.NextPageAsync());

// Hard-delete all definitions for a tenant (frees all slots)
await _customFieldDefinitionRepository.RemoveAllAsync(q => q
    .FieldEquals(d => d.EntityType, nameof(Employee))
    .FieldEquals(d => d.TenantKey, "tenant-123"));
```

## Cleanup Patterns

::: warning
There is **no built-in cleanup job** for custom field definitions. Applications must manage the lifecycle of their custom field definitions.
:::

### Tenant Offboarding

When a tenant is removed, hard-delete all their custom field definitions to free slots:

```csharp
await _customFieldDefinitionRepository.RemoveAllAsync(q => q
    .FieldEquals(d => d.EntityType, nameof(Employee))
    .FieldEquals(d => d.TenantKey, "tenant-123"));
```

### Slot Reclamation

Soft-deleted definitions still occupy slots. Periodically hard-delete old soft-deleted definitions to reclaim them:

```csharp
await _customFieldDefinitionRepository.RemoveAllAsync(q => q
    .FieldEquals(d => d.IsDeleted, true)
    .FieldEquals(d => d.EntityType, nameof(Employee))
    .DateRange(null, DateTime.UtcNow.AddDays(-30), (CustomFieldDefinition d) => d.UpdatedUtc),
    o => o.IncludeSoftDeletes());
```

### Synchronizing With Domain Model Changes

When your domain model controls which custom fields exist (e.g., tenant settings define available fields), you need to keep `CustomFieldDefinition` records in sync. A common pattern is subscribing to `DocumentsChanged` events and comparing the original vs. modified documents.

Below is a **simplified** example. Real-world implementations will handle more edge cases depending on your domain model.

```csharp
public class CustomFieldSyncService : IStartupAction
{
    private readonly ICustomFieldDefinitionRepository _customFieldDefinitionRepository;
    private readonly ITenantSettingsRepository _settingsRepository;

    public CustomFieldSyncService(
        ICustomFieldDefinitionRepository customFieldDefinitionRepository,
        ITenantSettingsRepository settingsRepository)
    {
        _customFieldDefinitionRepository = customFieldDefinitionRepository;
        _settingsRepository = settingsRepository;
    }

    public Task RunAsync(CancellationToken shutdownToken = default)
    {
        _settingsRepository.DocumentsChanged.AddHandler((_, args) =>
            SynchronizeCustomFieldsAsync(args.Documents));
        return Task.CompletedTask;
    }

    private async Task SynchronizeCustomFieldsAsync(
        IReadOnlyCollection<ModifiedDocument<TenantSettings>> changes)
    {
        var toAdd = new List<CustomFieldDefinition>();
        var toDelete = new List<CustomFieldDefinition>();

        foreach (var change in changes)
        {
            string tenantKey = change.Value?.Id ?? change.Original.Id;

            var originalFieldNames = (change.Original?.FieldNames ?? []).ToHashSet();
            var modifiedFieldNames = (change.Value?.FieldNames ?? []).ToHashSet();

            // New fields: in modified but not in original
            foreach (string name in modifiedFieldNames.Except(originalFieldNames))
            {
                toAdd.Add(new CustomFieldDefinition
                {
                    EntityType = nameof(Employee),
                    TenantKey = tenantKey,
                    Name = name,
                    IndexType = StringFieldType.IndexType
                });
            }

            // Removed fields: in original but not in modified — soft-delete them
            var removedNames = originalFieldNames.Except(modifiedFieldNames).ToHashSet();
            if (removedNames.Count > 0)
            {
                var existing = await _customFieldDefinitionRepository.FindByTenantAsync(
                    nameof(Employee), tenantKey);
                do
                {
                    toDelete.AddRange(existing.Documents.Where(f => removedNames.Contains(f.Name)));
                } while (await existing.NextPageAsync());
            }
        }

        foreach (var def in toDelete)
            def.IsDeleted = true;

        if (toDelete.Count > 0)
            await _customFieldDefinitionRepository.SaveAsync(toDelete);

        if (toAdd.Count > 0)
            await _customFieldDefinitionRepository.AddAsync(toAdd);
    }
}
```

::: tip
Soft-delete before adding. When soft-deleting and adding fields in the same batch, process deletes first. This ensures that any freed names are available for the new definitions and avoids name collision errors from orphaned records.
:::

## Slot Management

Custom fields use pooled slots in the index mapping to avoid mapping explosions.

### How Slots Work

```
Logical Field Name          Slot Assignment         Physical Field
─────────────────          ────────────────         ──────────────
Tenant A: "department"  →  string slot 1        →   idx.string-1
Tenant A: "region"      →  string slot 2        →   idx.string-2
Tenant B: "department"  →  string slot 1        →   idx.string-1  (same slot, different tenant)
Tenant B: "priority"    →  int slot 1           →   idx.int-1
Tenant C: "projectId"   →  string slot 1        →   idx.string-1
```

Each tenant gets their own slot namespace per `(EntityType, TenantKey, IndexType)`, so "department" for Tenant A and "department" for Tenant B both map to `idx.string-1` but are isolated by tenant-scoped queries.

### Slot Naming Convention

Slot names follow the pattern `{IndexType}-{IndexSlot}`:
```
idx.string-1, idx.string-2, idx.string-3...  - String/text slots
idx.keyword-1, idx.keyword-2...              - Keyword slots
idx.int-1, idx.int-2...                      - Integer slots
idx.double-1, idx.double-2...                - Double slots
idx.bool-1, idx.bool-2...                    - Boolean slots
idx.date-1, idx.date-2...                    - Date slots
```

These are **automatically mapped** by Elasticsearch dynamic templates. You do not need to pre-declare individual slot fields.

### Slot Exhaustion

Elasticsearch dynamic templates can create new sub-fields on demand, so slot capacity is effectively unlimited per type. However, each additional slot increases the total field count in the index. Monitor your total field usage relative to Elasticsearch's `index.mapping.total_fields.limit`.

## Calculated / Computed Fields

Use `ProcessMode = CustomFieldProcessMode.AlwaysProcess` to create fields that are computed from other field values during save. Combined with a custom `ICustomFieldType`, this enables derived fields.

### Processing Order

1. **`ProcessOnValue` fields** run first — only when a matching key exists in `Data`
2. **`AlwaysProcess` fields** run after all `ProcessOnValue` fields — regardless of whether a value exists
3. Within each phase, fields are processed in `ProcessOrder` order

### Example: Calculated Integer Field

Define the custom field type:

```csharp
public class CalculatedIntegerFieldType : IntegerFieldType
{
    private readonly ScriptService _scriptService;

    public CalculatedIntegerFieldType(ScriptService scriptService)
    {
        _scriptService = scriptService;
    }

    public override async Task<ProcessFieldValueResult> ProcessValueAsync<T>(
        T document, object value, CustomFieldDefinition fieldDefinition) where T : class
    {
        if (!fieldDefinition.Data.TryGetValue("Expression", out object expression))
            return await base.ProcessValueAsync(document, value, fieldDefinition);

        var result = await _scriptService.EvaluateForSourceAsync(document, expression.ToString());

        if (result.IsCancelled || result.Value is Double.NaN)
            return new ProcessFieldValueResult { Value = null };

        return new ProcessFieldValueResult { Value = result.Value };
    }
}
```

Register it in your index:

```csharp
AddStandardCustomFieldTypes();
AddCustomFieldType(new CalculatedIntegerFieldType(scriptService));
```

Create the calculated field definition with an expression in `Data`:

```csharp
await _customFieldDefinitionRepository.AddAsync([
    new CustomFieldDefinition
    {
        EntityType = nameof(Employee),
        TenantKey = "1",
        Name = "Field1",
        IndexType = IntegerFieldType.IndexType
    },
    new CustomFieldDefinition
    {
        EntityType = nameof(Employee),
        TenantKey = "1",
        Name = "Field2",
        IndexType = IntegerFieldType.IndexType
    },
    new CustomFieldDefinition
    {
        EntityType = nameof(Employee),
        TenantKey = "1",
        Name = "Calculated",
        IndexType = IntegerFieldType.IndexType,
        ProcessMode = CustomFieldProcessMode.AlwaysProcess,
        Data = new Dictionary<string, object>
        {
            { "Expression", "source.Data.Field1 + source.Data.Field2" }
        }
    }
]);
```

Now when a document is saved with `Field1 = 1` and `Field2 = 2`, the `Calculated` field automatically computes to `3` and is indexed for querying:

```csharp
employee.Data["Field1"] = 1;
employee.Data["Field2"] = 2;
await _repository.AddAsync(employee, o => o.ImmediateConsistency());

var results = await _repository.FindAsync(q => q
    .FilterExpression("calculated:3"));
```

## Virtual Custom Fields

For entities where custom fields are not stored in a flat `Data` dictionary, implement `IHaveVirtualCustomFields` instead of `IHaveCustomFields`:

```csharp
public interface IHaveVirtualCustomFields
{
    IDictionary<string, object> GetCustomFields();
    object GetCustomField(string name);
    void SetCustomField(string name, object value);
    void RemoveCustomField(string name);
    IDictionary<string, object> Idx { get; }
    string GetTenantKey();
}
```

This gives you full control over how custom field values are read and written, while the framework still handles slot assignment, `Idx` population, and query field resolution.

## Concurrency and Locking

### Thread-Safe Slot Allocation

Slot allocation uses **distributed locks** (via `CacheLockProvider`) scoped per `(EntityType, TenantKey, IndexType)` to prevent duplicate slot assignment under concurrent writes. The lock key follows the pattern `customfield:{entityType}:{tenantKey}:{indexType}`.

### Caching Behavior

| Cache | TTL | Key Pattern | Description |
|-------|-----|-------------|-------------|
| Field mapping | 15 min | `customfield:{entityType}:{tenantKey}` | Name-to-definition dictionary |
| Available slots | 5 min | `customfield:{entityType}:{tenantKey}:{indexType}:slots` | List of free slot numbers |
| Used names | 5 min | `customfield:{entityType}:{tenantKey}:names` | Set of active field names |

Cache invalidation happens automatically on add, save, and remove operations. Bulk removal (`RemoveAllAsync`) clears all custom field caches by prefix when the affected scope cannot be determined from the query.

### Consistency

The `CustomFieldDefinitionRepository` defaults to `Consistency.Immediate` (all writes use `refresh=wait_for`), ensuring that newly created definitions are immediately visible for subsequent queries and slot allocation checks.

## Best Practices

### 1. Use Appropriate Field Types

```csharp
// Good: use correct types for proper indexing and querying
employee.Data["count"] = 42;                        // int
employee.Data["price"] = 19.99;                     // double
employee.Data["isActive"] = true;                   // bool
employee.Data["createdAt"] = DateTime.UtcNow;       // date

// Bad: storing everything as strings loses type-specific query capabilities
employee.Data["count"] = "42";
```

### 2. Index Only What You Query

```csharp
// Queryable data goes in Data (gets indexed via CustomFieldDefinitions)
employee.Data["searchableField"] = "value";

// Large or rarely queried data can also go in Data without a definition
// (it will be stored but not indexed if there's no matching definition
// and AutoCreateCustomFields is false)
```

### 3. Handle Missing Fields

```csharp
if (employee.Data.TryGetValue("department", out var dept))
{
    Console.WriteLine($"Department: {dept}");
}
```

### 4. Plan for Cleanup

- Always soft-delete before hard-deleting to allow graceful migration
- Implement periodic cleanup of soft-deleted definitions older than a threshold
- Clean up definitions when tenants are offboarded
- Monitor slot usage relative to Elasticsearch field limits

### 5. Design Tenant Keys Carefully

The `TenantKey` returned by `GetTenantKey()` scopes all custom field definitions. Choose a key that matches your multi-tenancy boundary:

```csharp
// Simple: one set of custom fields per company
public string GetTenantKey() => CompanyId;

// Composite: separate custom fields per company + entity subtype
public string GetTenantKey() => $"{CompanyId}-{SubType}";
```

::: tip Tenant Key Guidelines
- Keep tenant keys **as simple as possible** — use only the fields that define your tenancy boundary.
- Each unique tenant key gets its own independent pool of field slots and names.
- More granular keys mean more isolation but also more `CustomFieldDefinition` records to manage.
- Tenant keys are **immutable** on `CustomFieldDefinition` — plan your key structure before deploying.
:::

## Next Steps

- [Querying](/guide/querying) - Query custom fields
- [Index Management](/guide/index-management) - Configure index mappings
- [Configuration](/guide/configuration) - Custom field configuration
