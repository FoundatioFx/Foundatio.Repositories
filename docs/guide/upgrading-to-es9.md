# Migrating to Elastic.Clients.Elasticsearch (ES9)

This guide covers breaking changes when upgrading from `NEST` (ES7) to `Elastic.Clients.Elasticsearch` (ES8/ES9).

## Package Changes

**Before:**
```xml
<PackageReference Include="NEST" Version="7.x" />
```

**After:**
```xml
<PackageReference Include="Foundatio.Repositories.Elasticsearch" Version="..." />
<!-- Transitively brings in Elastic.Clients.Elasticsearch -->
```

## Namespace Changes

Remove old NEST namespaces and add new ones:

```csharp
// Remove:
using Elasticsearch.Net;
using Nest;

// Add:
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Transport;
```

## ElasticConfiguration Changes

### Client Type

| Before | After |
|--------|-------|
| `IElasticClient Client` | `ElasticsearchClient Client` |
| `new ElasticClient(settings)` | `new ElasticsearchClient(settings)` |

### Connection Pool

| Before | After |
|--------|-------|
| `IConnectionPool` | `NodePool` |
| `new SingleNodeConnectionPool(uri)` | `new SingleNodePool(uri)` |
| `new StaticConnectionPool(nodes)` | `new StaticNodePool(nodes)` |
| `new SniffingConnectionPool(nodes)` | `new SniffingNodePool(nodes)` |

### Settings

| Before | After |
|--------|-------|
| `ConnectionSettings` | `ElasticsearchClientSettings` |
| `settings.BasicAuthentication(u, p)` | `settings.Authentication(new BasicAuthentication(u, p))` |
| `settings.ApiKeyAuthentication(id, key)` | `settings.Authentication(new ApiKey(encoded))` |

**Before:**
```csharp
protected override IConnectionPool CreateConnectionPool()
{
    return new SingleNodeConnectionPool(new Uri("http://localhost:9200"));
}

protected override void ConfigureSettings(ConnectionSettings settings)
{
    base.ConfigureSettings(settings);
    settings.BasicAuthentication("user", "pass");
}
```

**After:**
```csharp
protected override NodePool CreateConnectionPool()
{
    return new SingleNodePool(new Uri("http://localhost:9200"));
}

protected override void ConfigureSettings(ElasticsearchClientSettings settings)
{
    base.ConfigureSettings(settings);
    settings.Authentication(new BasicAuthentication("user", "pass"));
}
```

## Index Configuration Changes

### ConfigureIndex Return Type

`ConfigureIndex` changed from returning `CreateIndexDescriptor` to `void`:

**Before:**
```csharp
public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx)
{
    return base.ConfigureIndex(idx
        .Settings(s => s.NumberOfReplicas(0))
        .Map<Employee>(m => m.AutoMap().Properties(p => p.SetupDefaults())));
}
```

**After:**
```csharp
public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
{
    base.ConfigureIndex(idx
        .Settings(s => s.NumberOfReplicas(0))
        .Mappings<Employee>(m => m.Properties(p => p.SetupDefaults())));
}
```

> **Note:** `AutoMap<T>()` has been removed. Define all property mappings explicitly via `.Properties(...)`.

### ConfigureIndexMapping Return Type

`ConfigureIndexMapping` changed from returning `TypeMappingDescriptor<T>` to `void`:

**Before:**
```csharp
public override TypeMappingDescriptor<Employee> ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
{
    return map
        .Dynamic(false)
        .Properties(p => p
            .SetupDefaults()
            .Keyword(f => f.Name(e => e.Id))
            .Text(f => f.Name(e => e.Name).AddKeywordAndSortFields())
        );
}
```

**After:**
```csharp
public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
{
    map
        .Dynamic(DynamicMapping.False)
        .Properties(p => p
            .SetupDefaults()
            .Keyword(e => e.Id)
            .Text(e => e.Name, t => t.AddKeywordAndSortFields())
        );
}
```

### ConfigureIndexAliases Signature

**Before:**
```csharp
public override IPromise<IAliases> ConfigureIndexAliases(AliasesDescriptor aliases)
{
    return aliases.Alias("my-alias");
}
```

**After:**
```csharp
public override void ConfigureIndexAliases(FluentDictionaryOfNameAlias aliases)
{
    aliases.Add("my-alias", a => a);
}
```

### ConfigureSettings on Index

**Before:**
```csharp
public override void ConfigureSettings(ConnectionSettings settings) { }
```

**After:**
```csharp
public override void ConfigureSettings(ElasticsearchClientSettings settings) { }
```

## Property Mapping Changes

The new client uses a simpler expression syntax for property mappings. Property name inference is now directly via the expression:

| Before | After |
|--------|-------|
| `.Keyword(f => f.Name(e => e.Id))` | `.Keyword(e => e.Id)` |
| `.Text(f => f.Name(e => e.Name))` | `.Text(e => e.Name)` |
| `.Number(f => f.Name(e => e.Age).Type(NumberType.Integer))` | `.IntegerNumber(e => e.Age)` |
| `.Date(f => f.Name(e => e.CreatedUtc))` | `.Date(e => e.CreatedUtc)` |
| `.Boolean(f => f.Name(e => e.IsActive))` | `.Boolean(e => e.IsActive)` |
| `.Object<T>(f => f.Name(e => e.Address).Properties(...))` | `.Object(e => e.Address, o => o.Properties(...))` |
| `.Nested<T>(f => f.Name(e => e.Items).Properties(...))` | `.Nested(e => e.Items, n => n.Properties(...))` |
| `.Dynamic(false)` | `.Dynamic(DynamicMapping.False)` |

## Response Validation

The `IsValid` property on responses was renamed to `IsValidResponse`:

| Before | After |
|--------|-------|
| `response.IsValid` | `response.IsValidResponse` |
| `response.OriginalException` | `response.OriginalException()` (method call) |
| `response.ServerError?.Status` | `response.ElasticsearchServerError?.Status` |

## Serialization

The new client uses **System.Text.Json** instead of Newtonsoft.Json.

- The `NEST.JsonNetSerializer` package is **no longer needed or supported**.
- Custom converters using `JsonConverter` (Newtonsoft) must be rewritten for `System.Text.Json`.
- Document classes that relied on `[JsonProperty]` attributes must switch to `[JsonPropertyName]`.

## Ingest Pipeline on Update

The old client supported `Pipeline` on bulk update operations via a custom extension. **This feature is not supported by the Elasticsearch Update API** and has been removed. Use the Ingest pipeline on index (PUT) operations only.

## Custom Field Type Mapping

`ICustomFieldType.ConfigureMapping<T>` changed its signature:

**Before:**
```csharp
public IProperty ConfigureMapping<T>(SingleMappingSelector<T> map) where T : class
{
    return map.Number(n => n.Type(NumberType.Integer));
}
```

**After:**
```csharp
public Func<PropertyFactory<T>, IProperty> ConfigureMapping<T>() where T : class
{
    return factory => factory.IntegerNumber();
}
```

## Snapshot API

The `Snapshot.SnapshotAsync` method was renamed to `Snapshot.CreateAsync` in the new client.

## Counting with Index Filtering

**Before:**
```csharp
await client.CountAsync<Employee>(d => d.Index(indexName), cancellationToken);
```

**After:**
```csharp
await client.CountAsync<Employee>(d => d.Indices(indexName));
```

## Parent-Child Documents

The `RoutingField` configuration on `TypeMappingDescriptor` is no longer available as a direct mapping property. Routing is now handled at the index settings level or through query routing parameters.

## RefreshInterval

**Before:**
```csharp
settings.RefreshInterval(TimeSpan.FromSeconds(30));
```

**After:**
```csharp
settings.RefreshInterval(Duration.FromSeconds(30));
```

## TopHits Aggregation Round-Trip

The `TopHitsAggregate` now serializes the raw document JSON in its `Hits` property, enabling round-trip serialization for caching purposes. The `Documents<T>()` method checks both the in-memory `ILazyDocument` list (from a live ES response) and the serialized `Hits` list (from cache deserialization).

## Migration Checklist

- [ ] Replace `using Elasticsearch.Net;` and `using Nest;` with `using Elastic.Clients.Elasticsearch;`
- [ ] Update `CreateConnectionPool()` return type from `IConnectionPool` to `NodePool`
- [ ] Update pool class names (`SingleNodeConnectionPool` → `SingleNodePool`, etc.)
- [ ] Update `ConfigureSettings` parameter from `ConnectionSettings` to `ElasticsearchClientSettings`
- [ ] Update authentication calls (`.BasicAuthentication` → `.Authentication(new BasicAuthentication(...))`)
- [ ] Change `ConfigureIndex` return type from `CreateIndexDescriptor` to `void` (remove `return`)
- [ ] Change `ConfigureIndexMapping` return type to `void` (remove `return`)
- [ ] Update property mapping syntax (remove `.Name(e => e.Prop)` wrapper)
- [ ] Replace `NumberType.Integer` with `.IntegerNumber()` extension
- [ ] Replace `.Dynamic(false)` with `.Dynamic(DynamicMapping.False)`
- [ ] Replace `response.IsValid` with `response.IsValidResponse`
- [ ] Remove `NEST.JsonNetSerializer` dependency
- [ ] Update custom serializers to `System.Text.Json`
- [ ] Update `ICustomFieldType.ConfigureMapping<T>` to new `Func<PropertyFactory<T>, IProperty>` signature
- [ ] Remove `AutoMap<T>()` calls; define all mappings explicitly
