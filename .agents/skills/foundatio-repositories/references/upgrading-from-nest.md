# Migrating from NEST (v7) to Elastic.Clients.Elasticsearch (v8+)

> Applies to: Users upgrading from Foundatio.Repositories v7.x (NEST) to v8+ (Elastic.Clients.Elasticsearch).

Full guide: https://github.com/FoundatioFx/Foundatio.Repositories/blob/main/docs/guide/upgrading-to-elastic-clients-elasticsearch.md

## Migration Procedure

Follow this order:

1. Update package references
2. Fix namespaces
3. Update ElasticConfiguration (connection pool, settings, client type)
4. Update index configuration (ConfigureIndex, ConfigureIndexMapping, ConfigureIndexAliases)
5. Update property mappings to new syntax
6. Update serialization (Newtonsoft.Json → System.Text.Json)
7. Fix response handling (IsValid → IsValidResponse)
8. Update custom field types if any
9. Invalidate caches that contain Newtonsoft-serialized data
10. Test document round-tripping

## Package Changes

```xml
<!-- Remove -->
<PackageReference Include="NEST" Version="7.x" />

<!-- Add (transitively brings in Elastic.Clients.Elasticsearch) -->
<PackageReference Include="Foundatio.Repositories.Elasticsearch" Version="8.x" />
```

## Namespace Changes

```csharp
// Remove:
using Elasticsearch.Net;
using Nest;

// Add:
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport;
```

| Feature | Namespace |
|---------|-----------|
| Aggregations | `Elastic.Clients.Elasticsearch.Aggregations` |
| Bulk operations | `Elastic.Clients.Elasticsearch.Core.Bulk` |
| Search types | `Elastic.Clients.Elasticsearch.Core.Search` |
| Analysis | `Elastic.Clients.Elasticsearch.Analysis` |

## Configuration Changes

| Before (NEST) | After (v8+) |
|--------|-------|
| `IElasticClient Client` | `ElasticsearchClient Client` |
| `new ElasticClient(settings)` | `new ElasticsearchClient(settings)` |
| `IConnectionPool` | `NodePool` |
| `new SingleNodeConnectionPool(uri)` | `new SingleNodePool(uri)` |
| `new StaticConnectionPool(nodes)` | `new StaticNodePool(nodes)` |
| `new SniffingConnectionPool(nodes)` | `new SniffingNodePool(nodes)` |
| `ConnectionSettings` | `ElasticsearchClientSettings` |
| `settings.BasicAuthentication(u, p)` | `settings.Authentication(new BasicAuthentication(u, p))` |
| `settings.ApiKeyAuthentication(id, key)` | `settings.Authentication(new ApiKey(encoded))` |

### Before:

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

### After:

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

### ConfigureIndex

| Before | After |
|--------|-------|
| `CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx)` | `void ConfigureIndex(CreateIndexRequestDescriptor idx)` |
| Returns the descriptor | Mutates in place (remove `return`) |

### ConfigureIndexMapping

| Before | After |
|--------|-------|
| `TypeMappingDescriptor<T> ConfigureIndexMapping(TypeMappingDescriptor<T> map)` | `void ConfigureIndexMapping(TypeMappingDescriptor<T> map)` |
| Returns the descriptor | Mutates in place (remove `return`) |

### ConfigureIndexAliases

| Before | After |
|--------|-------|
| `IPromise<IAliases> ConfigureIndexAliases(AliasesDescriptor aliases)` | `void ConfigureIndexAliases(FluentDictionaryOfNameAlias aliases)` |
| `aliases.Alias("name")` | `aliases.Add("name", a => a)` |

## Property Mapping Syntax

The `.Name(e => e.Prop)` wrapper is gone. Property name inference comes directly from the expression. Configuration lambdas are a second parameter:

| Before (NEST) | After (v8+) |
|--------|-------|
| `.Keyword(f => f.Name(e => e.Id))` | `.Keyword(e => e.Id)` |
| `.Text(f => f.Name(e => e.Name))` | `.Text(e => e.Name)` |
| `.Text(f => f.Name(e => e.Name).Analyzer("x"))` | `.Text(e => e.Name, t => t.Analyzer("x"))` |
| `.Number(f => f.Name(e => e.Age).Type(NumberType.Integer))` | `.IntegerNumber(e => e.Age)` |
| `.Number(f => f.Name(e => e.Score).Type(NumberType.Double))` | `.DoubleNumber(e => e.Score)` |
| `.Number(f => f.Name(e => e.Count).Type(NumberType.Long))` | `.LongNumber(e => e.Count)` |
| `.Date(f => f.Name(e => e.CreatedUtc))` | `.Date(e => e.CreatedUtc)` |
| `.Boolean(f => f.Name(e => e.IsActive))` | `.Boolean(e => e.IsActive)` |
| `.Object<T>(f => f.Name(e => e.Addr).Properties(...))` | `.Object(e => e.Addr, o => o.Properties(...))` |
| `.Nested<T>(f => f.Name(e => e.Items).Properties(...))` | `.Nested(e => e.Items, n => n.Properties(...))` |
| `.Dynamic(false)` | `.Dynamic(DynamicMapping.False)` |
| `.Map<T>(m => m.Properties(...))` | `.Mappings<T>(m => m.Properties(...))` |

## Serialization Changes

| Before | After |
|--------|-------|
| Newtonsoft.Json | System.Text.Json |
| `[JsonProperty("name")]` | `[JsonPropertyName("name")]` |
| `[JsonIgnore]` (Newtonsoft) | `[JsonIgnore]` (System.Text.Json) |
| `JsonConvert.SerializeObject(obj)` | `JsonSerializer.Serialize(obj, options)` |
| `JsonConvert.DeserializeObject<T>(json)` | `JsonSerializer.Deserialize<T>(json, options)` |

Use `ConfigureFoundatioRepositoryDefaults()` on `JsonSerializerOptions`:

```csharp
var options = new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults();
```

This registers `DoubleSystemTextJsonConverter`, `ObjectToInferredTypesConverter`, and case-insensitive property matching.

## Response Handling

| Before | After |
|--------|-------|
| `response.IsValid` | `response.IsValidResponse` |
| `response.OriginalException` | `response.OriginalException()` (method call) |
| `response.ServerError` | `response.ElasticsearchServerError` |

## Custom Field Types (ICustomFieldType)

| Before | After |
|--------|-------|
| `IProperty ConfigureMapping<T>(SingleMappingSelector<T> map)` | `Func<PropertyFactory<T>, IProperty> ConfigureMapping<T>()` |
| `return map.Number(n => n.Type(NumberType.Integer));` | `return factory => factory.IntegerNumber();` |

## Top 10 Gotchas

1. **Fluent return → void**: Remove all `return` statements from `ConfigureIndex`, `ConfigureIndexMapping`, `ConfigureIndexAliases`. Change return types to `void`.
2. **AutoMap is gone**: Define every property mapping explicitly. No `AutoMap<T>()` in the new client.
3. **Serializer mismatch**: Cached data serialized with Newtonsoft.Json will fail with System.Text.Json. Invalidate caches during migration.
4. **Enum serialization**: Both serialize as integers by default. Only add `[JsonConverter(typeof(JsonStringEnumConverter))]` for enums you intentionally store as strings.
5. **Double precision**: System.Text.Json may drop `.0` on whole-number doubles. `ConfigureFoundatioRepositoryDefaults()` handles this.
6. **object-typed properties**: Without `ObjectToInferredTypesConverter`, System.Text.Json deserializes `object` as `JsonElement` instead of CLR primitives.
7. **`Indices.Parse` vs `IndexName` cast**: Use `(IndexName)name` for single names, `Indices.Parse(name)` for wildcards/comma-separated.
8. **OriginalException is a method**: `response.OriginalException` → `response.OriginalException()`.
9. **ResolveIndexAsync broken in ES 9.x**: Use `Indices.GetAsync` with `IgnoreUnavailable()` instead.
10. **EnableApiVersioningHeader removed**: Delete any `settings.EnableApiVersioningHeader()` calls.

## Migration Checklist

- [ ] Replace NEST package with Foundatio.Repositories.Elasticsearch v8+
- [ ] Update namespaces (Nest → Elastic.Clients.Elasticsearch)
- [ ] Update `CreateConnectionPool()` return type to `NodePool`
- [ ] Update pool class names (SingleNodeConnectionPool → SingleNodePool)
- [ ] Update `ConfigureSettings` parameter to `ElasticsearchClientSettings`
- [ ] Update authentication calls
- [ ] Remove `EnableApiVersioningHeader()` calls
- [ ] Change `ConfigureIndex` return type to `void`, parameter to `CreateIndexRequestDescriptor`
- [ ] Change `ConfigureIndexMapping` return type to `void`
- [ ] Change `ConfigureIndexAliases` to `FluentDictionaryOfNameAlias` + `void`
- [ ] Remove `.Map<T>(...)`, use `.Mappings<T>(...)`
- [ ] Remove `AutoMap<T>()` calls
- [ ] Update property mappings (remove `.Name()` wrapper, use typed number methods)
- [ ] Replace `.Dynamic(false)` with `.Dynamic(DynamicMapping.False)`
- [ ] Replace `[JsonProperty]` with `[JsonPropertyName]`
- [ ] Rewrite custom `JsonConverter` classes for System.Text.Json
- [ ] Use `ConfigureFoundatioRepositoryDefaults()` on JsonSerializerOptions
- [ ] Replace `response.IsValid` with `response.IsValidResponse`
- [ ] Replace `response.OriginalException` with `response.OriginalException()`
- [ ] Update `ICustomFieldType.ConfigureMapping<T>` signature
- [ ] Replace `ResolveIndexAsync` with `Indices.GetAsync`
- [ ] Invalidate caches with Newtonsoft-serialized data
- [ ] Test document round-tripping with System.Text.Json
