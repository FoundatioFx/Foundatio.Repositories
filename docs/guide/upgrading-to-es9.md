# Migrating to Elastic.Clients.Elasticsearch (ES9)

This guide covers breaking changes when upgrading from `NEST` (ES7) to `Elastic.Clients.Elasticsearch` (ES8/ES9). The new Elasticsearch .NET client is a complete rewrite with a new API surface, so most code that interacts with Elasticsearch directly will need changes.

> **Query syntax changes**: If you use [Foundatio.Parsers](https://github.com/FoundatioFx/Foundatio.Parsers) for query parsing (e.g., `ElasticQueryParser`, `ElasticMappingResolver`, aggregation parsing), refer to the [Foundatio.Parsers documentation](https://github.com/FoundatioFx/Foundatio.Parsers) for its own ES9-related migration notes. The query parser APIs have been updated to work with the new `Elastic.Clients.Elasticsearch` types.

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
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport;
```

Additional namespaces you may need depending on usage:

| Feature | Namespace |
|---------|-----------|
| Aggregations | `Elastic.Clients.Elasticsearch.Aggregations` |
| Bulk operations | `Elastic.Clients.Elasticsearch.Core.Bulk` |
| Search types | `Elastic.Clients.Elasticsearch.Core.Search` |
| Async search | `Elastic.Clients.Elasticsearch.AsyncSearch` |
| Analysis (analyzers, tokenizers) | `Elastic.Clients.Elasticsearch.Analysis` |
| Fluent helpers | `Elastic.Clients.Elasticsearch.Fluent` |

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

### Constructor: Serializer Parameter

`ElasticConfiguration` now accepts an `ITextSerializer` parameter. If you don't provide one, a default `SystemTextJsonSerializer` is created with `ConfigureFoundatioRepositoryDefaults()`. If you have custom serialization needs, pass your own serializer:

```csharp
var serializer = new SystemTextJsonSerializer(
    new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults());

var config = new MyElasticConfiguration(
    serializer: serializer,
    cacheClient: cache,
    messageBus: bus);
```

### Client Disposal

`ElasticsearchClientSettings` implements `IDisposable` internally but doesn't expose it on its public API. The `ElasticConfiguration.Dispose()` method now handles this by casting to `IDisposable`. If you manage the client lifecycle yourself, be aware of this.

## Serialization Changes (Newtonsoft.Json to System.Text.Json)

This is one of the largest breaking changes. The new Elasticsearch client uses **System.Text.Json** instead of Newtonsoft.Json for all serialization.

### What Changed

| Before | After |
|--------|-------|
| `NEST.JsonNetSerializer` package | **Removed** — no longer needed or supported |
| `Newtonsoft.Json.JsonConverter` | `System.Text.Json.Serialization.JsonConverter<T>` |
| `[JsonProperty("name")]` | `[JsonPropertyName("name")]` |
| `[JsonIgnore]` (Newtonsoft) | `[JsonIgnore]` (System.Text.Json — same name, different namespace) |
| `[JsonConverter(typeof(...))]` (Newtonsoft) | `[JsonConverter(typeof(...))]` (System.Text.Json) |
| `JsonConvert.SerializeObject(obj)` | `JsonSerializer.Serialize(obj, options)` |
| `JsonConvert.DeserializeObject<T>(json)` | `JsonSerializer.Deserialize<T>(json, options)` |

### ConfigureFoundatioRepositoryDefaults

Foundatio.Repositories provides a `ConfigureFoundatioRepositoryDefaults()` extension method on `JsonSerializerOptions` that registers converters needed for correct round-tripping of repository documents:

```csharp
using Foundatio.Repositories.Serialization;

var options = new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults();
```

This registers:
- `DoubleSystemTextJsonConverter` to preserve decimal points on whole-number doubles
- `ObjectToInferredTypesConverter` to deserialize `object`-typed properties as CLR primitives instead of `JsonElement` (required for `Dictionary<string, object>` metadata bags unless you supply a custom dictionary converter)
- Case-insensitive property matching

System.Text.Json serializes enums as **integers** by default, same as Newtonsoft.Json/NEST unless you opted in with `[JsonConverter(typeof(StringEnumConverter))]` or similar. No change is required for typical repository documents. Only add `[JsonConverter(typeof(JsonStringEnumConverter))]` (or a custom converter) on enums you intentionally store as strings in Elasticsearch `_source`.

### LazyDocument Serializer Requirement

`LazyDocument` no longer falls back to a default Newtonsoft serializer. The `ITextSerializer` parameter is now **required**:

**Before:**
```csharp
new LazyDocument(data, serializer: null); // fell back to JsonNetSerializer
```

**After:**
```csharp
new LazyDocument(data, serializer); // serializer is required, throws if null
```

### Migration Tips for Custom Converters

If you have custom Newtonsoft `JsonConverter` implementations:

1. Create a new class inheriting from `System.Text.Json.Serialization.JsonConverter<T>`
2. Implement `Read` and `Write` methods using `Utf8JsonReader`/`Utf8JsonWriter`
3. Register converters via `JsonSerializerOptions.Converters.Add(...)` or the `[JsonConverter]` attribute
4. Be aware that System.Text.Json is stricter by default (no comments, trailing commas, or unquoted property names)

## Index Configuration Changes

### ConfigureIndex: Return Type and Descriptor

`ConfigureIndex` changed from returning a descriptor (fluent chaining) to `void` (mutating the descriptor in place). The descriptor type also changed:

| Before | After |
|--------|-------|
| `CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx)` | `void ConfigureIndex(CreateIndexRequestDescriptor idx)` |
| Returns the descriptor | Mutates the descriptor in place |

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

> **Note:** `AutoMap<T>()` has been removed from the new client. Define all property mappings explicitly via `.Properties(...)`.

### ConfigureIndexMapping: Return Type and API

`ConfigureIndexMapping` changed from returning `TypeMappingDescriptor<T>` to `void`:

| Before | After |
|--------|-------|
| `TypeMappingDescriptor<T> ConfigureIndexMapping(TypeMappingDescriptor<T> map)` | `void ConfigureIndexMapping(TypeMappingDescriptor<T> map)` |
| Returns the descriptor | Mutates the descriptor in place |

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

### CreateIndexAsync and UpdateIndexAsync

Internal methods that create or update indexes changed from `Func<Descriptor, Descriptor>` (fluent return) to `Action<Descriptor>` (void mutation):

| Before | After |
|--------|-------|
| `Func<CreateIndexDescriptor, CreateIndexDescriptor>` | `Action<CreateIndexRequestDescriptor>` |
| `Func<UpdateIndexSettingsDescriptor, ...>` | `Action<PutIndicesSettingsRequestDescriptor>` |

### ConfigureSettings on Index

**Before:**
```csharp
public override void ConfigureSettings(ConnectionSettings settings) { }
```

**After:**
```csharp
public override void ConfigureSettings(ElasticsearchClientSettings settings) { }
```

## Property Mapping (TypeMappingDescriptor) Changes

The new client uses a simpler expression syntax for property mappings. The `.Name(e => e.Prop)` wrapper is gone — property name inference comes directly from the expression. Configuration lambdas are now a second parameter:

| Before | After |
|--------|-------|
| `.Keyword(f => f.Name(e => e.Id))` | `.Keyword(e => e.Id)` |
| `.Text(f => f.Name(e => e.Name))` | `.Text(e => e.Name)` |
| `.Text(f => f.Name(e => e.Name).Analyzer("my_analyzer"))` | `.Text(e => e.Name, t => t.Analyzer("my_analyzer"))` |
| `.Number(f => f.Name(e => e.Age).Type(NumberType.Integer))` | `.IntegerNumber(e => e.Age)` |
| `.Number(f => f.Name(e => e.Score).Type(NumberType.Double))` | `.DoubleNumber(e => e.Score)` |
| `.Date(f => f.Name(e => e.CreatedUtc))` | `.Date(e => e.CreatedUtc)` |
| `.Boolean(f => f.Name(e => e.IsActive))` | `.Boolean(e => e.IsActive)` |
| `.Object<T>(f => f.Name(e => e.Address).Properties(...))` | `.Object(e => e.Address, o => o.Properties(...))` |
| `.Nested<T>(f => f.Name(e => e.Items).Properties(...))` | `.Nested(e => e.Items, n => n.Properties(...))` |
| `.Dynamic(false)` | `.Dynamic(DynamicMapping.False)` |
| `.Map<T>(m => m.Properties(...))` | `.Mappings<T>(m => m.Properties(...))` |

### Number Type Mapping

The generic `.Number()` with `NumberType` enum is replaced by specific typed methods:

| Before | After |
|--------|-------|
| `.Number(f => f.Type(NumberType.Integer))` | `.IntegerNumber(e => e.Field)` |
| `.Number(f => f.Type(NumberType.Long))` | `.LongNumber(e => e.Field)` |
| `.Number(f => f.Type(NumberType.Float))` | `.FloatNumber(e => e.Field)` |
| `.Number(f => f.Type(NumberType.Double))` | `.DoubleNumber(e => e.Field)` |

## Response Validation

The `IsValid` property on responses was renamed to `IsValidResponse`:

| Before | After |
|--------|-------|
| `response.IsValid` | `response.IsValidResponse` |
| `response.OriginalException` | `response.OriginalException()` (method call) |
| `response.ServerError?.Status` | `response.ElasticsearchServerError?.Status` |
| `response.ServerError.Error.Type` | `response.ElasticsearchServerError.Error.Type` |

## Custom Field Type Mapping (ICustomFieldType)

`ICustomFieldType.ConfigureMapping<T>` changed from accepting a `SingleMappingSelector<T>` parameter and returning `IProperty` to a parameterless method returning a factory function:

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

All standard field types (`IntegerFieldType`, `StringFieldType`, `BooleanFieldType`, `DateFieldType`, `KeywordFieldType`, `LongFieldType`, `FloatFieldType`, `DoubleFieldType`) have been updated to this pattern. If you have custom `ICustomFieldType` implementations, update them to match.

## Ingest Pipeline on Update

The old client supported `Pipeline` on bulk update operations via a custom extension. **This feature is not supported by the Elasticsearch Update API** and has been removed. Use the Ingest pipeline on index (PUT) operations only.

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

## Known Bugs and Workarounds

### ResolveIndexAsync Is Broken in ES 9.x Client

The `Indices.ResolveIndexAsync` method in the Elastic.Clients.Elasticsearch 9.x client is broken — it does not correctly resolve wildcard index patterns. Foundatio.Repositories works around this by using `Indices.GetAsync` with `IgnoreUnavailable()` instead:

```csharp
// DON'T use ResolveIndexAsync — it's broken in the ES 9.x client
// var resolved = await client.Indices.ResolveIndexAsync(pattern);

// DO use GetAsync to resolve wildcard patterns
var getResponse = await client.Indices.GetAsync(
    Indices.Parse("my-index-*"),
    d => d.IgnoreUnavailable());

if (getResponse.IsValidResponse && getResponse.Indices is not null)
{
    foreach (var kvp in getResponse.Indices)
        Console.WriteLine(kvp.Key); // actual index name
}
```

If you have code that calls `ResolveIndexAsync` directly, switch to `GetAsync`.

### EnableApiVersioningHeader Removed

The `settings.EnableApiVersioningHeader()` call from NEST is no longer needed and does not exist in the new client. Remove it.

## Common Gotchas

1. **Fluent return vs void**: The most pervasive change is that descriptor-based methods (`ConfigureIndex`, `ConfigureIndexMapping`, `ConfigureIndexAliases`) no longer return the descriptor. Remove all `return` statements and change return types to `void`.

2. **AutoMap is gone**: The new client does not support `AutoMap<T>()`. You must define every property mapping explicitly. This is actually safer — it prevents accidental mapping of fields you don't want indexed.

3. **Serializer mismatch**: If documents were serialized with Newtonsoft.Json (e.g., stored in a cache) and you try to deserialize with System.Text.Json, you may get errors or silent data loss. Ensure cached data is invalidated or re-serialized during migration.

4. **Enum serialization**: Both Newtonsoft.Json and System.Text.Json serialize enums as **integers** by default. `ConfigureFoundatioRepositoryDefaults()` does not register a global string-enum converter, and you usually need no extra attributes—existing indices that store enum values as integers stay compatible.

5. **Double precision**: System.Text.Json may round whole-number doubles (e.g., `1.0` becomes `1`). The `DoubleSystemTextJsonConverter` registered by `ConfigureFoundatioRepositoryDefaults()` preserves the decimal point, but only for `double` typed properties.

6. **object-typed properties**: Without `ObjectToInferredTypesConverter`, System.Text.Json deserializes `object` properties as `JsonElement` instead of CLR primitives. This converter is registered by `ConfigureFoundatioRepositoryDefaults()` but if you're using your own `JsonSerializerOptions`, you must add it manually.

7. **Indices.Parse vs IndexName cast**: When passing index names to API calls, use `(IndexName)name` for single names or `Indices.Parse(name)` for comma-separated or wildcard patterns.

8. **CancellationToken parameter changes**: Some API methods that previously accepted `CancellationToken` as a direct parameter now use it differently. Check each call site.

9. **OriginalException is a method**: `response.OriginalException` changed from a property to a method call `response.OriginalException()`. This will be a compile error, but it's easy to miss in string interpolation.

10. **ElasticsearchClientSettings is IDisposable**: The settings object implements `IDisposable` but hides it behind an explicit interface implementation. If you manage the client lifecycle yourself, cast to `IDisposable` and dispose it.

## Migration Checklist

### Packages and Namespaces
- [ ] Replace `using Elasticsearch.Net;` and `using Nest;` with `using Elastic.Clients.Elasticsearch;`
- [ ] Add additional namespaces as needed (`Mapping`, `IndexManagement`, `Aggregations`, etc.)
- [ ] Remove `NEST.JsonNetSerializer` dependency

### Configuration
- [ ] Update `CreateConnectionPool()` return type from `IConnectionPool` to `NodePool`
- [ ] Update pool class names (`SingleNodeConnectionPool` → `SingleNodePool`, etc.)
- [ ] Update `ConfigureSettings` parameter from `ConnectionSettings` to `ElasticsearchClientSettings`
- [ ] Update authentication calls (`.BasicAuthentication` → `.Authentication(new BasicAuthentication(...))`)
- [ ] Remove `settings.EnableApiVersioningHeader()` calls
- [ ] Pass an `ITextSerializer` to `ElasticConfiguration` if you need custom serialization

### Index Configuration
- [ ] Change `ConfigureIndex` return type from `CreateIndexDescriptor` to `void` (remove `return`)
- [ ] Change `ConfigureIndex` parameter from `CreateIndexDescriptor` to `CreateIndexRequestDescriptor`
- [ ] Change `ConfigureIndexMapping` return type to `void` (remove `return`)
- [ ] Change `ConfigureIndexAliases` to use `FluentDictionaryOfNameAlias` and `void` return
- [ ] Replace `.Map<T>(...)` with `.Mappings<T>(...)`
- [ ] Remove `AutoMap<T>()` calls; define all mappings explicitly

### Property Mappings
- [ ] Update property mapping syntax (remove `.Name(e => e.Prop)` wrapper)
- [ ] Replace `.Number(f => f.Type(NumberType.Integer))` with `.IntegerNumber(e => e.Field)`
- [ ] Replace `.Dynamic(false)` with `.Dynamic(DynamicMapping.False)`
- [ ] Update `.Text()`, `.Object()`, `.Nested()` to use two-parameter form for configuration

### Serialization
- [ ] Replace `[JsonProperty]` (Newtonsoft) with `[JsonPropertyName]` (System.Text.Json)
- [ ] Rewrite custom `JsonConverter` classes for System.Text.Json
- [ ] Use `ConfigureFoundatioRepositoryDefaults()` on your `JsonSerializerOptions`
- [ ] Update `LazyDocument` construction to provide a required `ITextSerializer`
- [ ] Invalidate caches that may contain Newtonsoft-serialized data

### Response Handling
- [ ] Replace `response.IsValid` with `response.IsValidResponse`
- [ ] Replace `response.OriginalException` with `response.OriginalException()` (method call)
- [ ] Replace `response.ServerError` with `response.ElasticsearchServerError`

### Custom Field Types
- [ ] Update `ICustomFieldType.ConfigureMapping<T>` to new `Func<PropertyFactory<T>, IProperty>` signature

### Known Issues
- [ ] Replace any `ResolveIndexAsync` calls with `Indices.GetAsync` (broken in ES 9.x client)
- [ ] Verify enum serialization compatibility with existing Elasticsearch data
- [ ] Test document round-tripping with System.Text.Json
