# Caching

Foundatio.Repositories provides built-in distributed caching with automatic invalidation. This guide covers cache configuration, behavior, and important gaps to be aware of.

## Overview

Caching in Foundatio.Repositories is built on Foundatio's `ICacheClient` abstraction, supporting:

- In-memory caching (development/testing)
- Redis (distributed)
- Hybrid (in-memory L1 + distributed L2 for reduced network round trips)
- Any custom `ICacheClient` implementation

## Configuration

### Enable Caching

Provide an `ICacheClient` to your Elasticsearch configuration:

```csharp
using Foundatio.Caching;

public class MyElasticConfiguration : ElasticConfiguration
{
    public MyElasticConfiguration(ICacheClient cache, ILoggerFactory loggerFactory)
        : base(cache: cache, loggerFactory: loggerFactory)
    {
        AddIndex(Employees = new EmployeeIndex(this));
    }

    public EmployeeIndex Employees { get; }
}
```

### Cache Implementations

```csharp
// In-memory (for development/testing)
services.AddSingleton<ICacheClient>(new InMemoryCacheClient());

// Redis
services.AddSingleton<ICacheClient>(sp =>
    new RedisCacheClient(o => o.ConnectionMultiplexer(sp.GetRequiredService<ConnectionMultiplexer>())));

// Hybrid (in-memory L1 + distributed L2 - best for production, reduces network round trips)
services.AddSingleton<ICacheClient>(sp =>
    new RedisHybridCacheClient(o => o.ConnectionMultiplexer(sp.GetRequiredService<ConnectionMultiplexer>())));
```

### Repository Cache Settings

```csharp
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeRepository(EmployeeIndex index) : base(index)
    {
        // Default cache expiration (default: 5 minutes)
        DefaultCacheExpiration = TimeSpan.FromMinutes(10);
    }
}
```

### Override Cache Client Per Repository

You can override the cache client for a specific repository by calling `SetCacheClient` in the constructor. This is useful when you need a different cache implementation (e.g., a hybrid cache) for a particular repository:

```csharp
public class MyRepository : ElasticRepositoryBase<MyDocument>
{
    public MyRepository(
        MyElasticConfiguration elasticConfig,
        [FromKeyedServices("hybrid")] ICacheClient cacheClient
    ) : base(elasticConfig.MyIndex)
    {
        SetCacheClient(cacheClient);
    }
}
```

You can also disable caching entirely for a repository:

```csharp
public class UncachedRepository : ElasticRepositoryBase<MyDocument>
{
    public UncachedRepository(MyElasticConfiguration elasticConfig)
        : base(elasticConfig.MyIndex)
    {
        DisableCache();
    }
}
```

## Using the Cache

### Cache on Read

```csharp
// Cache by document ID
var employee = await repository.GetByIdAsync(id, o => o.Cache());

// Cache with custom key
var hit = await repository.FindOneAsync(
    q => q.FieldEquals(e => e.Email, email),
    o => o.Cache($"employee:email:{email}"));
var employee = hit?.Document;

// Cache with custom expiration
var employee = await repository.GetByIdAsync(id,
    o => o.Cache().CacheExpiresIn(TimeSpan.FromMinutes(30)));
```

### Cache Options

```csharp
// Enable caching
o.Cache()

// Enable with specific key
o.Cache("my-cache-key")

// Enable with key and expiration
o.Cache("my-key", TimeSpan.FromMinutes(5))

// Set cache key separately
o.CacheKey("my-key")

// Set expiration
o.CacheExpiresIn(TimeSpan.FromMinutes(10))
o.CacheExpiresAt(DateTime.UtcNow.AddHours(1))

// Enable cache reads without enabling cache writes
o.ReadCache()

// Disable cache reads explicitly, including when Cache() is enabled
o.ReadCache(false)

// Disable cache writes and the default cache-read behavior
o.Cache(false)

// Bypass both when inherited options may already contain ReadCache()
o.Cache(false).ReadCache(false)
```

`ReadCache(bool)` overrides cache reads independently of cache writes. Calling `Cache(false)` does not clear a previously set `ReadCache()` override.

### Source Projections

ID reads requested with caller `Include`, `Exclude`, `IncludeMask`, or `ExcludeMask` command options bypass shared document-ID cache reads and do not populate those entries. A warm full-document entry therefore cannot bypass the requested projection, and a later unfiltered read cannot receive an incomplete document. `FindOneAsync` and `GetByIdsAsync` discovery also check the effective search request, including projections introduced by repository query hooks. A projected response can still be read and written under its own explicit cache key. Repository default exclusions define the normal document shape and do not, by themselves, disable ID caching.

Native `ConfigureGetRequest` and `ConfigureMultiGetRequest` hooks also affect cache population: source filters, disabled source, and stored-field requests that omit source cannot populate shared ID entries. Native `Realtime(false)` responses stay out of those entries even when repository options specify Immediate consistency. Multi-get evaluates each document's effective source settings, so a complete document can remain eligible beside a projected one; unsafe native groups and later search-discovery groups are combined. An explicit cache key can still hold the requested response.

These native hooks run when a request is sent to Elasticsearch, after the existing cache lookup. Use command-option projections or `ReadCache(false)` when a call must bypass an already warm cache entry. Full-source requests and repository default exclusions retain normal source eligibility; explicit per-document source settings override the multi-get request defaults.

A native hook that changes an index, ID, or routing value cannot establish that the original target is missing. Shared missing-document markers require a response or completed discovery that covered the original location, so a rewritten request cannot overwrite an unrelated warm cache entry with an invented miss. Explicit cache keys retain the requested result's normal envelope.

Command-option projections retain repository-required fields, including the native `IsDeleted` source field on readonly and writable soft-delete repositories. Required-field registrations are deduplicated when command options or their shallow clones are reused, while preserving caller-required fields. An exact exclusion of a required field keeps the existing required-field precedence. A wildcard or parent-path exclusion that would remove a required native field throws `ArgumentException` during source-filter preparation, before the filtered read request is sent. For example, `Exclude("is_*")` conflicts with a required field named `is_deleted`. Use an include projection or narrower explicit exclusions; the repository does not silently drop the broader exclusion and expose other fields.

When `GetByIdsAsync` combines real-time multi-get results with search discovery, eventual or projected discovery results stay out of the shared ID cache, including negative results. Independent real-time results remain eligible. If both kinds of result share one raw ID under different routes or indexes, that entire ID group is excluded because those locations share a cache entry. An explicit key can still cache the complete requested response.

## Automatic Cache Invalidation

The repository automatically invalidates cache in most scenarios.

### When Cache IS Automatically Invalidated

| Operation | Behavior |
|-----------|----------|
| `AddAsync` | Successful documents populate the cache when requested, except with a default ingest pipeline; the base implementation does not remove ID entries |
| `SaveAsync` | Successful documents invalidate existing entries and populate the cache when requested, except with a default ingest pipeline |
| `RemoveAsync` | Cache invalidated by document ID |
| `PatchAsync` (single ID) | Cache invalidated by ID |
| `PatchAsync` (multiple IDs) | Cache invalidated for all IDs |

### Code Flow for Save

```csharp
// When you call SaveAsync:
await repository.SaveAsync(employee, o => o.Cache());

// Internally:
// 1. Document is indexed to Elasticsearch
// 2. Cache is invalidated for the document ID
// 3. Eligible writes populate the cache with the persisted source and new version
// 4. DocumentsSaved and DocumentsChanged handlers run, followed by EntityChanged notifications
```

### Writes with a Default Ingest Pipeline

When `DefaultPipeline` is configured, `AddAsync` and `SaveAsync` do not populate the cache from the submitted model: Elasticsearch may have transformed that document. `SaveAsync` retains its normal ID-cache invalidation; `AddAsync` retains its existing behavior of not removing ID entries. When write caching is requested, the operation's explicit cache key is removed. A later read from Elasticsearch obtains the transformed source and can cache it normally. Versioned models receive the new version metadata, but submitted models are not reloaded with pipeline-transformed field values.

### Repeated IDs in Write Batches

Before populating results from a cached bulk write, the repository checks every attempted outcome for each repeated raw ID. That ID group is eligible only when all its writes succeeded and each response names a distinct, nonempty concrete index. Different routing values can reach the same shard, and different request index names can be aliases for one index; neither proves that the writes were independent. Input order also cannot identify the final source when retries finish later or a failed response hides an applied write.

The repository evicts an unsafe ID group and skips populating it; other eligible ID groups can still be cached. If an explicit cache key was requested, that key and the successful write ID entries are evicted, and population for the write is skipped so the key cannot contain an incomplete result. These checks include failed attempts as well as confirmed successes.

### Post-write Handlers

Cache invalidation and eligible Add/Save cache population finish before post-write handlers or notifications run, including population under an explicit cache key. A handler may save the same model or a fresh instance; the nested write performs its own cache handling, and the outer write does not overwrite that result afterward. The repository's default cache clones values, so mutating a model in a post-write handler does not change the already-cached persisted source. Custom cache clients retain their own cloning and serialization behavior. Remove also invalidates its cache entries before `DocumentsRemoved`, so a throwing handler cannot leave the deleted source cached.

Patches invalidate their actual write IDs before `DocumentsChanged`, including patches that change the source `Id`. Mixed deletes also evict already-missing documents before post-remove handlers. Missing documents do not produce removal notifications.

During awaited post-write callbacks, the repository tracks the written IDs associated with that write's explicit cache key. An overlapping nested write or ID invalidation removes the key immediately, including writes through a fresh repository instance sharing the same underlying cache client and scope. The key is removed again when those callbacks finish, even if a callback throws, because a callback may have refilled it. Callbacks that do not invalidate a written ID leave the populated key in place and require no additional cache reads or removals. Different IDs, cache clients and scopes remain independent. This tracking ends with the awaited callback flow; it does not establish persistent dependencies for arbitrary query keys or unrelated writes.

## Cache Behavior on Partial Failure

When a bulk `AddAsync` or `SaveAsync` partially fails, cache changes follow the confirmed write outcomes:

| Document Status | Cache Action |
|-----------------|--------------|
| Succeeded | Normal operation-specific handling: Add may populate; Save invalidates and may repopulate. Pipeline inputs are not cached |
| Failed (any error) | Cache entry **unchanged** unless the repeated-ID protection above evicts its ID group; an ambiguous failure may still have changed Elasticsearch |

A definitive rejection, such as a version conflict or rate limit, does not confirm a change by this call. Successful writes run their operation-specific cache handling and send `EntityChanged` notifications. Gateway failures and timeouts can leave the write outcome unknown; a failure does not prove the stored document or a cached copy is unchanged. Re-read with `o.Cache(false)` before deciding whether to retry an ambiguous write.

```csharp
try
{
    await repository.SaveAsync(documents, o => o.Cache());
}
catch (VersionConflictDocumentException)
{
    // Successful docs: cache handled; pipeline inputs are not cached
    // Conflicting docs: cache unchanged (the successful concurrent writer handles its own cache update)
}
```

## Cached Documents and Versions

Cached reads return documents with the `Version` they had when they were cached. If the document has changed since, `SaveAsync` on the cached copy throws `VersionConflictDocumentException` instead of overwriting the newer data. That's the safe outcome, but it means a conflict retry should re-read with `o.Cache(false)`:

```csharp
var employee = await repository.GetByIdAsync(id, o => o.Cache(false));
```

The version of a cached document always comes from Elasticsearch metadata captured when it was cached (it is copied back onto the document when read from the cache), so cached copies retain optimistic concurrency even when an explicit `IVersioned.Version` implementation or `[JsonIgnore]` omits the token from model JSON. A missing envelope token clears a stale model token. Patches bypass the cached document and invalidate successful changes; `IfVersion` can explicitly require a token obtained from a cached read. See [Versioning](/guide/versioning#caching-and-versions). The race where stale data can be re-cached right after an invalidation is tracked in [#323](https://github.com/FoundatioFx/Foundatio.Repositories/issues/323).

Supplying routing to `GetByIdAsync` or `GetByIdsAsync` restricts cached matches to that route. Batched cached reads retain documents from different physical indexes, even when their IDs and routing values are the same. Successful cached writes also retain the response's physical index and the write's routing value.

For repositories with parent routing, a cached document on one route does not establish that an unrouted ID has no other matches. `GetByIdsAsync` still searches Elasticsearch for unresolved locations. That discovery search bypasses inherited cache-read and cache-write options, so a partial custom-key result cannot replace discovery; the outer read retains its requested caching behavior.

## Cache Invalidation Gaps

::: warning Important
There are scenarios where cache is NOT automatically invalidated. Understanding these gaps is critical for maintaining cache consistency.
:::

### PatchAllAsync - Partial Gap

Server-side `PatchAllAsync` operations invalidate document IDs without loading the modified documents. Client-side JSON, action and pipeline partial patches pass the patched documents to custom `InvalidateCacheAsync` overrides. Arbitrary query-cache keys still need explicit invalidation.

```csharp
// This will invalidate cache by document IDs
await repository.PatchAllAsync(
    q => q.FieldEquals(e => e.Department, "Engineering"),
    new ScriptPatch("ctx._source.salary += 1000"));

// But if you cached by a custom key like "dept:engineering:employees",
// that cache key is NOT invalidated
```

**Solution:** Use document-based invalidation overrides for client-side patches, or manually invalidate custom query keys:

```csharp
// After PatchAllAsync, manually invalidate custom keys
await repository.InvalidateCacheAsync("dept:engineering:employees");
```

### RemoveAllAsync (Query) - Conditional Gap

When `RemoveAllAsync` uses `DeleteByQuery` (no event listeners and cache disabled), only query IDs are invalidated:

```csharp
// If no event listeners are registered and cache is disabled,
// this uses DeleteByQuery which only invalidates query IDs
await repository.RemoveAllAsync(q => q.FieldEquals(e => e.Status, "inactive"));
```

**Solution:** Ensure cache is enabled or add event listeners:

```csharp
// Enable cache for the operation
await repository.RemoveAllAsync(query, o => o.Cache());
```

### Direct Elasticsearch Operations

Any operations performed directly via the Elasticsearch client bypass the repository entirely:

```csharp
// This bypasses the repository - NO cache invalidation
await _elasticClient.IndexAsync(document, i => i.Index("employees"));
```

**Solution:** Always use repository methods for data operations.

## Manual Cache Invalidation

### Invalidate by Document

```csharp
// Single document
await repository.InvalidateCacheAsync(employee);

// Multiple documents
await repository.InvalidateCacheAsync(employees);
```

### Invalidate by Cache Key

```csharp
// Single key
await repository.InvalidateCacheAsync("my-cache-key");

// Multiple keys
await repository.InvalidateCacheAsync(new[] { "key1", "key2", "key3" });
```

### Custom Cache Key Invalidation Pattern

Override `InvalidateCacheAsync` to handle custom cache keys:

```csharp
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    // Override to invalidate email cache when documents change
    protected override async Task InvalidateCacheAsync(
        IReadOnlyCollection<ModifiedDocument<Employee>> documents,
        ChangeType? changeType = null)
    {
        // Call base implementation for ID-based invalidation
        await base.InvalidateCacheAsync(documents, changeType);

        // Invalidate custom cache keys for current email addresses
        var emailKeys = documents
            .Where(d => !string.IsNullOrEmpty(d.Value.Email))
            .Select(d => $"employee:email:{d.Value.Email.ToLowerInvariant()}")
            .ToList();

        if (emailKeys.Count > 0)
            await Cache.RemoveAllAsync(emailKeys);

        // Also invalidate original email if it changed
        var originalEmailKeys = documents
            .Where(d => d.Original != null && !string.IsNullOrEmpty(d.Original.Email))
            .Where(d => d.Original.Email != d.Value.Email)
            .Select(d => $"employee:email:{d.Original.Email.ToLowerInvariant()}")
            .ToList();

        if (originalEmailKeys.Count > 0)
            await Cache.RemoveAllAsync(originalEmailKeys);
    }
}
```

## Soft Delete Cache Behavior

For entities implementing `ISupportSoftDeletes`, the repository maintains a special cache list to handle eventual consistency.

### How It Works

When a document is soft-deleted:

```csharp
employee.IsDeleted = true;
await repository.SaveAsync(employee);

// Internally:
// 1. Document ID is added to "deleted" list in cache
// 2. List has 30-second TTL
// 3. Queries automatically exclude IDs in the "deleted" list
```

### Query Filtering

Before queries execute, soft-deleted IDs are excluded:

```csharp
// When you query:
var results = await repository.FindAsync(query);

// Internally (if SoftDeleteQueryMode.ActiveOnly):
// 1. Check cache for "deleted" list
// 2. Add excluded IDs to query
// 3. Execute query
```

### Purpose

This handles the eventual consistency window where:

1. Document is soft-deleted
2. Elasticsearch hasn't indexed the change yet
3. Cache knows about the deletion
4. Queries correctly exclude the document

After 30 seconds, Elasticsearch should have indexed the change, and the cache entry expires.

## Distributed Cache Consistency

### Message Bus Integration

For distributed scenarios, subscribe to `EntityChanged` messages:

```csharp
await messageBus.SubscribeAsync<EntityChanged>(async (msg, ct) =>
{
    if (msg.Type == nameof(Employee))
    {
        // Invalidate local cache when other instances make changes
        await repository.InvalidateCacheAsync(msg.Id);
    }
});
```

### NotificationDeliveryDelay

Use `NotificationDeliveryDelay` to allow Elasticsearch indexing to complete before consumers read:

```csharp
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeRepository(EmployeeIndex index) : base(index)
    {
        NotificationDeliveryDelay = TimeSpan.FromSeconds(1);
    }
}
```

## Cache Best Practices

### 1. Use Consistent Cache Keys

```csharp
// Good: Consistent key format
o.Cache($"employee:email:{email.ToLowerInvariant()}")

// Bad: Inconsistent casing
o.Cache($"employee:email:{email}")  // May not match invalidation
```

### 2. Override InvalidateCacheAsync for Custom Keys

```csharp
protected override async Task InvalidateCacheAsync(
    IReadOnlyCollection<ModifiedDocument<Employee>> documents,
    ChangeType? changeType = null)
{
    await base.InvalidateCacheAsync(documents, changeType);

    // Add custom key invalidation
    var customKeys = documents.Select(d => $"custom:{d.Value.CustomField}");
    await Cache.RemoveAllAsync(customKeys);
}
```

### 3. Be Aware of PatchAllAsync Gaps

```csharp
// After bulk operations, manually invalidate if needed
await repository.PatchAllAsync(query, patch);
await repository.InvalidateCacheAsync("affected-cache-key");
```

### 4. Use Repository Methods

```csharp
// Good: Uses repository, cache is managed
await repository.SaveAsync(employee);

// Bad: Bypasses repository, cache not invalidated
await _elasticClient.IndexAsync(employee);
```

### 5. Test Cache Behavior

```csharp
// Use InMemoryCacheClient for testing
var cache = new InMemoryCacheClient();
// ... perform operations ...

// Check cache statistics
var stats = cache.GetStats();
Console.WriteLine($"Hits: {stats.Hits}, Misses: {stats.Misses}");
```

## Summary: Cache Invalidation Matrix

| Operation | Auto-Invalidated | Arbitrary Custom Keys | Notes |
|-----------|------------------|-------------|-------|
| `AddAsync` | No ID removal | No | Optional cache population; pipeline inputs are not cached |
| `SaveAsync` | Yes | No | Override `InvalidateCacheAsync` |
| `RemoveAsync` | Yes | No | Override `InvalidateCacheAsync` |
| `PatchAsync` (ID) | Yes | No | Override `InvalidateCacheAsync` |
| `PatchAllAsync` | Partial (IDs only) | No | Manual invalidation needed |
| `RemoveAllAsync` | Partial | No | Use `o.Cache()` or add listeners |
| Direct ES | No | No | Always use repository |

## Advanced: How Caching Handles Dirty Reads

Elasticsearch's search path is eventually consistent -- see [Consistency and Dirty Reads](consistency.md) for the full explanation of which operations are real-time vs. near real-time.

The cache layer is aware of this. When a search-based method like `FindOneAsync` returns results without `ImmediateConsistency`, the repository treats them as a potential dirty read and skips caching them by document ID. Caller source projections are also excluded from ID caching, including real-time reads. This prevents stale or incomplete results from polluting the ID-based cache that `GetByIdAsync` relies on:

```csharp
// Internal behavior in AddDocumentsToCacheAsync:
protected virtual async Task AddDocumentsToCacheAsync(
    ICollection<FindHit<T>> findHits,
    ICommandOptions options,
    bool isDirtyRead)
{
    // Custom cache keys are always cached (you explicitly requested it)
    if (options.HasCacheKey())
    {
        await Cache.SetAsync(options.GetCacheKey(), findHits, options.GetExpiresIn());
        // ...
    }

    // Caller source projections belong only to their explicit cache key
    if (options.SafeGetOption<bool>(SkipIdCacheKey)
        || options.GetIncludes().Count > 0 || options.GetExcludes().Count > 0
        || !String.IsNullOrEmpty(options.GetIncludeMask())
        || !String.IsNullOrEmpty(options.GetExcludeMask()))
        return;

    // Don't add dirty read documents by ID - they may be out of sync
    if (isDirtyRead)
        return;

    // Only cache by ID for real-time reads
    // ...
}
```

### Custom Cache Keys for Eventual Consistency

You can use custom cache keys to make `FindOneAsync` work reliably without requiring immediate consistency:

```csharp
public class UserRepository : ElasticRepositoryBase<User>
{
    public async Task<User?> GetByEmailAddressAsync(string emailAddress)
    {
        if (String.IsNullOrWhiteSpace(emailAddress))
            return null;

        emailAddress = emailAddress.Trim().ToLowerInvariant();

        // Use a custom cache key - this caches the result even for dirty reads
        var hit = await FindOneAsync(
            q => q.FieldEquals(u => u.EmailAddress, emailAddress),
            o => o.Cache(EmailCacheKey(emailAddress)));

        return hit?.Document;
    }

    // Override to add documents to cache by email
    protected override async Task AddDocumentsToCacheAsync(
        ICollection<FindHit<User>> findHits,
        ICommandOptions options,
        bool isDirtyRead)
    {
        await base.AddDocumentsToCacheAsync(findHits, options, isDirtyRead);

        // Cache by email address for future lookups
        var cacheEntries = new Dictionary<string, FindHit<User>>();
        foreach (var hit in findHits.Where(d => !String.IsNullOrEmpty(d.Document?.EmailAddress)))
            cacheEntries.Add(EmailCacheKey(hit.Document.EmailAddress), hit);

        if (cacheEntries.Count > 0)
            await AddDocumentsToCacheWithKeyAsync(cacheEntries, options.GetExpiresIn());
    }

    // Override to invalidate email cache when documents change
    protected override Task InvalidateCacheAsync(
        IReadOnlyCollection<ModifiedDocument<User>> documents,
        ChangeType? changeType = null)
    {
        // Union originals and modified values to handle field renames
        var keysToRemove = documents.UnionOriginalAndModified()
            .Where(u => !string.IsNullOrEmpty(u.EmailAddress))
            .Select(u => EmailCacheKey(u.EmailAddress))
            .Distinct();

        return Task.WhenAll(
            Cache.RemoveAllAsync(keysToRemove),
            base.InvalidateCacheAsync(documents, changeType));
    }

    private static string EmailCacheKey(string emailAddress)
        => String.Concat("Email:", emailAddress.Trim().ToLowerInvariant());
}
```

**How this works:**

1. **First call**: `FindOneAsync` searches Elasticsearch (may be a dirty read), caches result by email key
2. **Subsequent calls**: Returns cached result immediately, no Elasticsearch query needed
3. **On save/update**: `InvalidateCacheAsync` clears the email cache key
4. **Next lookup**: Fresh search, re-cached

This pattern provides **eventual consistency with caching** - you don't need `Consistency.Immediate` because:
- The cache is populated on first successful lookup
- The cache is invalidated when the document changes
- Subsequent lookups hit the cache, not Elasticsearch

## Advanced: Originals for Change Detection

### What Are Originals?

When `OriginalsEnabled = true`, the repository fetches the current document from the database before saving. This allows you to:

- Detect what fields changed
- Access the previous values in event handlers
- Properly invalidate cache for changed values (like email addresses)

### Enabling Originals

```csharp
public class UserRepository : ElasticRepositoryBase<User>
{
    public UserRepository(UserIndex index) : base(index)
    {
        OriginalsEnabled = true;  // Fetch original before save
    }
}
```

### Using Originals in Event Handlers

```csharp
DocumentsChanging.AddHandler(async (sender, args) =>
{
    foreach (var doc in args.Documents)
    {
        if (doc.Original != null)
        {
            // Compare old and new values
            if (doc.Original.Email != doc.Value.Email)
            {
                _logger.LogInformation(
                    "Email changed from {Old} to {New}",
                    doc.Original.Email,
                    doc.Value.Email);
            }
        }
    }
});
```

### UnionOriginalAndModified Pattern

A common pattern for cache invalidation is to collect keys from both the original and modified documents (to handle field changes like email address renames). The built-in `UnionOriginalAndModified` extension method (in `Foundatio.Repositories.Extensions`) combines both the current and previous versions of each document:

```csharp
protected override Task InvalidateCacheAsync(
    IReadOnlyCollection<ModifiedDocument<User>> documents,
    ChangeType? changeType = null)
{
    var keysToRemove = documents.UnionOriginalAndModified()
        .Where(u => !string.IsNullOrEmpty(u.EmailAddress))
        .Select(u => EmailCacheKey(u.EmailAddress))
        .Distinct();

    return Task.WhenAll(
        Cache.RemoveAllAsync(keysToRemove),
        base.InvalidateCacheAsync(documents, changeType));
}
```

### Per-Operation Control

```csharp
// Enable originals for a specific operation
await repository.SaveAsync(user, o => o.Originals(true));

// Disable originals for a specific operation (performance optimization)
await repository.SaveAsync(user, o => o.Originals(false));

// Pass known originals to avoid extra database fetch
await repository.SaveAsync(user, o => o.AddOriginals(originalUser));
```

### Performance Consideration

Enabling `OriginalsEnabled` adds an extra database read before each save. Use it when you need:
- Change detection in event handlers
- Proper cache invalidation for non-ID fields
- Soft delete notifications (to know the document was active before deletion)

## Advanced: Required Fields for Cache Invalidation

### The Problem

When callers request partial documents (via `.Include()`, `.Exclude()`, or field masks), fields needed for cache invalidation or event handling may be missing. For example, if you cache by `EmailAddress` and a caller requests only `Id` and `Name`, the `InvalidateCacheAsync` override won't have the email to clear the cache.

### AddRequiredField

Register fields that must always be returned when any caller-specified source filtering is active:

```csharp
public class UserRepository : ElasticRepositoryBase<User>
{
    public UserRepository(UserIndex index) : base(index)
    {
        AddRequiredField(u => u.EmailAddress, u => u.OrganizationIds);
    }
}
```

Required fields are automatically injected when caller-specified source filtering is active: added to the include set when includes are present, or removed from exact exclusions when only excludes are present. Conflicting wildcard or parent-path exclusions throw `ArgumentException`; use an include projection or narrower exclusions. This applies to all source-filtered operations: `GetByIdAsync`, `GetByIdsAsync`, `FindAsync`, `RemoveAllAsync`, and `PatchAllAsync`.

See [Required Fields](querying.md#required-fields) for full details on injection behavior, precedence rules, and interactions with default excludes.

### Default Required Fields

The repository automatically registers these as required fields:

- `IsDeleted` on both read-only and writable repositories when the entity implements `ISupportSoftDeletes`, so projections preserve `ActiveOnly` and `DeletedOnly` filtering
- `Id` on writable repositories
- `CreatedUtc` on writable repositories when the entity implements `IHaveCreatedDate`
- `UpdatedUtc` on writable repositories when the entity implements `IHaveDates`

### Use Cases

1. **Cache invalidation by non-ID fields**: Need the email to invalidate email cache
2. **Event handlers that need specific data**: Audit logging, notifications
3. **Cascade operations**: Need organization IDs to update related entities
4. **Multi-tenancy**: Authorization checks that need tenant identifiers

## Next Steps

- [Consistency and Dirty Reads](consistency.md) - Which operations are real-time vs. eventually consistent
- [Message Bus](/guide/message-bus) - Distributed cache invalidation
- [Configuration](/guide/configuration) - Cache configuration options
- [Soft Deletes](/guide/soft-deletes) - Soft delete cache behavior
