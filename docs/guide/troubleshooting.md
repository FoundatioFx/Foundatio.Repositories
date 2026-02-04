# Troubleshooting

This guide covers common issues and solutions when working with Foundatio.Repositories.

## Connection Issues

### Cannot Connect to Elasticsearch

**Symptoms:**
- `No connection could be made`
- `Connection refused`
- Timeout errors

**Solutions:**

1. **Verify Elasticsearch is running:**

```bash
curl http://localhost:9200
```

2. **Check connection string:**

```csharp
protected override IConnectionPool CreateConnectionPool()
{
    // Ensure URL is correct
    return new SingleNodeConnectionPool(new Uri("http://localhost:9200"));
}
```

3. **Check firewall/network:**

```bash
# Test connectivity
telnet localhost 9200
```

4. **Enable debug logging:**

```csharp
protected override void ConfigureSettings(ConnectionSettings settings)
{
    settings.DisableDirectStreaming();
    settings.PrettyJson();
    settings.EnableDebugMode();
}
```

### Authentication Errors

**Symptoms:**
- `401 Unauthorized`
- `403 Forbidden`

**Solutions:**

```csharp
protected override void ConfigureSettings(ConnectionSettings settings)
{
    // Basic authentication
    settings.BasicAuthentication("username", "password");
    
    // Or API key
    settings.ApiKeyAuthentication("api-key-id", "api-key");
}
```

## Index Issues

### Index Not Found

**Symptoms:**
- `index_not_found_exception`
- `no such index`

**Solutions:**

1. **Configure indexes on startup:**

```csharp
await configuration.ConfigureIndexesAsync();
```

2. **Check index name:**

```csharp
// Versioned indexes have version suffix
// "employees" -> "employees-v1"
var indexName = index.VersionedName;
```

3. **Verify index exists:**

```bash
curl http://localhost:9200/_cat/indices
```

### Mapping Conflicts

**Symptoms:**
- `mapper_parsing_exception`
- `failed to parse field`

**Solutions:**

1. **Increment index version:**

```csharp
// Change version to trigger reindex
public EmployeeIndex(...) : base(configuration, "employees", version: 2) { }
```

2. **Delete and recreate index (development only):**

```csharp
await configuration.DeleteIndexesAsync();
await configuration.ConfigureIndexesAsync();
```

3. **Check field types match:**

```csharp
// Ensure mapping matches data types
.Number(f => f.Name(e => e.Age).Type(NumberType.Integer))
```

## Query Issues

### No Results Returned

**Symptoms:**
- Empty results when data exists
- `Total: 0`

**Solutions:**

1. **Check soft delete mode:**

```csharp
// Include soft-deleted documents
var results = await repository.FindAsync(query, o => o.IncludeSoftDeletes());
```

2. **Use immediate consistency:**

```csharp
// Wait for index refresh
await repository.AddAsync(entity, o => o.ImmediateConsistency());
var results = await repository.FindAsync(query);
```

3. **Verify filter syntax:**

```csharp
// Check filter expression
var results = await repository.FindAsync(q => q.FilterExpression("status:active"));

// Debug: Log the query
var results = await repository.FindAsync(query, o => o.QueryLogLevel(LogLevel.Debug));
```

4. **Check field names:**

```csharp
// Use exact field names from mapping
// "name" vs "name.keyword" for exact match
```

### Query Syntax Errors

**Symptoms:**
- `query_parsing_exception`
- `failed to parse query`

**Solutions:**

1. **Escape special characters:**

```csharp
// Escape: + - = && || > < ! ( ) { } [ ] ^ " ~ * ? : \ /
var escaped = Regex.Escape(userInput);
```

2. **Use strongly-typed queries:**

```csharp
// Instead of filter expression
var results = await repository.FindAsync(q => q
    .FieldEquals(e => e.Status, "active")
    .FieldCondition(e => e.Age, ComparisonOperator.GreaterThan, 25));
```

## Cache Issues

### Stale Data

**Symptoms:**
- Old data returned after updates
- Changes not reflected

**Solutions:**

1. **Manually invalidate cache:**

```csharp
await repository.InvalidateCacheAsync(document);
await repository.InvalidateCacheAsync("custom-cache-key");
```

2. **Disable cache for debugging:**

```csharp
var results = await repository.FindAsync(query, o => o.Cache(false));
```

3. **Check cache invalidation gaps:**

```csharp
// PatchAllAsync doesn't invalidate custom keys
await repository.PatchAllAsync(query, patch);
await repository.InvalidateCacheAsync("affected-key");
```

### Cache Key Conflicts

**Symptoms:**
- Wrong data returned
- Data from different queries mixed

**Solutions:**

```csharp
// Use unique, consistent cache keys
var key = $"employee:email:{email.ToLowerInvariant()}";
var results = await repository.FindOneAsync(query, o => o.Cache(key));
```

## Version Conflicts

### VersionConflictDocumentException

**Symptoms:**
- `version_conflict_engine_exception`
- `VersionConflictDocumentException`

**Solutions:**

1. **Implement retry logic:**

```csharp
int retries = 3;
while (retries > 0)
{
    try
    {
        var doc = await repository.GetByIdAsync(id);
        doc.Name = "Updated";
        await repository.SaveAsync(doc);
        break;
    }
    catch (VersionConflictDocumentException)
    {
        retries--;
        if (retries == 0) throw;
    }
}
```

2. **Skip version check (if appropriate):**

```csharp
await repository.SaveAsync(document, o => o.SkipVersionCheck());
```

3. **Use atomic operations:**

```csharp
// Atomic increment avoids conflicts
await repository.PatchAsync(id, new ScriptPatch("ctx._source.counter++"));
```

## Performance Issues

### Slow Queries

**Symptoms:**
- High query latency
- Timeouts

**Solutions:**

1. **Add appropriate indexes:**

```csharp
// Ensure fields are properly mapped
.Keyword(f => f.Name(e => e.Status))  // For filtering
.Text(f => f.Name(e => e.Name).AddKeywordAndSortFields())  // For search + sort
```

2. **Limit result size:**

```csharp
var results = await repository.FindAsync(query, o => o.PageLimit(100));
```

3. **Use field selection:**

```csharp
var results = await repository.FindAsync(query, o => o
    .Include(e => e.Id)
    .Include(e => e.Name));
```

4. **Use search-after for deep pagination:**

```csharp
var results = await repository.FindAsync(query, o => o.SearchAfterPaging());
```

### Memory Issues

**Symptoms:**
- `OutOfMemoryException`
- High memory usage

**Solutions:**

1. **Use batch processing:**

```csharp
await repository.BatchProcessAsync(query, async batch =>
{
    // Process in batches
    return true;
}, o => o.PageLimit(500));
```

2. **Use snapshot paging for large exports:**

```csharp
var results = await repository.FindAsync(query, o => o.SnapshotPaging());
```

## Notification Issues

### EntityChanged Not Received

**Symptoms:**
- Subscribers not receiving notifications
- Message bus appears silent

**Solutions:**

1. **Verify message bus is configured:**

```csharp
public MyElasticConfiguration(IMessageBus messageBus, ...) 
    : base(messageBus: messageBus, ...) { }
```

2. **Check notifications are enabled:**

```csharp
// Repository level
NotificationsEnabled = true;

// Operation level
await repository.SaveAsync(entity, o => o.Notifications(true));
```

3. **Verify subscription:**

```csharp
await messageBus.SubscribeAsync<EntityChanged>(async (msg, ct) =>
{
    Console.WriteLine($"Received: {msg.Type} {msg.ChangeType}");
});
```

### Soft Delete Not Sending Removed

**Symptoms:**
- Soft delete sends `ChangeType.Saved` instead of `Removed`

**Solutions:**

```csharp
// Enable originals tracking
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeRepository(EmployeeIndex index) : base(index)
    {
        OriginalsEnabled = true;  // Required for soft delete detection
    }
}
```

## Debugging Tips

### Enable Detailed Logging

```csharp
// In configuration
protected override void ConfigureSettings(ConnectionSettings settings)
{
    settings.DisableDirectStreaming();
    settings.PrettyJson();
    settings.OnRequestCompleted(details =>
    {
        _logger.LogDebug("Request: {Method} {Uri}", 
            details.HttpMethod, details.Uri);
        if (details.RequestBodyInBytes != null)
            _logger.LogDebug("Body: {Body}", 
                Encoding.UTF8.GetString(details.RequestBodyInBytes));
    });
}

// Per query
var results = await repository.FindAsync(query, o => o.QueryLogLevel(LogLevel.Debug));
```

### Inspect Elasticsearch Directly

```bash
# Check cluster health
curl http://localhost:9200/_cluster/health

# List indexes
curl http://localhost:9200/_cat/indices

# View mapping
curl http://localhost:9200/employees/_mapping

# Search directly
curl -X POST http://localhost:9200/employees/_search -H 'Content-Type: application/json' -d '
{
  "query": { "match_all": {} }
}'
```

### Check Index Statistics

```bash
curl http://localhost:9200/employees/_stats
```

## Common Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| `index_not_found_exception` | Index doesn't exist | Run `ConfigureIndexesAsync()` |
| `mapper_parsing_exception` | Type mismatch | Check field types in mapping |
| `version_conflict_engine_exception` | Concurrent modification | Implement retry or skip version check |
| `search_phase_execution_exception` | Query error | Check query syntax |
| `circuit_breaking_exception` | Memory limit | Reduce batch size |
| `cluster_block_exception` | Cluster read-only | Check disk space |

## Getting Help

1. **Check logs** - Enable debug logging
2. **Inspect Elasticsearch** - Use Kibana or curl
3. **Review documentation** - Check specific feature guides
4. **GitHub Issues** - Search existing issues or create new one
5. **Discord** - Join the Foundatio Discord community

## Next Steps

- [Configuration](/guide/configuration) - Configuration options
- [Elasticsearch Setup](/guide/elasticsearch-setup) - Connection setup
- [Caching](/guide/caching) - Cache troubleshooting
