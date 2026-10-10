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
protected override NodePool CreateConnectionPool()
{
    // Ensure URL is correct
    return new SingleNodePool(new Uri("http://localhost:9200"));
}
```

3. **Check firewall/network:**

```bash
# Test connectivity
telnet localhost 9200
```

4. **Enable debug logging:**

```csharp
protected override void ConfigureSettings(ElasticsearchClientSettings settings)
{
    settings.DisableDirectStreaming();
    settings.PrettyJson();
}
```

### Authentication Errors

**Symptoms:**
- `401 Unauthorized`
- `403 Forbidden`

**Solutions:**

```csharp
protected override void ConfigureSettings(ElasticsearchClientSettings settings)
{
    // Basic authentication
    settings.Authentication(new BasicAuthentication("username", "password"));

    // Or API key
    settings.Authentication(new ApiKey("encoded-api-key"));
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

> See [Mapping Lifecycle](/guide/index-management#mapping-lifecycle) for a complete breakdown of how and when mappings are applied per index type, including important differences for `DailyIndex`/`MonthlyIndex`.

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
.IntegerNumber(e => e.Age)
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
var results = await repository.FindAsync(q => q.FieldEquals(e => e.Status, "active"));

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
    .FieldCondition(e => e.Name, ComparisonOperator.Contains, "John"));
```

For numeric comparisons, use `FilterExpression` with Lucene syntax:

```csharp
var results = await repository.FindAsync(q => q
    .FieldEquals(e => e.Status, "active")
    .FilterExpression("age:[25 TO *]"));
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

### Reindex Rejected Due to Indexing Pressure

**Symptoms:**

- A reindex fails (or the reindex task status can no longer be retrieved) with a server error like:

```text
Server Error (Index=): rejected execution of coordinating operation
[coordinating_and_primary_bytes=0, replica_bytes=0, all_bytes=0,
coordinating_operation_bytes=158478331, max_coordinating_bytes=107374182]
```

- The error `type` is `es_rejected_execution_exception` with an HTTP `429` status.
- Repeated `Error getting task status while reindexing: "{OldIndex}" -> "{NewIndex}"` log entries, possibly followed by `Failed to get the status {N} times in a row for reindex task ...`.

**Cause:**

Elasticsearch reserves a portion of JVM heap for in-flight indexing work — the [`indexing_pressure.memory.limit`](https://www.elastic.co/docs/reference/elasticsearch/configuration-reference/indexing-pressure-settings) node setting, which defaults to **10% of heap**. Every indexing request (including the internal bulk writes issued by the `_reindex` API) is accounted against this budget for the full duration of its coordinating/primary/replica stages. If a single bulk sub-request's estimated size exceeds the remaining budget, Elasticsearch immediately rejects it rather than queuing it — this is a deliberate back-pressure mechanism, not a bug. It's more likely to trigger during reindexing because reindex batches (default 1000 documents) scale with document size: large documents can produce a bulk payload well over 100MB on a modestly sized node. See [Rejected requests: Analyze indexing pressure](https://www.elastic.co/docs/troubleshoot/elasticsearch/rejected-requests#analyze-indexing-pressure) for the full explanation.

**Solutions:**

1. **Reduce the reindex batch size** so each internal bulk sub-request stays well under the indexing pressure limit:

```csharp
public EmployeeIndex(IElasticConfiguration configuration)
    : base(configuration, "employees", version: 2)
{
    ReindexBatchSize = 200; // default: 1000
}
```

2. **Throttle the reindex** to reduce sustained load on a cluster that's also serving other traffic:

```csharp
ReindexRequestsPerSecond = 500; // default: unlimited
```

3. **Check node heap and `indexing_pressure.memory.limit`** via the [node stats API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-stats) if rejections continue after lowering the batch size — the node may simply be undersized for the document volume/size involved.

See [Throttling Reindex Load](./index-management.md#throttling-reindex-load) for more on both properties. Reindex task-status polling also backs off automatically (starting at 1 second, doubling up to a 30 second cap, with +/-25% jitter) after this library's fix for this exact scenario, so transient rejections while polling no longer retry in a tight loop or in lockstep with other reindex operations hitting the same cluster-wide condition.

If you configure a low `ReindexRequestsPerSecond` to work around this, note that the reindex's stall-detection timeout (10 minutes by default) automatically extends to accommodate the resulting longer pause between batches, so throttling to avoid indexing pressure rejections won't itself cause the reindex to be cancelled as falsely "stalled."

## Reindex Failures

### ReindexIncompleteException

**Symptoms:**

- `ConfigureIndexesAsync()` or `ReindexAsync()` throws `ReindexIncompleteException`, or `ElasticConfiguration.ReindexAsync()` throws an `AggregateException` containing one. **Startup fails** rather than proceeding.
- The message names both indexes and a reason: `Reindex of employees-v1 -> employees-v2 did not complete: {reason}`.

**Cause:**

The destination could not be trusted as a complete replica of the source, so the library refused to treat the migration as successful. The `Reason` distinguishes the cases:

| Reason mentions | What happened | What to do |
|-----------------|---------------|------------|
| documents failed to copy, or a copy task error | Elasticsearch rejected documents — usually a mapping conflict or indexing pressure | Fix the mapping or lower `ReindexBatchSize`; see [Mapping Conflicts](#mapping-conflicts) and [Reindex Rejected Due to Indexing Pressure](#reindex-rejected-due-to-indexing-pressure) |
| the source changed and no catch-up pass is possible | The model has no timestamp field and non-ObjectId ids, and the source was written to during the copy | Set `QuiesceSourceOnReindex`, add `IHaveDates`, or migrate when writes are stopped |
| documents changed that the catch-up pass cannot reach | The model uses ObjectId ids and a **pre-existing** document was updated in place during the copy — the id-range catch-up encodes creation time only | Use quiescence, reliable update timestamps, or externally stop writes; creation-time ranges do not discover old-ID updates |
| unexpected final output count | Quiesced output differs from the final task's expected count, including intentional no-ops/deletes | Investigate missing/extra documents, routing, and transformations; confirm the source still owns its aliases and prior tasks stopped before retrying |
| the source kept changing while blocked | Under `QuiesceSourceOnReindex`, one or more primary checkpoint values changed despite the write block | Inspect competing migrations and operator changes to the block; direct physical-index writes are blocked too |

**Solutions:**

1. **Establish the cutover state before retrying.** Failure before any promotion request leaves the source authoritative, but an alias request with a lost or cancelled response may already have applied. Confirm physical aliases, task termination, and block ownership; an exception alone does not prove rollback. Then fix the cause and retry only if the replay gates permit it.

2. **If you are refused because of live writes**, the structural fix is to quiesce the source:

```csharp
public class EmployeeIndex : VersionedIndex<Employee>
{
    public EmployeeIndex(IElasticConfiguration configuration)
        : base(configuration, "employees", 2)
    {
        QuiesceSourceOnReindex = true; // blocks writes to the source while it is reconciled
    }
}
```

   This keeps the first pass writable and blocks writes during the full final copy, deletion reconciliation, verification, and cutover. See [Quiescing writes for a verified cutover](./index-management.md#quiescing-writes-for-a-verified-cutover) before enabling it.

3. **Do not suppress it globally.** If a specific deployment must proceed anyway, catch it explicitly and alert:

```csharp
try
{
    await configuration.ReindexAsync();
}
catch (AggregateException ex) when (ex.InnerExceptions.All(e => e is ReindexIncompleteException))
{
    logger.LogCritical(ex, "Index migration incomplete; indexes may be missing documents");
}
```

4. **If the alias moved, do not replay the old source over the live destination.** Default-mode catch-up failures can happen after promotion. Quiesced alias-response, callback, or completion-write failures can also occur after promotion. Preserve both indexes and decide recovery from authoritative data; creating a fresh version from an already-incomplete destination alone does not recover missing records. See the [recovery decision table](/guide/reindex-safety#cancellation-and-outcome-handling).

5. **Find what was left behind** using the technique in [Finding the documents that were left behind](./index-management.md#finding-the-documents-that-were-left-behind).

### ReindexCompletionUnknownException

**Symptoms:**

- A queued reindex work item fails with `Reindex of {source} -> {destination} for alias {alias} cannot be confirmed complete: {reason}`.
- The alias already points at the destination version.
- The work item is abandoned and eventually dead-letters rather than being acknowledged.

**Cause:**

The destination was already promoted, but no completion record exists in the `foundatio-reindex-completions` index, so there is no trustworthy evidence the migration ever finished. Completion is **never** inferred from alias state or document counts. Two situations produce this:

- A first attempt failed *after* the alias cutover, which advances the version without finishing the work.
- The migration completed before this library recorded completion evidence, and a stale work item for it was redelivered.

This also applies to **quiesced** work items: the current flag does not prove how the earlier alias promotion happened. Missing completion is never fabricated. Unknown task dispatch or ambiguous block ownership can also raise this exception before promotion; inspect the exception reason and durable safety records.

**Solutions:**

1. **The replay guard does not authorize another copy into a live destination.** It does not prove that no earlier side effect occurred: a previous task may still be writing, or promotion may already have succeeded. Read the exception reason and durable state before intervening; never fabricate completion from alias state.

2. **Inspect the actual membership and content.** Counts below are only a starting point, not evidence of equality:

```bash
# Compare counts; the source may still exist
curl "localhost:9200/employees-v1/_count"
curl "localhost:9200/employees-v2/_count"
```

3. **Then decide deliberately:** accept an independently inspected destination and retire the stale queue item using application operations, or recover into a fresh destination using authoritative application data/backups. Do not assume a new index version can reconstruct records missing from its chosen source.

4. **For a pre-existing migration with no record**, this only surfaces on redelivery of a stale work item. Startup is unaffected and nothing is scanned automatically — discard the stale item once you have confirmed the migration did complete.

### An index is stuck read-only after a reindex

A source write block is a persistent Elasticsearch setting, not a client session. A failed cleanup or terminated process can leave `403 cluster_block_exception` responses after the worker exits.

Check the physical source's UUID, aliases, and `index.blocks.write`, then correlate its `foundatio-reindex-safety` block entry and task state. A fresh attempt can recover a confirmed migration-owned block for the same physical generation. A pre-existing unowned block is treated as operator-owned and preserved. A pending/unconfirmed block intent, mismatched UUID, or another owner requires an explicit operator decision.

```bash
curl "localhost:9200/employees-v1/_settings?filter_path=**.uuid,**.blocks"
curl "localhost:9200/employees-v1/_alias"
curl "localhost:9200/_tasks?actions=*reindex&detailed=true"
```

**Do not clear every block or delete safety records merely to make retries proceed.** First stop competing workers and confirm no earlier task can still write. Only clear a setting after establishing that the block is yours and no operator still requires it. A stuck block on a retained old source does not prove that the live alias is read-only.

In-process cleanup uses an independent bounded timeout. Explicit release failures propagate on the success path; disposal during another failure logs without masking the original error. Automatic recovery handles proven ownership, not arbitrary cluster-state ambiguity. See the [recovery runbook](/guide/reindex-safety).

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
protected override void ConfigureSettings(ElasticsearchClientSettings settings)
{
    settings.DisableDirectStreaming();
    settings.PrettyJson();
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
| `cluster_block_exception` | Cluster read-only, or the index is write-blocked by an in-progress quiesced reindex | Check disk space; if a reindex is running with `QuiesceSourceOnReindex`, writes to the source are rejected until it completes — see [Quiescing writes](./index-management.md#quiescing-writes-for-a-verified-cutover). If no reindex is running, the block may have been left behind: see [An index is stuck read-only after a reindex](#an-index-is-stuck-read-only-after-a-reindex) |
| `es_rejected_execution_exception` ("rejected execution of coordinating operation") | Indexing pressure limit exceeded, often during reindex of large documents | Lower `ReindexBatchSize`/`ReindexRequestsPerSecond`, see [Reindex Rejected Due to Indexing Pressure](#reindex-rejected-due-to-indexing-pressure) |

## Repository Exception Types

Foundatio.Repositories uses typed exceptions so callers can handle specific failure modes. The exceptions listed below inherit from `DocumentException`.

| Exception | When Thrown | Retryable? |
|-----------|------------|------------|
| `DuplicateDocumentException` | `AddAsync` when a document with the same ID already exists | No — remove the existing document or use `SaveAsync` |
| `VersionConflictDocumentException` | `SaveAsync` / `PatchAsync` when the document version doesn't match (HTTP 409) | Yes — re-fetch the document and retry |
| `DocumentNotFoundException` | `PatchAsync` when the target document doesn't exist (HTTP 404) | No — verify the document ID |
| `DocumentValidationException` | Any write operation when document validation fails | No — fix the document data |
| `DocumentException` | Other Elasticsearch errors not covered above | Depends on the underlying cause |

Index migrations throw the following, which inherit from `RepositoryException` rather than `DocumentException`:

| Exception | When Thrown | Retryable? |
|-----------|------------|------------|
| `ReindexIncompleteException` | Copy, verification, or promotion could not be confirmed | Inspect aliases and task/block state first. A pre-promotion failure may permit retry; an ambiguous/promoted destination requires recovery, not blind replay |
| `ReindexCompletionUnknownException` | Missing completion or ambiguous task/block ownership | No blind replay; use [ReindexCompletionUnknownException](#reindexcompletionunknownexception) and the durable-state recovery runbook |

### Partial Failures on Bulk Operations

When `AddAsync` or `SaveAsync` is called with multiple documents, some may succeed and others may fail. The repository:

1. **Processes all successes first** — fires events, populates cache, sends notifications.
2. **Leaves failed documents' cache unchanged** — failed writes don't mutate Elasticsearch, so existing cache entries remain valid. The writer that caused a conflict handles its own cache update via message bus notifications.
3. **Throws a typed exception** — `DuplicateDocumentException` for add, `VersionConflictDocumentException` for save.

```csharp
try
{
    await repository.AddAsync(documents);
}
catch (DuplicateDocumentException ex)
{
    // Successful documents were fully processed.
    // Duplicate documents: existing cache entries preserved (nothing was mutated).
    _logger.LogWarning(ex, "Partial failure: some documents already existed");
}
catch (VersionConflictDocumentException ex)
{
    _logger.LogWarning(ex, "Partial failure: some documents had version conflicts");
}
```

### Transient Error Retries

The repository automatically retries transient Elasticsearch errors:

- **HTTP 429** (Too Many Requests) — retried with exponential backoff, up to 3 retries (4 total attempts)
- **HTTP 503** (Service Unavailable) — retried with exponential backoff, up to 3 retries (4 total attempts)
- **HTTP 409** (Version Conflict) — **not** retried; the caller must handle conflict resolution
- `DuplicateDocumentException` — **not** retried by the resilience policy

::: info Reindex task-status polling uses its own backoff
Do not wrap a whole migration or asynchronous reindex kickoff in an unconditional retry policy. Dispatch can succeed even when its response is lost; replay must first pass the durable task and completion checks. The reindexer requests `MaxRetries(0)` locally, but the pinned transport does not reliably honor that request on multi-node pools; do not assume a kickoff was dispatched only once. This is a [current release gate](/guide/reindex-safety#current-release-gates), not a guarantee supplied by the task journal. Task-status polling is separate from dispatch and migration retries. That polling loop has its own dedicated exponential backoff (1 second, doubling up to a 30 second cap, with +/-25% jitter so concurrent reindex operations failing for the same reason don't retry in lockstep) on failure. See [Reindex Rejected Due to Indexing Pressure](#reindex-rejected-due-to-indexing-pressure) for the scenario this protects against.
:::

## Aggregation Warnings

### doc_count_error_upper_bound Warning

**Symptoms:**
- Warning-level log message about `doc_count_error_upper_bound` in terms aggregation results

**Explanation:**

When running terms aggregations across multiple shards, Elasticsearch returns an approximate count. The `doc_count_error_upper_bound` field indicates the maximum potential error in document counts for each term bucket. A non-zero value means shard-level approximations may have affected the results.

**Solutions:**

1. **Increase `shard_size`** if accuracy matters for your use case — this makes Elasticsearch consider more terms per shard before combining results
2. **Use a single shard** for small indexes where exact counts are important
3. **Ignore the warning** if approximate counts are acceptable for your use case (this is common for analytics and dashboards)

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
