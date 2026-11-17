# Foundatio.Repositories -- v8 API Patterns

> Applies to: v8+ (Elastic.Clients.Elasticsearch). For v7/NEST see [patterns-v7.md](patterns-v7.md).

## Querying

### Strongly-Typed Queries (preferred)

```csharp
var results = await repository.FindAsync(q => q
    .FieldEquals(e => e.CompanyId, companyId)
    .DateRange(start, end, e => e.CreatedUtc)
    .SortDescending(e => e.CreatedUtc));

var hit = await repository.FindOneAsync(q => q
    .FieldEquals(e => e.EmailAddress, "jane@example.com"));
var employee = hit?.Document;
```

**Available query methods:**

| Method | Purpose |
| --- | --- |
| `.FieldEquals(e => e.Field, value)` | Exact match (multiple values = OR) |
| `.FieldNotEquals(e => e.Field, value)` | Negated exact match |
| `.FieldCondition(e => e.Field, op, value)` | Comparison (any operator) |
| `.FieldHasValue(e => e.Field)` | Field is not null/empty |
| `.FieldEmpty(e => e.Field)` | Field is null/empty |
| `.FieldContains(e => e.Field, "token")` | Full-text token match (analyzed fields only) |
| `.FieldNotContains(e => e.Field, "token")` | Negated full-text token match |
| `.FieldGreaterThan(e => e.Field, value)` | Strictly greater than |
| `.FieldGreaterThanOrEqual(e => e.Field, value)` | Greater than or equal |
| `.FieldLessThan(e => e.Field, value)` | Strictly less than |
| `.FieldLessThanOrEqual(e => e.Field, value)` | Less than or equal |
| `.FieldOr(g => g.FieldEquals(...).FieldEquals(...))` | OR grouping |
| `.FieldAnd(g => g.FieldEquals(...).FieldEquals(...))` | AND grouping (explicit) |
| `.FieldNot(g => g.FieldEquals(...))` | NOT grouping (AND-NOT semantics) |
| `.DateRange(start, end, e => e.DateField)` | Date range filter |
| `.SortAscending(e => e.Field)` | Sort ascending |
| `.SortDescending(e => e.Field)` | Sort descending |
| `.Include(e => e.Field)` | Return only specific fields |
| `.Exclude(e => e.Field)` | Exclude fields from response |

Most single-field `Field*` predicate methods have `*If` variants for conditional application (e.g., `.FieldEqualsIf(e => e.Field, value, condition)`).
Range operators support DateTime, DateTimeOffset, numeric (int/long/double/float/decimal), and string types.

### FilterExpression (Lucene-style)

Use for dynamic or user-provided queries. Parsed by Foundatio.Parsers.

```csharp
var results = await repository.FindAsync(q => q
    .FilterExpression($"company_id:{companyId} employment_type:FullTime age:>=25"));

// OR logic
var results = await repository.FindAsync(q => q
    .FilterExpression("employment_type:FullTime OR employment_type:Contract"));

// Build from collection
string filter = String.Join(" OR ", companyIds.Select(id => $"company_id:{id}"));
```

### Date Range with Daily Index Routing

For `DailyIndex` / `MonthlyIndex`, `.Index()` limits which physical Elasticsearch indexes are queried. `.DateRange()` filters documents within those indexes.

```csharp
var results = await repository.FindAsync(q => q
    .Index(start, end)
    .DateRange(start, end, e => e.CreatedUtc)
    .FieldEquals(e => e.CompanyId, companyId));
```

## Aggregations

`CountAsync` returns a `CountResult` with `.Total` (long) and `.Aggregations`.

### AggregationsExpression DSL

Multiple aggregations are space-separated. Nested sub-aggregations use parentheses.

| Expression                             | Meaning                            |
| -------------------------------------- | ---------------------------------- |
| `terms:field`                          | Terms aggregation                  |
| `terms:(field~SIZE)`                   | Terms with bucket size limit       |
| `terms:(field~SIZE sub_agg)`           | Terms with nested sub-aggregation  |
| `terms:(field @include:VALUE)`         | Terms with include filter          |
| `date:field` / `date:field~1d`         | Date histogram (auto or fixed)     |
| `date:(field sub_agg)`                 | Date histogram with nested agg     |
| `cardinality:field`                    | Distinct count                     |
| `min:field` / `max:field`              | Min/Max                            |
| `avg:field` / `sum:field`              | Average / Sum                      |
| `sum:field~DEFAULT`                    | Sum with default value             |
| `missing:field`                        | Count where field is missing       |
| `percentiles:field`                    | Percentile distribution            |
| Prefix `-` (e.g. `-sum:field~1`)       | Sort descending by this agg        |

Also supported: `geogrid:`, `tophits:` (sub-agg only). See context7 for full syntax.

### Accessing Aggregation Results

Result key naming convention: `{type}_{field}`.

```csharp
var result = await repository.CountAsync(q => q
    .FieldEquals(e => e.CompanyId, companyId)
    .AggregationsExpression("terms:employment_type avg:age cardinality:company_id"));

double? avgAge = result.Aggregations.Average("avg_age")?.Value;
double? unique = result.Aggregations.Cardinality("cardinality_company_id")?.Value;

foreach (var bucket in result.Aggregations.Terms("terms_employment_type").Buckets)
{
    // bucket.Key = "FullTime", bucket.Total = 42
}
```

### Terms with Nested Min/Max

```csharp
var result = await repository.CountAsync(q => q
    .AggregationsExpression("terms:(company_id~50 min:created_utc max:created_utc)"));

foreach (var b in result.Aggregations.Terms("terms_company_id").Buckets)
{
    DateTime earliest = b.Aggregations.Min<DateTime>("min_created_utc").Value;
    DateTime latest = b.Aggregations.Max<DateTime>("max_created_utc").Value;
}
```

### Date Histogram

```csharp
var result = await repository.CountAsync(q => q
    .FieldEquals(e => e.CompanyId, companyId)
    .AggregationsExpression("date:(created_utc~1M cardinality:company_id)"));

foreach (var bucket in result.Aggregations.DateHistogram("date_created_utc").Buckets)
{
    // bucket.Date, bucket.Total
    double? uniqueInMonth = bucket.Aggregations.Cardinality("cardinality_company_id")?.Value;
}
```

## Pagination

Always use `SearchAfterPaging()` for deep pagination. Never use offset-based `.PageNumber()` for large result sets.

```csharp
var results = await repository.GetAllAsync(o => o.SearchAfterPaging().PageLimit(500));
do
{
    foreach (var doc in results.Documents)
    {
        await ProcessAsync(doc);
    }
} while (!cancellationToken.IsCancellationRequested && await results.NextPageAsync());
```

Works the same with `FindAsync`:

```csharp
var results = await repository.FindAsync(
    q => q.FieldEquals(e => e.CompanyId, companyId),
    o => o.SearchAfterPaging().PageLimit(1000));

do
{
    foreach (var doc in results.Documents)
    {
        await ProcessAsync(doc);
    }
} while (!cancellationToken.IsCancellationRequested && await results.NextPageAsync());
```

## Patch Operations

Four patch types are available. All automatically set `UpdatedUtc` on models implementing `IHaveDates`.

**Date serialization:** Default timestamp injection for single-document `JsonPatch`, `PartialPatch`, and `ScriptPatch` uses UTC ISO 8601. Source converters attached to the target model member do not control injected JSON or script values. For another stored date representation, use `ActionPatch<T>` (typed dates followed by native serialization), batch `JsonPatch`, or a `PartialPatch` with an explicit non-null timestamp in the stored format. Batch JSON means `PatchAllAsync` or a `PatchAsync(Ids)` input with more than one ID; exactly one ID uses the single-document path. For example, a target `updatedUtc` member that expects epoch milliseconds needs `new PartialPatch(new { name = "Updated", updatedUtc = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() })`. A member converter on the target type is not inherited by an anonymous partial object's `DateTime` property. For scripts, supply a correctly formatted parameter and assign it explicitly; supplying the timestamp parameter suppresses the automatic assignment. Automatic injection does not support arbitrary member date converters.

| Patch Type | How It Works | When to Use |
| --- | --- | --- |
| `PartialPatch` | Server-side field update via ES Update API | Simple field value changes |
| `ScriptPatch` | Server-side Painless script via ES Update API | Atomic increments, conditional logic |
| `ActionPatch` | Client-side fetch-mutate-reindex | Complex logic, full document-based cache invalidation |
| `JsonPatch` | Client-side RFC 6902 JSON Patch | Standard patch operations |

```csharp
// PartialPatch -- update fields by value
await repository.PatchAllAsync(
    q => q.FieldEquals(e => e.CompanyId, companyId),
    new PartialPatch(new { company_name = "New Corp" }));

// ScriptPatch -- atomic server-side update
await repository.PatchAsync(id, new ScriptPatch("ctx._source.years_employed++"));

// ScriptPatch with params
await repository.PatchAsync(id, new ScriptPatch(
    """
    ctx._source.years_employed += params.years;
    ctx._source.last_review = params.reviewDate;
    """)
{
    Params = new Dictionary<string, object>
    {
        { "years", 1 },
        { "reviewDate", DateTime.UtcNow }
    }
});

// ActionPatch -- return false to skip the write (noop)
bool modified = await repository.PatchAsync(id, new ActionPatch<Employee>(e =>
{
    if (e.YearsEmployed >= 10)
        return false;

    e.YearsEmployed = 10;
    return true;
}));

// Patch multiple by IDs
await repository.PatchAsync(new Ids(id1, id2, id3),
    new ScriptPatch("ctx._source.years_employed++"));
```

**Return values:** `PatchAsync(Id)` returns `Task<bool>`, `PatchAsync(Ids)` and `PatchAllAsync` return `Task<long>`.

**Concurrency:** For `IVersioned` entities, patches re-apply themselves to the latest document on conflict (up to `o.Retry(n)`, default 10). Client-side patches on non-versioned entities have no implicit version check. Batch JSON/action patches retry conflicting documents in grouped rounds (one mget + one bulk). To patch only if the document is unchanged since you read it:

```csharp
await repository.PatchAsync(id, new PartialPatch(new { Name = "New" }), o => o.IfVersion(employee.Version));
// throws VersionConflictDocumentException if the document changed
```

Models own their JSON representation. Use an explicit `string? IVersioned.Version { get; set; }` or `[JsonIgnore]` to omit the token, or keep an existing public property. GET/search/mget metadata and the cached `FindHit.Version` envelope are authoritative, including null; a separate public business `Version` can coexist with explicit concurrency metadata. Client-side patches preserve that metadata after applying the configured serializer contract. They do not infer reserved JSON paths from CLR property names.

`PatchAllAsync` reads batches from a point-in-time snapshot (or explicit scroll snapshot), so patches that modify sort fields do not revisit documents after refreshes. Patch reads that discover locations by search use snapshot paging too. Multi-ID pipeline `PartialPatch` uses real-time multi-get for known index/routing locations, including daily/monthly ObjectIds, and searches only unresolved locations; a mixed batch preserves real-time visibility for known locations. The single-ID overload requires a resolvable location. Multi-ID JSON/action patches always use search and skip initially unmatched or filtered IDs, even on a single index. Pipeline `PartialPatch` recursively merges objects, retaining unspecified nested fields.

Batch and ID-removal selection require complete search results and bypass query-result caching. A timeout, failed shard, or lost PIT/scroll context throws before processing that page; earlier writes and notifications remain applied. Repository-owned PIT cleanup follows the latest returned ID through option clones, while caller-supplied PIT IDs remain caller-owned. PIT and scroll cleanup also runs when a batch stops or throws; cleanup failures are logged without losing a fetched final batch or the original exception. Failed scroll and repository-owned PIT continuations are terminal. Ordinary reads retain their partial-response policy.

Multi-ID JSON/action patches write and notify each search page before reading the next (`PageLimit`, default 500); failures are accumulated in bounded summaries and thrown after the remaining batches are processed. Their `UpdateCallback` runs per successful batch. If a client-side patch changes the source `Id` property, notifications retain the original write target ID while document-based cache hooks receive the patched model.

Multi-ID pipeline partial patches also bound real-time reads and bulk writes with `PageLimit` (default 500). They write full known-location batches before opening the discovery snapshot and retain only a bounded document buffer. Operation-wide location and ID sets keep real-time results authoritative, including missing/error results, and prevent duplicate writes when routed and unrouted inputs overlap. `UpdateCallback` and notifications run per written batch; small mixed known/discovered inputs can share one bulk request.

`PatchAllAsync` with `ScriptPatch` uses the bulk Update API. `o.UseUpdateByQuery()` opts into update-by-query (faster, but skips concurrently changed documents and throws unless `o.ProceedOnQueryConflicts()`).

`PatchAllAsync` with `UseUpdateByQuery()` and `RemoveAllAsync` using delete-by-query apply `BeforeQuery`, `SystemFilter`, and `ConfigureQuery` before preparing their requests, preserving repository selection filters. Retries reuse that prepared query. Batched patch failures accumulate counts and at most 10 sampled IDs across processed batches; exception IDs are not a complete failure list.

By-query completion uses the effective configured query for invalidation and notifications. Completed asynchronous update-by-query counts come from the final response, not interim task status. Require readable, nonnegative integer `total`, `created`, `updated`, `deleted` and `version_conflicts` counters within `Int64` range; the affected-count sum must not overflow. Missing or invalid counters leave the outcome unknown: notify the effective query and throw `DocumentException`, without reporting zero or resubmitting the task. An externally canceled Elasticsearch operation also throws after accounting for any applied changes; it must not return a success count or replay a partially applied request.

Synchronous per-ID pipeline script completion requires `total`, `updated`, `deleted` and `version_conflicts`, with nonnegative counters that do not exceed the total; `created` is not required. Unknown completion and ambiguous transport failures are terminal and issue a conservative cache invalidation/refresh hint for the attempted ID, without implying that it changed or notifying future undispatched IDs. These unknown outcomes do not invoke `UpdateCallback`; confirmed changes retain that callback even when other writes fail. Native serialization failures can require the same hint when dispatch cannot be distinguished. Pipeline script selection is search-based even for one ID: wait for a preceding write's refresh when the script must see it; the patch's final refresh does not make initial selection real-time.

## Remove and Fetch

```csharp
// Remove by query
await repository.RemoveAllAsync(q => q
    .FieldEquals(e => e.CompanyId, companyId));

// Batch fetch (returns IReadOnlyCollection<T>)
var employees = await repository.GetByIdsAsync(ids, o => o.Cache());

// Existence check
bool exists = await repository.ExistsAsync(id);
```

**Gotcha:** When caching is disabled and no event listeners are registered, `RemoveAllAsync` uses Elasticsearch `delete_by_query`, which counts (and skips) version conflicts when concurrent writes modify matching documents. Since `delete_by_query` has no `retry_on_conflict`, the repository re-runs the query up to `o.Retry(n)` more times (default 10, so up to 11 passes) with backoff and a refresh between passes. The returned count is the cumulative number deleted; if conflicts persist after the budget, `VersionConflictDocumentException` is thrown with the counts unless `o.ProceedOnQueryConflicts()` is set (then a warning is logged and the partial count returned).

**Gotcha:** `RemoveAsync(T)`/`RemoveAsync(IEnumerable<T>)` check the version of `IVersioned` documents (like `SaveAsync`). Use `RemoveAsync(id)` or `o.SkipVersionCheck()` to delete regardless.

ID-based and batched query removals retain the selected hit's index, ID and routing even if a source ID differs or pre-remove handlers change source fields. The source model is not rewritten. Model-based removal still resolves its target after pre-remove handlers. Preserve virtual `GetByIdsAsync` and `RemoveAsync` dispatch. The private read capture follows the requested selection until the virtual read returns, and the delete handoff consumes metadata per selected document instance across shallow options clones and partitioned base calls. Correlate found native multi-get results to their original request slots: stored routing can differ from requested routing on the same shard, so capture ownership follows the active base-read request selection. Prerequisite calls leave the remaining selection available. Clear the private handoff when the operation ends; existing overrides need no new public contract.

**Gotcha:** Cached/listener `RemoveAllAsync` batches return the number processed, including documents concurrently deleted before their remove request; only actual deletions are notified. A failed batch stops later batches. The delete-by-query path returns the actual deleted count.

## Command Options

| Option                          | Purpose                                      |
| ------------------------------- | -------------------------------------------- |
| `o => o.Cache()`                | Enable cache read/write                      |
| `o => o.Cache("key")`           | Cache with specific key                      |
| `o => o.ReadCache(false)`        | Disable cache reads without changing writes  |
| `o => o.ImmediateConsistency()` | ES refresh after write (use in tests only)   |
| `o => o.SearchAfterPaging()`    | Deep pagination with search_after            |
| `o => o.PageLimit(N)`           | Page size                                    |
| `o => o.SoftDeleteMode(mode)`   | `ActiveOnly` (default), `All`, `DeletedOnly` |
| `o => o.Notifications(false)`   | Suppress change notifications                |
| `o => o.Originals()`            | Track original values for change detection   |
| `o => o.SkipVersionCheck()`     | Write without the optimistic concurrency check |
| `o => o.IfVersion(version)`     | `PatchAsync(id)` only if still at `version`  |
| `o => o.Retry(n)`               | n retries after the first attempt (default 10) |
| `o => o.IncludeSoftDeletes()`   | Include soft-deleted docs in queries         |

`ReadCache(bool)` overrides cache reads independently; use `.Cache(false).ReadCache(false)` to bypass both when options may already enable read-only caching. Caller include/exclude options and masks bypass shared document-ID cache reads and writes, though an explicit projection key can store the filtered response. Repository default source filtering does not disable normal ID caching. `GetByIdsAsync` discovery bypasses inherited cache options so a partial custom-key result or cached parent route cannot suppress discovery of other locations. Eventual or projected discovery results, including misses, cannot populate shared raw-ID groups; independent realtime multi-get hits remain eligible. Mixed safe/unsafe routes for one raw ID skip that whole shared group. Soft-delete repositories require the native `IsDeleted` source field so projections cannot change the requested soft-delete mode.

Native GET/mget source filters, disabled source, stored-field requests without source and `Realtime(false)` prevent shared ID-cache population. Multi-get per-document source settings override request defaults, and complete groups remain eligible independently of projected or eventual groups. The hooks run after cache lookup; use command-option projections or `ReadCache(false)` to bypass a warm entry. Rewritten native ID, routing or index targets cannot establish an original-location miss: cache missing markers only when the native response or completed discovery covered that original location. Explicit cache keys retain the requested result.

Required-field registrations are deduplicated when original command options or shallow clones are reused, while retaining caller-required fields. Exact exclusions retain the existing required-field precedence. Resolve native names before rejecting wildcard or parent-path exclusions that cover a required field or one of its ancestors. These conflicts throw `ArgumentException` before the filtered read request is sent. Source-filter matching is case-sensitive and only `*` is a wildcard; `?` is literal. Recommend include projections or narrower explicit exclusions instead of silently dropping broader filters.

With `DefaultPipeline`, Add/Save skip cache population from submitted models because Elasticsearch may transform them. Save still invalidates ID entries; Add does not remove them. When write caching is requested, the provided custom key is evicted. Read from Elasticsearch for transformed field values. Successful cached writes without a pipeline retain response index, routing and version metadata.

When populating a successful batch, examine all attempted outcomes for each repeated raw ID, including unknown failures. Cache that group only when every write succeeded and every response names a distinct, nonempty concrete index. Different routes can share a shard, and request aliases can name one physical index; neither proves independence. Input order cannot select a winner when retries finish later. Evict unsafe raw-ID groups. If an explicit-key result would be incomplete, evict that key and the successful write ID entries, and skip population for the write.

Complete cache invalidation and eligible population, including explicit custom keys, before post-write handlers run; nested writes then own the final cache state. During awaited callbacks, an overlapping nested write or ID invalidation removes the outer write's custom key immediately and again after callbacks in case it was refilled. This also covers fresh repository instances sharing the same underlying cache client and scope. Harmless callbacks retain the populated key without extra cache reads or removals; tracking ends with the callback flow. The repository's default cache clones values; external cache clients retain their configured behavior. Remove invalidates before `DocumentsRemoved`, including when the handler throws.

`SaveAsync` pairs originals with documents by concrete index, routing and ID. `Originals()` loads exact locations for routed and multi-index repositories; supplied originals suppress only the corresponding reads. Successful locations retain their own original values and notifications when other items fail.

Post-write notifications retain the completed write ID even when event handlers mutate a model or replace `ModifiedDocument.Value`. Preserve the full modified-document group through the virtual notification hook so batch soft-delete classification remains unchanged. The timestamp helper's default native name is a literal root source member; existing `GetUpdatedUtcFieldPath()` overrides keep nested dot-path semantics, including inherited overrides returning the same string.

## Index Mapping

All index configurations use `.Dynamic(false)`, which disables Elasticsearch's automatic field mapping. **Every field you query, filter, sort, or aggregate on MUST have an explicit mapping** in `ConfigureIndexMapping`. Unmapped fields are stored in `_source` but never indexed -- queries against them silently return zero results.

### Field Type Reference

| Mapping Call | When to Use | ES Type |
| --- | --- | --- |
| `.Keyword(e => e.Field)` | Exact match, filtering, aggregations | `keyword` |
| `.Text(e => e.Field, t => t.AddKeywordAndSortFields())` | Full-text search + exact match + sorting | `text` + `.keyword` + `.sort` sub-fields |
| `.IntegerNumber(e => e.Field)` | Integer numeric fields | `integer` |
| `.LongNumber(e => e.Field)` | Long numeric fields | `long` |
| `.DoubleNumber(e => e.Field)` | Double numeric fields | `double` |
| `.FloatNumber(e => e.Field)` | Float numeric fields | `float` |
| `.Date(e => e.Field)` | DateTime / DateTimeOffset | `date` |
| `.Boolean(e => e.Field)` | Boolean flags | `boolean` |
| `.GeoPoint(e => e.Field)` | Latitude/longitude | `geo_point` |
| `.Nested(e => e.Field, n => n.Properties(...))` | Nested objects needing independent querying | `nested` |
| `.Object(e => e.Field, o => o.Properties(...))` | Embedded objects (flattened) | `object` |

`.SetupDefaults()` automatically maps `Id` (keyword), `CreatedUtc` (date), `UpdatedUtc` (date), and `IsDeleted` (boolean) based on which model interfaces are implemented. It does not map `IVersioned.Version`: concurrency uses `_seq_no`/`_primary_term` metadata. The model's configured serializer controls whether its public version property is stored in `_source`.

### When to Bump the Index Version

**No version bump needed** -- adding a mapping for a brand-new field:

- For `Index<T>` / `VersionedIndex<T>`: `ConfigureIndexesAsync` (or the lazy first-write path) calls the PUT Mapping API on the existing index, adding the new field mapping additively.
- For `DailyIndex<T>` / `MonthlyIndex<T>`: new daily/monthly physical indexes are created on-demand with the full current mapping. **Already-created daily/monthly indexes are NOT updated** -- `DailyIndex.ConfigureAsync` is a no-op.

**Version bump IS required** -- changing an existing field's type or analyzer (e.g., `Text` to `Keyword`). Elasticsearch does not allow in-place type changes. Bump the `VersionedIndex` version to trigger creation of a new index with the correct mapping and automatic reindexing.

### Existing Documents After Adding a New Mapping

After adding a mapping for a previously unmapped field, only **newly saved/indexed documents** will be searchable on that field. To make existing documents searchable:

- **Foundatio migration** -- create a `MigrationBase` subclass that uses `PatchAllAsync` or `BatchProcessAsync` to touch all affected documents (recommended for production).
- **`PatchAllAsync`** with a script that leaves field values unchanged but still writes (for example, `ctx._source.id = ctx._source.id`). Do not set `ctx.op = 'noop'`, which skips reindexing.
- **Elasticsearch Update By Query API** with no script: `POST /{index}/_update_by_query` re-indexes every document in place.

For `DailyIndex`/`MonthlyIndex`, you must also apply the mapping to existing physical indexes before the update-by-query will help.

**Trade-off for Daily/Monthly indexes**: Rolling forward (doing nothing to old partitions and waiting for retention to cycle out old data) is often the cheapest strategy.

**Mapping resolver cache**: After applying a manual PUT Mapping, the in-process `ElasticMappingResolver` auto-refreshes from the server within ~60 seconds. To force immediate recognition, call `index.MappingResolver.RefreshMapping()`.

### Checklist: Adding a Queryable Model Field

1. Add the property to the model class.
2. Add the mapping in `ConfigureIndexMapping` (choose the correct field type from the table above).
3. If changing an existing field's mapping, bump the index version and add a reindex script.
4. If brand-new field, no version bump is needed.
5. Add to `AllowedQueryFields` / `AllowedAggregationFields` / `AllowedSortFields` if the index restricts those sets.
6. Decide whether existing documents need to be searchable on the new field immediately. If yes, create a Foundatio migration to backfill.
