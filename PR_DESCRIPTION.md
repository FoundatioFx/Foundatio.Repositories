# Enhance FieldConditions with Range Operators, Grouping, and Runtime Validation

## Summary

- **Range operators**: `FieldGreaterThan`, `FieldGreaterThanOrEqual`, `FieldLessThan`, `FieldLessThanOrEqual` with type-aware query generation (DateRangeQuery, NumericRangeQuery, TermRangeQuery)
- **OR/AND/NOT grouping**: `FieldOr`, `FieldAnd`, `FieldNot` APIs with both lambda and builder patterns for complex boolean logic without dropping to raw NEST
- **Contains/NotContains shorthands**: `FieldContains` and `FieldNotContains` for full-text token matching on analyzed fields
- **Conditional variants**: `*If` variants for all new operators (`FieldGreaterThanIf`, `FieldContainsIf`, `FieldHasValueIf`, `FieldEmptyIf`, etc.)
- **Runtime validation**: New `QueryValidationException` catches common misuse at query build time with actionable error messages
- **File reorganization**: Split monolithic builder into three focused files for maintainability

## Motivation

Gap analysis across multiple production codebases revealed that the majority of raw NEST `ElasticFilter` / `QueryDescriptor` usage falls into three categories that the existing `FieldCondition` API didn't cover:

1. **OR / boolean grouping** (~25+ locations) -- the #1 reason consumers drop to raw NEST. Any query requiring "A OR B" forced writing a `BoolQuery { Should = [...] }` by hand.
2. **Range comparisons** (~12+ locations) -- date cutoffs (`SnoozeUntilUtc < now`), numeric bounds (`Age > 18`), and keyword ranges. Each required a raw `DateRangeQuery`, `NumericRangeQuery`, or `TermRangeQuery`.
3. **Missing convenience shorthands** -- no `FieldContains` for full-text token matching, no conditional `*If` variants for HasValue/Empty.

Additionally, a critical silent failure mode was discovered: `FieldEquals` on an analyzed text field with no `.keyword` sub-field would produce a `TermQuery` that almost never matches (Elasticsearch stores lowercase tokens but TermQuery matches the exact input), returning zero results without any error.

This PR eliminates the need for raw NEST in these cases, providing type-safe, validated alternatives that guide developers toward correct usage.

## New API Surface

### Range Operators

```csharp
// Before: raw NEST DateRangeQuery
query.ElasticFilter(Query<T>.DateRange(d => d
    .Field(f => f.SnoozeUntilUtc)
    .LessThanOrEquals(utcNow)));

// After: type-safe one-liner
query.FieldLessThanOrEqual(f => f.SnoozeUntilUtc, utcNow);
```

Type-aware query generation based on the value type:

| Value Type | Generated ES Query | Example |
|---|---|---|
| `DateTime` / `DateTimeOffset` | `DateRangeQuery` | `.FieldGreaterThan(f => f.CreatedUtc, cutoff)` |
| `int` / `long` / `double` / `float` / `decimal` | `NumericRangeQuery` | `.FieldGreaterThan(f => f.Age, 18)` |
| `string` | `TermRangeQuery` | `.FieldGreaterThanOrEqual(f => f.Key, "2024")` |

Range operators do NOT apply `.keyword` resolution -- they operate on the field's natural mapping type. This is intentional: date and numeric fields have no keyword sub-field, and string ranges should compare the analyzed or keyword value directly.

**Conditional variants** skip the filter when the condition is false:

```csharp
DateTime? minDate = GetOptionalDate();
query.FieldGreaterThanIf(f => f.CreatedUtc, minDate, minDate.HasValue);
```

### Relationship with existing `DateRange`

The existing `DateRange(start, end)` extension is designed for bounded date windows with timezone support, and it integrates with time-series index selection (`.Index(start, end)` limits which daily/monthly partitions are queried). The new `FieldCondition` range operators handle one-sided comparisons (`CreatedUtc > cutoff`) and non-date types. Both can coexist. Use `DateRange` for bounded date windows on time-series indices; use `FieldLessThan`/`FieldGreaterThan` for one-sided comparisons or non-date fields.

**Anti-pattern**: Using `FieldCondition` date ranges on time-series indices without also calling `.Index(start, end)` will query all partitions. If you need date-bounded queries on time-series indices, prefer the `DateRange` extension which handles partition selection.

### Contains / NotContains (Full-Text Token Matching)

```csharp
// Generates: MatchQuery { Field = "name", Query = "Eric", Operator = And }
query.FieldContains(f => f.Name, "Eric");

// Multi-token: all tokens must be present (order-independent)
query.FieldContains(f => f.Name, "Smith Eric"); // matches "Eric J. Smith"

// Negated: BoolQuery { MustNot = MatchQuery }
query.FieldNotContains(f => f.Name, "Eric");
```

`FieldContains` generates a `MatchQuery` with `Operator = And`, which means:
- All tokens in the search value must be present in the field
- Token order does not matter ("Smith Eric" matches "Eric J. Smith")
- Matching is token-level, not substring-level ("Er" does NOT match "Eric")
- The field must be analyzed (text type) for tokenization to work

**This is NOT wildcard/substring matching.** For prefix or wildcard patterns, use `FilterExpression("field:pattern*")` instead.

### OR / AND / NOT Grouping

The #1 gap. Two API patterns cover static and dynamic use cases:

**Lambda API** (for groups known at compile time):

```csharp
query.FieldOr(g => g
    .FieldEquals(f => f.IsPrivate, false)
    .FieldAnd(g2 => g2
        .FieldEquals(f => f.IsEnabled, true)
        .FieldLessThanOrEqual(f => f.NextRunDateUtc, now)
    )
);
```

**Builder API** (for dynamic/conditional groups):

```csharp
var group = FieldConditionGroup<Employee>.Or();
group.FieldEquals(f => f.CompanyId, companyId);
if (includeAgeFilter)
    group.FieldGreaterThan(f => f.Age, 18);
query.FieldOr(group);
```

Generated Elasticsearch queries:

| API | Generated ES Query |
|---|---|
| `FieldOr(g => g.A().B())` | `BoolQuery { Should = [A, B], MinimumShouldMatch = 1 }` |
| `FieldAnd(g => g.A().B())` | `BoolQuery { Must = [A, B] }` |
| `FieldNot(g => g.A().B())` | `BoolQuery { MustNot = [A, B] }` |
| `FieldOr(g => g.A())` (single item) | Unwrapped to just `A` (no BoolQuery wrapper) |
| `FieldOr(g => { })` (empty) | Skipped entirely (no clause added) |

### FieldHasValueIf / FieldEmptyIf

```csharp
query.FieldHasValueIf(f => f.Name, applyFilter);  // ExistsQuery when true, no-op when false
query.FieldEmptyIf(f => f.Name, applyFilter);      // !ExistsQuery when true, no-op when false
```

### Runtime Validation

`QueryValidationException` (extends `RepositoryException`) is thrown at query build time for detectable misuse. Every error message follows a three-part pattern: **what went wrong** → **why it's a problem** → **how to fix it**.

| Scenario | What Happens | Why |
|---|---|---|
| `FieldEquals` on text-only field (no `.keyword`) | Throws with suggestion to use `FieldContains` or add `.keyword` sub-field | TermQuery on analyzed text almost never matches (ES stores lowercase tokens, TermQuery matches exact input) |
| `FieldContains` on keyword field | Throws with suggestion to use `FieldEquals` | MatchQuery requires an analyzed field for tokenization |
| `FieldEquals(IsDeleted, true)` with ActiveOnly soft-delete | Throws explaining the contradictory filter | SoftDeletesQueryBuilder auto-adds `isDeleted: false`, so `isDeleted: true` creates an impossible AND |
| Range operator with `null` value | Throws with suggestion to use `*If` variant or `FieldHasValue`/`FieldEmpty` | Null range bound is meaningless ("greater than nothing") |
| Range operator with collection value | Throws with suggestion to use `FieldEquals` | Ranges compare a single scalar |
| Range operator with unsupported type | Throws listing supported types | Only DateTime, DateTimeOffset, numeric types, and string are supported |
| FieldCondition with `null` field | Throws immediately | Bug in query construction code |

Example error message:

```
FieldEquals cannot be used on field 'name' because it is an analyzed text field with no .keyword sub-field.
A TermQuery on an analyzed text field almost never matches because Elasticsearch stores lowercased tokens (e.g., "eric") but TermQuery matches the exact input (e.g., "Eric").

To fix this, either:
  - Use FieldContains() for full-text token matching
  - Add a .keyword sub-field to the field mapping in your index configuration:
    m.Text(p => p.Name(f => f.Property).Fields(f => f.Keyword(k => k.Name("keyword"))))
```

## Design Decisions

### 1. Naming: `FieldAnd` / `FieldOr` / `FieldNot`

The `Field` prefix is the "brand" of the field condition API family. All methods start with `Field` for consistent IntelliSense grouping -- typing `query.Field` shows the complete API surface.

`FieldAnd` at the top level is technically redundant (default query behavior is AND) but is available for symmetry and is required when nesting inside OR/NOT groups.

**Alternatives considered:**
- Unprefixed `And`/`Or`/`Not` -- too generic, high collision risk with LINQ and other extensions
- `Group` for implicit AND -- unclear semantics, doesn't communicate the boolean operator
- `Where` prefix (EF Core style) -- diverges from established `Field*` convention

### 2. `FieldConditionGroupOperator` Naming

Named `FieldConditionGroupOperator` instead of `GroupOperator` to avoid ambiguous reference with `Foundatio.Parsers.LuceneQueries.Nodes.GroupOperator`, which is in scope on `QueryBuilderContext<T>` via `IQueryVisitorContext.DefaultOperator`.

### 3. `FieldNot` Semantics (AND-NOT, not NAND)

Multiple conditions inside `FieldNot` produce `BoolQuery { MustNot = [A, B] }`, which means **NOT A AND NOT B** -- exclude documents matching any clause. This is Elasticsearch's native MustNot behavior.

If you need **NOT (A AND B)** (exclude only documents matching ALL clauses), nest an explicit AND:

```csharp
// NOT A AND NOT B -- exclude docs matching A or B
query.FieldNot(g => g
    .FieldEquals(f => f.Status, "Draft")
    .FieldEquals(f => f.Status, "Archived")
);

// NOT (A AND B) -- exclude only docs matching both A and B
query.FieldNot(g => g.FieldAnd(g2 => g2
    .FieldEquals(f => f.IsEnabled, true)
    .FieldEquals(f => f.IsPrivate, true)
));
```

### 4. Expression-Only Shorthand Methods

New shorthand methods (`FieldContains`, `FieldGreaterThan`, etc.) are expression-only on `IRepositoryQuery<T>`. The generic `FieldCondition(Field, ComparisonOperator, value)` on untyped `IRepositoryQuery` serves as the escape hatch for dynamic/string field names.

**Why expression-only?**
- Preserves compile-time property metadata for potential future Roslyn analyzer
- Guides users toward the type-safe API as the default
- The generic overload remains available for advanced scenarios (dynamic field names, reflection-based query building)

### 5. Throw vs Log for Validation

Query builders use parameterless constructors and are registered via `Register<T>()`, so they don't have access to `ILogger`. All validation failures represent definite bugs (zero results, contradictory filters, wrong field type) rather than recoverable conditions. Throwing `QueryValidationException` at build time is the correct choice:
- **Fail-fast**: Developers discover the issue immediately during development/testing
- **Actionable**: Every error message tells you exactly what to do
- **No silent failures**: A zero-result query caused by a text-field TermQuery is invisible in production; an exception is not

### 6. Null Value Rewriting

When a null value is passed to `FieldEquals` or `FieldContains`, the operator is automatically rewritten:
- `FieldEquals(field, null)` → `FieldEmpty(field)` (generates `BoolQuery { MustNot = ExistsQuery }`)
- `FieldNotEquals(field, null)` → `FieldHasValue(field)` (generates `ExistsQuery`)

This matches developer intent ("find documents where this field is null") and produces the correct Elasticsearch query.

### 7. Empty Group Behavior

| Scenario | Behavior | Rationale |
|---|---|---|
| `FieldOr(g => { })` | Skip entirely (no clause added) | Empty groups should be no-ops |
| Single-item OR/AND group | Unwrapped to just the inner query | Avoids unnecessary BoolQuery nesting |
| `FieldNot(g => { })` | Skip entirely | NOT-nothing is meaningless |
| Nested empty groups | Pruned before translation | Prevents generating empty BoolQuery clauses |

### 8. Performance Considerations

All `FieldCondition` allocations happen once per query construction (a few `List<FieldCondition>` and `FieldConditionGroup` instances). The Elasticsearch network round-trip dominates by orders of magnitude. The `ElasticMappingResolver` lookups for validation are already cached. No per-document or per-request allocations are introduced.

### 9. Portability to Non-Elasticsearch Repositories

The `FieldCondition` model classes, `ComparisonOperator` enum, and group types currently live in the `Foundatio.Repositories.Elasticsearch` project and reference NEST's `Field` type for Elasticsearch-native field resolution (including `Expression<Func<T, object>>` to field name mapping). This means they are not directly reusable by non-Elasticsearch providers without a NEST reference.

However, the **design** is portable: the `ComparisonOperator` enum and grouping tree (`FieldConditionGroup` with `And`/`Or`/`Not`) map directly to SQL `WHERE` clauses. A future SQL Server or other provider implementation could either:
1. Reference the NEST package purely for the `Field` type (lightweight, no ES connection needed)
2. Introduce a provider-agnostic field abstraction and adapter layer
3. Read the `FieldCondition` / `FieldConditionGroup` from the options bag and translate to SQL with its own validation (e.g., SQL doesn't have analyzed vs keyword fields)

Moving these models to the core project was considered but deferred because `Nest.Field` provides significant value (type-safe expression resolution, camelCase conversion) that a hand-rolled abstraction would need to replicate.

### 10. What This Does NOT Cover (Intentionally)

- **Wildcard/prefix matching**: `FieldContains` is token-level full-text search, not substring matching. For patterns like `field:prefix*`, use `FilterExpression` or `SearchExpression`. The few real-world wildcard usages can remain as `ElasticFilter` or `FilterExpression`.
- **IdsQuery optimization**: `FieldEquals` on an ID field produces `TermsQuery` on `_id`, which is functionally equivalent to `IdsQuery`. The existing `Id()` / `ExcludedId()` extensions use `IdsQuery` and remain the optimal path.
- **Date range partition selection**: `FieldCondition` range operators don't influence time-series index selection. Use the existing `DateRange` extension for bounded date windows on time-series indices.

### 11. Industry Comparison

| Library | Validation Approach | Grouping API |
|---|---|---|
| **MongoDB C# driver** | `FilterDefinitionBuilder<T>` with expression overloads, runtime validation | Builder pattern with `&` / `|` operators |
| **EF Core** | Runtime `InvalidOperationException` for untranslatable LINQ | LINQ `Where` with `&&` / `||` |
| **NEST (raw)** | Zero validation | Manual `BoolQuery` construction |
| **This PR** | `QueryValidationException` at build time with actionable messages | `FieldOr`/`FieldAnd`/`FieldNot` with lambda and builder APIs |

## Anti-Patterns to Avoid

1. **FieldContains for substring/prefix** -- `FieldContains("Er")` won't match "Eric" because matching is at the token level. Use `FilterExpression("name:Er*")` for prefix matching.
2. **FieldEquals on analyzed-only text fields** -- now throws `QueryValidationException` at build time. Either use `FieldContains` or add a `.keyword` sub-field to the mapping.
3. **FieldContains on keyword fields** -- now throws `QueryValidationException`. Use `FieldEquals` for exact matching on keyword fields.
4. **Mixing FieldCondition ranges with DateRange on same field** -- technically works but confusing. Pick one approach per field.
5. **FieldCondition date ranges without `.Index(start, end)` on time-series indices** -- queries all partitions. Use `DateRange` with `.Index()` for partition-aware date filtering.
6. **Assuming FieldNot with multiple conditions is NOT (A AND B)** -- it's actually NOT A AND NOT B. For NOT (A AND B), nest `FieldAnd` inside `FieldNot`.
7. **Lambda parameter names `or`/`and`/`not`** -- these are C# contextual keywords. Use neutral names like `g` or `g2`: `FieldOr(g => g.FieldEquals(...))`.

## File Organization

Split the original monolithic `FieldConditionsQueryBuilder.cs` into three focused files:

| File | Contents | Namespace |
|---|---|---|
| `FieldCondition.cs` | `FieldCondition`, `ComparisonOperator`, `FieldConditionGroupOperator`, `FieldConditionGroup`, `FieldConditionGroup<T>` | `Foundatio.Repositories` |
| `FieldConditionQueryExtensions.cs` | All `Field*` extension methods on `IRepositoryQuery` / `IRepositoryQuery<T>`, plus `ReadFieldConditionQueryExtensions` | `Foundatio.Repositories` / `Foundatio.Repositories.Options` |
| `FieldConditionsQueryBuilder.cs` | `IElasticQueryBuilder` implementation, validation logic, query translation | `Foundatio.Repositories.Elasticsearch.Queries.Builders` |
| `QueryValidationException.cs` | `QueryValidationException` exception type | `Foundatio.Repositories.Exceptions` |

## Test Coverage

37 new integration tests covering:

- **Range operators**: `DateTime`, `DateTimeOffset`, `double`, `int`, combined bounds, conditional `*If` variants
- **Contains behavior**: single token match, multi-token order-independence, partial token non-match, negation
- **Field-type validation**: analyzed-only field throws on `FieldEquals`, keyword field throws on `FieldContains`
- **Soft-delete validation**: `IsDeleted` with `ActiveOnly` mode throws, with `SoftDeleteMode.All` succeeds
- **Null/collection rewriting**: null `Contains` → `IsEmpty`, null `NotContains` → `HasValue`
- **OR grouping**: simple OR, nested AND-inside-OR, empty group skip, single-item unwrap, builder API
- **NOT grouping**: single condition exclusion, multiple condition AND-NOT exclusion
- **Mixed operators**: range + equals in same OR group, FieldEmpty + FieldEquals in same OR
- **Range validation**: null value throws, collection value throws

All 502 existing tests continue to pass.

## Breaking Changes

None. All additions are new API surface. Existing `FieldEquals`, `FieldNotEquals`, `FieldEmpty`, `FieldHasValue` APIs are unchanged.
