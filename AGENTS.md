# Agent Guidelines for Foundatio.Repositories

You are an expert .NET engineer working on Foundatio.Repositories, a production-grade repository pattern library built on top of Foundatio. This library is used by developers to build robust data access layers with Elasticsearch. Your changes must maintain backward compatibility, performance, and reliability. Approach each task methodically: research existing patterns, make surgical changes, and validate thoroughly.

**Craftsmanship Mindset**: Every line of code should be intentional, readable, and maintainable. Write code you'd be proud to have reviewed by senior engineers. Prefer simplicity over cleverness. When in doubt, favor explicitness and clarity.

## Repository Overview

Foundatio.Repositories provides a generic repository pattern implementation built on Foundatio building blocks:

- **Repository Pattern** (`IRepository<T>`, `IReadOnlyRepository<T>`) - CRUD operations with async events
- **Searchable Repositories** (`ISearchableRepository<T>`) - Dynamic querying with Foundatio.Parsers
- **Elasticsearch Implementation** - Full-featured Elasticsearch repository with index management
- **Caching Integration** - Real-time cache invalidation with distributed cache support
- **Message Bus Integration** - Entity change notifications for real-time applications
- **Patch Operations** - JSON patch, partial document, and script-based updates
- **Soft Deletes** - Built-in soft delete support with query filtering
- **Document Versioning** - Optimistic concurrency with version tracking
- **Index Management** - Schema versioning, daily/monthly strategies, migrations
- **Jobs** - Index maintenance, snapshots, reindexing

Design principles: **interface-first**, **built on Foundatio primitives**, **Elasticsearch-optimized**, **testable with in-memory implementations**.

## Quick Start

```bash
# Build
dotnet build Foundatio.Repositories.slnx

# Test shared repository tests
dotnet test tests/Foundatio.Repositories.Tests/Foundatio.Repositories.Tests.csproj

# Test Elasticsearch implementation (requires running Elasticsearch)
docker compose up -d
dotnet test tests/Foundatio.Repositories.Elasticsearch.Tests/Foundatio.Repositories.Elasticsearch.Tests.csproj

# Format code
dotnet format Foundatio.Repositories.slnx
```

**Note**: When building within a workspace, use `Foundatio.All.slnx` instead to include all Foundatio projects in the build and test cycle.

## Project Structure

```text
src
├── Foundatio.Repositories             # Core repository abstractions
│   ├── Exceptions                     # Repository-specific exceptions
│   ├── Extensions                     # Extension methods for repositories
│   ├── JsonPatch                      # JSON patch operation support
│   ├── Migration                      # Document migration infrastructure
│   ├── Models                         # Entity interfaces and base models
│   ├── Options                        # Command and query options
│   ├── Queries                        # Query builders and interfaces
│   └── Utility                        # ObjectId, helpers
└── Foundatio.Repositories.Elasticsearch  # Elasticsearch implementation
    ├── Configuration                  # Index configuration and mappings
    ├── CustomFields                   # Dynamic field support
    ├── Extensions                     # Elasticsearch-specific extensions
    ├── Jobs                           # Maintenance and migration jobs
    ├── Options                        # Elasticsearch-specific options
    ├── Queries                        # Query builders for Elasticsearch
    ├── Repositories                   # Base repository implementations
    └── Utility                        # Elasticsearch utilities
tests
├── Foundatio.Repositories.Tests       # Core repository unit tests
└── Foundatio.Repositories.Elasticsearch.Tests  # Elasticsearch integration tests
    └── Repositories                   # Test repository implementations
        ├── Configuration              # Test index configurations
        ├── Models                     # Test entity models
        └── Queries                    # Test query implementations
samples
└── Foundatio.SampleApp                # Sample Blazor application
    ├── Client                         # Blazor WebAssembly client
    ├── Server                         # ASP.NET Core server with repositories
    └── Shared                         # Shared models
```

## Coding Standards

### Style & Formatting

- Follow `.editorconfig` rules and [Microsoft C# conventions](https://learn.microsoft.com/en-us/dotnet/csharp/fundamentals/coding-style/coding-conventions)
- Run `dotnet format` to auto-format code
- Match existing file style; minimize diffs
- No code comments unless necessary—code should be self-explanatory

### Architecture Patterns

- **Interface-first design**: All core features expose interfaces (`IRepository<T>`, `ISearchableRepository<T>`)
- **Dependency Injection**: Use constructor injection; extend via `IServiceCollection` extensions
- **Foundatio Integration**: Built on Foundatio primitives (caching, messaging, queues, jobs)
- **Naming**: `Foundatio.Repositories.[Feature]` for projects, `I[Feature]Repository` for interfaces
- **Elasticsearch Provider**: Primary implementation in `Foundatio.Repositories.Elasticsearch`

### Code Quality

- Write complete, runnable code—no placeholders, TODOs, or `// existing code...` comments
- Use modern C# features: pattern matching, nullable references, `is` expressions, target-typed `new()`
- Follow SOLID, DRY principles; remove unused code and parameters
- Clear, descriptive naming; prefer explicit over clever
- Use `AnyContext()` (e.g., `ConfigureAwait(false)`) in library code (not in tests)
- Prefer `ValueTask<T>` for hot paths that may complete synchronously
- Always dispose resources: use `using` statements or `IAsyncDisposable`
- Handle cancellation tokens properly: check `token.IsCancellationRequested`, pass through call chains

### Common Patterns

- **Async suffix**: All async methods end with `Async` (e.g., `GetAsync`, `AddAsync`)
- **CancellationToken**: Last parameter, defaulted to `default` in public APIs
- **Extension methods**: Place in `Extensions/` directory, use descriptive class names (e.g., `FindResultsExtensions`)
- **Logging**: Use structured logging with `ILogger`, log at appropriate levels
- **Exceptions**: Use `ArgumentException.ThrowIfNullOrEmpty(parameter)` for validation. For repository-specific errors, use `DocumentNotFoundException`, `DocumentValidationException`, `VersionConflictDocumentException`. This ensures consumers get predictable exception types. Throw `ArgumentNullException`, `ArgumentException`, `InvalidOperationException` with clear messages for general validation and operation errors.

### Single Responsibility

- Each class has one reason to change
- Methods do one thing well; extract when doing multiple things
- Keep files focused: one primary type per file
- Separate concerns: don't mix I/O, business logic, and presentation
- If a method needs a comment explaining what it does, it should probably be extracted

### Performance Considerations

- **Avoid allocations in hot paths**: Use `Span<T>`, `Memory<T>`, pooled buffers
- **Prefer structs for small, immutable types**: But be aware of boxing
- **Cache expensive computations**: Use `Lazy<T>` or explicit caching
- **Batch operations when possible**: Reduce round trips for I/O
- **Profile before optimizing**: Don't guess—measure with benchmarks
- **Consider concurrent access**: Use `ConcurrentDictionary`, `Interlocked`, or proper locking
- **Avoid async in tight loops**: Consider batching or `ValueTask` for hot paths
- **Dispose resources promptly**: Don't hold connections/handles longer than needed

## Making Changes

### Before Starting

1. **Gather context**: Read related files, search for similar implementations, understand the full scope
2. **Research patterns**: Find existing usages of the code you're modifying using grep/semantic search
3. **Understand completely**: Know the problem, side effects, and edge cases before coding
4. **Plan the approach**: Choose the simplest solution that satisfies all requirements
5. **Check dependencies**: Verify you understand how changes affect dependent code

### Pre-Implementation Analysis

Before writing any implementation code, think critically:

1. **What could go wrong?** Consider race conditions, null references, edge cases, resource exhaustion
2. **What are the failure modes?** Network failures, timeouts, out-of-memory, concurrent access
3. **What assumptions am I making?** Validate each assumption against the codebase
4. **Is this the root cause?** Don't fix symptoms—trace to the core problem
5. **Will this scale?** Consider performance under load, memory allocation patterns
6. **Is there existing code that does this?** Search before creating new utilities

### Test-First Development

**Always write or extend tests before implementing changes:**

1. **Find existing tests first**: Search for tests covering the code you're modifying
2. **Extend existing tests**: Add test cases to existing test classes/methods when possible for maintainability
3. **Write failing tests**: Create tests that demonstrate the bug or missing feature
4. **Implement the fix**: Write minimal code to make tests pass
5. **Refactor**: Clean up while keeping tests green
6. **Verify edge cases**: Add tests for boundary conditions and error paths

**Why extend existing tests?** Consolidates related test logic, reduces duplication, improves discoverability, maintains consistent test patterns.

### While Coding

- **Minimize diffs**: Change only what's necessary, preserve formatting and structure
- **Preserve behavior**: Don't break existing functionality or change semantics unintentionally
- **Build incrementally**: Run `dotnet build` after each logical change to catch errors early
- **Test continuously**: Run `dotnet test` frequently to verify correctness
- **Match style**: Follow the patterns in surrounding code exactly

### Validation

Before marking work complete, verify:

1. **Builds successfully**: `dotnet build Foundatio.Repositories.slnx` exits with code 0
2. **All tests pass**: `dotnet test Foundatio.Repositories.slnx` shows no failures
3. **No new warnings**: Check build output for new compiler warnings
4. **API compatibility**: Public API changes are intentional and backward-compatible when possible
5. **Documentation updated**: XML doc comments added/updated for public APIs
6. **Interface documentation**: Update interface definitions and docs with any API changes
7. **Feature documentation**: Add entries to [docs/](docs/) folder for new features or significant changes
8. **Breaking changes flagged**: Clearly identify any breaking changes for review

### Error Handling

- **Validate inputs**: Check for null, empty strings, invalid ranges at method entry
- **Fail fast**: Throw exceptions immediately for invalid arguments (don't propagate bad data)
- **Meaningful messages**: Include parameter names and expected values in exception messages
- **Don't swallow exceptions**: Log and rethrow, or let propagate unless you can handle properly
- **Use guard clauses**: Early returns for invalid conditions, keep happy path unindented

## Security

- **Validate all inputs**: Use guard clauses, check bounds, validate formats before processing
- **Sanitize external data**: Never trust data from queues, caches, or external sources
- **Avoid injection attacks**: Use parameterized queries, escape user input, validate file paths
- **No sensitive data in logs**: Never log passwords, tokens, keys, or PII
- **Use secure defaults**: Default to encrypted connections, secure protocols, restricted permissions
- **Follow OWASP guidelines**: Review [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- **Dependency security**: Check for known vulnerabilities before adding dependencies
- **No deprecated APIs**: Avoid obsolete cryptography, serialization, or framework features

## Testing

### Philosophy: Battle-Tested Code

Tests are not just validation—they're **executable documentation** and **design tools**. Well-tested code is:

- **Trustworthy**: Confidence to refactor and extend
- **Documented**: Tests show how the API should be used
- **Resilient**: Edge cases are covered before they become production bugs

### Framework

- **xUnit** as the primary testing framework
- **ElasticRepositoryTestBase** provides shared base class for Elasticsearch integration testing
- Follow [Microsoft unit testing best practices](https://learn.microsoft.com/en-us/dotnet/core/testing/unit-testing-best-practices)

### Test-First Workflow

1. **Search for existing tests**: `dotnet test --filter "FullyQualifiedName~MethodYouAreChanging"`
2. **Extend existing test classes**: Add new `[Fact]` or `[Theory]` cases to existing files
3. **Write the failing test first**: Verify it fails for the right reason
4. **Implement minimal code**: Just enough to pass the test
5. **Add edge case tests**: Null inputs, empty collections, boundary values, concurrent access
6. **Run full test suite**: Ensure no regressions

### Test Principles (FIRST)

- **Fast**: Tests execute quickly
- **Isolated**: No dependencies on external services or execution order
- **Repeatable**: Consistent results every run
- **Self-checking**: Tests validate their own outcomes
- **Timely**: Write tests alongside code

### Naming Convention

Use the pattern: `MethodName_StateUnderTest_ExpectedBehavior`

Examples:

- `AddAsync_WithValidDocument_ReturnsDocumentWithId`
- `GetByIdAsync_WhenNotFound_ReturnsNull`
- `RemoveAsync_WithSoftDelete_SetsIsDeletedFlag`

### Test Structure

Follow the AAA (Arrange-Act-Assert) pattern:

```csharp
[Fact]
public async Task AddAsync_WithValidDocument_ReturnsDocumentWithId()
{
    // Arrange
    var identity = IdentityGenerator.Generate();

    // Act
    var result = await _identityRepository.AddAsync(identity);

    // Assert
    Assert.NotNull(result?.Id);
}
```

### Parameterized Tests

Use `[Theory]` with `[InlineData]` for multiple scenarios:

```csharp
[Theory]
[InlineData(null)]
[InlineData("")]
[InlineData(" ")]
public async Task GetByIdAsync_WithInvalidId_ReturnsNull(string id)
{
    var result = await _identityRepository.GetByIdAsync(id);
    Assert.Null(result);
}
```

### Test Organization

- Mirror the main code structure (e.g., `Repositories/` tests for repository implementations)
- Use constructors and `IDisposable` for setup/teardown
- Inject `ITestOutputHelper` for test logging
- Inherit from `ElasticRepositoryTestBase` for Elasticsearch integration tests

### Integration Testing

- Use in-memory implementations (from Foundatio) for unit tests
- For Elasticsearch tests, use `docker compose up` to start Elasticsearch
- Inherit from `ElasticRepositoryTestBase` which provides `_configuration`, `_cache`, `_client`
- Use `RemoveDataAsync()` to clean state between tests
- Keep integration tests separate from unit tests

### Running Tests

```bash
# All tests
dotnet test Foundatio.Repositories.slnx

# Core repository tests only
dotnet test tests/Foundatio.Repositories.Tests/Foundatio.Repositories.Tests.csproj

# Elasticsearch tests (requires running Elasticsearch)
dotnet test tests/Foundatio.Repositories.Elasticsearch.Tests/Foundatio.Repositories.Elasticsearch.Tests.csproj

# Specific test file
dotnet test --filter "FullyQualifiedName~RepositoryTests"

# With logging
dotnet test --logger "console;verbosity=detailed"
```

## Debugging

1. **Reproduce** with minimal steps
2. **Understand** the root cause before fixing
3. **Test** the fix thoroughly
4. **Document** non-obvious fixes in code if needed

## Resilience & Reliability

- **Expect failures**: Network calls fail, resources exhaust, concurrent access races
- **Timeouts everywhere**: Never wait indefinitely; use cancellation tokens
- **Retry with backoff**: Use exponential backoff with jitter for transient failures
- **Circuit breakers**: Prevent cascading failures in distributed systems
- **Graceful degradation**: Return cached data, default values, or partial results when appropriate
- **Idempotency**: Design operations to be safely retryable
- **Resource limits**: Bound queues, caches, and buffers to prevent memory exhaustion

## Resources

- [README.md](README.md) - Overview and feature list
- [samples/](samples/) - Sample Blazor application with repository usage
- [Foundatio](https://github.com/FoundatioFx/Foundatio) - Core building blocks this library depends on
- [Foundatio.Parsers](https://github.com/FoundatioFx/Foundatio.Parsers) - Query parsing for searchable repositories
