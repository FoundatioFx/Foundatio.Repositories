# Migrations

Foundatio.Repositories provides a migration system for evolving your data schema over time. This guide covers creating and running migrations.

## Overview

Migrations allow you to:
- Transform existing documents when schema changes
- Run one-time data fixes
- Perform repeatable maintenance tasks
- Track migration history

## Migration Types

### MigrationType Enum

```csharp
public enum MigrationType
{
    Versioned,           // Run once, sequential, fails permanently on error
    VersionedAndResumable,  // Run once, sequential, auto-retry on failure
    Repeatable           // Run after versioned migrations, can run multiple times
}
```

| Type | Runs | On Failure | Use Case |
|------|------|------------|----------|
| `Versioned` | Once | Fails permanently | Schema changes |
| `VersionedAndResumable` | Once | Auto-retry | Long-running migrations |
| `Repeatable` | Multiple times | Retry | Maintenance tasks |

## Creating Migrations

### IMigration Interface

```csharp
public interface IMigration
{
    MigrationType MigrationType { get; }
    int? Version { get; }
    bool RequiresOffline { get; }
    Task RunAsync(MigrationContext context);
}
```

### Basic Migration

```csharp
public class AddDepartmentFieldMigration : IMigration
{
    private readonly IEmployeeRepository _repository;

    public AddDepartmentFieldMigration(IEmployeeRepository repository)
    {
        _repository = repository;
    }

    public MigrationType MigrationType => MigrationType.Versioned;
    public int? Version => 1;
    public bool RequiresOffline => false;

    public async Task RunAsync(MigrationContext context)
    {
        await _repository.PatchAllAsync(
            q => q.FieldEmpty(e => e.Department),
            new PartialPatch(new { Department = "General" }));
    }
}
```

### Using MigrationBase

```csharp
public class AddDepartmentFieldMigration : MigrationBase
{
    private readonly IEmployeeRepository _repository;

    public AddDepartmentFieldMigration(IEmployeeRepository repository)
    {
        _repository = repository;
        MigrationType = MigrationType.Versioned;
    }

    public override int? Version => 1;

    public override async Task RunAsync(MigrationContext context)
    {
        await _repository.PatchAllAsync(
            q => q.FieldEmpty(e => e.Department),
            new PartialPatch(new { Department = "General" }));
        
        _logger.LogInformation("Added default department to employees");
    }
}
```

### Resumable Migration

For long-running migrations that should resume on failure:

```csharp
public class BackfillDataMigration : MigrationBase
{
    private readonly IEmployeeRepository _repository;

    public BackfillDataMigration(IEmployeeRepository repository)
    {
        _repository = repository;
        MigrationType = MigrationType.VersionedAndResumable;
    }

    public override int? Version => 2;

    public override async Task RunAsync(MigrationContext context)
    {
        long processed = 0;
        
        await _repository.BatchProcessAsync(
            q => q.FieldEmpty(e => e.CalculatedField),
            async batch =>
            {
                foreach (var employee in batch.Documents)
                {
                    employee.CalculatedField = CalculateValue(employee);
                }
                await _repository.SaveAsync(batch.Documents);
                
                processed += batch.Documents.Count;
                _logger.LogInformation("Processed {Count} employees", processed);
                
                return true;  // Continue processing
            },
            o => o.PageLimit(100));
    }
}
```

### Repeatable Migration

For tasks that should run periodically:

```csharp
public class CleanupExpiredDataMigration : MigrationBase
{
    private readonly IEmployeeRepository _repository;

    public CleanupExpiredDataMigration(IEmployeeRepository repository)
    {
        _repository = repository;
        MigrationType = MigrationType.Repeatable;
    }

    public override int? Version => null;  // No version for repeatable

    public override async Task RunAsync(MigrationContext context)
    {
        var cutoffDate = DateTime.UtcNow.AddYears(-7);
        
        var deleted = await _repository.RemoveAllAsync(
            q => q.DateRange(null, cutoffDate, e => e.TerminationDate),
            o => o.IncludeSoftDeletes());
        
        _logger.LogInformation("Deleted {Count} expired employee records", deleted);
    }
}
```

## Running Migrations

### MigrationManager

```csharp
var manager = new MigrationManager(
    serviceProvider,
    migrationStateRepository,
    lockProvider,
    loggerFactory);

// Register migrations from assembly
manager.AddMigrationsFromAssembly<AddDepartmentFieldMigration>();

// Run all pending migrations
var result = await manager.RunMigrationsAsync();

Console.WriteLine($"Success: {result == MigrationResult.Success}");
```

### Migration Result

```csharp
public enum MigrationResult
{
    Success,
    Failed,
    UnableToAcquireLock,
    Cancelled
}
```

### Dependency Injection Setup

```csharp
// Register migration services
services.AddSingleton<IMigrationStateRepository, MigrationStateRepository>();
services.AddSingleton<ILockProvider>(new InMemoryLockProvider());

// Register migrations
services.AddTransient<AddDepartmentFieldMigration>();
services.AddTransient<BackfillDataMigration>();

// Register migration manager
services.AddSingleton<MigrationManager>(sp =>
{
    var manager = new MigrationManager(
        sp,
        sp.GetRequiredService<IMigrationStateRepository>(),
        sp.GetRequiredService<ILockProvider>(),
        sp.GetRequiredService<ILoggerFactory>());
    
    manager.AddMigrationsFromAssembly<AddDepartmentFieldMigration>();
    return manager;
});
```

### Running on Startup

```csharp
public class MigrationStartupAction : IStartupAction
{
    private readonly MigrationManager _migrationManager;
    private readonly ILogger _logger;

    public MigrationStartupAction(MigrationManager migrationManager, ILogger<MigrationStartupAction> logger)
    {
        _migrationManager = migrationManager;
        _logger = logger;
    }

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Running migrations...");
        
        var result = await _migrationManager.RunMigrationsAsync();
        
        if (result != MigrationResult.Success)
        {
            _logger.LogError("Migration failed: {Result}", result);
            throw new InvalidOperationException($"Migration failed: {result}");
        }
        
        _logger.LogInformation("Migrations completed successfully");
    }
}

// Register startup action
services.AddStartupAction<MigrationStartupAction>();
```

## Migration Context

The `MigrationContext` provides access to the migration lock, logger, and cancellation:

```csharp
public class MigrationContext
{
    public ILock Lock { get; }
    public ILogger Logger { get; }
    public CancellationToken CancellationToken { get; }
}
```

### Renewing Locks for Long-Running Migrations

For long-running migrations, you should periodically renew the lock to prevent it from expiring:

```csharp
public class LongRunningMigration : MigrationBase
{
    private readonly IEmployeeRepository _repository;

    public LongRunningMigration(IEmployeeRepository repository)
    {
        _repository = repository;
        MigrationType = MigrationType.VersionedAndResumable;
    }

    public override int? Version => 10;

    public override async Task RunAsync(MigrationContext context)
    {
        long processed = 0;
        
        await _repository.BatchProcessAsync(
            q => q.All(),
            async batch =>
            {
                // Process batch
                foreach (var employee in batch.Documents)
                {
                    employee.CalculatedField = CalculateValue(employee);
                }
                await _repository.SaveAsync(batch.Documents, o => o.Notifications(false));
                
                processed += batch.Documents.Count;
                
                // Renew lock every batch to prevent expiration
                await context.Lock.RenewAsync(TimeSpan.FromMinutes(30));
                
                context.Logger.LogInformation("Processed {Count} employees, lock renewed", processed);
                
                // Check for cancellation
                return !context.CancellationToken.IsCancellationRequested;
            },
            o => o.PageLimit(500));
    }
}
```

::: tip Lock Renewal Best Practices
- Renew the lock at regular intervals (e.g., every batch or every few minutes)
- The `MigrationManager` automatically renews locks for `VersionedAndResumable` migrations before each retry
- For `Versioned` migrations, you must manually renew if the migration takes longer than the lock timeout (30 minutes by default)
:::

### Using the Logger

The context provides a logger scoped to your migration class:

```csharp
public override async Task RunAsync(MigrationContext context)
{
    context.Logger.LogInformation("Starting migration...");
    
    try
    {
        await DoMigrationWorkAsync();
        context.Logger.LogInformation("Migration completed successfully");
    }
    catch (Exception ex)
    {
        context.Logger.LogError(ex, "Migration failed");
        throw;
    }
}
```

### Handling Cancellation

```csharp
public override async Task RunAsync(MigrationContext context)
{
    await _repository.BatchProcessAsync(
        query,
        async batch =>
        {
            // Check for cancellation
            context.CancellationToken.ThrowIfCancellationRequested();
            
            // Process batch...
            return true;
        });
}
```

## Migration Patterns

### Schema Migration

```csharp
public class RenameFieldMigration : MigrationBase
{
    private readonly IEmployeeRepository _repository;

    public RenameFieldMigration(IEmployeeRepository repository)
    {
        _repository = repository;
        MigrationType = MigrationType.Versioned;
    }

    public override int? Version => 3;

    public override async Task RunAsync(MigrationContext context)
    {
        // Copy old field to new field
        await _repository.PatchAllAsync(
            q => q.FieldHasValue(e => e.OldFieldName),
            new ScriptPatch(@"
                ctx._source.newFieldName = ctx._source.oldFieldName;
                ctx._source.remove('oldFieldName');
            "));
    }
}
```

### Data Transformation

```csharp
public class NormalizeEmailMigration : MigrationBase
{
    private readonly IEmployeeRepository _repository;

    public NormalizeEmailMigration(IEmployeeRepository repository)
    {
        _repository = repository;
        MigrationType = MigrationType.Versioned;
    }

    public override int? Version => 4;

    public override async Task RunAsync(MigrationContext context)
    {
        await _repository.PatchAllAsync(
            q => q.FieldHasValue(e => e.Email),
            new ScriptPatch("ctx._source.email = ctx._source.email.toLowerCase()"));
    }
}
```

### Backfill Calculated Fields

```csharp
public class BackfillFullNameMigration : MigrationBase
{
    private readonly IEmployeeRepository _repository;

    public BackfillFullNameMigration(IEmployeeRepository repository)
    {
        _repository = repository;
        MigrationType = MigrationType.VersionedAndResumable;
    }

    public override int? Version => 5;

    public override async Task RunAsync(MigrationContext context)
    {
        await _repository.BatchProcessAsync(
            q => q.FieldEmpty(e => e.FullName),
            async batch =>
            {
                foreach (var emp in batch.Documents)
                {
                    emp.FullName = $"{emp.FirstName} {emp.LastName}";
                }
                await _repository.SaveAsync(batch.Documents, o => o.Notifications(false));
                return true;
            },
            o => o.PageLimit(500));
    }
}
```

### Data Cleanup

```csharp
public class RemoveOrphanedRecordsMigration : MigrationBase
{
    private readonly IEmployeeRepository _employeeRepo;
    private readonly ICompanyRepository _companyRepo;

    public RemoveOrphanedRecordsMigration(
        IEmployeeRepository employeeRepo,
        ICompanyRepository companyRepo)
    {
        _employeeRepo = employeeRepo;
        _companyRepo = companyRepo;
        MigrationType = MigrationType.Repeatable;
    }

    public override async Task RunAsync(MigrationContext context)
    {
        // Find all company IDs
        var companies = await _companyRepo.GetAllAsync();
        var validCompanyIds = companies.Documents.Select(c => c.Id).ToHashSet();
        
        // Remove employees with invalid company IDs
        await _employeeRepo.BatchProcessAsync(
            q => q.All(),
            async batch =>
            {
                var orphaned = batch.Documents
                    .Where(e => !validCompanyIds.Contains(e.CompanyId))
                    .ToList();
                
                if (orphaned.Any())
                {
                    await _employeeRepo.RemoveAsync(orphaned);
                    _logger.LogInformation("Removed {Count} orphaned employees", orphaned.Count);
                }
                
                return true;
            });
    }
}
```

## Best Practices

### 1. Make Migrations Idempotent

```csharp
// Good: Check before modifying
await repository.PatchAllAsync(
    q => q.FieldEmpty(e => e.NewField),  // Only update if not set
    new PartialPatch(new { NewField = "default" }));

// Bad: Always update
await repository.PatchAllAsync(
    q => q.All(),
    new PartialPatch(new { NewField = "default" }));
```

### 2. Use Batch Processing for Large Datasets

```csharp
await repository.BatchProcessAsync(
    query,
    async batch =>
    {
        // Process in batches to avoid memory issues
        return true;
    },
    o => o.PageLimit(500));
```

### 3. Disable Notifications for Bulk Updates

```csharp
await repository.SaveAsync(documents, o => o.Notifications(false));
```

### 4. Log Progress

```csharp
long processed = 0;
await repository.BatchProcessAsync(query, async batch =>
{
    processed += batch.Documents.Count;
    _logger.LogInformation("Processed {Count} of {Total}", processed, batch.Total);
    return true;
});
```

### 5. Test Migrations

```csharp
[Fact]
public async Task Migration_Should_Update_Department()
{
    // Arrange
    var employee = await _repository.AddAsync(new Employee { Name = "Test" });
    var lock_ = await _lockProvider.AcquireAsync("test");
    
    // Act
    var migration = new AddDepartmentFieldMigration(_repository);
    await migration.RunAsync(new MigrationContext(lock_, _logger, CancellationToken.None));
    
    // Assert
    var updated = await _repository.GetByIdAsync(employee.Id);
    Assert.Equal("General", updated.Department);
}
```

## Next Steps

- [Index Management](/guide/index-management) - Schema versioning
- [Jobs](/guide/jobs) - Scheduled maintenance
- [Configuration](/guide/configuration) - Migration configuration
