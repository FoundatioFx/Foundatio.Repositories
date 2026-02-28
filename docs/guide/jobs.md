# Jobs

Foundatio.Repositories provides built-in jobs for index maintenance, snapshots, and cleanup. This guide covers the available jobs and how to use them.

## Overview

Jobs are background tasks that perform maintenance operations on your Elasticsearch indexes. They're built on Foundatio's job infrastructure.

## Available Jobs

### MaintainIndexesJob

Runs maintenance tasks on all configured indexes:

```csharp
public class MaintainIndexesJob : IJob
{
    private readonly IElasticConfiguration _configuration;

    public MaintainIndexesJob(IElasticConfiguration configuration)
    {
        _configuration = configuration;
    }

    public async Task<JobResult> RunAsync(CancellationToken cancellationToken = default)
    {
        await _configuration.MaintainIndexesAsync();
        return JobResult.Success;
    }
}
```

**Tasks performed:**
- Update aliases for time-series indexes
- Delete expired indexes (if `DiscardExpiredIndexes` is true)
- Ensure index consistency

**Usage:**

```csharp
// Register the job
services.AddJob<MaintainIndexesJob>();

// Run manually
var job = new MaintainIndexesJob(configuration);
await job.RunAsync();
```

### SnapshotJob

Creates Elasticsearch snapshots for backup:

```csharp
public class SnapshotJob : IJob
{
    private readonly ElasticsearchClient _client;
    private readonly string _repositoryName;

    public SnapshotJob(ElasticsearchClient client, string repositoryName = "backups")
    {
        _client = client;
        _repositoryName = repositoryName;
    }

    public async Task<JobResult> RunAsync(CancellationToken cancellationToken = default)
    {
        var snapshotName = $"snapshot-{DateTime.UtcNow:yyyy-MM-dd-HH-mm-ss}";
        
        var response = await _client.Snapshot.CreateAsync(
            _repositoryName,
            snapshotName,
            s => s.WaitForCompletion(false));
        
        if (!response.IsValidResponse)
            return JobResult.FromException(response.OriginalException);
        
        return JobResult.Success;
    }
}
```

**Configuration:**

```csharp
// Register snapshot repository in Elasticsearch first
await client.Snapshot.CreateRepositoryAsync("backups", r => r
    .FileSystem(fs => fs
        .Location("/mnt/backups")
        .Compress(true)));

// Then use the job
var job = new SnapshotJob(client, "backups");
await job.RunAsync();
```

### CleanupSnapshotJob

Cleans up old snapshots:

```csharp
public class CleanupSnapshotJob : IJob
{
    private readonly ElasticsearchClient _client;
    private readonly string _repositoryName;
    private readonly TimeSpan _maxAge;

    public CleanupSnapshotJob(ElasticsearchClient client, string repositoryName, TimeSpan maxAge)
    {
        _client = client;
        _repositoryName = repositoryName;
        _maxAge = maxAge;
    }

    public async Task<JobResult> RunAsync(CancellationToken cancellationToken = default)
    {
        var cutoffDate = DateTime.UtcNow - _maxAge;
        
        var snapshots = await _client.Snapshot.GetAsync(_repositoryName, "_all");
        
        foreach (var snapshot in snapshots.Snapshots)
        {
            if (snapshot.StartTime < cutoffDate)
            {
                await _client.Snapshot.DeleteAsync(_repositoryName, snapshot.Name);
            }
        }
        
        return JobResult.Success;
    }
}
```

### CleanupIndexesJob

Deletes old indexes based on patterns and age:

```csharp
public class CleanupIndexesJob : IJob
{
    private readonly ElasticsearchClient _client;
    private readonly string _indexPattern;
    private readonly TimeSpan _maxAge;

    public CleanupIndexesJob(ElasticsearchClient client, string indexPattern, TimeSpan maxAge)
    {
        _client = client;
        _indexPattern = indexPattern;
        _maxAge = maxAge;
    }

    public async Task<JobResult> RunAsync(CancellationToken cancellationToken = default)
    {
        var cutoffDate = DateTime.UtcNow - _maxAge;
        
        var indices = await _client.Cat.IndicesAsync(i => i.Index(_indexPattern));
        
        foreach (var index in indices.Records)
        {
            // Parse date from index name (e.g., logs-2024.01.15)
            if (TryParseIndexDate(index.Index, out var indexDate) && indexDate < cutoffDate)
            {
                await _client.Indices.DeleteAsync(index.Index);
            }
        }
        
        return JobResult.Success;
    }
}
```

### ReindexWorkItemHandler

Handles reindexing operations as background work items with automatic lock renewal and progress reporting:

```csharp
public class ReindexWorkItem
{
    public string OldIndex { get; set; }
    public string NewIndex { get; set; }
    public string Alias { get; set; }
    public string Script { get; set; }        // Painless script for data transformation
    public bool DeleteOld { get; set; }       // Delete old index after successful reindex
    public string TimestampField { get; set; } // Field for incremental reindex
    public DateTime? StartUtc { get; set; }   // Start time for incremental reindex
}
```

**Features:**
- **Automatic Lock Renewal**: The handler sets `AutoRenewLockOnProgress = true`, which automatically renews the distributed lock whenever progress is reported
- **Progress Reporting**: Reports progress percentage and status messages during reindex
- **Two-Pass Reindex**: Performs a second pass to catch documents modified during the first pass (if `TimestampField` is set)
- **Error Handling**: Failed documents are stored in an error index (`{newIndex}-error`)

**Usage:**

```csharp
// Queue a reindex work item
await queue.EnqueueAsync(new ReindexWorkItem
{
    OldIndex = "employees-v1",
    NewIndex = "employees-v2",
    Alias = "employees",
    Script = "ctx._source.department = ctx._source.dept; ctx._source.remove('dept');",
    DeleteOld = true,
    TimestampField = "updatedUtc"  // Enable two-pass reindex
});
```

## Reindex Progress Monitoring

The `ElasticReindexer` provides detailed progress reporting during reindex operations:

### Progress Callback

```csharp
await configuration.ReindexAsync(async (progress, message) =>
{
    // progress: 0-100 percentage
    // message: Status description
    
    _logger.LogInformation("Reindex {Progress}%: {Message}", progress, message);
    
    // Update metrics or UI
    await UpdateProgressMetricAsync(progress);
});
```

### Progress Stages

The reindex process reports progress through several stages:

| Progress | Stage |
|----------|-------|
| 0% | Starting reindex |
| 0-90% | First pass: copying documents |
| 91% | First pass complete, updating aliases |
| 92% | Aliases updated |
| 92-96% | Second pass: catching modified documents |
| 97% | Second pass complete |
| 98% | Verifying document counts |
| 99% | Deleting old index (if configured) |
| 100% | Complete |

### Progress Messages

Example progress messages during reindex:

```
0%: Starting reindex...
45%: Total: 1,000,000 Completed: 450,000 VersionConflicts: 0
90%: Total: 1,000,000 Completed: 900,000 VersionConflicts: 12
91%: Total: 1,000,000 Completed: 1,000,000
92%: Updated aliases: employees Remove: employees-v1 Add: employees-v2
97%: Total: 150 Completed: 150 (second pass)
98%: Old Docs: 1,000,000 New Docs: 1,000,012
99%: Deleted index: employees-v1
100%: Complete
```

### Monitoring Reindex in Migrations

When using reindex within a migration, combine progress reporting with lock renewal:

```csharp
public class ReindexMigration : MigrationBase
{
    private readonly IElasticConfiguration _configuration;

    public ReindexMigration(IElasticConfiguration configuration)
    {
        _configuration = configuration;
    }

    public override MigrationType MigrationType => MigrationType.VersionedAndResumable;
    public override int? Version => 15;

    public override async Task RunAsync(MigrationContext context)
    {
        await _configuration.ReindexAsync(async (progress, message) =>
        {
            context.Logger.LogInformation("Reindex {Progress}%: {Message}", progress, message);
            
            // Renew lock during long reindex operations
            await context.Lock.RenewAsync(TimeSpan.FromMinutes(30));
        });
    }
}
```

### Handling Reindex Failures

Failed documents are stored in an error index for investigation:

```csharp
// After reindex, check for failures
var errorIndex = "employees-v2-error";
var existsResponse = await client.Indices.ExistsAsync(errorIndex);

if (existsResponse.Exists)
{
    var failures = await client.SearchAsync<object>(s => s
        .Index(errorIndex)
        .Size(100));
    
    foreach (var hit in failures.Hits)
    {
        _logger.LogError("Failed to reindex document: {Id}", hit.Id);
        // hit.Source contains: Index, Id, Version, Routing, Source, Cause, Status, Found
    }
}
```

## Running Jobs

### Manual Execution

```csharp
var job = new MaintainIndexesJob(configuration);
var result = await job.RunAsync();

if (result.IsSuccess)
{
    Console.WriteLine("Maintenance completed");
}
else
{
    Console.WriteLine($"Maintenance failed: {result.Error}");
}
```

### Scheduled Execution

Using Foundatio's job runner:

```csharp
// Run job on a schedule
var runner = new JobRunner(
    new MaintainIndexesJob(configuration),
    loggerFactory,
    runContinuous: true,
    interval: TimeSpan.FromHours(1));

await runner.RunAsync();
```

### Hosted Service

```csharp
public class MaintenanceHostedService : BackgroundService
{
    private readonly IElasticConfiguration _configuration;
    private readonly ILogger _logger;

    public MaintenanceHostedService(
        IElasticConfiguration configuration,
        ILogger<MaintenanceHostedService> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                _logger.LogInformation("Running index maintenance...");
                await _configuration.MaintainIndexesAsync();
                _logger.LogInformation("Index maintenance completed");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Index maintenance failed");
            }

            await Task.Delay(TimeSpan.FromHours(1), stoppingToken);
        }
    }
}

// Register
services.AddHostedService<MaintenanceHostedService>();
```

### Cron-Based Scheduling

```csharp
public class ScheduledMaintenanceJob : IJob
{
    private readonly IElasticConfiguration _configuration;

    public ScheduledMaintenanceJob(IElasticConfiguration configuration)
    {
        _configuration = configuration;
    }

    public async Task<JobResult> RunAsync(CancellationToken cancellationToken = default)
    {
        await _configuration.MaintainIndexesAsync();
        return JobResult.Success;
    }
}

// Register with cron schedule (using Foundatio.Jobs.Hosting)
services.AddCronJob<ScheduledMaintenanceJob>("0 0 * * *");  // Daily at midnight
```

## Custom Jobs

### Index Statistics Job

```csharp
public class IndexStatisticsJob : IJob
{
    private readonly ElasticsearchClient _client;
    private readonly ILogger _logger;

    public IndexStatisticsJob(ElasticsearchClient client, ILogger<IndexStatisticsJob> logger)
    {
        _client = client;
        _logger = logger;
    }

    public async Task<JobResult> RunAsync(CancellationToken cancellationToken = default)
    {
        var stats = await _client.Indices.StatsAsync("_all");
        
        foreach (var index in stats.Indices)
        {
            _logger.LogInformation(
                "Index {Name}: {Docs} docs, {Size}",
                index.Key,
                index.Value.Primaries.Documents.Count,
                index.Value.Primaries.Store.Size);
        }
        
        return JobResult.Success;
    }
}
```

### Health Check Job

```csharp
public class ElasticsearchHealthJob : IJob
{
    private readonly ElasticsearchClient _client;
    private readonly ILogger _logger;

    public ElasticsearchHealthJob(ElasticsearchClient client, ILogger<ElasticsearchHealthJob> logger)
    {
        _client = client;
        _logger = logger;
    }

    public async Task<JobResult> RunAsync(CancellationToken cancellationToken = default)
    {
        var health = await _client.Cluster.HealthAsync();
        
        _logger.LogInformation(
            "Cluster health: {Status}, Nodes: {Nodes}, Shards: {Shards}",
            health.Status,
            health.NumberOfNodes,
            health.ActiveShards);
        
        if (health.Status == Health.Red)
        {
            _logger.LogError("Cluster is in RED status!");
            return JobResult.FromException(new Exception("Cluster health is RED"));
        }
        
        return JobResult.Success;
    }
}
```

### Data Archival Job

```csharp
public class ArchiveOldDataJob : IJob
{
    private readonly IEmployeeRepository _repository;
    private readonly IArchiveRepository _archiveRepository;
    private readonly ILogger _logger;

    public ArchiveOldDataJob(
        IEmployeeRepository repository,
        IArchiveRepository archiveRepository,
        ILogger<ArchiveOldDataJob> logger)
    {
        _repository = repository;
        _archiveRepository = archiveRepository;
        _logger = logger;
    }

    public async Task<JobResult> RunAsync(CancellationToken cancellationToken = default)
    {
        var cutoffDate = DateTime.UtcNow.AddYears(-5);
        long archived = 0;
        
        await _repository.BatchProcessAsync(
            q => q.DateRange(null, cutoffDate, (Employee e) => e.TerminationDate),
            async batch =>
            {
                // Archive to cold storage
                await _archiveRepository.AddAsync(batch.Documents);
                
                // Remove from hot storage
                await _repository.RemoveAsync(batch.Documents);
                
                archived += batch.Documents.Count;
                _logger.LogInformation("Archived {Count} records", archived);
                
                return !cancellationToken.IsCancellationRequested;
            },
            o => o.IncludeSoftDeletes());
        
        _logger.LogInformation("Archival completed: {Total} records archived", archived);
        return JobResult.Success;
    }
}
```

## Job Patterns

### Retry with Backoff

```csharp
public async Task<JobResult> RunAsync(CancellationToken cancellationToken = default)
{
    int retries = 3;
    TimeSpan delay = TimeSpan.FromSeconds(1);
    
    while (retries > 0)
    {
        try
        {
            await DoWorkAsync(cancellationToken);
            return JobResult.Success;
        }
        catch (Exception ex) when (retries > 1)
        {
            _logger.LogWarning(ex, "Job failed, retrying in {Delay}...", delay);
            await Task.Delay(delay, cancellationToken);
            delay *= 2;  // Exponential backoff
            retries--;
        }
    }
    
    return JobResult.FromException(new Exception("Job failed after retries"));
}
```

### Distributed Locking

```csharp
public async Task<JobResult> RunAsync(CancellationToken cancellationToken = default)
{
    await using var lockHandle = await _lockProvider.AcquireAsync(
        "maintenance-job",
        TimeSpan.FromMinutes(30),
        cancellationToken);
    
    if (lockHandle == null)
    {
        _logger.LogInformation("Could not acquire lock, another instance is running");
        return JobResult.Success;
    }
    
    await DoMaintenanceAsync(cancellationToken);
    return JobResult.Success;
}
```

### Progress Reporting

```csharp
public async Task<JobResult> RunAsync(CancellationToken cancellationToken = default)
{
    long total = await _repository.CountAsync();
    long processed = 0;
    
    await _repository.BatchProcessAsync(
        q => q.All(),
        async batch =>
        {
            await ProcessBatchAsync(batch);
            
            processed += batch.Documents.Count;
            var progress = (double)processed / total * 100;
            _logger.LogInformation("Progress: {Progress:F1}%", progress);
            
            return true;
        });
    
    return JobResult.Success;
}
```

## Best Practices

### 1. Use Distributed Locks

```csharp
await using var lockHandle = await _lockProvider.AcquireAsync("job-name");
if (lockHandle == null) return JobResult.Success;
```

### 2. Handle Cancellation

```csharp
while (!cancellationToken.IsCancellationRequested)
{
    await ProcessNextBatchAsync();
}
```

### 3. Log Progress

```csharp
_logger.LogInformation("Processed {Count} of {Total}", processed, total);
```

### 4. Use Appropriate Intervals

```csharp
// Maintenance: hourly or daily
// Snapshots: daily
// Cleanup: weekly
```

### 5. Monitor Job Health

```csharp
if (!result.IsSuccess)
{
    _metrics.IncrementCounter("job_failures", new { job = "maintenance" });
}
```

## Next Steps

- [Index Management](/guide/index-management) - Index configuration
- [Migrations](/guide/migrations) - Data migrations
- [Configuration](/guide/configuration) - Job configuration
