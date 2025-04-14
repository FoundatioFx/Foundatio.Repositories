using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Exceptionless.DateTimeExtensions;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Jobs;

public class CleanupSnapshotJob : IJob
{
    protected readonly ElasticsearchClient _client;
    protected readonly ILockProvider _lockProvider;
    protected readonly TimeProvider _timeProvider;
    protected readonly ILogger _logger;
    private readonly ICollection<RepositoryMaxAge> _repositories = new List<RepositoryMaxAge>();

    public CleanupSnapshotJob(ElasticsearchClient client, ILockProvider lockProvider, ILoggerFactory loggerFactory) : this(client, lockProvider, TimeProvider.System, loggerFactory)
    {
    }

    public CleanupSnapshotJob(ElasticsearchClient client, ILockProvider lockProvider, TimeProvider timeProvider, ILoggerFactory loggerFactory)
    {
        _client = client;
        _lockProvider = lockProvider;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = loggerFactory?.CreateLogger(GetType()) ?? NullLogger.Instance;
    }

    public void AddRepository(string name, TimeSpan maxAge)
    {
        _repositories.Add(new RepositoryMaxAge { Name = name, MaxAge = maxAge });
    }

    public virtual async Task<JobResult> RunAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting snapshot cleanup...");
        if (_repositories.Count == 0)
            _repositories.Add(new RepositoryMaxAge { Name = "data", MaxAge = TimeSpan.FromDays(3) });

        // need retries, need check for snapshot running, use cat for snapshot names
        foreach (var repo in _repositories)
            await DeleteOldSnapshotsAsync(repo.Name, repo.MaxAge, cancellationToken).AnyContext();

        _logger.LogInformation("Finished snapshot cleanup.");
        return JobResult.Success;
    }

    private async Task DeleteOldSnapshotsAsync(string repo, TimeSpan maxAge, CancellationToken cancellationToken)
    {
        var sw = Stopwatch.StartNew();
        var result = await _client.Snapshot.GetAsync(
            repo,
            "_all",
            d => d.RequestConfiguration(r =>
                r.RequestTimeout(TimeSpan.FromMinutes(5))), cancellationToken).AnyContext();
        sw.Stop();
        _logger.LogRequest(result);

        var snapshots = new List<SnapshotDate>();
        if (result.IsValidResponse && result.Snapshots != null)
        {
            snapshots = result.Snapshots?
                .Where(r => !String.Equals(r.State, "IN_PROGRESS"))
                .Select(r => new SnapshotDate { Name = r.Name, Date = r.EndTime })
                .ToList();
        }

        if (result.IsValidResponse)
            _logger.LogInformation("Retrieved list of {SnapshotCount} snapshots from {Repo} in {Duration:g}", snapshots.Count, repo, sw.Elapsed);
        else
            _logger.LogErrorRequest(result, "Failed to retrieve list of snapshots from {Repo} in {Duration:g}", repo, sw.Elapsed);

        if (snapshots.Count == 0)
            return;

        var oldestValidSnapshot = _timeProvider.GetUtcNow().UtcDateTime.Subtract(maxAge);
        var snapshotsToDelete = snapshots.Where(r => r.Date.IsBefore(oldestValidSnapshot)).OrderBy(s => s.Date).ToList();
        if (snapshotsToDelete.Count == 0)
            return;

        // log that we are seeing snapshots that should have been deleted already
        var oldSnapshots = snapshots.Where(s => s.Date < oldestValidSnapshot.AddDays(-1)).Select(s => s.Name).ToList();
        if (oldSnapshots.Count > 0)
            _logger.LogError("Found old snapshots that should have been deleted: {SnapShots}", String.Join(", ", oldSnapshots));

        _logger.LogInformation("Selected {SnapshotCount} snapshots for deletion", snapshotsToDelete.Count);

        bool shouldContinue = true;
        int batchSize = snapshotsToDelete.Count > 10 ? 25 : 1;
        int batch = 0;
        foreach (var snapshotBatch in snapshotsToDelete.Chunk(batchSize))
        {
            if (!shouldContinue)
            {
                _logger.LogInformation("Stopped deleted snapshots");
                break;
            }

            batch++;
            int snapshotCount = snapshotBatch.Count();
            string snapshotNames = String.Join(",", snapshotBatch.Select(s => s.Name));

            try
            {
                sw.Restart();
                await Run.WithRetriesAsync(async () =>
                {
                    _logger.LogInformation("Deleting {SnapshotCount} expired snapshot(s) from {Repo}: {SnapshotNames}", snapshotCount, repo, snapshotNames);

                    var response = await _client.Snapshot.DeleteAsync(repo, snapshotNames, r => r.RequestConfiguration(c => c.RequestTimeout(TimeSpan.FromMinutes(5))), ct: cancellationToken).AnyContext();
                    _logger.LogRequest(response);

                    if (response.IsValidResponse)
                    {
                        await OnSnapshotDeleted(snapshotNames, sw.Elapsed).AnyContext();
                    }
                    else
                    {
                        shouldContinue = await OnSnapshotDeleteFailure(snapshotNames, sw.Elapsed, response, null).AnyContext();
                        if (shouldContinue)
                            throw response.OriginalException ?? new ApplicationException($"Failed deleting snapshot(s) \"{snapshotNames}\"");
                    }
                }, 5, TimeSpan.Zero, _timeProvider, cancellationToken);
                sw.Stop();
            }
            catch (Exception ex)
            {
                sw.Stop();
                shouldContinue = await OnSnapshotDeleteFailure(snapshotNames, sw.Elapsed, null, ex).AnyContext();
            }
        }

        await OnCompleted().AnyContext();
    }

    public virtual Task OnSnapshotDeleted(string snapshotName, TimeSpan duration)
    {
        _logger.LogInformation("Completed delete snapshot(s) {SnapshotName} in {Duration:g}", snapshotName, duration);
        return Task.CompletedTask;
    }

    public virtual Task<bool> OnSnapshotDeleteFailure(string snapshotName, TimeSpan duration, DeleteSnapshotResponse response, Exception ex)
    {
        _logger.LogErrorRequest(ex, response, "Failed to delete snapshot(s) {SnapshotName} after {Duration:g}", snapshotName, duration);
        return Task.FromResult(true);
    }

    public virtual Task OnCompleted()
    {
        return Task.CompletedTask;
    }

    [DebuggerDisplay("{Name}")]
    private class RepositoryMaxAge
    {
        public string Name { get; set; }
        public TimeSpan MaxAge { get; set; }
    }

    [DebuggerDisplay("{Name} ({Date})")]
    private class SnapshotDate
    {
        public string Name { get; set; }
        public DateTime Date { get; set; }
    }
}

