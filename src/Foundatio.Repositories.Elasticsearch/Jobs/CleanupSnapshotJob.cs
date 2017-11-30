using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Jobs {
    public class CleanupSnapshotJob : IJob {
        protected readonly IElasticClient _client;
        protected readonly ILockProvider _lockProvider;
        protected readonly ILogger _logger;
        private static readonly CultureInfo _enUS = new CultureInfo("en-US");
        private readonly ICollection<RepositoryMaxAge> _repositories = new List<RepositoryMaxAge>();

        public CleanupSnapshotJob(IElasticClient client, ILockProvider lockProvider, ILoggerFactory loggerFactory) {
            _client = client;
            _lockProvider = lockProvider;
            _logger = loggerFactory?.CreateLogger(GetType()) ?? NullLogger.Instance;
        }

        protected void AddRepository(string name, TimeSpan maxAge) {
            _repositories.Add(new RepositoryMaxAge { Name = name, MaxAge = maxAge });
        }

        public virtual async Task<JobResult> RunAsync(CancellationToken cancellationToken = default) {
            _logger.LogInformation("Starting snapshot cleanup...");
            if (_repositories.Count == 0)
                _repositories.Add(new RepositoryMaxAge { Name = "data", MaxAge = TimeSpan.FromDays(3) });

            foreach (var repo in _repositories)
                await DeleteOldSnapshotsAsync(repo.Name, repo.MaxAge, cancellationToken).AnyContext();

            _logger.LogInformation("Finished snapshot cleanup.");
            return JobResult.Success;
        }

        private async Task DeleteOldSnapshotsAsync(string repo, TimeSpan maxAge, CancellationToken cancellationToken) {
            var sw = Stopwatch.StartNew();
            var result = await _client.GetSnapshotAsync(
                repo,
                "_all",
                d => d.RequestConfiguration(r =>
                    r.RequestTimeout(TimeSpan.FromMinutes(5))), cancellationToken).AnyContext();

            sw.Stop();
            var snapshots = new List<SnapshotDate>();
            if (result.IsValid && result.Snapshots != null)
                snapshots = result.Snapshots?.Select(r => new SnapshotDate { Name = r.Name, Date = GetSnapshotDate(repo, r.Name) }).ToList();

            if (result.IsValid)
                _logger.LogInformation("Retrieved list of {SnapshotCount} snapshots from {Repo} in {Duration}", snapshots.Count, repo, sw.Elapsed);
            else
                _logger.LogError($"Failed to retrieve list of snapshots from {repo}: {result.GetErrorMessage()}");

            if (snapshots.Count == 0)
                return;

            var oldestValidSnapshot = SystemClock.UtcNow.Subtract(maxAge);
            var snapshotsToDelete = snapshots.Where(r => r.Date.IsBefore(oldestValidSnapshot)).ToList();
            if (snapshotsToDelete.Count == 0)
                return;

            // log that we are seeing snapshots that should have been deleted already
            var oldSnapshots = snapshots.Where(s => s.Date < oldestValidSnapshot.AddDays(-1)).ToList();
            if (oldSnapshots.Count > 0)
                _logger.LogError($"Found old snapshots that should've been deleted: {String.Join(", ", oldSnapshots)}");

            _logger.LogInformation("Selected {SnapshotCount} snapshots for deletion", snapshotsToDelete.Count);

            bool shouldContinue = true;
            foreach (var snapshot in snapshotsToDelete) {
                if (!shouldContinue) {
                    _logger.LogInformation("Stopped deleted snapshots.");
                    break;
                }

                _logger.LogInformation("Acquiring snapshot lock to delete {SnapshotName} from {Repo}", snapshot.Name, repo);
                try {
                    await _lockProvider.TryUsingAsync("es-snapshot", async t => {
                        _logger.LogInformation("Got snapshot lock to delete {SnapshotName} from {Repo}", snapshot.Name, repo);
                        sw.Restart();
                        var response = await _client.DeleteSnapshotAsync(repo, snapshot.Name, r => r.RequestConfiguration(c => c.RequestTimeout(TimeSpan.FromMinutes(15))), cancellationToken: t).AnyContext();
                        sw.Stop();

                        if (response.IsValid)
                            await OnSnapshotDeleted(snapshot.Name, sw.Elapsed).AnyContext();
                        else
                            shouldContinue = await OnSnapshotDeleteFailure(snapshot.Name, sw.Elapsed, response, null).AnyContext();
                    }, TimeSpan.FromMinutes(30), cancellationToken).AnyContext();
                } catch (Exception ex) {
                    sw.Stop();
                    shouldContinue = await OnSnapshotDeleteFailure(snapshot.Name, sw.Elapsed, null, ex).AnyContext();
                }
            }

            await OnCompleted().AnyContext();
        }

        public virtual Task OnSnapshotDeleted(string snapshotName, TimeSpan duration) {
            _logger.LogInformation("Completed delete snapshot {SnapshotName} in {Duration}", snapshotName, duration);
            return Task.CompletedTask;
        }

        public virtual Task<bool> OnSnapshotDeleteFailure(string snapshotName, TimeSpan duration, IDeleteSnapshotResponse response, Exception ex) {
            _logger.LogError(ex, $"Failed to delete snapshot {snapshotName} after {duration.ToWords(true)}: {(response != null ? response.GetErrorMessage() : ex?.Message)}");
            return Task.FromResult(true);
        }

        public virtual Task OnCompleted() {
            return Task.CompletedTask;
        }

        private DateTime GetSnapshotDate(string repo, string name) {
            if (DateTime.TryParseExact(name, "'" + repo + "-'yyyy-MM-dd-HH-mm", _enUS, DateTimeStyles.None, out DateTime result))
                return result;

            return DateTime.MaxValue;
        }

        [DebuggerDisplay("{Name}")]
        private class RepositoryMaxAge {
            public string Name { get; set; }
            public TimeSpan MaxAge { get; set; }
        }

        [DebuggerDisplay("{Name} ({Date})")]
        private class SnapshotDate {
            public string Name { get; set; }
            public DateTime Date { get; set; }
        }
    }
}

