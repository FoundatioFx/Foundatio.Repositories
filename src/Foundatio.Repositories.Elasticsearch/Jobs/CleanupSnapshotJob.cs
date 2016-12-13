﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Logging;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Jobs {
    public class CleanupSnapshotJob : IJob {
        private readonly IElasticClient _client;
        private readonly ILockProvider _lockProvider;
        private readonly ILogger _logger;
        private static readonly CultureInfo _enUS = new CultureInfo("en-US");
        private readonly ICollection<RepositoryMaxAge> _repositories = new List<RepositoryMaxAge>();

        public CleanupSnapshotJob(IElasticClient client, ILockProvider lockProvider, ILoggerFactory loggerFactory) {
            _client = client;
            _lockProvider = lockProvider;
            _logger = loggerFactory.CreateLogger(GetType());
        }

        protected void AddRepository(string name, TimeSpan maxAge) {
            _repositories.Add(new RepositoryMaxAge { Name = name, MaxAge = maxAge });
        }

        public async Task<JobResult> RunAsync(CancellationToken cancellationToken = default(CancellationToken)) {
            _logger.Info("Starting snapshot cleanup...");
            if (_repositories.Count == 0)
                _repositories.Add(new RepositoryMaxAge { Name = "data", MaxAge = TimeSpan.FromDays(3) });

            foreach (var repo in _repositories)
                await DeleteOldSnapshotsAsync(repo.Name, repo.MaxAge, cancellationToken).AnyContext();

            _logger.Info("Finished snapshot cleanup.");

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
                _logger.Info($"Retrieved list of {snapshots.Count} snapshots from {repo} in {sw.Elapsed.ToWords(true)}");
            else
                _logger.Error($"Failed to retrieve list of snapshots from {repo}: {result.GetErrorMessage()}");

            if (snapshots.Count == 0)
                return;

            DateTime now = SystemClock.UtcNow;
            var snapshotsToDelete = snapshots.Where(r => r.Date < now.Subtract(maxAge)).ToList();

            if (snapshotsToDelete.Count == 0)
                return;

            // log that we are seeing snapshots that should have been deleted already
            var oldSnapshots = snapshots.Where(s => s.Date < now.Subtract(maxAge).AddDays(-1)).ToList();
            if (oldSnapshots.Count > 0)
                _logger.Error($"Found old snapshots that should've been deleted: {String.Join(", ", oldSnapshots)}");

            _logger.Info($"Selected {snapshotsToDelete.Count} snapshots for deletion");

            bool shouldContinue = true;
            foreach (var snapshot in snapshotsToDelete) {
                if (!shouldContinue) {
                    _logger.Info("Stopped deleted snapshots.");
                    break;
                }

                _logger.Info($"Acquiring snapshot lock to delete {snapshot.Name} from {repo}");
                try {
                    await _lockProvider.TryUsingAsync("es-snapshot", async t => {
                        _logger.Info($"Got snapshot lock to delete {snapshot.Name} from {repo}");
                        sw.Restart();
                        var response = await _client.DeleteSnapshotAsync(repo, snapshot.Name, cancellationToken: t).AnyContext();
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
            _logger.Info($"Completed delete snapshot {snapshotName} in {duration.ToWords(true)}");
            return Task.CompletedTask;
        }

        public virtual Task<bool> OnSnapshotDeleteFailure(string snapshotName, TimeSpan duration, IDeleteSnapshotResponse response, Exception ex) {
            _logger.Error($"Failed to delete snapshot {snapshotName} after {duration.ToWords(true)}: {(response != null ? response.GetErrorMessage() : ex?.Message)}");
            return Task.FromResult(true);
        }

        public virtual Task OnCompleted() {
            return Task.CompletedTask;
        }

        private DateTime GetSnapshotDate(string repo, string name) {
            DateTime result;
            if (DateTime.TryParseExact(name, "'" + repo + "-'yyyy-MM-dd-HH-mm", _enUS, DateTimeStyles.None, out result))
                return result;

            return DateTime.MaxValue;
        }

        private class RepositoryMaxAge {
            public string Name { get; set; }
            public TimeSpan MaxAge { get; set; }
        }

        private class SnapshotDate {
            public string Name { get; set; }
            public DateTime Date { get; set; }
        }
    }
}

