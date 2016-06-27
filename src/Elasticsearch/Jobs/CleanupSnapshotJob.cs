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
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Extensions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Jobs {
    public class CleanupSnapshotJob : IJob {
        private readonly IElasticClient _client;
        private readonly ILockProvider _lockProvider;
        private readonly ILogger _logger;
        private static readonly CultureInfo _enUS = new CultureInfo("en-US");

        public CleanupSnapshotJob(IElasticClient client, ILockProvider lockProvider, ILoggerFactory loggerFactory) {
            _client = client;
            _lockProvider = lockProvider;
            _logger = loggerFactory.CreateLogger(GetType());
        }

        protected ICollection<RepositoryMaxAge> Repositories { get; } = new List<RepositoryMaxAge>();

        protected class RepositoryMaxAge {
            public string Name { get; set; }
            public TimeSpan MaxAge { get; set; }
        }

        public async Task<JobResult> RunAsync(CancellationToken cancellationToken = default(CancellationToken)) {
            _logger.Info("Starting snapshot cleanup...");

            foreach (var repo in Repositories.Count > 0 ? Repositories : new[] { new RepositoryMaxAge { Name = "data", MaxAge = TimeSpan.FromDays(3) } })
                await DeleteOldSnapshotsAsync(repo.Name, repo.MaxAge).AnyContext();

            _logger.Info("Finished snapshot cleanup.");

            return JobResult.Success;
        }

        private async Task DeleteOldSnapshotsAsync(string repo, TimeSpan maxAge) {
            var sw = Stopwatch.StartNew();
            var result = await _client.GetSnapshotAsync(
                repo,
                "_all",
                d => d.RequestConfiguration(r =>
                    r.RequestTimeout(5 * 60 * 1000))).AnyContext();

            sw.Stop();
            var snapshots = result.Snapshots.Select(s => new { Date = GetSnapshotDate(repo, s.Name), s.Name }).ToList();

            if (result.IsValid)
                _logger.Info($"Retrieved list of {snapshots.Count} snapshots from {repo} in {sw.Elapsed.ToWords(true)}");
            else
                _logger.Error($"Failed to retrieve list of snapshots from {repo}: {result.GetErrorMessage()}");

            DateTime now = DateTime.UtcNow;
            var snapshotsToDelete = snapshots.Where(s => s.Date < now.Subtract(maxAge)).ToList();
            _logger.Info($"Selected {snapshotsToDelete.Count} snapshots for deletion from {repo}");
            foreach (var snapshot in snapshotsToDelete) {
                _logger.Info($"Acquiring snapshot lock to delete {snapshot.Name} from {repo}");
                await _lockProvider.TryUsingAsync("es-snapshot", async t => {
                    _logger.Info($"Got snapshot lock to delete {snapshot.Name} from {repo}");
                    sw.Restart();
                    var deleteResult = await _client.DeleteSnapshotAsync(repo, snapshot.Name).AnyContext();
                    sw.Stop();
                    if (deleteResult.IsValid)
                        _logger.Info($"Deleted snapshot {snapshot.Name} of age {now.Subtract(snapshot.Date).ToWords(true)} from {repo} in {sw.Elapsed.ToWords(true)}");
                    else
                        _logger.Error($"Failed to delete snapshot {snapshot.Name} from {repo}: {deleteResult.GetErrorMessage()}");
                }, TimeSpan.FromMinutes(30), TimeSpan.FromMinutes(30)).AnyContext();
            }
        }

        private DateTime GetSnapshotDate(string repo, string name) {
            DateTime result;
            if (DateTime.TryParseExact(name, "'" + repo + "-'yyyy-MM-dd-HH-mm", _enUS, DateTimeStyles.None, out result))
                return result;

            return DateTime.MaxValue;
        }
    }
}
