using System;
using System.Collections.Generic;
using System.Diagnostics;
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
    public class SnapshotJob : IJob {
        protected readonly IElasticClient _client;
        protected readonly ILockProvider _lockProvider;
        protected readonly ILogger _logger;

        public SnapshotJob(IElasticClient client, ILockProvider lockProvider, ILoggerFactory loggerFactory) {
            _client = client;
            _lockProvider = lockProvider;
            _logger = loggerFactory?.CreateLogger(GetType()) ?? NullLogger.Instance;
        }

        public virtual async Task<JobResult>
            RunAsync(CancellationToken cancellationToken = default) {
            string snapshotName = SystemClock.UtcNow.ToString("'" + Repository + "-'yyyy-MM-dd-HH-mm");
            _logger.LogInformation("Starting {Repository} snapshot {SnapshotName}...", Repository, snapshotName);

            await _lockProvider.TryUsingAsync("es-snapshot", async t => {
                var sw = Stopwatch.StartNew();
                var result = await Run.WithRetriesAsync(async () => {
                        var response = await _client.SnapshotAsync(
                            Repository,
                            snapshotName,
                            d => d
                                .Indices(IncludedIndexes.Count > 0 ? String.Join(",", IncludedIndexes) : "*")
                                .IgnoreUnavailable()
                                .IncludeGlobalState(false)
                                .WaitForCompletion(false)
                            , cancellationToken).AnyContext();

                        // 400 means the snapshot already exists
                        if (!response.IsValid && response.ApiCall.HttpStatusCode != 400)
                            throw new ApplicationException($"Snapshot failed: {response.GetErrorMessage()}", response.OriginalException);

                        return response;
                    },
                    maxAttempts: 5,
                    retryInterval: TimeSpan.FromSeconds(10),
                    cancellationToken: cancellationToken,
                    logger: _logger).AnyContext();

                _logger.LogTrace("Started snapshot {SnapshotName} in {Repository}: httpstatus={StatusCode}", snapshotName, Repository, result.ApiCall?.HttpStatusCode);

                bool success = false;
                do {
                    await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken).AnyContext();

                    var status = await _client.SnapshotStatusAsync(s => s.Snapshot(snapshotName).RepositoryName(Repository), cancellationToken).AnyContext();
                    if (status.IsValid && status.Snapshots.Count > 0) {
                        string state = status.Snapshots.First().State;
                        if (state.Equals("SUCCESS", StringComparison.OrdinalIgnoreCase)) {
                            success = true;
                            break;
                        }

                        if (state.Equals("FAILED", StringComparison.OrdinalIgnoreCase) || state.Equals("ABORTED", StringComparison.OrdinalIgnoreCase) || state.Equals("MISSING", StringComparison.OrdinalIgnoreCase))
                            break;
                    }

                    // max time to wait for a snapshot to complete
                    if (sw.Elapsed > TimeSpan.FromHours(1)) {
                        _logger.LogError("Timed out waiting for snapshot {SnapshotName} in {Repository}.", snapshotName, Repository);
                        break;
                    }
                } while (!cancellationToken.IsCancellationRequested);
                sw.Stop();

                if (success)
                    await OnSuccess(snapshotName, sw.Elapsed).AnyContext();
                else
                    await OnFailure(snapshotName, result).AnyContext();
            }, TimeSpan.FromMinutes(30), TimeSpan.FromMinutes(30)).AnyContext();

            return JobResult.Success;
        }

        public virtual Task OnSuccess(string snapshotName, TimeSpan duration) {
            _logger.LogInformation("Completed snapshot \"{SnapshotName}\" in \"{Repository}\" in {Duration:g}", snapshotName, Repository, duration);
            return Task.CompletedTask;
        }

        public virtual Task OnFailure(string snapshotName, ISnapshotResponse response) {
            _logger.LogError("Failed snapshot {SnapshotName} in {Repository}: {ErrorMessage}", snapshotName, Repository, response.GetErrorMessage());
            return Task.CompletedTask;
        }

        protected string Repository { get; set; } = "data";
        protected ICollection<string> IncludedIndexes { get; } = new List<string>();
    }
}
