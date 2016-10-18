using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Jobs {
    public class SnapshotJob : IJob {
        private readonly IElasticClient _client;
        private readonly ILockProvider _lockProvider;
        private readonly ILogger _logger;

        public SnapshotJob(IElasticClient client, ILockProvider lockProvider, ILoggerFactory loggerFactory) {
            _client = client;
            _lockProvider = lockProvider;
            _logger = loggerFactory.CreateLogger(GetType());
        }

        public async Task<JobResult> RunAsync(CancellationToken cancellationToken = default(CancellationToken)) {
            string snapshotName = SystemClock.UtcNow.ToString("'" + Name + "-'yyyy-MM-dd-HH-mm");
            _logger.Info($"Starting {Name} snapshot {snapshotName}...");

            await _lockProvider.TryUsingAsync("es-snapshot", async t => {
                var sw = Stopwatch.StartNew();
                var result = await Run.WithRetriesAsync(async () => {
                    var response = await _client.SnapshotAsync(
                        Repository,
                        snapshotName,
                        d => d
                            .Indices(IncludedIndexes.Count > 0 ? IncludedIndexes.ToArray() : new[] { "*" })
                            .IgnoreUnavailable()
                            .IncludeGlobalState(false)
                            .WaitForCompletion()).AnyContext();

                    if (!response.IsValid)
                        throw new ApplicationException($"Snapshot failed: {response.GetErrorMessage()}", response.ConnectionStatus.OriginalException);

                    return response;
                },
                    maxAttempts: 5,
                    retryInterval: TimeSpan.FromSeconds(10),
                    cancellationToken: cancellationToken,
                    logger: _logger).AnyContext();
                sw.Stop();

                if (result.IsValid)
                    _logger.Info($"Completed {Name} snapshot {snapshotName} in {sw.Elapsed.ToWords(true)}");
                else
                    _logger.Error($"Failed {Name} snapshot {snapshotName}: {result.GetErrorMessage()}");
            }, TimeSpan.FromMinutes(30), TimeSpan.FromMinutes(30)).AnyContext();

            return JobResult.Success;
        }

        protected string Name { get; set; } = "data";
        protected string Repository { get; set; } = "data";
        protected ICollection<string> IncludedIndexes { get; } = new List<string>();
    }
}
