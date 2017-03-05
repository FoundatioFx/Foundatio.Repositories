using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Elasticsearch.Jobs {
    public class MaintainIndexesJob : IJob {
        private readonly IElasticConfiguration _configuration;
        private readonly ILogger _logger;
        private readonly ILockProvider _lockProvider;

        public MaintainIndexesJob(IElasticConfiguration configuration, ILockProvider lockProvider, ILoggerFactory loggerFactory) {
            _configuration = configuration;
            _lockProvider = lockProvider;
            _logger = loggerFactory.CreateLogger(GetType());
        }

        public virtual async Task<JobResult> RunAsync(CancellationToken cancellationToken = default(CancellationToken)) {
            _logger.Info("Starting index maintenance...");

            var sw = Stopwatch.StartNew();
            try {
                bool success = await _lockProvider.TryUsingAsync("es-maintain-indexes", async t => {
                    _logger.Info("Got lock to maintain indexes");
                    await _configuration.MaintainIndexesAsync().AnyContext();
                    sw.Stop();

                    await OnCompleted(sw.Elapsed).AnyContext();
                }, TimeSpan.FromMinutes(30), cancellationToken).AnyContext();

                if (!success)
                    _logger.Info("Unable to acquire index maintenance lock.");

            } catch (Exception ex) {
                sw.Stop();
                await OnFailure(sw.Elapsed, ex).AnyContext();
                throw;
            }

            return JobResult.Success;
        }

        public virtual Task<bool> OnFailure(TimeSpan duration, Exception ex) {
            _logger.Error($"Failed to maintain indexes after {duration.ToWords(true)}: {ex?.Message}");
            return Task.FromResult(true);
        }

        public virtual Task OnCompleted(TimeSpan duration) {
            _logger.Info($"Finished index maintenance in {duration.ToWords(true)}.");
            return Task.CompletedTask;
        }
    }
}
