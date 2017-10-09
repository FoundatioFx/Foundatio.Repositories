using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Jobs {
    public class MaintainIndexesJob : IJob {
        private readonly IElasticConfiguration _configuration;
        private readonly ILogger _logger;
        private readonly ILockProvider _lockProvider;

        public MaintainIndexesJob(IElasticConfiguration configuration, ILockProvider lockProvider, ILoggerFactory loggerFactory) {
            _configuration = configuration;
            _lockProvider = lockProvider;
            _logger = loggerFactory?.CreateLogger(GetType()) ?? NullLogger.Instance;
        }

        public virtual async Task<JobResult> RunAsync(CancellationToken cancellationToken = default) {
            _logger.LogInformation("Starting index maintenance...");

            var sw = Stopwatch.StartNew();
            try {
                bool success = await _lockProvider.TryUsingAsync("es-maintain-indexes", async t => {
                    _logger.LogInformation("Got lock to maintain indexes");
                    await _configuration.MaintainIndexesAsync().AnyContext();
                    sw.Stop();

                    await OnCompleted(sw.Elapsed).AnyContext();
                }, TimeSpan.FromMinutes(30), cancellationToken).AnyContext();

                if (!success)
                    _logger.LogInformation("Unable to acquire index maintenance lock.");

            } catch (Exception ex) {
                sw.Stop();
                await OnFailure(sw.Elapsed, ex).AnyContext();
                throw;
            }

            return JobResult.Success;
        }

        public virtual Task<bool> OnFailure(TimeSpan duration, Exception ex) {
            _logger.LogError(ex, "Failed to maintain indexes after {Duration:g}: {ErrorMessage}", duration, ex?.Message);
            return Task.FromResult(true);
        }

        public virtual Task OnCompleted(TimeSpan duration) {
            _logger.LogInformation("Finished index maintenance in {Duration:g}.", duration);
            return Task.CompletedTask;
        }
    }
}
