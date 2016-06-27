using System;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Jobs;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Extensions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Jobs {
    public class CleanupIndexesJob : IJob {
        private readonly IElasticClient _client;
        private readonly ILogger _logger;
        private static readonly CultureInfo _enUS = new CultureInfo("en-US");

        public CleanupIndexesJob(IElasticClient client, ILoggerFactory loggerFactory) {
            _client = client;
            _logger = loggerFactory.CreateLogger(GetType());
        }

        public async Task<JobResult> RunAsync(CancellationToken cancellationToken = default(CancellationToken)) {
            _logger.Info("Starting index cleanup...");

            await DeleteOldIndexesAsync(TimeSpan.FromDays(3)).AnyContext();

            _logger.Info("Finished index cleanup.");

            return JobResult.Success;
        }

        private async Task DeleteOldIndexesAsync(TimeSpan maxAge) {
            var sw = Stopwatch.StartNew();
            var result = await _client.CatIndicesAsync(
                d => d.RequestConfiguration(r =>
                    r.RequestTimeout(5 * 60 * 1000))).AnyContext();

            sw.Stop();
            var indices = result.Records.Select(r => new { Date = GetIndexDate(r.Index), r.Index }).ToList();

            if (result.IsValid)
                _logger.Info($"Retrieved list of {indices.Count} indexes in {sw.Elapsed.ToWords(true)}");
            else
                _logger.Error($"Failed to retrieve list of indexes: {result.GetErrorMessage()}");

            DateTime now = DateTime.UtcNow;
            foreach (var index in indices.Where(s => s.Date < now.Subtract(maxAge))) {
                sw.Restart();
                var deleteResult = await _client.DeleteIndexAsync(index.Index, d => d).AnyContext();
                sw.Stop();
                if (deleteResult.IsValid)
                    _logger.Info($"Deleted index {index.Index} of age {now.Subtract(index.Date).ToWords(true)} in {sw.Elapsed.ToWords(true)}");
                else
                    _logger.Error($"Failed to delete index {index.Index}: {deleteResult.GetErrorMessage()}");
            }
        }

        private DateTime GetIndexDate(string name) {
            DateTime result;
            if (DateTime.TryParseExact(name, "'logstash-'yyyy.MM.dd", _enUS, DateTimeStyles.None, out result))
                return result;

            if (DateTime.TryParseExact(name, "'.marvel-'yyyy.MM.dd", _enUS, DateTimeStyles.None, out result))
                return result;
            
            return DateTime.MaxValue;
        }
    }
}
