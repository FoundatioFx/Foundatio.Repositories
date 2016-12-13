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
using Foundatio.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Jobs {
    public class CleanupIndexesJob : IJob {
        private readonly IElasticClient _client;
        private readonly ILogger _logger;
        private readonly ILockProvider _lockProvider;
        private static readonly CultureInfo _enUS = new CultureInfo("en-US");
        private readonly ICollection<IndexMaxAge> _indexes = new List<IndexMaxAge>();

        public CleanupIndexesJob(IElasticClient client, ILockProvider lockProvider, ILoggerFactory loggerFactory) {
            _client = client;
            _lockProvider = lockProvider;
            _logger = loggerFactory.CreateLogger(GetType());
        }

        protected void AddIndex(TimeSpan maxAge, Func<string, DateTime?> getAge) {
            _indexes.Add(new IndexMaxAge(maxAge, getAge));
        }

        protected void AddIndex(string prefix, TimeSpan maxAge) {
            _indexes.Add(new IndexMaxAge(maxAge, idx => {
                DateTime result;
                if (DateTime.TryParseExact(idx, "'" + prefix + "-'yyyy.MM.dd", _enUS, DateTimeStyles.None, out result))
                    return result;

                return null;
            }));
        }

        public virtual async Task<JobResult> RunAsync(CancellationToken cancellationToken = default(CancellationToken)) {
            _logger.Info("Starting index cleanup...");

            await DeleteOldIndexesAsync(cancellationToken).AnyContext();

            _logger.Info("Finished index cleanup.");

            return JobResult.Success;
        }

        private async Task DeleteOldIndexesAsync(CancellationToken cancellationToken) {
            var sw = Stopwatch.StartNew();
            var result = await _client.CatIndicesAsync(
                d => d.RequestConfiguration(r => r.RequestTimeout(5 * 60 * 1000))).AnyContext();

            sw.Stop();
            var indexes = new List<IndexDate>();
            if (result.IsValid && result.Records != null)
                indexes = result.Records?.Select(r => GetIndexDate(r.Index)).ToList();

            if (result.IsValid)
                _logger.Info($"Retrieved list of {indexes.Count} indexes in {sw.Elapsed.ToWords(true)}");
            else
                _logger.Error($"Failed to retrieve list of indexes: {result.GetErrorMessage()}");

            if (indexes.Count == 0)
                return;

            DateTime now = SystemClock.UtcNow;
            var indexesToDelete = indexes.Where(r => r.Date < now.Subtract(r.MaxAge)).ToList();

            if (indexesToDelete.Count == 0)
                return;

            // log that we are seeing indexes that should have been deleted already
            var oldIndexes = indexes.Where(s => s.Date < now.Subtract(s.MaxAge).AddDays(-1)).ToList();
            if (oldIndexes.Count > 0)
                _logger.Error($"Found old indexes that should've been deleted: {String.Join(", ", oldIndexes)}");

            _logger.Info($"Selected {indexesToDelete.Count} indexes for deletion");

            bool shouldContinue = true;
            foreach (var oldIndex in indexesToDelete) {
                if (!shouldContinue) {
                    _logger.Info("Stopped deleted snapshots.");
                    break;
                }

                _logger.Info($"Acquiring lock to delete index {oldIndex.Index}");
                try {
                    await _lockProvider.TryUsingAsync("es-delete-index", async t => {
                        _logger.Info($"Got lock to delete index {oldIndex.Index}");
                        sw.Restart();
                        var response = await _client.DeleteIndexAsync(d => d.Index(oldIndex.Index)).AnyContext();
                        sw.Stop();

                        if (response.IsValid)
                            await OnIndexDeleted(oldIndex.Index, sw.Elapsed).AnyContext();
                        else
                            shouldContinue = await OnIndexDeleteFailure(oldIndex.Index, sw.Elapsed, response, null).AnyContext();
                    }, TimeSpan.FromMinutes(30), cancellationToken).AnyContext();
                } catch (Exception ex) {
                    sw.Stop();
                    shouldContinue = await OnIndexDeleteFailure(oldIndex.Index, sw.Elapsed, null, ex).AnyContext();
                }
            }

            await OnCompleted().AnyContext();
        }

        public virtual Task OnIndexDeleted(string indexName, TimeSpan duration) {
            _logger.Info($"Completed delete index {indexName} in {duration.ToWords(true)}");
            return Task.CompletedTask;
        }

        public virtual Task<bool> OnIndexDeleteFailure(string indexName, TimeSpan duration, IIndicesResponse response, Exception ex) {
            _logger.Error($"Failed to delete index {indexName} after {duration.ToWords(true)}: {(response != null ? response.GetErrorMessage() : ex?.Message)}");
            return Task.FromResult(true);
        }

        public virtual Task OnCompleted() {
            return Task.CompletedTask;
        }

        private IndexDate GetIndexDate(string name) {
            if (_indexes.Count == 0) {
                AddIndex("logstash", TimeSpan.FromDays(7));
                AddIndex(".marvel", TimeSpan.FromDays(7));
            }

            foreach (var index in _indexes) {
                var date = index.GetDate(name);
                if (date == null)
                    continue;

                return new IndexDate { Index = name, Date = date.Value, MaxAge = index.MaxAge };
            }

            return null;
        }

        private class IndexMaxAge {
            public IndexMaxAge(TimeSpan maxAge, Func<string, DateTime?> getAge) {
                MaxAge = maxAge;
                GetDate = getAge;
            }

            public TimeSpan MaxAge { get; }
            public Func<string, DateTime?> GetDate { get; }
        }

        private class IndexDate {
            public string Index { get; set; }
            public DateTime Date { get; set; }
            public TimeSpan MaxAge { get; set; }
        }
    }
}
