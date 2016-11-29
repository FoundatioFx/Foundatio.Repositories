using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Jobs;
using Foundatio.Logging;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Jobs {
    public class CleanupIndexesJob : IJob {
        private readonly IElasticClient _client;
        private readonly ILogger _logger;
        private static readonly CultureInfo _enUS = new CultureInfo("en-US");
        private readonly ICollection<IndexMaxAge> _indexes = new List<IndexMaxAge>();

        public CleanupIndexesJob(IElasticClient client, ILoggerFactory loggerFactory) {
            _client = client;
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

        public async Task<JobResult> RunAsync(CancellationToken cancellationToken = default(CancellationToken)) {
            _logger.Info("Starting index cleanup...");

            await DeleteOldIndexesAsync().AnyContext();

            _logger.Info("Finished index cleanup.");

            return JobResult.Success;
        }

        private async Task DeleteOldIndexesAsync() {
            var sw = Stopwatch.StartNew();
            var result = await _client.CatIndicesAsync(
                d => d.RequestConfiguration(r => r.RequestTimeout(TimeSpan.FromMinutes(5)))).AnyContext();

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

            foreach (var oldIndex in indexesToDelete)
                await _client.DeleteIndexAsync(oldIndex.Index, d => d).AnyContext();
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
