using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Nest;
using Exceptionless.DateTimeExtensions;
using Foundatio.Utility;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class DailyIndexType<T> : TimeSeriesIndexType<T> where T : class {
        public DailyIndexType(IIndex index, string name = null, Func<T, DateTime> getDocumentDateUtc = null) : base(index, name, getDocumentDateUtc) { }
    }

    public class DailyIndex : IndexBase, ITimeSeriesIndex {
        protected static readonly CultureInfo EnUs = new CultureInfo("en-US");
        private readonly List<IndexAliasAge> _aliases = new List<IndexAliasAge>();
        private readonly Lazy<IReadOnlyCollection<IndexAliasAge>> _frozenAliases;
        protected readonly string _currentVersionedNamePrefix;

        public DailyIndex(IElasticClient client, string name, int version = 1, ILoggerFactory loggerFactory = null): base(client, name, loggerFactory) {
            Version = version;
            AddAlias(name);
            _currentVersionedNamePrefix = String.Concat(Name, "-v", version);
            _frozenAliases = new Lazy<IReadOnlyCollection<IndexAliasAge>>(() => _aliases.AsReadOnly());
        }

        public int Version { get; }
        public TimeSpan? MaxIndexAge { get; } = null;
        public IReadOnlyCollection<IndexAliasAge> Aliases => _frozenAliases.Value;

        public string GetVersionedNamePrefix(int? version = null) {
            return version == null ? _currentVersionedNamePrefix : String.Concat(Name, "-v", version);
        }

        public void AddAlias(string name, TimeSpan? maxAge = null) {
            _aliases.Add(new IndexAliasAge { Name = name, MaxAge = maxAge ?? TimeSpan.MaxValue });
        }

        public override void Configure() {
            var response = _client.PutTemplate(Name, ConfigureTemplate);
            _logger.Trace(() => response.GetRequest());

            if (!response.IsValid)
                throw new ApplicationException("An error occurred creating the template: " + response.ServerError.Error, response.ConnectionStatus.OriginalException);
        }

        public virtual PutTemplateDescriptor ConfigureTemplate(PutTemplateDescriptor template) {
            template.Template(GetVersionedNamePrefix() + "-*");
            foreach (var alias in Aliases)
                template.AddAlias(alias.Name);

            // TODO: What should happen if there are types that don't implement ITemplatedIndexType?
            foreach (var type in IndexTypes.OfType<ITemplatedIndexType>())
                type.ConfigureTemplate(template);

            return template;
        }
        
        public virtual string GetIndex(DateTime utcDate) {
            return $"{_currentVersionedNamePrefix}-{utcDate:yyyy.MM.dd}";
        }

        public virtual string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd) {
            if (!utcStart.HasValue)
                utcStart = SystemClock.UtcNow;

            if (!utcEnd.HasValue || utcEnd.Value < utcStart)
                utcEnd = SystemClock.UtcNow;

            var utcEndOfDay = utcEnd.Value.EndOfDay();

            var indices = new List<string>();
            for (DateTime current = utcStart.Value; current <= utcEndOfDay; current = current.AddDays(1))
                indices.Add(GetIndex(current));

            return indices.ToArray();
        }

        public override void Delete() {
            // delete all indexes by prefix
            var response = _client.DeleteIndex(GetVersionedNamePrefix() + "-*");
            _logger.Trace(() => response.GetRequest());
            if (!response.IsValid)
                throw new ApplicationException("An error occurred deleting the indexes: " + response.ServerError?.Error, response.ConnectionStatus.OriginalException);

            // delete the template
            IIndicesOperationResponse deleteResponse = null;

            if (_client.TemplateExists(Name).Exists)
                deleteResponse = _client.DeleteTemplate(Name);

            _logger.Trace(() => deleteResponse.GetRequest());
            if (deleteResponse != null && !deleteResponse.IsValid)
                throw new ApplicationException("An error occurred deleting the index template: " + response.ServerError.Error, response.ConnectionStatus.OriginalException);
        }

        public override Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null) {
            return Task.CompletedTask;
            // TODO: Get all versioned indexes and reindex them starting with newest 1st
        }

        public virtual void Maintain() {
            RemoveOldIndexesFromAliases();
            DeleteOldIndexes();
        }

        protected IList<IndexInfo> GetIndexList(int? version = null) {
            if (version == null)
                version = Version;

            var sw = Stopwatch.StartNew();
            var result = _client.CatIndices(
                d => d.RequestConfiguration(r =>
                    r.RequestTimeout(5 * 60 * 1000)));

            _logger.Trace(() => result.GetRequest());
            sw.Stop();
            var indices = result.Records.Where(i => i.Index.StartsWith(GetVersionedNamePrefix(version))).Select(r => new IndexInfo { DateUtc = GetIndexDate(r.Index), Index = r.Index }).ToList();

            if (result.IsValid)
                _logger.Info($"Retrieved list of {indices.Count} indexes in {sw.Elapsed.ToWords(true)}");
            else
                _logger.Error(result.ConnectionStatus.OriginalException, $"Failed to retrieve list of indexes: {result.GetErrorMessage()}");

            return indices;
        }

        protected void RemoveOldIndexesFromAliases() {
            if (!MaxIndexAge.HasValue)
                return;

            var indexes = GetIndexList();

            DateTime now = SystemClock.UtcNow;
            foreach (var alias in Aliases) {
                var oldIndexes = indexes.Where(i => i.DateUtc < now.Subtract(alias.MaxAge)).ToList();
                if (oldIndexes.Count == 0)
                    continue;

                var a = new AliasDescriptor();
                foreach (var index in oldIndexes)
                    a.Remove(r => r.Index(index.Index).Alias(alias.Name));

                var result = _client.Alias(a);
                _logger.Trace(() => result.GetRequest());
                if (result.IsValid)
                    _logger.Info($"Removed indexes ({String.Join(",", oldIndexes)}) from alias {alias.Name}");
                else
                    _logger.Error(result.ConnectionStatus.OriginalException, $"Failed to remove indexes ({String.Join(",", oldIndexes)}) from alias {alias.Name}");
            }
        }

        protected void DeleteOldIndexes() {
            if (!MaxIndexAge.HasValue)
                return;

            var sw = Stopwatch.StartNew();
            var indexes = GetIndexList();

            DateTime now = SystemClock.UtcNow;
            foreach (var index in indexes.Where(s => s.DateUtc < now.Subtract(MaxIndexAge.Value))) {
                sw.Restart();
                var deleteResult = _client.DeleteIndex(index.Index, d => d);
                _logger.Trace(() => deleteResult.GetRequest());
                sw.Stop();
                if (deleteResult.IsValid)
                    _logger.Info($"Deleted index {index.Index} of age {now.Subtract(index.DateUtc).ToWords(true)} in {sw.Elapsed.ToWords(true)}");
                else
                    _logger.Error(deleteResult.ConnectionStatus.OriginalException, $"Failed to delete index {index.Index}: {deleteResult.GetErrorMessage()}");
            }
        }

        protected virtual DateTime GetIndexDate(string name) {
            DateTime result;
            if (DateTime.TryParseExact(name, "'" + _currentVersionedNamePrefix + "-'yyyy.MM.dd", EnUs, DateTimeStyles.None, out result))
                return result;

            return DateTime.MaxValue;
        }

        public class IndexAliasAge {
            public string Name { get; set; }
            public TimeSpan MaxAge { get; set; }
        }

        public class IndexInfo {
            public string Index { get; set; }
            public DateTime DateUtc { get; set; }
        }
    }
}