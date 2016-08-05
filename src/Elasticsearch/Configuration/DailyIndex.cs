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
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Utility;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class DailyIndexType<T> : TimeSeriesIndexType<T> where T : class {
        public DailyIndexType(IIndex index, string name = null, Func<T, DateTime> getDocumentDateUtc = null) : base(index, name, getDocumentDateUtc) {}
    }

    public class DailyIndex : VersionedIndex, ITimeSeriesIndex {
        protected static readonly CultureInfo EnUs = new CultureInfo("en-US");
        private readonly List<IndexAliasAge> _aliases = new List<IndexAliasAge>();
        private readonly Lazy<IReadOnlyCollection<IndexAliasAge>> _frozenAliases;

        public DailyIndex(IElasticClient client, string name, int version = 1, ILoggerFactory loggerFactory = null) : base(client, name, version, loggerFactory) {
            AddAlias(name);
            _frozenAliases = new Lazy<IReadOnlyCollection<IndexAliasAge>>(() => _aliases.AsReadOnly());
        }

        public TimeSpan? MaxIndexAge { get; } = null;
        public IReadOnlyCollection<IndexAliasAge> Aliases => _frozenAliases.Value;

        public override void AddType(IIndexType type) {
            if (!(type is ITimeSeriesIndexType))
                throw new ArgumentException($"Type must implement {nameof(ITimeSeriesIndexType)}", nameof(type));

            base.AddType(type);
        }

        public void AddAlias(string name, TimeSpan? maxAge = null) {
            _aliases.Add(new IndexAliasAge {
                Name = name,
                MaxAge = maxAge ?? TimeSpan.MaxValue
            });
        }

        public override void Configure() {
            var indexes = this.GetIndexList().OrderBy(i => i.Version).GroupBy(i => i.DateUtc);
            var addedAliases = new List<string>();
            foreach (var index in indexes) {
                if (index.Key == DateTime.MaxValue)
                    continue;

                // TODO: MAX AGE?
                string alias = GetIndex(index.Key);
                if (_aliasCache.Contains(alias))
                    continue;

                if (_client.AliasExists(alias).Exists) {
                    _aliasCache.Add(alias);
                    continue;
                }

                var response = _client.Alias(a => a.Add(s => s.Index(index.First().Index).Alias(alias)));
                if (response.IsValid) { // TODO: What if a second instance created it..
                    _aliasCache.Add(alias);
                    addedAliases.Add(alias);
                    continue;
                }

                string message = $"An error occurred creating the alias {alias} for index {index.First().Index}: {response.GetErrorMessage()}";
                _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
            }

            foreach (var alias in addedAliases) {
                while (!_client.AliasExists(alias).Exists)
                    SystemClock.Sleep(100);
            }
        }
        
        public virtual string GetIndex(DateTime utcDate) {
            return $"{Name}-{utcDate:yyyy.MM.dd}";
        }

        public virtual string GetVersionedIndex(DateTime utcDate, int? version = null) {
            if (version == null || version < 0)
                version = Version;

            return $"{Name}-v{Version}-{utcDate:yyyy.MM.dd}";
        }
        
        protected override DateTime GetIndexDate(string name) {
            DateTime result;
            if (DateTime.TryParseExact(name, $"\'{VersionedName}-\'yyyy.MM.dd", EnUs, DateTimeStyles.AssumeUniversal, out result))
                return result.Date;

            return DateTime.MaxValue;
        }

        // add needs to call ensure index + save
        private readonly HashSet<string> _aliasCache = new HashSet<string>();

        public virtual void EnsureIndex(DateTime utcDate) {
            string alias = GetIndex(utcDate);
            if (_aliasCache.Contains(alias))
                return;

            if (_client.AliasExists(alias).Exists) {
                _aliasCache.Add(alias);
                return;
            }

            // TODO: check max age..
            // Try creating the index.
            var index = GetVersionedIndex(utcDate);
            var response = _client.CreateIndex(index, descriptor => {
                var d = ConfigureDescriptor(descriptor).AddAlias(alias);
                foreach (var a in Aliases)
                    d.AddAlias(a.Name);

                return d;
            });
            
            _logger.Trace(() => response.GetRequest());
            if (response.IsValid) {
                while (!_client.AliasExists(alias).Exists)
                    SystemClock.Sleep(1);

                _aliasCache.Add(alias);
                return;
            }

            // The create might of failed but the index already exists..
            if (_client.AliasExists(alias).Exists) {
                _aliasCache.Add(alias);
                return;
            }

            string message = $"An error occurred creating the index: {response.GetErrorMessage()}";
            _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
            throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
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
            var response = _client.DeleteIndex($"{VersionedName}-*");
            _logger.Trace(() => response.GetRequest());
            _aliasCache.Clear();

            if (response.IsValid)
                return;

            string message = $"An error occurred deleting the indexes: {response.GetErrorMessage()}";
            _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
            throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
        }

        public override async Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null) {
            int currentVersion = GetCurrentVersion();
            if (currentVersion < 0 || currentVersion == Version)
                return;

            var indexes = GetIndexList(currentVersion);
            if (indexes.Count == 0)
                return;

            // TODO: MaxAge??

            var reindexer = new ElasticReindexer(_client, _logger);
            foreach (var index in indexes) {
                var reindexWorkItem = new ReindexWorkItem {
                    OldIndex = index.Index,
                    NewIndex = VersionedName,
                    Alias = Name
                };

                reindexWorkItem.DeleteOld = reindexWorkItem.OldIndex != reindexWorkItem.NewIndex;
                
                foreach (var type in IndexTypes.OfType<IChildIndexType>())
                    reindexWorkItem.ParentMaps.Add(new ParentMap { Type = type.Name, ParentPath = type.ParentPath });

                // TODO: progress callback will report 0-100% multiple times...
                await reindexer.ReindexAsync(reindexWorkItem, progressCallbackAsync);
            }
        }
        
        public virtual void Maintain() {
            if (!MaxIndexAge.HasValue)
                return;

            var indexes = GetIndexList();
            RemoveOldIndexesFromAliases(indexes);
            DeleteOldIndexes(indexes);
        }
        
        protected void RemoveOldIndexesFromAliases(IList<IndexInfo> indexes) {
            if (!MaxIndexAge.HasValue)
                return;
            
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

        protected void DeleteOldIndexes(IList<IndexInfo> indexes) {
            if (!MaxIndexAge.HasValue)
                return;

            var sw = Stopwatch.StartNew();
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

        public class IndexAliasAge {
            public string Name { get; set; }
            public TimeSpan MaxAge { get; set; }
        }
    }
}