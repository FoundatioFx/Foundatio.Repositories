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
using Foundatio.Caching;
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
        private readonly ICacheClient _aliasCache;

        public DailyIndex(IElasticClient client, string name, int version = 1, ICacheClient cache = null, ILoggerFactory loggerFactory = null) : base(client, name, version, cache, loggerFactory) {
            AddAlias(Name);
            _frozenAliases = new Lazy<IReadOnlyCollection<IndexAliasAge>>(() => _aliases.AsReadOnly());
            _aliasCache = new ScopedCacheClient(_cache, "alias");
        }

        // TODO: Should we make this non nullable and do validation in the setter.
        // This should never be be negative or less than the index time period (day or a month)
        public TimeSpan? MaxIndexAge { get; set; }

        protected virtual DateTime GetIndexExpirationDate(DateTime utcDate) {
            return MaxIndexAge.HasValue && MaxIndexAge > TimeSpan.Zero ? utcDate.Date.Add(MaxIndexAge.Value) : DateTime.MaxValue;
        }

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

        public override void Configure() {}

        protected override void CreateAlias(string index, string name) {
            base.CreateAlias(index, name);

            var utcDate = GetIndexDate(index);
            string alias = GetIndex(utcDate);
            var indexExpirationUtcDate = GetIndexExpirationDate(utcDate);
            var expires = indexExpirationUtcDate < DateTime.MaxValue ? indexExpirationUtcDate : (DateTime?)null;
            _aliasCache.SetAsync(alias, alias, expires).GetAwaiter().GetResult();
        }

        protected string DateFormat { get; set; } = "yyyy.MM.dd";

        public virtual string GetIndex(DateTime utcDate) {
            return $"{Name}-{utcDate.ToString(DateFormat)}";
        }

        public virtual string GetVersionedIndex(DateTime utcDate, int? version = null) {
            if (version == null || version < 0)
                version = Version;

            return $"{Name}-v{version}-{utcDate.ToString(DateFormat)}";
        }
        
        protected override DateTime GetIndexDate(string index) {
            var version = GetIndexVersion(index);
            if (version < 0)
                version = Version;

            DateTime result;
            if (DateTime.TryParseExact(index, $"\'{Name}-v{version}-\'{DateFormat}", EnUs, DateTimeStyles.AdjustToUniversal, out result))
                return result.Date;

            return DateTime.MaxValue;
        }

        public virtual void EnsureIndex(DateTime utcDate) {
            var indexExpirationUtcDate = GetIndexExpirationDate(utcDate);
            if (SystemClock.UtcNow >= indexExpirationUtcDate)
                throw new ArgumentException($"Index max age exceeded: {indexExpirationUtcDate}", nameof(utcDate));

            var expires = indexExpirationUtcDate < DateTime.MaxValue ? indexExpirationUtcDate : (DateTime?)null;
            string alias = GetIndex(utcDate);
            if (_aliasCache.ExistsAsync(alias).GetAwaiter().GetResult())
                return;

            if (_client.AliasExists(alias).Exists) {
                _aliasCache.AddAsync(alias, alias, expires).GetAwaiter().GetResult();
                return;
            }

            // Try creating the index.
            var index = GetVersionedIndex(utcDate);
            var response = _client.CreateIndex(index, descriptor => {
                var d = ConfigureDescriptor(descriptor).AddAlias(alias);
                foreach (var a in Aliases.Where(a => ShouldCreateAlias(utcDate, a)))
                    d.AddAlias(a.Name);

                return d;
            });

            _logger.Trace(() => response.GetRequest());
            if (response.IsValid) {
                while (!_client.AliasExists(alias).Exists)
                    SystemClock.Sleep(1);

                _aliasCache.AddAsync(alias, alias, expires).GetAwaiter().GetResult();
                return;
            }

            // The create might of failed but the index already exists..
            if (_client.AliasExists(alias).Exists) {
                _aliasCache.AddAsync(alias, alias, expires).GetAwaiter().GetResult();
                return;
            }

            string message = $"Error creating index {index}: {response.GetErrorMessage()}";
            _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
            throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
        }

        protected virtual bool ShouldCreateAlias(DateTime documentDateUtc, IndexAliasAge alias) {
            return SystemClock.UtcNow.Date.SafeSubtract(alias.MaxAge) <= documentDateUtc;
        }

        public override int GetCurrentVersion() {
            var indexes = GetIndexList();
            if (indexes.Count == 0)
                return Version;

            return indexes.Where(i => SystemClock.UtcNow <= GetIndexExpirationDate(i.DateUtc)).Select(i => i.CurrentVersion >= 0 ? i.CurrentVersion : i.Version).OrderBy(v => v).First();
        }

        public virtual string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd) {
            if (!utcStart.HasValue)
                utcStart = SystemClock.UtcNow;

            if (!utcEnd.HasValue || utcEnd.Value < utcStart)
                utcEnd = SystemClock.UtcNow;

            var utcEndOfDay = utcEnd.Value.EndOfDay();

            var indices = new List<string>();
            for (DateTime current = utcStart.Value.StartOfDay(); current <= utcEndOfDay; current = current.AddDays(1))
                indices.Add(GetIndex(current));

            return indices.ToArray();
        }

        public override void Delete() {
            var aliasesResponse = _client.GetAlias(a => a.Alias($"{Name}-*"));
            if (!aliasesResponse.IsValid && aliasesResponse.ConnectionStatus.HttpStatusCode.GetValueOrDefault() != 404) {
                string message = $"Error getting the aliases: {aliasesResponse.GetErrorMessage()}";
                _logger.Error().Exception(aliasesResponse.ConnectionStatus.OriginalException).Message(message).Property("request", aliasesResponse.GetRequest()).Write();
                throw new ApplicationException(message, aliasesResponse.ConnectionStatus.OriginalException);
            }

            var aliasesToCheck = new List<string>(aliasesResponse.Indices.Count);
            foreach (var kvp in aliasesResponse.Indices) {
                if (kvp.Key.StartsWith(VersionedName))
                    aliasesToCheck.Add(kvp.Key);
            }

            // delete all indexes by prefix
            DeleteIndex($"{VersionedName}-*");

            foreach (var alias in aliasesToCheck) {
                while (_client.AliasExists(alias).Exists)
                    SystemClock.Sleep(1);
            }

            _aliasCache.RemoveAllAsync().GetAwaiter().GetResult();
        }

        public override async Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null) {
            int currentVersion = GetCurrentVersion();
            if (currentVersion < 0 || currentVersion >= Version)
                return;

            var indexes = GetIndexList(currentVersion);
            if (indexes.Count == 0)
                return;

            var reindexer = new ElasticReindexer(_client, _cache, _logger);
            foreach (var index in indexes) {
                if (SystemClock.UtcNow > GetIndexExpirationDate(index.DateUtc))
                    continue;

                if (index.CurrentVersion > Version)
                    continue;

                var reindexWorkItem = new ReindexWorkItem {
                    OldIndex = index.Index,
                    NewIndex = GetVersionedIndex(GetIndexDate(index.Index), Version),
                    Alias = Name
                };

                reindexWorkItem.DeleteOld = DiscardIndexesOnReindex && reindexWorkItem.OldIndex != reindexWorkItem.NewIndex;

                foreach (var type in IndexTypes.OfType<IChildIndexType>())
                    reindexWorkItem.ParentMaps.Add(new ParentMap {
                        Type = type.Name,
                        ParentPath = type.ParentPath
                    });

                if (!_client.IndexExists(reindexWorkItem.NewIndex).Exists) {
                    var response = _client.CreateIndex(reindexWorkItem.NewIndex, ConfigureDescriptor);
                    if (!response.IsValid) {
                        string message = $"Error creating the index: {response.GetErrorMessage()}";
                        _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                        throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
                    }
                }

                // TODO: progress callback will report 0-100% multiple times...
                await reindexer.ReindexAsync(reindexWorkItem, progressCallbackAsync);
            }
        }

        public virtual void Maintain() {
            if (!MaxIndexAge.HasValue || MaxIndexAge <= TimeSpan.Zero)
                return;

            var indexes = GetIndexList();
            SyncAliases(indexes);
            DeleteOldIndexes(indexes);
        }

        protected virtual void SyncAliases(IList<IndexInfo> indexes) {
            var aliasDescriptor = new AliasDescriptor();
            var aliases = Aliases.Where(e => !String.Equals(e.Name, Name, StringComparison.InvariantCultureIgnoreCase)).ToList();
            foreach (var indexGroup in indexes.OrderBy(i => i.Version).GroupBy(i => i.DateUtc)) {
                var indexExpirationDate = GetIndexExpirationDate(indexGroup.Key);
                foreach (var index in indexGroup) {
                    if (SystemClock.UtcNow >= indexExpirationDate) {
                        foreach (var alias in aliases)
                            aliasDescriptor = aliasDescriptor.Remove(r => r.Index(index.Index).Alias(alias.Name));

                        continue;
                    }

                    foreach (var alias in aliases) {
                        if (ShouldCreateAlias(indexGroup.Key, alias))
                            aliasDescriptor = aliasDescriptor.Add(r => r.Index(index.Index).Alias(alias.Name));
                        else
                            aliasDescriptor = aliasDescriptor.Remove(r => r.Index(index.Index).Alias(alias.Name));
                    }
                }

                if (SystemClock.UtcNow >= indexExpirationDate)
                    continue;

                var oldestIndex = indexGroup.First();
                if (oldestIndex.CurrentVersion < 0) {
                    try {
                        CreateAlias(oldestIndex.Index, GetIndex(indexGroup.Key));
                        foreach (var indexInfo in indexGroup)
                            indexInfo.CurrentVersion = oldestIndex.CurrentVersion;
                    } catch (Exception) {}
                }
            }
            
            var response = _client.Alias(aliasDescriptor);
            if (!response.IsValid) {
                string message = $"Error syncing aliases: {response.GetErrorMessage()}";
                _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
            }
        }

        protected virtual void DeleteOldIndexes(IList<IndexInfo> indexes) {
            if (!MaxIndexAge.HasValue || MaxIndexAge <= TimeSpan.Zero)
                return;

            var sw = new Stopwatch();
            foreach (var index in indexes.Where(i => SystemClock.UtcNow > GetIndexExpirationDate(i.DateUtc))) {
                sw.Restart();
                try {
                    DeleteIndex(index.Index);
                    _logger.Info($"Deleted index {index.Index} of age {SystemClock.UtcNow.Subtract(index.DateUtc).ToWords(true)} in {sw.Elapsed.ToWords(true)}");
                } catch (Exception) {}

                sw.Stop();
            }
        }
        
        protected override IList<IndexInfo> GetIndexList(int version = -1) {
            var indexes = base.GetIndexList(version);
            if (indexes.Count == 0)
                return indexes;
            
            // TODO: Optimize with cat aliases.
            // TODO: Should this return indexes that fall outside of the max age?
            foreach (var indexGroup in indexes.GroupBy(i => GetIndex(i.DateUtc))) {
                var v = GetVersionFromAlias(indexGroup.Key);
                foreach (var indexInfo in indexGroup)
                    indexInfo.CurrentVersion = v;
            }
            
            return indexes;
        }

        protected override void DeleteIndex(string name) {
            base.DeleteIndex(name);
            _aliasCache.RemoveAsync(GetIndex(GetIndexDate(name))).GetAwaiter().GetResult();
        }

        [DebuggerDisplay("Name: {Name} Max Age: {MaxAge}")]
        public class IndexAliasAge {
            public string Name { get; set; }
            public TimeSpan MaxAge { get; set; }
        }
    }
}