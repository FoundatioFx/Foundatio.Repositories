using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Nest;
using Exceptionless.DateTimeExtensions;
using Foundatio.Caching;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class DailyIndexType<T> : TimeSeriesIndexType<T> where T : class {
        public DailyIndexType(IIndex index, string name = null, Func<T, DateTime> getDocumentDateUtc = null) : base(index, name, getDocumentDateUtc) {}
    }

    public class DailyIndex : VersionedIndex, ITimeSeriesIndex {
        protected static readonly CultureInfo EnUs = new CultureInfo("en-US");
        private readonly List<IndexAliasAge> _aliases = new List<IndexAliasAge>();
        private readonly Lazy<IReadOnlyCollection<IndexAliasAge>> _frozenAliases;
        private readonly ICacheClient _aliasCache;
        private TimeSpan? _maxIndexAge;

        public DailyIndex(IElasticConfiguration configuration, string name, int version = 1)
            : base(configuration, name, version) {
            AddAlias(Name);
            _frozenAliases = new Lazy<IReadOnlyCollection<IndexAliasAge>>(() => _aliases.AsReadOnly());
            _aliasCache = new ScopedCacheClient(configuration.Cache, "alias");
        }

        /// <summary>
        /// This should never be be negative or less than the index time period (day or a month)
        /// </summary>
        public TimeSpan? MaxIndexAge {
            get { return _maxIndexAge; }
            set {
                if (value.HasValue && value.Value <= TimeSpan.Zero)
                    throw new ArgumentException($"{nameof(MaxIndexAge)} cannot be negative. ");

                _maxIndexAge = value;
            }
        }

        public bool DiscardExpiredIndexes { get; set; } = true;

        protected virtual DateTime GetIndexExpirationDate(DateTime utcDate) {
            return MaxIndexAge.HasValue && MaxIndexAge > TimeSpan.Zero ? utcDate.EndOfDay().SafeAdd(MaxIndexAge.Value) : DateTime.MaxValue;
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

        public override async Task ConfigureAsync() {
            foreach (var t in IndexTypes)
                await t.ConfigureAsync().AnyContext();
        }

        protected override async Task CreateAliasAsync(string index, string name) {
            await base.CreateAliasAsync(index, name).AnyContext();

            var utcDate = GetIndexDate(index);
            string alias = GetIndex(utcDate);
            var indexExpirationUtcDate = GetIndexExpirationDate(utcDate);
            var expires = indexExpirationUtcDate < DateTime.MaxValue ? indexExpirationUtcDate : (DateTime?)null;
            await _aliasCache.SetAsync(alias, alias, expires).AnyContext();
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
            int version = GetIndexVersion(index);
            if (version < 0)
                version = Version;

            if (DateTime.TryParseExact(index, $"\'{Name}-v{version}-\'{DateFormat}", EnUs, DateTimeStyles.AdjustToUniversal, out var result))
                return DateTime.SpecifyKind(result.Date, DateTimeKind.Utc);

            return DateTime.MaxValue;
        }

        public virtual async Task EnsureIndexAsync(DateTime utcDate) {
            var indexExpirationUtcDate = GetIndexExpirationDate(utcDate);
            if (SystemClock.UtcNow > indexExpirationUtcDate)
                throw new ArgumentException($"Index max age exceeded: {indexExpirationUtcDate}", nameof(utcDate));

            var expires = indexExpirationUtcDate < DateTime.MaxValue ? indexExpirationUtcDate : (DateTime?)null;
            string unversionedIndexAlias = GetIndex(utcDate);
            if (await _aliasCache.ExistsAsync(unversionedIndexAlias).AnyContext())
                return;

            if (await AliasExistsAsync(unversionedIndexAlias).AnyContext()) {
                await _aliasCache.SetAsync(unversionedIndexAlias, unversionedIndexAlias, expires).AnyContext();
                return;
            }

            // try creating the index.
            string index = GetVersionedIndex(utcDate);
            await CreateIndexAsync(index, descriptor => {
                var aliasesDescriptor = new AliasesDescriptor().Alias(unversionedIndexAlias);
                foreach (var a in Aliases.Where(a => ShouldCreateAlias(utcDate, a)))
                    aliasesDescriptor.Alias(a.Name);

                return ConfigureIndex(descriptor).Aliases(a => aliasesDescriptor);
            }).AnyContext();

            await _aliasCache.SetAsync(unversionedIndexAlias, unversionedIndexAlias, expires).AnyContext();
        }

        protected virtual bool ShouldCreateAlias(DateTime documentDateUtc, IndexAliasAge alias) {
            if (alias.MaxAge == TimeSpan.MaxValue)
                return true;

            return SystemClock.UtcNow.Date.SafeSubtract(alias.MaxAge) <= documentDateUtc.EndOfDay();
        }

        public override async Task<int> GetCurrentVersionAsync() {
            var indexes = await GetIndexesAsync().AnyContext();
            if (indexes.Count == 0)
                return Version;

            return indexes
                .Where(i => SystemClock.UtcNow <= GetIndexExpirationDate(i.DateUtc))
                .Select(i => i.CurrentVersion >= 0 ? i.CurrentVersion : i.Version)
                .OrderBy(v => v)
                .First();
        }

        public virtual string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd) {
            if (!utcStart.HasValue)
                utcStart = SystemClock.UtcNow;

            if (!utcEnd.HasValue || utcEnd.Value < utcStart)
                utcEnd = SystemClock.UtcNow;

            var utcStartOfDay = utcStart.Value.StartOfDay();
            var utcEndOfDay = utcEnd.Value.EndOfDay();
            var period = utcEndOfDay - utcStartOfDay;
            if ((MaxIndexAge.HasValue && period > MaxIndexAge.Value) || period.GetTotalMonths() >= 3)
                return new string[0];

            // TODO: Look up aliases that fit these ranges.
            var indices = new List<string>();
            for (var current = utcStartOfDay; current <= utcEndOfDay; current = current.AddDays(1))
                indices.Add(GetIndex(current));

            return indices.ToArray();
        }

        public override Task DeleteAsync() {
            return DeleteIndexAsync($"{VersionedName}-*");
        }

        public override async Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null) {
            int currentVersion = await GetCurrentVersionAsync().AnyContext();
            if (currentVersion < 0 || currentVersion >= Version)
                return;

            var indexes = await GetIndexesAsync(currentVersion).AnyContext();
            if (indexes.Count == 0)
                return;

            var reindexer = new ElasticReindexer(Configuration.Client, _logger);
            foreach (var index in indexes) {
                if (SystemClock.UtcNow > GetIndexExpirationDate(index.DateUtc))
                    continue;

                if (index.CurrentVersion > Version)
                    continue;

                var reindexWorkItem = new ReindexWorkItem {
                    OldIndex = index.Index,
                    NewIndex = GetVersionedIndex(GetIndexDate(index.Index), Version),
                    Alias = Name,
                    TimestampField = GetTimeStampField()
                };

                reindexWorkItem.DeleteOld = DiscardIndexesOnReindex && reindexWorkItem.OldIndex != reindexWorkItem.NewIndex;

                // attempt to create the index. If it exists the index will not be created.
                await CreateIndexAsync(reindexWorkItem.NewIndex, ConfigureIndex).AnyContext();

                // TODO: progress callback will report 0-100% multiple times...
                await reindexer.ReindexAsync(reindexWorkItem, progressCallbackAsync).AnyContext();
            }
        }

        public override async Task MaintainAsync(bool includeOptionalTasks = true) {
            var indexes = await GetIndexesAsync().AnyContext();
            if (indexes.Count == 0)
                return;

            await UpdateAliasesAsync(indexes).AnyContext();

            if (includeOptionalTasks && DiscardExpiredIndexes && MaxIndexAge.HasValue && MaxIndexAge > TimeSpan.Zero)
                await DeleteOldIndexesAsync(indexes).AnyContext();
        }

        protected virtual async Task UpdateAliasesAsync(IList<IndexInfo> indexes) {
            if (indexes.Count == 0)
                return;

            var aliasDescriptor = new BulkAliasDescriptor();
            foreach (var indexGroup in indexes.OrderBy(i => i.Version).GroupBy(i => i.DateUtc)) {
                var indexExpirationDate = GetIndexExpirationDate(indexGroup.Key);

                // Ensure the current version is always set.
                if (SystemClock.UtcNow < indexExpirationDate) {
                    var oldestIndex = indexGroup.First();
                    if (oldestIndex.CurrentVersion < 0) {
                        try {
                            await CreateAliasAsync(oldestIndex.Index, GetIndex(indexGroup.Key)).AnyContext();
                        } catch (Exception ex) {
                            _logger.LogError(ex, "Error setting current index version. Will use oldest index version: {OldestIndexVersion}", oldestIndex.Version);
                        }

                        foreach (var indexInfo in indexGroup)
                            indexInfo.CurrentVersion = oldestIndex.Version;
                    }
                }

                foreach (var index in indexGroup) {
                    if (SystemClock.UtcNow >= indexExpirationDate || index.Version != index.CurrentVersion) {
                        foreach (var alias in Aliases)
                            aliasDescriptor = aliasDescriptor.Remove(r => r.Index(index.Index).Alias(alias.Name));

                        continue;
                    }

                    foreach (var alias in Aliases) {
                        if (ShouldCreateAlias(indexGroup.Key, alias))
                            aliasDescriptor = aliasDescriptor.Add(r => r.Index(index.Index).Alias(alias.Name));
                        else
                            aliasDescriptor = aliasDescriptor.Remove(r => r.Index(index.Index).Alias(alias.Name));
                    }
                }
            }

            var response = await Configuration.Client.AliasAsync(aliasDescriptor).AnyContext();
            if (_logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Trace))
                _logger.LogTrace(response.GetRequest());

            if (!response.IsValid) {
                if (response.ApiCall.HttpStatusCode.GetValueOrDefault() == 404)
                    return;

                string message = $"Error updating aliases: {response.GetErrorMessage()}";
                _logger.LogError(response.OriginalException, "Error updating aliases: {0}", message);
                throw new ApplicationException(message, response.OriginalException);
            }
        }

        protected virtual async Task DeleteOldIndexesAsync(IList<IndexInfo> indexes) {
            if (indexes.Count == 0 || !MaxIndexAge.HasValue || MaxIndexAge <= TimeSpan.Zero)
                return;

            var sw = new Stopwatch();
            foreach (var index in indexes.Where(i => SystemClock.UtcNow > GetIndexExpirationDate(i.DateUtc))) {
                sw.Restart();
                try {
                    await DeleteIndexAsync(index.Index).AnyContext();
                    _logger.LogInformation("Deleted index {Index} of age {Age:g} in {Duration:g}", index.Index, SystemClock.UtcNow.Subtract(index.DateUtc), sw.Elapsed);
                } catch (Exception) {}

                sw.Stop();
            }
        }

        protected override async Task<IList<IndexInfo>> GetIndexesAsync(int version = -1) {
            var indexes = await base.GetIndexesAsync(version).AnyContext();
            if (indexes.Count == 0)
                return indexes;

            // TODO: Optimize with cat aliases.
            // TODO: Should this return indexes that fall outside of the max age?
            foreach (var indexGroup in indexes.GroupBy(i => GetIndex(i.DateUtc))) {
                int v = await GetVersionFromAliasAsync(indexGroup.Key).AnyContext();
                foreach (var indexInfo in indexGroup)
                    indexInfo.CurrentVersion = v;
            }

            return indexes;
        }

        protected override async Task DeleteIndexAsync(string name) {
            await base.DeleteIndexAsync(name).AnyContext();

            if (name.EndsWith("*"))
                await _aliasCache.RemoveAllAsync().AnyContext();
            else
                await _aliasCache.RemoveAsync(GetIndex(GetIndexDate(name))).AnyContext();
        }

        [DebuggerDisplay("Name: {Name} Max Age: {MaxAge}")]
        public class IndexAliasAge {
            public string Name { get; set; }
            public TimeSpan MaxAge { get; set; }
        }
    }
}