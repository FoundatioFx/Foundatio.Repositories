using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Caching;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class VersionedIndex : IndexBase, IMaintainableIndex {
        public VersionedIndex(IElasticClient client, string name, int version = 1, ICacheClient cache = null, ILoggerFactory loggerFactory = null)
            : base(client, name, cache, loggerFactory) {
            Version = version;
            VersionedName = String.Concat(Name, "-v", Version);
        }

        public int Version { get; }
        public string VersionedName { get; }
        public bool DiscardIndexesOnReindex { get; set; } = true;

        public override async Task ConfigureAsync() {
            if (!await IndexExistsAsync(VersionedName).AnyContext()) {
                if (!await AliasExistsAsync(Name).AnyContext())
                    await CreateIndexAsync(VersionedName, d => ConfigureDescriptor(d).AddAlias(Name)).AnyContext();
                else
                    await CreateIndexAsync(VersionedName, ConfigureDescriptor).AnyContext();
            }
        }

        protected virtual async Task CreateAliasAsync(string index, string name) {
            if (await AliasExistsAsync(name).AnyContext())
                return;
            
            var response = await _client.AliasAsync(a => a.Add(s => s.Index(index).Alias(name))).AnyContext();
            if (response.IsValid) {
                while (!await AliasExistsAsync(name).AnyContext())
                    SystemClock.Sleep(100);

                return;
            }

            if (await AliasExistsAsync(name).AnyContext())
                return;

            string message = $"Error creating alias {name}: {response.GetErrorMessage()}";
            _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
            throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
        }

        protected async Task<bool> AliasExistsAsync(string alias) {
            var response = await _client.AliasExistsAsync(alias).AnyContext();
            if (response.IsValid)
                return response.Exists;
            
            string message = $"Error checking to see if alias {alias} exists: {response.GetErrorMessage()}";
            _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
            throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
        }

        public virtual CreateIndexDescriptor ConfigureDescriptor(CreateIndexDescriptor idx) {
            foreach (var t in IndexTypes)
                t.Configure(idx);

            return idx;
        }

        public override Task DeleteAsync() {
            return DeleteIndexAsync(VersionedName);
        }

        public override async Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null) {
            int currentVersion = await GetCurrentVersionAsync().AnyContext();
            if (currentVersion < 0 || currentVersion >= Version)
                return;

            var reindexWorkItem = new ReindexWorkItem {
                OldIndex = String.Concat(Name, "-v", currentVersion),
                NewIndex = VersionedName,
                Alias = Name
            };

            reindexWorkItem.DeleteOld = DiscardIndexesOnReindex && reindexWorkItem.OldIndex != reindexWorkItem.NewIndex;

            foreach (var type in IndexTypes.OfType<IChildIndexType>())
                reindexWorkItem.ParentMaps.Add(new ParentMap { Type = type.Name, ParentPath = type.ParentPath });

            var reindexer = new ElasticReindexer(_client, _cache, _logger);
            await reindexer.ReindexAsync(reindexWorkItem, progressCallbackAsync).AnyContext();
        }
        
        public virtual async Task MaintainAsync() {
            if (await AliasExistsAsync(Name).AnyContext())
                return;

            var currentVersion = await GetCurrentVersionAsync().AnyContext();
            if (currentVersion < 0)
                currentVersion = Version;

            await CreateAliasAsync(String.Concat(Name, "-v", currentVersion), Name).AnyContext();
        }

        /// <summary>
        /// Returns the current index version (E.G., the oldest index version).
        /// </summary>
        /// <returns>-1 if there are no indexes.</returns>
        public virtual async Task<int> GetCurrentVersionAsync() {
            int version = await GetVersionFromAliasAsync(Name).AnyContext();
            if (version >= 0)
                return version;

            var indexes = await GetIndexesAsync().AnyContext();
            if (indexes.Count == 0)
                return Version;

            return indexes.Select(i => i.Version).OrderBy(v => v).First();
        }

        protected virtual async Task<int> GetVersionFromAliasAsync(string alias) {
            var response = await  _client.GetAliasAsync(a => a.Alias(alias)).AnyContext();
            _logger.Trace(() => response.GetRequest());

            if (response.IsValid && response.Indices.Count > 0)
                return response.Indices.Keys.Select(GetIndexVersion).OrderBy(v => v).First();

            return -1;
        }
        
        protected virtual int GetIndexVersion(string name) {
            if (String.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));

            string input = name.Substring($"{Name}-v".Length);
            int index = input.IndexOf('-');
            if (index > 0)
                input = input.Substring(0, index);

            int version;
            if (Int32.TryParse(input, out version))
                return version;

            return -1;
        }

        protected virtual async Task<IList<IndexInfo>> GetIndexesAsync(int version = -1) {
            // TODO: Update this to use a index filter once we upgrade to elastic 2.x+
            var sw = Stopwatch.StartNew();
            var response = await _client.CatIndicesAsync(i => i.Pri().H("index").RequestConfiguration(r => r.RequestTimeout(5 * 60 * 1000))).AnyContext();
            sw.Stop();
            _logger.Trace(() => response.GetRequest());

            if (!response.IsValid) {
                string message = $"Error getting indices: {response.GetErrorMessage()}";
                _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
            }

            string index = version < 0 ? $"{Name}-v" : $"{Name}-v{version}";
            var indices = response.Records
                .Where(i => i.Index.StartsWith(index) && (version < 0 || GetIndexVersion(i.Index) == version))
                .Select(i => new IndexInfo { DateUtc = GetIndexDate(i.Index), Index = i.Index, Version = GetIndexVersion(i.Index) })
                .ToList();
            
            _logger.Info($"Retrieved list of {indices.Count} indexes in {sw.Elapsed.ToWords(true)}");
            return indices;
        }

        protected virtual DateTime GetIndexDate(string name) {
            return DateTime.MaxValue;
        }

        protected class IndexInfo {
            public string Index { get; set; }
            public int Version { get; set; }
            public int CurrentVersion { get; set; } = -1;
            public DateTime DateUtc { get; set; }
        }
    }
}