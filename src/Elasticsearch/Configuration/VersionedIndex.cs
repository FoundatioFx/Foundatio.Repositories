using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class VersionedIndex : IndexBase {
        public VersionedIndex(IElasticClient client, string name, int version = 1, ILoggerFactory loggerFactory = null): base(client, name, loggerFactory) {
            Version = version;
            VersionedName = String.Concat(Name, "-v", Version);
        }

        public int Version { get; }
        public string VersionedName { get; }
        public bool DiscardIndexesOnReindex { get; set; } = true;

        public override void Configure() {
            IIndicesOperationResponse response;
            if (!_client.IndexExists(VersionedName).Exists) {
                if (!_client.AliasExists(Name).Exists)
                    response = _client.CreateIndex(VersionedName, descriptor => ConfigureDescriptor(descriptor).AddAlias(Name));
                else
                    response = _client.CreateIndex(VersionedName, ConfigureDescriptor);
                
                _logger.Trace(() => response.GetRequest());
                if (response.IsValid)
                    return;

                string message = $"Error creating the index {VersionedName}: {response.GetErrorMessage()}";
                _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
            }
            
            if (_client.AliasExists(Name).Exists)
                return;

            var currentVersion = GetCurrentVersion();
            if (currentVersion < 0)
                currentVersion = Version;
            
            CreateAlias(String.Concat(Name, "-v", currentVersion), Name);
        }

        protected void CreateAlias(string index, string name) {
            if (_client.AliasExists(name).Exists)
                return;
            
            var response = _client.Alias(a => a.Add(s => s.Index(index).Alias(name)));
            if (response.IsValid) {
                while (!_client.AliasExists(name).Exists)
                    SystemClock.Sleep(1);

                return;
            }

            if (_client.AliasExists(name).Exists)
                return;

            string message = $"Error creating alias {name}: {response.GetErrorMessage()}";
            _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
            throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
        }

        public virtual CreateIndexDescriptor ConfigureDescriptor(CreateIndexDescriptor idx) {
            foreach (var t in IndexTypes)
                t.Configure(idx);

            return idx;
        }

        public override void Delete() {
            DeleteIndex(VersionedName);
        }

        public override Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null) {
            int currentVersion = GetCurrentVersion();
            if (currentVersion < 0 || currentVersion >= Version)
                return Task.CompletedTask;

            var reindexWorkItem = new ReindexWorkItem {
                OldIndex = String.Concat(Name, "-v", currentVersion),
                NewIndex = VersionedName,
                Alias = Name
            };

            reindexWorkItem.DeleteOld = DiscardIndexesOnReindex && reindexWorkItem.OldIndex != reindexWorkItem.NewIndex;

            foreach (var type in IndexTypes.OfType<IChildIndexType>())
                reindexWorkItem.ParentMaps.Add(new ParentMap { Type = type.Name, ParentPath = type.ParentPath });

            var reindexer = new ElasticReindexer(_client, _logger);
            return reindexer.ReindexAsync(reindexWorkItem, progressCallbackAsync);
        }

        /// <summary>
        /// Returns the current index version (E.G., the oldest index version).
        /// </summary>
        /// <returns>-1 if there are no indexes.</returns>
        public virtual int GetCurrentVersion() {
            int version = GetVersionFromAlias(Name);
            if (version >= 0)
                return version;

            var indexes = GetIndexList();
            if (indexes.Count == 0)
                return Version;

            return indexes.Select(i => i.Version).OrderBy(v => v).First();
        }

        protected virtual int GetVersionFromAlias(string alias) {
            var response = _client.GetAlias(a => a.Alias(alias));
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

        protected virtual IList<IndexInfo> GetIndexList(int version = -1) {
            // TODO: Update this to use a index filter once we upgrade to elastic 2.x+
            var sw = Stopwatch.StartNew();
            var response = _client.CatIndices(i => i.Pri().H("index").RequestConfiguration(r => r.RequestTimeout(5 * 60 * 1000)));
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