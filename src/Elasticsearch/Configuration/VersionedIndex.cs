using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class VersionedIndex : IndexBase {
        public VersionedIndex(IElasticClient client, string name, int version = 1, ILoggerFactory loggerFactory = null): base(client, name, loggerFactory) {
            Version = version;
            VersionedName = String.Concat(Name, "-v", Version);
        }

        public int Version { get; }
        public string VersionedName { get; }

        public override void Configure() {
            IIndicesOperationResponse response = null;

            if (!_client.IndexExists(VersionedName).Exists) {
                if (!_client.AliasExists(Name).Exists)
                    response = _client.CreateIndex(VersionedName, descriptor => ConfigureDescriptor(descriptor).AddAlias(Name));
                else
                    response = _client.CreateIndex(VersionedName, ConfigureDescriptor);
                
                _logger.Trace(() => response.GetRequest());
            }

            if (response == null || response.IsValid)
                return;

            string message = $"An error occurred creating the index: {response.GetErrorMessage()}";
            _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
            throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
        }

        public virtual CreateIndexDescriptor ConfigureDescriptor(CreateIndexDescriptor idx) {
            foreach (var t in IndexTypes)
                t.Configure(idx);

            return idx;
        }

        public override void Delete() {
            IIndicesResponse response = null;

            if (_client.IndexExists(VersionedName).Exists) {
                response = _client.DeleteIndex(VersionedName);
                _logger.Trace(() => response.GetRequest());
            }

            if (response == null || response.IsValid)
                return;

            string message = $"An error occurred deleting the index: {response.GetErrorMessage()}";
            _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
            throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
        }

        public override Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null) {
            int currentVersion = GetCurrentVersion();
            if (currentVersion < 0 || currentVersion == Version)
                return Task.CompletedTask;

            var reindexWorkItem = new ReindexWorkItem {
                OldIndex = String.Concat(Name, "-v", currentVersion),
                NewIndex = VersionedName,
                Alias = Name
            };

            reindexWorkItem.DeleteOld = reindexWorkItem.OldIndex != reindexWorkItem.NewIndex;

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
            //var res = _client.GetAlias(a => a.Alias(Name));
            //if (!res.Indices.Any())
            //    return -1;

            var indexes = GetIndexList();

            var currentVersionIndex = indexes.FirstOrDefault(i => i.Version == Version);
            if (currentVersionIndex != null)
                indexes.Remove(currentVersionIndex);
            
            if (indexes.Count == 0)
                return currentVersionIndex != null ? Version : -1;
            
            if (indexes.Count > 1)
                throw new ApplicationException($"Multiple index versions found: {String.Join(", ", indexes.Select(i => i.Index))}");

            return indexes.Single().Version;
        }

        protected IList<IndexInfo> GetIndexList(int version = -1) {
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

        protected class IndexInfo {
            public string Index { get; set; }
            public int Version { get; set; }
            public DateTime DateUtc { get; set; }
        }
    }
}