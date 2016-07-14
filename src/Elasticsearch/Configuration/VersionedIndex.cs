using System;
using System.Linq;
using System.Threading.Tasks;
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
                response = _client.CreateIndex(VersionedName, descriptor => ConfigureDescriptor(descriptor));
                _logger.Trace(() => response.GetRequest());
            }

            if (response == null || response.IsValid)
                throw new ApplicationException("An error occurred creating the index: " + response?.ServerError.Error);
        }

        public virtual CreateIndexDescriptor ConfigureDescriptor(CreateIndexDescriptor idx) {
            idx.AddAlias(Name);

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

            if (response != null && !response.IsValid)
                throw new ApplicationException("An error occurred deleting the index: " + response.ServerError.Error);
        }

        public override Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null) {
            var reindexer = new ElasticReindexer(_client, _logger);
            int currentVersion = GetVersion();

            var reindexWorkItem = new ReindexWorkItem {
                OldIndex = String.Concat(Name, "-v", currentVersion),
                NewIndex = VersionedName,
                Alias = Name,
                DeleteOld = true
            };

            foreach (var type in IndexTypes.OfType<IChildIndexType>())
                reindexWorkItem.ParentMaps.Add(new ParentMap { Type = type.Name, ParentPath = type.ParentPath });

            return reindexer.ReindexAsync(reindexWorkItem, progressCallbackAsync);
        }

        public virtual int GetVersion() {
            var res = _client.GetAlias(a => a.Alias(Name));
            if (!res.Indices.Any())
                return -1;

            string indexName = res.Indices.FirstOrDefault().Key;
            string versionString = indexName.Substring(indexName.LastIndexOf("-", StringComparison.Ordinal));

            int version;
            if (!Int32.TryParse(versionString.Substring(2), out version))
                return -1;

            return version;
        }
    }
}