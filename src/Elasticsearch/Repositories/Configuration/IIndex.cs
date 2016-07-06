using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IIndex {
        int Version { get; }
        string AliasName { get; }
        string VersionedName { get; }
        ICollection<IIndexType> IndexTypes { get; }
        void Configure();
        void Delete();
        int GetVersion();
        Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null);
    }

    public interface ITimeSeriesIndex : IIndex {
        string GetIndex(DateTime utcDate);
        string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd);
        void Maintain();
    }

    public class Index : IIndex {
        private readonly IElasticClient _client;
        private readonly ILogger _logger;

        public Index(IElasticClient client, string name, int version = 1, ILoggerFactory loggerFactory = null) {
            AliasName = name;
            Version = version;
            VersionedName = String.Concat(AliasName, "-v", Version);
            _client = client;
            _logger = loggerFactory.CreateLogger(this.GetType());
        }

        public int Version { get; }
        public string AliasName { get; }
        public string VersionedName { get; }

        public ICollection<IIndexType> IndexTypes { get; } = new List<IIndexType>();

        public virtual void Configure() {
            IIndicesOperationResponse response = null;

            if (!_client.IndexExists(VersionedName).Exists)
                response = _client.CreateIndex(VersionedName, descriptor => ConfigureDescriptor(descriptor));

            if (response == null || response.IsValid)
                throw new ApplicationException("An error occurred creating the index or template: " + response?.ServerError.Error);

            if (_client.AliasExists(AliasName).Exists)
                return;

            response = _client.Alias(a => a.Add(add => add.Index(VersionedName).Alias(AliasName)));

            if (response == null || response.IsValid)
                throw new ApplicationException("An error occurred creating the alias: " + response?.ServerError.Error);
        }

        public virtual void Delete() {
            var response = _client.DeleteIndex(VersionedName);

            if (response == null || response.IsValid)
                throw new ApplicationException("An error occurred deleting the indexes: " + response?.ServerError.Error);
        }

        public virtual int GetVersion() {
            var res = _client.GetAlias(a => a.Alias(AliasName));
            if (!res.Indices.Any())
                return -1;

            string indexName = res.Indices.FirstOrDefault().Key;
            string versionString = indexName.Substring(indexName.LastIndexOf("-", StringComparison.Ordinal));

            int version;
            if (!Int32.TryParse(versionString.Substring(2), out version))
                return -1;

            return version;
        }

        public virtual Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null) {
            var reindexer = new ElasticReindexer(_client, _logger);
            int currentVersion = GetVersion();

            var reindexWorkItem = new ReindexWorkItem {
                OldIndex = String.Concat(AliasName, "-v", currentVersion),
                NewIndex = VersionedName,
                Alias = AliasName,
                DeleteOld = true
            };

            foreach (var type in IndexTypes.OfType<IChildIndexType>())
                reindexWorkItem.ParentMaps.Add(new ParentMap { Type = type.Name, ParentPath = type.ParentPath });

            return reindexer.ReindexAsync(reindexWorkItem, progressCallbackAsync);
        }

        public virtual CreateIndexDescriptor ConfigureDescriptor(CreateIndexDescriptor idx) {
            idx.AddAlias(AliasName);

            foreach (var t in IndexTypes)
                t.Configure(idx);
            
            return idx;
        }
    }

    public class MonthlyIndex: Index, ITimeSeriesIndex {
        public MonthlyIndex(string name, int version = 1): base(name, version) {}

        public PutTemplateDescriptor ConfigureTemplate(PutTemplateDescriptor template) {
            template.Template(VersionedName + "-*");
            template.AddAlias(AliasName);

            foreach (var t in IndexTypes) {
                var type = t as ITimeSeriesIndexType;
                type?.ConfigureTemplate(template);
            }

            return template;
        }

        public string GetIndex(DateTime utcDate) {
            return $"{VersionedName}-{utcDate:yyyy.MM}";
        }

        public string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd) {
            if (!utcStart.HasValue)
                utcStart = DateTime.UtcNow;

            if (!utcEnd.HasValue || utcEnd.Value < utcStart)
                utcEnd = DateTime.UtcNow;

            var utcEndOfDay = utcEnd.Value.EndOfDay();

            var indices = new List<string>();
            for (DateTime current = utcStart.Value; current <= utcEndOfDay; current = current.AddMonths(1))
                indices.Add(GetIndex(current));

            return indices.ToArray();
        }
    }

    public class DailyIndex : Index, ITimeSeriesIndex {
        public DailyIndex(string name, int version = 1) : base(name, version) { }

        public PutTemplateDescriptor ConfigureTemplate(PutTemplateDescriptor template) {
            template.Template(VersionedName + "-*");
            template.AddAlias(AliasName);

            foreach (var t in IndexTypes) {
                var type = t as ITimeSeriesIndexType;
                type?.ConfigureTemplate(template);
            }

            return template;
        }

        public string GetIndex(DateTime utcDate) {
            return $"{VersionedName}-{utcDate:yyyy.MM.dd}";
        }

        public string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd) {
            if (!utcStart.HasValue)
                utcStart = DateTime.UtcNow;

            if (!utcEnd.HasValue || utcEnd.Value < utcStart)
                utcEnd = DateTime.UtcNow;

            var utcEndOfDay = utcEnd.Value.EndOfDay();

            var indices = new List<string>();
            for (DateTime current = utcStart.Value; current <= utcEndOfDay; current = current.AddDays(1))
                indices.Add(GetIndex(current));

            return indices.ToArray();
        }
    }
}
