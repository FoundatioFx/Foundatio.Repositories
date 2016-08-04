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
            // TODO: Populate the alias cache...

            //var response = _client.PutTemplate(Name, ConfigureTemplate);
            //_logger.Trace(() => response.GetRequest());

            //if (response.IsValid)
            //    return;

            //string message = $"An error occurred creating the template: {response.GetErrorMessage()}";
            //_logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
            //throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
        }

        //public virtual PutTemplateDescriptor ConfigureTemplate(PutTemplateDescriptor template) {
        //    template.Template($"{VersionedName}-*");
        //    template.AddAlias("{index}-alias");
        //    foreach (var alias in Aliases)
        //        template.AddAlias(alias.Name);

        //    // TODO: What should happen if there are types that don't implement ITemplatedIndexType?
        //    foreach (var type in IndexTypes.OfType<ITemplatedIndexType>())
        //        type.ConfigureTemplate(template);

        //    return template;
        //}

        public virtual string GetIndex(DateTime utcDate) {
            return $"{Name}-{utcDate:yyyy.MM.dd}";
        }

        public virtual string GetVersionedIndex(DateTime utcDate) {
            return $"{VersionedName}-{utcDate:yyyy.MM.dd}";
        }

        // add needs to call ensure index + save
        private readonly HashSet<string> _cache = new HashSet<string>();

        public virtual void EnsureIndex(DateTime utcDate) {
            string alias = GetIndex(utcDate);
            if (_cache.Contains(alias))
                return;

            if (_client.AliasExists(alias).Exists) {
                _cache.Add(alias);
                return;
            }

            // TODO: check max age..

            var index = GetVersionedIndex(utcDate);
            var response = _client.CreateIndex(index, descriptor => {
                var d = ConfigureDescriptor(descriptor).AddAlias(alias);
                foreach (var a in Aliases)
                    d.AddAlias(a.Name);

                return d;
            });

            _logger.Trace(() => response.GetRequest());
            if (response.IsValid || _client.IndexExists(index).Exists) {
                _cache.Add(alias);
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

            string message;
            if (!response.IsValid) {
                message = $"An error occurred deleting the indexes: {response.GetErrorMessage()}";
                _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
            }
 
            // delete the template
            if (!_client.TemplateExists(Name).Exists)
                return;

            var deleteResponse = _client.DeleteTemplate(Name);
            _logger.Trace(() => deleteResponse.GetRequest());

            if (deleteResponse.IsValid)
                return;

            message = $"An error occurred deleting the index template: {deleteResponse.GetErrorMessage()}";
            _logger.Error().Exception(deleteResponse.ConnectionStatus.OriginalException).Message(message).Property("request", deleteResponse.GetRequest()).Write();
            throw new ApplicationException(message, deleteResponse.ConnectionStatus.OriginalException);
        }

        public override async Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null) {
            int currentVersion = GetCurrentVersion();
            if (currentVersion < 0 || currentVersion == Version)
                return;

            var indexes = GetIndexList(currentVersion);
            if (indexes.Count == 0)
                return;

            var reindexer = new ElasticReindexer(_client, _logger);
            foreach (var index in indexes) {
                var reindexWorkItem = new ReindexWorkItem {
                    OldIndex = String.Concat(Name, "-v", currentVersion),
                    NewIndex = VersionedName,
                    Alias = Name,
                    DeleteOld = true
                };
                
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

        protected override DateTime GetIndexDate(string name) {
            DateTime result;
            if (DateTime.TryParseExact(name, $"\'{VersionedName}-\'yyyy.MM.dd", EnUs, DateTimeStyles.None, out result))
                return result;

            return DateTime.MaxValue;
        }

        public class IndexAliasAge {
            public string Name { get; set; }
            public TimeSpan MaxAge { get; set; }
        }
    }
}