using System;
using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Queries;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IIndexType {
        string Name { get; }
        IIndex Index { get; }
        int DefaultCacheExpirationSeconds { get; set; }
        int BulkBatchSize { get; set; }
        ISet<string> AllowedFacetFields { get; }
        CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx);
    }

    public interface IChildIndexType : IIndexType {
        string ParentPath { get; }
    }

    public interface IChildIndexType<T>: IChildIndexType {
        string GetParentId(T document);
    }

    public interface ITimeSeriesIndexType : IIndexType {
        PutTemplateDescriptor ConfigureTemplate(PutTemplateDescriptor idx);
        string GetIndexById(string id);
        string[] GetIndexesByQuery(object query);
    }

    public interface IIndexType<T>: IIndexType where T : class {
        string GetDocumentId(T document);
    }

    public interface ITimeSeriesIndexType<T> : IIndexType<T>, ITimeSeriesIndexType where T : class {
        string GetDocumentIndex(T document);
    }

    public class IndexType<T> : IIndexType<T> where T : class {
        private readonly string _typeName = typeof(T).Name.ToLower();

        public IndexType(string name = null, IIndex index = null) {
            Name = name ?? _typeName;
            Index = index ?? new Index(Name);
        }

        public string Name { get; }
        public IIndex Index { get; }
        public ISet<string> AllowedFacetFields { get; } = new HashSet<string>();

        public virtual string GetDocumentId(T document) {
            return ObjectId.GenerateNewId().ToString();
        }

        public CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            return idx.AddMapping<T>(BuildMapping);
        }

        public virtual PutMappingDescriptor<T> BuildMapping(PutMappingDescriptor<T> map) {
            return map;
        }

        public int DefaultCacheExpirationSeconds { get; set; } = RepositoryConstants.DEFAULT_CACHE_EXPIRATION_SECONDS;
        public int BulkBatchSize { get; set; } = 1000;
    }

    public class TimeSeriesIndexType<T> : IndexType<T>, ITimeSeriesIndexType<T> where T : class {
        protected readonly Func<T, DateTime> _getDocumentDateUtc;

        public TimeSeriesIndexType(string name = null, IIndex index = null, Func<T, DateTime> getDocumentDateUtc = null) : base(name, index ?? new MonthlyIndex(name)) {
            _getDocumentDateUtc = getDocumentDateUtc;
            if (_getDocumentDateUtc == null && typeof(T).IsAssignableFrom(typeof(IHaveCreatedDate)))
                _getDocumentDateUtc = d => ((IHaveCreatedDate)d).CreatedUtc;
        }

        public override string GetDocumentId(T document) {
            if (_getDocumentDateUtc == null)
                return ObjectId.GenerateNewId().ToString();

            var date = _getDocumentDateUtc(document);
            return ObjectId.GenerateNewId(date).ToString();
        }

        public virtual string GetDocumentIndex(T document) {
            if (_getDocumentDateUtc == null)
                return TimeSeriesIndex.GetIndex(DateTime.UtcNow);

            var date = _getDocumentDateUtc(document);
            return TimeSeriesIndex.GetIndex(date);
        }

        public virtual string[] GetIndexesByQuery(object query) {
            var withIndexesQuery = query as IElasticIndexesQuery;
            if (withIndexesQuery == null)
                return new string[0];

            var indexes = new List<string>();
            if (withIndexesQuery.Indexes.Count > 0)
                indexes.AddRange(withIndexesQuery.Indexes);

            if (withIndexesQuery.UtcStartIndex.HasValue || withIndexesQuery.UtcEndIndex.HasValue)
                indexes.AddRange(TimeSeriesIndex.GetIndexes(withIndexesQuery.UtcStartIndex, withIndexesQuery.UtcEndIndex));

            return indexes.ToArray();
        }

        public virtual string GetIndexById(string id) {
            ObjectId objectId;
            if (!ObjectId.TryParse(id, out objectId))
                return TimeSeriesIndex.GetIndex(DateTime.UtcNow);

            return TimeSeriesIndex.GetIndex(objectId.CreationTime);
        }

        protected ITimeSeriesIndex TimeSeriesIndex => (ITimeSeriesIndex)Index;

        public PutTemplateDescriptor ConfigureTemplate(PutTemplateDescriptor template) {
            return template.AddMapping<T>(BuildMapping);
        }
    }

    public class MonthlyIndexType<T> : TimeSeriesIndexType<T> where T : class {
        public MonthlyIndexType(string name = null, IIndex index = null, Func<T, DateTime> getDocumentDateUtc = null) : base(name, index, getDocumentDateUtc) {}
    }

    public class DailyIndexType<T> : TimeSeriesIndexType<T> where T : class {
        public DailyIndexType(string name = null, IIndex index = null, Func<T, DateTime> getDocumentDateUtc = null) : base(name, index, getDocumentDateUtc) { }
    }

    public class ChildIndexType<T> : IndexType<T>, IChildIndexType<T> where T : class {
        protected readonly Func<T, string> _getParentId;

        public ChildIndexType(string parentPath, Func<T, string> getParentId, string name = null, IIndex index = null): base(name, index) {
            ParentPath = parentPath;
            _getParentId = getParentId;
        }

        public string ParentPath { get; }

        public virtual string GetParentId(T document) {
            return _getParentId(document);
        }
    }
}