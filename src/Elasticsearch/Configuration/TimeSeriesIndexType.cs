using System;
using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Queries;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface ITimeSeriesIndexType : IIndexType, ITemplatedIndexType {
        string GetIndexById(string id);
        string[] GetIndexesByQuery(object query);
    }

    public interface ITimeSeriesIndexType<T> : IIndexType<T>, ITimeSeriesIndexType where T : class {
        string GetDocumentIndex(T document);
    }

    public class TimeSeriesIndexType<T> : IndexType<T>, ITimeSeriesIndexType<T> where T : class {
        protected readonly Func<T, DateTime> _getDocumentDateUtc;

        public TimeSeriesIndexType(IIndex index, string name = null, Func<T, DateTime> getDocumentDateUtc = null) : base(index, name) {
            _getDocumentDateUtc = getDocumentDateUtc;
            
            if (_getDocumentDateUtc == null && typeof(IHaveCreatedDate).IsAssignableFrom(typeof(T)))
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
}