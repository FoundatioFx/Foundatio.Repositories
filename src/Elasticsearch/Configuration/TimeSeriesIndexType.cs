using System;
using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Queries;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface ITimeSeriesIndexType : IIndexType {
        string GetIndexById(string id);

        string[] GetIndexesByQuery(object query);
    }

    public interface ITimeSeriesIndexType<T> : IIndexType<T>, ITimeSeriesIndexType where T : class {
        string GetDocumentIndex(T document);
        void EnsureIndex(T document);
    }

    public class TimeSeriesIndexType<T> : IndexType<T>, ITimeSeriesIndexType<T> where T : class {
        protected readonly Func<T, DateTime> _getDocumentDateUtc;
        protected readonly string[] _defaultIndexes;

        public TimeSeriesIndexType(IIndex index, string name = null, Func<T, DateTime> getDocumentDateUtc = null) : base(index, name) {
            _getDocumentDateUtc = getDocumentDateUtc;
            _defaultIndexes = new[] { index.Name };

            if (_getDocumentDateUtc != null)
                return;

            if (!HasIdentity && !HasCreatedDate)
                throw new ArgumentNullException(nameof(getDocumentDateUtc));

            _getDocumentDateUtc = document => {
                if (document == null)
                    throw new ArgumentNullException(nameof(document));

                if (HasCreatedDate) {
                    var date = ((IHaveCreatedDate)document).CreatedUtc;
                    if (date != DateTime.MinValue)
                        return date;
                }

                if (HasIdentity) {
                    // This is also called when trying to create the document id.
                    var id = ((IIdentity)document).Id;
                    ObjectId objectId;
                    if (id != null && ObjectId.TryParse(id, out objectId) && objectId.CreationTime != DateTime.MinValue)
                        return objectId.CreationTime;
                }

                throw new ArgumentException("Unable to get document date.", nameof(document));
            };
        }

        public override string CreateDocumentId(T document) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            if (HasIdentity) {
                var id = ((IIdentity)document).Id;
                if (!String.IsNullOrEmpty(id))
                    return id;
            }

            try {
                var date = _getDocumentDateUtc?.Invoke(document);
                if (date.HasValue && date.Value != DateTime.MinValue)
                    return ObjectId.GenerateNewId(date.Value).ToString();
            } catch (ArgumentException) {}

            return ObjectId.GenerateNewId().ToString();
        }

        public virtual string GetDocumentIndex(T document) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            if (_getDocumentDateUtc == null)
                throw new ArgumentException("Unable to get document index", nameof(document));

            var date = _getDocumentDateUtc(document);
            return TimeSeriesIndex.GetIndex(date);
        }

        public virtual void EnsureIndex(T document) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            if (_getDocumentDateUtc == null)
                throw new ArgumentException("Unable to get document index", nameof(document));

            var date = _getDocumentDateUtc(document);
            TimeSeriesIndex.EnsureIndex(date);
        }

        public virtual string GetIndexById(string id) {
            if (String.IsNullOrEmpty(id))
                throw new ArgumentNullException(nameof(id));

            ObjectId objectId;
            if (!ObjectId.TryParse(id, out objectId))
                throw new ArgumentException("Unable to parse ObjectId", nameof(id));

            return TimeSeriesIndex.GetIndex(objectId.CreationTime);
        }

        public virtual string[] GetIndexesByQuery(object query) {
            var withIndexesQuery = query as IElasticIndexesQuery;
            if (withIndexesQuery == null)
                return _defaultIndexes;

            var indexes = new List<string>();
            if (withIndexesQuery.Indexes.Count > 0)
                indexes.AddRange(withIndexesQuery.Indexes);

            if (withIndexesQuery.UtcStartIndex.HasValue || withIndexesQuery.UtcEndIndex.HasValue)
                indexes.AddRange(TimeSeriesIndex.GetIndexes(withIndexesQuery.UtcStartIndex, withIndexesQuery.UtcEndIndex));

            return indexes.Count > 0 ? indexes.ToArray() : _defaultIndexes;
        }

        protected ITimeSeriesIndex TimeSeriesIndex => (ITimeSeriesIndex)Index;

        public PutTemplateDescriptor ConfigureTemplate(PutTemplateDescriptor template) {
            return template.AddMapping<T>(BuildMapping);
        }
    }
}