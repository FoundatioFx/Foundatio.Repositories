using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface ITimeSeriesIndexType : IIndexType {
        string GetIndexById(Id id);

        string[] GetIndexesByQuery(IRepositoryQuery query);
    }

    public interface ITimeSeriesIndexType<T> : IIndexType<T>, ITimeSeriesIndexType where T : class {
        string GetDocumentIndex(T document);
        Task EnsureIndexAsync(T document);
    }

    public class TimeSeriesIndexType<T> : IndexTypeBase<T>, ITimeSeriesIndexType<T> where T : class {
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
                    string id = ((IIdentity)document).Id;
                    if (id != null && ObjectId.TryParse(id, out var objectId) && objectId.CreationTime != DateTime.MinValue)
                        return objectId.CreationTime;
                }

                throw new ArgumentException("Unable to get document date.", nameof(document));
            };
        }

        public override string CreateDocumentId(T document) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            if (HasIdentity) {
                string id = ((IIdentity)document).Id;
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

        public virtual Task EnsureIndexAsync(T document) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            if (_getDocumentDateUtc == null)
                throw new ArgumentException("Unable to get document index", nameof(document));

            var date = _getDocumentDateUtc(document);
            return TimeSeriesIndex.EnsureIndexAsync(date);
        }

        public virtual string GetIndexById(Id id) {
            if (String.IsNullOrEmpty(id.Value))
                throw new ArgumentNullException(nameof(id));

            if (!ObjectId.TryParse(id.Value, out var objectId))
                throw new ArgumentException("Unable to parse ObjectId", nameof(id));

            return TimeSeriesIndex.GetIndex(objectId.CreationTime);
        }

        public virtual string[] GetIndexesByQuery(IRepositoryQuery query) {
            var indexes = GetIndexes(query);
            return indexes.Count > 0 ? indexes.ToArray() : _defaultIndexes;
        }

        private HashSet<string> GetIndexes(IRepositoryQuery query) {
            var indexes = new HashSet<string>();

            var elasticIndexes = query.GetElasticIndexes();
            if (elasticIndexes.Count > 0)
                indexes.AddRange(elasticIndexes);

            var utcStart = query.GetElasticIndexesStartUtc();
            var utcEnd = query.GetElasticIndexesEndUtc();
            if (utcStart.HasValue || utcEnd.HasValue)
                indexes.AddRange(TimeSeriesIndex.GetIndexes(utcStart, utcEnd));

            return indexes;
        }

        protected ITimeSeriesIndex TimeSeriesIndex => (ITimeSeriesIndex)Index;
    }
}