using System;
using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Queries;
using Foundatio.Repositories.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IElasticIndexType {
        string Name { get; }
        bool HasParent { get; }
        bool HasMultipleIndexes { get; }
        string ParentPath { get; }
        IElasticIndex Index { get; }
        string[] GetIndexesByQuery(object query);
        string GetIndexById(string id);
        int DefaultCacheExpirationSeconds { get; }
        int BulkBatchSize { get; }
        ISet<string> AllowedFacetFields { get; }
        CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx);
        PutTemplateDescriptor ConfigureTemplate(PutTemplateDescriptor idx);
    }

    public interface IElasticIndexType<T>: IElasticIndexType where T : class {
        string GetDocumentId(T document);
        string GetDocumentIndex(T document);
        string GetParentId(T document);
    }

    public class ElasticIndexType<T> : IElasticIndexType<T> where T : class {
        private readonly string TypeName = typeof(T).Name;

        public ElasticIndexType(string name = null, IElasticIndex index = null, bool hasParent = false, bool hasMultipleIndexes = false) {
            Name = name ?? TypeName;
            Index = index ?? new ElasticIndex(Name);
            HasParent = hasParent;
            HasMultipleIndexes = hasMultipleIndexes;
        }

        public string Name { get; }
        public bool HasParent { get; }
        public bool HasMultipleIndexes { get; }
        public string ParentPath { get; }
        public IElasticIndex Index { get; }
        public ISet<string> AllowedFacetFields { get; } = new HashSet<String>();

        public virtual string GetDocumentId(T document) {
            return ObjectId.GenerateNewId().ToString();
        }

        public virtual string GetDocumentIndex(T document) {
            return null;
        }

        public virtual string GetParentId(T document) {
            return null;
        }

        public virtual string[] GetIndexesByQuery(object query) {
            var withIndicesQuery = query as IElasticIndicesQuery;
            return withIndicesQuery?.Indices.ToArray();
        }

        public virtual string GetIndexById(string id) => null;

        public CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            return idx.AddMapping<T>(BuildMapping);
        }

        public PutTemplateDescriptor ConfigureTemplate(PutTemplateDescriptor template) {
            return template.AddMapping<T>(BuildMapping);
        }

        public virtual PutMappingDescriptor<T> BuildMapping(PutMappingDescriptor<T> map) {
            return map;
        }

        public int DefaultCacheExpirationSeconds { get; } = RepositoryConstants.DEFAULT_CACHE_EXPIRATION_SECONDS;
        public int BulkBatchSize { get; } = 1000;
    }
}