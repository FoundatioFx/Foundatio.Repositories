using System;
using System.Collections.Generic;
using Foundatio.Repositories.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IIndexType {
        string Name { get; }
        IIndex Index { get; }
        int DefaultCacheExpirationSeconds { get; set; }
        int BulkBatchSize { get; set; }
        ISet<string> AllowedAggregationFields { get; }
        CreateIndexDescriptor Configure(CreateIndexDescriptor idx);
    }

    public interface ITemplatedIndexType {
        PutTemplateDescriptor ConfigureTemplate(PutTemplateDescriptor idx);
    }

    public interface IIndexType<T>: IIndexType where T : class {
        string GetDocumentId(T document);
    }

    public class IndexType<T> : IIndexType<T> where T : class {
        private readonly string _typeName = typeof(T).Name.ToLower();

        public IndexType(IIndex index, string name = null) {
            if (index == null)
                throw new ArgumentNullException(nameof(index));

            Name = name ?? _typeName;
            Index = index;
        }

        public string Name { get; }
        public IIndex Index { get; }
        public ISet<string> AllowedAggregationFields { get; } = new HashSet<string>();

        public virtual string GetDocumentId(T document) {
            return ObjectId.GenerateNewId().ToString();
        }

        public CreateIndexDescriptor Configure(CreateIndexDescriptor idx) {
            return idx.AddMapping<T>(BuildMapping);
        }

        public virtual PutMappingDescriptor<T> BuildMapping(PutMappingDescriptor<T> map) {
            return map;
        }

        public int DefaultCacheExpirationSeconds { get; set; } = RepositoryConstants.DEFAULT_CACHE_EXPIRATION_SECONDS;
        public int BulkBatchSize { get; set; } = 1000;
    }
}