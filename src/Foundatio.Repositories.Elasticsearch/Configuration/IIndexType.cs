﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IIndexType : IDisposable {
        string Name { get; }
        Type Type { get; }
        IIndex Index { get; }
        IElasticConfiguration Configuration { get; }
        int DefaultCacheExpirationSeconds { get; set; }
        int BulkBatchSize { get; set; }
        ISet<string> AllowedAggregationFields { get; }
        Task ConfigureAsync();
        AliasesDescriptor ConfigureIndexAliases(AliasesDescriptor aliases);
        MappingsDescriptor ConfigureIndexMappings(MappingsDescriptor mappings);
        IElasticQueryBuilder QueryBuilder { get; }
    }

    public interface IIndexType<T>: IIndexType where T : class {
        /// <summary>
        /// Creates a new document id. If a date can be resolved, it will be taken into account when creating a new id.
        /// </summary>
        string CreateDocumentId(T document);
    }

    public abstract class IndexTypeBase<T> : IIndexType<T> where T : class {
        protected static readonly bool HasIdentity = typeof(IIdentity).IsAssignableFrom(typeof(T));
        protected static readonly bool HasCreatedDate = typeof(IHaveCreatedDate).IsAssignableFrom(typeof(T));
        private readonly string _typeName = typeof(T).Name.ToLower();
        private readonly Lazy<IElasticQueryBuilder> _queryBuilder;

        public IndexTypeBase(IIndex index, string name = null) {
            if (index == null)
                throw new ArgumentNullException(nameof(index));

            Name = name ?? _typeName;
            Index = index;
            Type = typeof(T);
            _queryBuilder = new Lazy<IElasticQueryBuilder>(CreateQueryBuilder);
        }

        protected virtual IElasticQueryBuilder CreateQueryBuilder() {
            var builder = new ElasticQueryBuilder();

            Configuration.ConfigureGlobalQueryBuilders(builder);
            ConfigureQueryBuilder(builder);

            return builder;
        }

        protected virtual void ConfigureQueryBuilder(ElasticQueryBuilder builder) {}

        public string Name { get; }
        public Type Type { get; }
        public IIndex Index { get; }
        public IElasticConfiguration Configuration => Index.Configuration;
        public ISet<string> AllowedAggregationFields { get; } = new HashSet<string>();

        public virtual string CreateDocumentId(T document) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            if (HasIdentity) {
                var id = ((IIdentity)document).Id;
                if (!String.IsNullOrEmpty(id))
                    return id;
            }

            if (HasCreatedDate) {
                var date = ((IHaveCreatedDate)document).CreatedUtc;
                if (date != DateTime.MinValue)
                    return ObjectId.GenerateNewId(date).ToString();
            }

            return ObjectId.GenerateNewId().ToString();
        }

        public virtual Task ConfigureAsync() {
            return Task.CompletedTask;
        }

        public virtual AliasesDescriptor ConfigureIndexAliases(AliasesDescriptor aliases) {
            return aliases;
        }

        public virtual MappingsDescriptor ConfigureIndexMappings(MappingsDescriptor mappings) {
            return mappings.Map<T>(Name, BuildMapping);
        }

        public virtual TypeMappingDescriptor<T> BuildMapping(TypeMappingDescriptor<T> map) {
            return map.Properties(p => p.SetupDefaults());
        }

        public IElasticQueryBuilder QueryBuilder => _queryBuilder.Value;

        public virtual void Dispose() {}

        public int DefaultCacheExpirationSeconds { get; set; } = RepositoryConstants.DEFAULT_CACHE_EXPIRATION_SECONDS;
        public int BulkBatchSize { get; set; } = 1000;
    }

    public interface IHavePipelinedIndexType {
        string Pipeline { get; }
    }
}