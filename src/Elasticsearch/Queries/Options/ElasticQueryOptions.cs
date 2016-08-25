using System;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries.Options {
    public interface IElasticQueryOptions : IQueryOptions {
        bool SupportsSoftDeletes { get; }
        bool HasIdentity { get; }
        string[] AllowedAggregationFields { get; }
        string[] AllowedSortFields { get; }
        string[] DefaultExcludes { get; }
    }

    public class ElasticQueryOptions : IElasticQueryOptions {
        public ElasticQueryOptions(Type entityType) {
            SupportsSoftDeletes = typeof(ISupportSoftDeletes).IsAssignableFrom(entityType);
            HasIdentity = typeof(IIdentity).IsAssignableFrom(entityType);
        }

        public bool SupportsSoftDeletes { get; protected set; }
        public bool HasIdentity { get; protected set; }
        public string[] AllowedAggregationFields { get; set; }
        public string[] AllowedSortFields { get; set; }
        public string[] DefaultExcludes { get; set; }
    }
}