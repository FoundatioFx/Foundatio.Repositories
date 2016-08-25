using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries.Options {
    public interface IElasticQueryOptions : IQueryOptions {
        IIndexType IndexType { get; }
        IIndex Index { get; }
        bool SupportsSoftDeletes { get; }
        bool HasIdentity { get; }
        string[] AllowedAggregationFields { get; }
        string[] AllowedSortFields { get; }
        string[] DefaultExcludes { get; }
        bool HasParent { get; }
        bool ParentSupportsSoftDeletes { get; }
        IChildIndexType ChildType { get; }
        bool HasMultipleIndexes { get; }
        ITimeSeriesIndexType TimeSeriesType { get; }
    }

    public class ElasticQueryOptions : IElasticQueryOptions {
        public ElasticQueryOptions() {}

        public ElasticQueryOptions(IIndexType indexType) {
            IndexType = indexType;
            Index = indexType.Index;
            ChildType = indexType as IChildIndexType;
            TimeSeriesType = indexType as ITimeSeriesIndexType;
            HasParent = ChildType != null;
            HasMultipleIndexes = TimeSeriesType != null;
            ParentSupportsSoftDeletes = ChildType != null && typeof(ISupportSoftDeletes).IsAssignableFrom(ChildType.ParentType);
        }

        public IIndexType IndexType { get; }
        public bool SupportsSoftDeletes { get; set; }
        public bool HasIdentity { get; set; }
        public string[] AllowedAggregationFields { get; set; }
        public string[] AllowedSortFields { get; set; }
        public string[] DefaultExcludes { get; set; }

        public IIndex Index { get; }
        public bool HasParent { get; }
        public bool ParentSupportsSoftDeletes { get; }
        public IChildIndexType ChildType { get; }
        public bool HasMultipleIndexes { get; }
        public ITimeSeriesIndexType TimeSeriesType { get; }
    }
}