using System;
using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries.Options {
    public interface IElasticQueryOptions : IQueryOptions {
        IIndexType IndexType { get; }
        IIndex Index { get; }
        bool SupportsSoftDeletes { get; }
        bool HasIdentity { get; }
        ISet<string> AllowedAggregationFields { get; }
        ISet<string> AllowedSortFields { get; }
        ISet<string> DefaultExcludes { get; }
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
            AllowedAggregationFields = indexType.AllowedAggregationFields;
            SupportsSoftDeletes = typeof(ISupportSoftDeletes).IsAssignableFrom(indexType.Type);
            HasIdentity = typeof(IIdentity).IsAssignableFrom(indexType.Type);
            ParentSupportsSoftDeletes = ChildType != null && typeof(ISupportSoftDeletes).IsAssignableFrom(ChildType.GetParentIndexType().Type);
        }

        public IIndexType IndexType { get; }
        public bool SupportsSoftDeletes { get; }
        public bool HasIdentity { get; }
        public ISet<string> AllowedAggregationFields { get; }
        public ISet<string> AllowedSortFields { get; set; }
        public ISet<string> DefaultExcludes { get; set; }

        public IIndex Index { get; }
        public bool HasParent { get; }
        public bool ParentSupportsSoftDeletes { get; }
        public IChildIndexType ChildType { get; }
        public bool HasMultipleIndexes { get; }
        public ITimeSeriesIndexType TimeSeriesType { get; }
    }
}