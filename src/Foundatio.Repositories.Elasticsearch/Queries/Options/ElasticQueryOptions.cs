using System;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries.Options {
    public interface IElasticQueryOptions : IQueryOptions {
        IIndexType IndexType { get; }
        IIndex Index { get; }
        bool SupportsSoftDeletes { get; }
        bool HasIdentity { get; }
        ISet<string> AllowedSearchFields { get; }
        ISet<string> AllowedAggregationFields { get; }
        ISet<string> AllowedSortFields { get; }
        ISet<string> DefaultExcludes { get; }
        bool HasParent { get; }
        bool ParentSupportsSoftDeletes { get; }
        IChildIndexType ChildType { get; }
        bool HasMultipleIndexes { get; }
        ITimeSeriesIndexType TimeSeriesType { get; }
        AliasResolver RootAliasResolver { get; set; }
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
            AllowedSearchFields = indexType.AllowedSearchFields;
            AllowedAggregationFields = indexType.AllowedAggregationFields;
            AllowedSortFields = indexType.AllowedSortFields;
            SupportsSoftDeletes = typeof(ISupportSoftDeletes).IsAssignableFrom(indexType.Type);
            HasIdentity = typeof(IIdentity).IsAssignableFrom(indexType.Type);
            ParentSupportsSoftDeletes = ChildType != null && typeof(ISupportSoftDeletes).IsAssignableFrom(ChildType.GetParentIndexType().Type);
        }

        public IIndexType IndexType { get; }
        public bool SupportsSoftDeletes { get; }
        public bool HasIdentity { get; }
        public ISet<string> AllowedSearchFields { get; set; }
        public ISet<string> AllowedAggregationFields { get; set; }
        public ISet<string> AllowedSortFields { get; set; }
        public ISet<string> DefaultExcludes { get; set; }

        public IIndex Index { get; }
        public bool HasParent { get; }
        public bool ParentSupportsSoftDeletes { get; }
        public IChildIndexType ChildType { get; }
        public bool HasMultipleIndexes { get; }
        public ITimeSeriesIndexType TimeSeriesType { get; }
        public AliasResolver RootAliasResolver { get; set; }
    }

    public static class ElasticQueryOptionsExtensions {
        public static bool CanSearchByField(this IQueryOptions options, string field) {
            var elasticOptions = options as IElasticQueryOptions;
            if (elasticOptions?.AllowedSearchFields == null || elasticOptions.AllowedSearchFields.Count == 0)
                return true;

            return elasticOptions.AllowedSearchFields.Contains(field);
        }

        public static bool CanSortByField(this IQueryOptions options, string field) {
            var elasticOptions = options as IElasticQueryOptions;
            if (elasticOptions?.AllowedSortFields == null || elasticOptions.AllowedSortFields.Count == 0)
                return true;

            return elasticOptions.AllowedSortFields.Contains(field);
        }

        public static bool CanAggregateByField(this IQueryOptions options, string field) {
            var elasticOptions = options as IElasticQueryOptions;
            if (elasticOptions?.AllowedAggregationFields == null || elasticOptions.AllowedAggregationFields.Count == 0)
                return true;

            return elasticOptions.AllowedAggregationFields.Contains(field);
        }
    }
}