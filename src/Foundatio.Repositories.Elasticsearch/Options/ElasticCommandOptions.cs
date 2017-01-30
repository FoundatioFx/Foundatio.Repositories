using System;
using System.Collections.Generic;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Models;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Elasticsearch.Options {
    public interface IElasticCommandOptions : ICommandOptions {
        IIndexType IndexType { get; }
        IIndex Index { get; }
        bool SupportsSoftDeletes { get; }
        bool HasIdentity { get; }
        ISet<string> DefaultExcludes { get; }
        bool HasParent { get; }
        bool ParentSupportsSoftDeletes { get; }
        IChildIndexType ChildType { get; }
        bool HasMultipleIndexes { get; }
        ITimeSeriesIndexType TimeSeriesType { get; }
        AliasResolver RootAliasResolver { get; set; }
    }

    public class ElasticCommandOptions : CommandOptions, IElasticCommandOptions, IElasticPagingOptions {
        public ElasticCommandOptions() {}

        public ElasticCommandOptions(IIndexType indexType) {
            IndexType = indexType;
            Index = indexType.Index;
            ChildType = indexType as IChildIndexType;
            TimeSeriesType = indexType as ITimeSeriesIndexType;
            HasParent = ChildType != null;
            HasMultipleIndexes = TimeSeriesType != null;
            SupportsSoftDeletes = typeof(ISupportSoftDeletes).IsAssignableFrom(indexType.Type);
            HasIdentity = typeof(IIdentity).IsAssignableFrom(indexType.Type);
            ParentSupportsSoftDeletes = ChildType != null && typeof(ISupportSoftDeletes).IsAssignableFrom(ChildType.GetParentIndexType().Type);
        }

        public IIndexType IndexType { get; }
        public bool SupportsSoftDeletes { get; }
        public bool HasIdentity { get; }
        public ISet<string> DefaultExcludes { get; set; }
        public IIndex Index { get; }
        public bool HasParent { get; }
        public bool ParentSupportsSoftDeletes { get; }
        public IChildIndexType ChildType { get; }
        public bool HasMultipleIndexes { get; }
        public ITimeSeriesIndexType TimeSeriesType { get; }
        public AliasResolver RootAliasResolver { get; set; }
        public bool UseSnapshotPaging { get; set; }
        public string ScrollId { get; set; }
        public TimeSpan? SnapshotLifetime { get; set; }
    }
}