using System;
using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries {
    public class Query : ISystemFilterQuery, IIdentityQuery, ICachableQuery, IDateRangeQuery,
        IFieldConditionsQuery, IPagableQuery, ISearchQuery, IAggregationQuery,
        ISelectedFieldsQuery, ISortableQuery, IParentQuery, IChildQuery, ISoftDeletesQuery {

        public ISet<string> Ids { get; } = new HashSet<string>();
        public ISet<string> ExcludedIds { get; } = new HashSet<string>();
        public string CacheKey { get; set; }
        public TimeSpan? ExpiresIn { get; set; }
        public DateTime? ExpiresAt { get; set; }
        public ICollection<DateRange> DateRanges { get; } = new List<DateRange>();
        public ICollection<FieldCondition> FieldConditions { get; } = new List<FieldCondition>();
        public IRepositoryQuery SystemFilter { get; set; }
        public string Filter { get; set; }
        public string Criteria { get; set; }
        public SearchOperator DefaultCriteriaOperator { get; set; }
        public ICollection<string> SelectedFields { get; } = new List<string>();
        public ICollection<FieldSort> SortBy { get; } = new List<FieldSort>();
        public bool SortByScore { get; set; }
        public ICollection<AggregationField> AggregationFields { get; } = new List<AggregationField>();
        public IRepositoryQuery ParentQuery { get; set; }
        public ITypeQuery ChildQuery { get; set; }
        public bool IncludeSoftDeletes { get; set; }
        public IPagingOptions Options { get; set; }
    }
}