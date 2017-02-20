using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Queries;
using Nest;
using IDateRangeQuery = Foundatio.Repositories.Elasticsearch.Queries.Builders.IDateRangeQuery;

namespace Foundatio.Repositories.Elasticsearch.Queries {
    public class Query : ISystemFilterQuery, IIdentityQuery, IDateRangeQuery,
        IFieldConditionsQuery, ISearchQuery, IAggregationQuery,
        IFieldIncludesQuery, ISortableQuery, IParentQuery, IChildQuery, ISoftDeletesQuery {

        public ISet<string> Ids { get; } = new HashSet<string>();
        public ISet<string> ExcludedIds { get; } = new HashSet<string>();
        public string CacheKey { get; set; }
        public TimeSpan? ExpiresIn { get; set; }
        public DateTime? ExpiresAt { get; set; }
        public ICollection<Builders.DateRange> DateRanges { get; } = new List<Builders.DateRange>();
        public ICollection<FieldCondition> FieldConditions { get; } = new List<FieldCondition>();
        public IRepositoryQuery SystemFilter { get; set; }
        public string Filter { get; set; }
        public string Criteria { get; set; }
        public SearchOperator DefaultCriteriaOperator { get; set; }
        public ICollection<Field> FieldIncludes { get; } = new List<Field>();
        public ICollection<Field> FieldExcludes { get; } = new List<Field>();
        public ICollection<IFieldSort> SortFields { get; } = new List<IFieldSort>();
        public string Sort { get; set; }
        public bool SortByScore { get; set; }
        public string Aggregations { get; set; }
        public IRepositoryQuery ParentQuery { get; set; }
        public ITypeQuery ChildQuery { get; set; }
        public SoftDeleteQueryMode? SoftDeleteMode { get; set; }
    }
}