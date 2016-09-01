using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries {
    public class ParentQuery : ISystemFilterQuery, IIdentityQuery, IDateRangeQuery, IFieldConditionsQuery, ISearchQuery, ITypeQuery, ISoftDeletesQuery {
        public ISet<string> Ids { get; } = new HashSet<string>();
        public ISet<string> ExcludedIds { get; } = new HashSet<string>();
        public string Type { get; set; }
        public ICollection<DateRange> DateRanges { get; } = new List<DateRange>();
        public ICollection<FieldCondition> FieldConditions { get; } = new List<FieldCondition>();
        public IRepositoryQuery SystemFilter { get; set; }
        public string Filter { get; set; }
        public string Criteria { get; set; }
        public SearchOperator DefaultCriteriaOperator { get; set; }
        public bool IncludeSoftDeletes { get; set; }
    }
}