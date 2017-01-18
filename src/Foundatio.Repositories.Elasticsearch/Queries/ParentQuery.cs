using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries {
    public class ParentQuery : IIdentityQuery, IDateRangeQuery, IFieldConditionsQuery, ISearchQuery, ISoftDeletesQuery {
        public ISet<string> Ids { get; } = new HashSet<string>();
        public ISet<string> ExcludedIds { get; } = new HashSet<string>();
        public ICollection<DateRange> DateRanges { get; } = new List<DateRange>();
        public ICollection<FieldCondition> FieldConditions { get; } = new List<FieldCondition>();
        public string Filter { get; set; }
        public string Criteria { get; set; }
        public string Sort { get; set; }
        public SearchOperator DefaultCriteriaOperator { get; set; }
        public SoftDeleteQueryMode? SoftDeleteMode { get; set; }
    }
}