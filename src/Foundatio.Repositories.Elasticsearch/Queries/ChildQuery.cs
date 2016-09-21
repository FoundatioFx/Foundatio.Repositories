using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries {
    public class ChildQuery : ISystemFilterQuery, IDateRangeQuery, IFieldConditionsQuery, ISearchQuery, ITypeQuery {
        public ICollection<DateRange> DateRanges { get; } = new List<DateRange>();
        public ICollection<FieldCondition> FieldConditions { get; } = new List<FieldCondition>();
        public IRepositoryQuery SystemFilter { get; set; }
        public string Filter { get; set; }
        public string Criteria { get; set; }
        public SearchOperator DefaultCriteriaOperator { get; set; }
        public string Type { get; set; }
    }
}
