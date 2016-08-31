using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries {
    public class ChildQuery : ISystemFilterQuery, IDateRangeQuery, IFieldConditionsQuery, ISearchQuery, ITypeQuery {
        public ChildQuery() {
            DateRanges = new List<DateRange>();
            FieldConditions = new List<FieldCondition>();
        }

        public List<DateRange> DateRanges { get; }
        public List<FieldCondition> FieldConditions { get; }
        public IRepositoryQuery SystemFilter { get; set; }
        public string Filter { get; set; }
        public string Criteria { get; set; }
        public SearchOperator DefaultCriteriaOperator { get; set; }
        public string Type { get; set; }
    }
}
