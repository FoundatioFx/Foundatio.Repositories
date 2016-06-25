using System;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class FieldConditionsQueryBuilder : ElasticQueryBuilderBase {
        public override void BuildFilter<T>(object query, object options, ref FilterContainer container) {
            var fieldValuesQuery = query as IFieldConditionsQuery;
            if (fieldValuesQuery?.FieldConditions == null || fieldValuesQuery.FieldConditions.Count <= 0)
                return;

            foreach (var fieldValue in fieldValuesQuery.FieldConditions) {
                switch (fieldValue.Operator) {
                    case ComparisonOperator.Equals:
                        container &= new TermFilter { Field = fieldValue.Field, Value = fieldValue.Value };
                        break;
                    case ComparisonOperator.NotEquals:
                        container &= new NotFilter { Filter = FilterContainer.From(new TermFilter { Field = fieldValue.Field, Value = fieldValue.Value }) };
                        break;
                    case ComparisonOperator.IsEmpty:
                        container &= new MissingFilter { Field = fieldValue.Field };
                        break;
                    case ComparisonOperator.HasValue:
                        container &= new ExistsFilter { Field = fieldValue.Field };
                        break;
                }
            }
        }
    }
}