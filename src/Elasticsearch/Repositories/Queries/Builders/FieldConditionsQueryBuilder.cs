using System;
using System.Collections.Generic;
using Nest;

namespace Foundatio.Elasticsearch.Repositories.Queries.Builders {
    public class FieldConditionsQueryBuilder : QueryBuilderBase {
        public override void BuildFilter<T>(object query, object options, ref QueryContainer container) {
            var fieldValuesQuery = query as IFieldConditionsQuery;
            if (fieldValuesQuery?.FieldConditions == null || fieldValuesQuery.FieldConditions.Count <= 0)
                return;

            foreach (var fieldValue in fieldValuesQuery.FieldConditions) {
                switch (fieldValue.Operator) {
                    case ComparisonOperator.Equals:
                        container &= new TermQuery { Field = fieldValue.Field, Value = fieldValue.Value };
                        break;
                    case ComparisonOperator.NotEquals:
                        container &= new BoolQuery { MustNot = new QueryContainer[] { new TermQuery { Field = fieldValue.Field, Value = fieldValue.Value } } };
                        break;
                    case ComparisonOperator.IsEmpty:
                        container &= new MissingQuery { Field = fieldValue.Field };
                        break;
                    case ComparisonOperator.HasValue:
                        container &= new ExistsQuery { Field = fieldValue.Field };
                        break;
                }
            }
        }
    }
}