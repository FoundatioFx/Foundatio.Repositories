using System;
using System.Collections.Generic;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IFieldConditionsQuery {
        ICollection<FieldCondition> FieldConditions { get; }
    }

    public class FieldCondition {
        public string Field { get; set; }
        public object Value { get; set; }
        public ComparisonOperator Operator { get; set; }
    }

    public enum ComparisonOperator {
        Equals,
        NotEquals,
        IsEmpty,
        HasValue
    }

    public class FieldConditionsQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var fieldValuesQuery = ctx.GetSourceAs<IFieldConditionsQuery>();
            if (fieldValuesQuery?.FieldConditions == null || fieldValuesQuery.FieldConditions.Count <= 0)
                return;

            foreach (var fieldValue in fieldValuesQuery.FieldConditions) {
                switch (fieldValue.Operator) {
                    case ComparisonOperator.Equals:
                        ctx.Filter &= new TermFilter { Field = fieldValue.Field, Value = fieldValue.Value };
                        break;
                    case ComparisonOperator.NotEquals:
                        ctx.Filter &= new NotFilter { Filter = FilterContainer.From(new TermFilter { Field = fieldValue.Field, Value = fieldValue.Value }) };
                        break;
                    case ComparisonOperator.IsEmpty:
                        ctx.Filter &= new MissingFilter { Field = fieldValue.Field };
                        break;
                    case ComparisonOperator.HasValue:
                        ctx.Filter &= new ExistsFilter { Field = fieldValue.Field };
                        break;
                }
            }
        }
    }

    public static class FieldValueQueryExtensions {
        public static T WithFieldEquals<T>(this T query, string field, object value) where T : IFieldConditionsQuery {
            query.FieldConditions?.Add(new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.Equals });
            return query;
        }

        public static T WithFieldNotEquals<T>(this T query, string field, object value) where T : IFieldConditionsQuery {
            query.FieldConditions?.Add(new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.NotEquals });
            return query;
        }

        public static T WithEmptyField<T>(this T query, string field) where T : IFieldConditionsQuery {
            query.FieldConditions?.Add(new FieldCondition { Field = field, Operator = ComparisonOperator.IsEmpty });
            return query;
        }

        public static T WithNonEmptyField<T>(this T query, string field) where T : IFieldConditionsQuery {
            query.FieldConditions?.Add(new FieldCondition { Field = field, Operator = ComparisonOperator.HasValue });
            return query;
        }
    }
}