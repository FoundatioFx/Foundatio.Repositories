using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Nest;
using System.Collections;
using System.Linq.Expressions;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IFieldConditionsQuery : IRepositoryQuery {
        ICollection<FieldCondition> FieldConditions { get; }
    }

    public class FieldCondition {
        public Field Field { get; set; }
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
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var fieldValuesQuery = ctx.GetSourceAs<IFieldConditionsQuery>();
            if (fieldValuesQuery?.FieldConditions == null || fieldValuesQuery.FieldConditions.Count <= 0)
                return Task.CompletedTask;

            foreach (var fieldValue in fieldValuesQuery.FieldConditions) {
                QueryBase query;
                switch (fieldValue.Operator) {
                    case ComparisonOperator.Equals:
                        if (fieldValue.Value is IEnumerable && !(fieldValue.Value is string))
                            query = new TermsQuery { Field = fieldValue.Field, Terms = (IEnumerable<object>)fieldValue.Value };
                        else
                            query = new TermQuery { Field = fieldValue.Field, Value = fieldValue.Value };
                        ctx.Filter &= query;

                        break;
                    case ComparisonOperator.NotEquals:
                        if (fieldValue.Value is IEnumerable && !(fieldValue.Value is string))
                            query = new TermsQuery { Field = fieldValue.Field, Terms = (IEnumerable<object>)fieldValue.Value };
                        else
                            query = new TermQuery { Field = fieldValue.Field, Value = fieldValue.Value };

                        ctx.Filter &= new BoolQuery { MustNot = new QueryContainer[] { query } };
                        break;
                    case ComparisonOperator.IsEmpty:
                        ctx.Filter &= new BoolQuery { MustNot = new QueryContainer[] { new ExistsQuery { Field = fieldValue.Field } } };
                        break;
                    case ComparisonOperator.HasValue:
                        ctx.Filter &= new ExistsQuery { Field = fieldValue.Field };
                        break;
                }
            }

            return Task.CompletedTask;
        }
    }

    public static class FieldValueQueryExtensions {
        public static T WithFieldCondition<T>(this T query, Field field, ComparisonOperator op, object value = null) where T : IFieldConditionsQuery {
            query.FieldConditions?.Add(new FieldCondition { Field = field, Value = value, Operator = op });
            return query;
        }

        public static T WithFieldEquals<T>(this T query, Field field, object value) where T : IFieldConditionsQuery {
            return query.WithFieldCondition(field, ComparisonOperator.Equals, value);
        }

        public static T WithFieldNotEquals<T>(this T query, Field field, object value) where T : IFieldConditionsQuery {
            return query.WithFieldCondition(field, ComparisonOperator.NotEquals, value);
        }

        public static T WithEmptyField<T>(this T query, Field field) where T : IFieldConditionsQuery {
            return query.WithFieldCondition(field, ComparisonOperator.IsEmpty);
        }

        public static T WithNonEmptyField<T>(this T query, Field field) where T : IFieldConditionsQuery {
            return query.WithFieldCondition(field, ComparisonOperator.HasValue);
        }

        public static T WithFieldCondition<T, TModel>(this T query, Expression<Func<TModel, object>> objectPath, ComparisonOperator op, object value = null) where T : IFieldConditionsQuery {
            query.FieldConditions?.Add(new FieldCondition { Field = objectPath, Value = value, Operator = op });
            return query;
        }

        public static T WithFieldEquals<T, TModel>(this T query, Expression<Func<TModel, object>> objectPath, object value) where T : IFieldConditionsQuery {
            return query.WithFieldCondition(objectPath, ComparisonOperator.Equals, value);
        }

        public static T WithFieldNotEquals<T, TModel>(this T query, Expression<Func<TModel, object>> objectPath, object value) where T : IFieldConditionsQuery {
            return query.WithFieldCondition(objectPath, ComparisonOperator.NotEquals, value);
        }

        public static T WithEmptyField<T, TModel>(this T query, Expression<Func<TModel, object>> objectPath) where T : IFieldConditionsQuery {
            return query.WithFieldCondition(objectPath, ComparisonOperator.IsEmpty);
        }

        public static T WithNonEmptyField<T, TModel>(this T query, Expression<Func<TModel, object>> objectPath) where T : IFieldConditionsQuery {
            return query.WithFieldCondition(objectPath, ComparisonOperator.HasValue);
        }
    }
}