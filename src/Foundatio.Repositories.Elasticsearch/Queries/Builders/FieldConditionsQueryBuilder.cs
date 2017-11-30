using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Nest;
using System.Collections;
using System.Linq.Expressions;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
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

    public static class FieldConditionQueryExtensions {
        internal const string FieldConditionsKey = "@FieldConditionsKey";

        public static T FieldCondition<T>(this T query, Field field, ComparisonOperator op, object value = null) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = value, Operator = op });
        }

        public static TQuery FieldConditionIf<TQuery, TValue>(this TQuery query, Field field, ComparisonOperator op, TValue value = default, Func<TValue, bool> condition = null) where TQuery : IRepositoryQuery {
            bool result = condition == null || condition(value);
            return result ? query.FieldCondition(field, op, value) : query;
        }

        public static TQuery FieldConditionIf<TQuery, TValue>(this TQuery query, Field field, ComparisonOperator op, TValue value = default, bool condition = true) where TQuery : IRepositoryQuery {
            return condition ? query.FieldCondition(field, op, value) : query;
        }

        public static IRepositoryQuery<T> FieldCondition<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath, ComparisonOperator op, object value = null) where T : class {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = op });
        }

        public static IRepositoryQuery<TModel> FieldConditionIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object>> objectPath, ComparisonOperator op, TValue value = default, Func<TValue, bool> condition = null) where TModel : class {
            bool result = condition == null || condition(value);
            return result ? query.FieldCondition(objectPath, op, value) : query;
        }

        public static IRepositoryQuery<TModel> FieldConditionIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object>> objectPath, ComparisonOperator op, TValue value = default, bool condition = true) where TModel : class {
            return condition ? query.FieldCondition(objectPath, op, value) : query;
        }


        public static T FieldEquals<T>(this T query, Field field, object value = null) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.Equals });
        }

        public static TQuery FieldEqualsIf<TQuery, TValue>(this TQuery query, Field field, TValue value = default, Func<TValue, bool> condition = null) where TQuery : IRepositoryQuery {
            bool result = condition == null || condition(value);
            return result ? query.FieldEquals(field, value) : query;
        }

        public static TQuery FieldEqualsIf<TQuery, TValue>(this TQuery query, Field field, TValue value = default, bool condition = true) where TQuery : IRepositoryQuery {
            return condition ? query.FieldEquals(field, value) : query;
        }

        public static IRepositoryQuery<T> FieldEquals<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath, object value = null) where T : class {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.Equals });
        }

        public static IRepositoryQuery<TModel> FieldEqualsIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object>> objectPath, TValue value = default, Func<TValue, bool> condition = null) where TModel : class {
            bool result = condition == null || condition(value);
            return result ? query.FieldEquals(objectPath, value) : query;
        }

        public static IRepositoryQuery<TModel> FieldEqualsIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object>> objectPath, TValue value = default, bool condition = true) where TModel : class {
            return condition ? query.FieldEquals(objectPath, value) : query;
        }


        public static T FieldNotEquals<T>(this T query, Field field, object value = null) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.NotEquals });
        }

        public static TQuery FieldNotEqualsIf<TQuery, TValue>(this TQuery query, Field field, TValue value = default, Func<TValue, bool> condition = null) where TQuery : IRepositoryQuery {
            bool result = condition == null || condition(value);
            return result ? query.FieldNotEquals(field, value) : query;
        }

        public static TQuery FieldNotEqualsIf<TQuery, TValue>(this TQuery query, Field field, TValue value = default, bool condition = true) where TQuery : IRepositoryQuery {
            return condition ? query.FieldNotEquals(field, value) : query;
        }

        public static IRepositoryQuery<T> FieldNotEquals<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath, object value = null) where T : class {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.NotEquals });
        }

        public static IRepositoryQuery<TModel> FieldNotEqualsIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object>> objectPath, TValue value = default, Func<TValue, bool> condition = null) where TModel : class {
            bool result = condition == null || condition(value);
            return result ? query.FieldNotEquals(objectPath, value) : query;
        }

        public static IRepositoryQuery<TModel> FieldNotEqualsIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object>> objectPath, TValue value = default, bool condition = true) where TModel : class {
            return condition ? query.FieldNotEquals(objectPath, value) : query;
        }


        public static T FieldHasValue<T>(this T query, Field field, object value = null) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.HasValue });
        }

        public static IRepositoryQuery<T> FieldHasValue<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath, object value = null) where T : class {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.HasValue });
        }

        public static T FieldEmpty<T>(this T query, Field field, object value = null) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.IsEmpty });
        }

        public static IRepositoryQuery<T> FieldEmpty<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath, object value = null) where T : class {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.IsEmpty });
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadFieldConditionQueryExtensions {
        public static ICollection<FieldCondition> GetFieldConditions(this IRepositoryQuery query) {
            return query.SafeGetCollection<FieldCondition>(FieldConditionQueryExtensions.FieldConditionsKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class FieldConditionsQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var fieldConditions = ctx.Source.SafeGetCollection<FieldCondition>(FieldConditionQueryExtensions.FieldConditionsKey);
            if (fieldConditions == null || fieldConditions.Count <= 0)
                return Task.CompletedTask;

            foreach (var fieldValue in fieldConditions) {
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
}