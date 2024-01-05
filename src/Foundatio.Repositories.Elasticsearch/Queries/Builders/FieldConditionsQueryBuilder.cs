using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories
{
    public class FieldCondition
    {
        public Field Field { get; set; }
        public object Value { get; set; }
        public ComparisonOperator Operator { get; set; }
    }

    public enum ComparisonOperator
    {
        Equals,
        NotEquals,
        IsEmpty,
        HasValue,
        Contains,
        NotContains
    }

    public static class FieldConditionQueryExtensions
    {
        internal const string FieldConditionsKey = "@FieldConditionsKey";

        public static T FieldCondition<T>(this T query, Field field, ComparisonOperator op, object value) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = value, Operator = op });
        }

        public static T FieldCondition<T>(this T query, Field field, ComparisonOperator op, params object[] values) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = values, Operator = op });
        }

        public static TQuery FieldConditionIf<TQuery, TValue>(this TQuery query, Field field, ComparisonOperator op, TValue value = default, Func<TValue, bool> condition = null) where TQuery : IRepositoryQuery
        {
            bool result = condition == null || condition(value);
            return result ? query.FieldCondition(field, op, value) : query;
        }

        public static TQuery FieldConditionIf<TQuery, TValue>(this TQuery query, Field field, ComparisonOperator op, TValue value = default, bool condition = true) where TQuery : IRepositoryQuery
        {
            return condition ? query.FieldCondition(field, op, value) : query;
        }

        public static IRepositoryQuery<T> FieldCondition<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath, ComparisonOperator op, object value) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = op });
        }

        public static IRepositoryQuery<T> FieldCondition<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath, ComparisonOperator op, params object[] values) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = values, Operator = op });
        }

        public static IRepositoryQuery<TModel> FieldConditionIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object>> objectPath, ComparisonOperator op, TValue value = default, Func<TValue, bool> condition = null) where TModel : class
        {
            bool result = condition == null || condition(value);
            return result ? query.FieldCondition(objectPath, op, value) : query;
        }

        public static IRepositoryQuery<TModel> FieldConditionIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object>> objectPath, ComparisonOperator op, TValue value = default, bool condition = true) where TModel : class
        {
            return condition ? query.FieldCondition(objectPath, op, value) : query;
        }

        public static T FieldEquals<T>(this T query, Field field, object value) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.Equals });
        }

        public static T FieldEquals<T>(this T query, Field field, params object[] values) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = values, Operator = ComparisonOperator.Equals });
        }

        public static TQuery FieldEqualsIf<TQuery, TValue>(this TQuery query, Field field, TValue value = default, Func<TValue, bool> condition = null) where TQuery : IRepositoryQuery
        {
            bool result = condition == null || condition(value);
            return result ? query.FieldEquals(field, value) : query;
        }

        public static TQuery FieldEqualsIf<TQuery, TValue>(this TQuery query, Field field, TValue value = default, bool condition = true) where TQuery : IRepositoryQuery
        {
            return condition ? query.FieldEquals(field, value) : query;
        }

        public static IRepositoryQuery<T> FieldEquals<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath, object value) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.Equals });
        }

        public static IRepositoryQuery<T> FieldEquals<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath, params object[] values) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = values, Operator = ComparisonOperator.Equals });
        }

        public static IRepositoryQuery<TModel> FieldEqualsIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object>> objectPath, TValue value = default, Func<TValue, bool> condition = null) where TModel : class
        {
            bool result = condition == null || condition(value);
            return result ? query.FieldEquals(objectPath, value) : query;
        }

        public static IRepositoryQuery<TModel> FieldEqualsIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object>> objectPath, TValue value = default, bool condition = true) where TModel : class
        {
            return condition ? query.FieldEquals(objectPath, value) : query;
        }

        public static T FieldNotEquals<T>(this T query, Field field, object value) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.NotEquals });
        }

        public static T FieldNotEquals<T>(this T query, Field field, params object[] values) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = values, Operator = ComparisonOperator.NotEquals });
        }

        public static TQuery FieldNotEqualsIf<TQuery, TValue>(this TQuery query, Field field, TValue value = default, Func<TValue, bool> condition = null) where TQuery : IRepositoryQuery
        {
            bool result = condition == null || condition(value);
            return result ? query.FieldNotEquals(field, value) : query;
        }

        public static TQuery FieldNotEqualsIf<TQuery, TValue>(this TQuery query, Field field, TValue value = default, bool condition = true) where TQuery : IRepositoryQuery
        {
            return condition ? query.FieldNotEquals(field, value) : query;
        }

        public static IRepositoryQuery<T> FieldNotEquals<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath, object value) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.NotEquals });
        }

        public static IRepositoryQuery<T> FieldNotEquals<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath, params object[] values) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = values, Operator = ComparisonOperator.NotEquals });
        }

        public static IRepositoryQuery<TModel> FieldNotEqualsIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object>> objectPath, TValue value = default, Func<TValue, bool> condition = null) where TModel : class
        {
            bool result = condition == null || condition(value);
            return result ? query.FieldNotEquals(objectPath, value) : query;
        }

        public static IRepositoryQuery<TModel> FieldNotEqualsIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object>> objectPath, TValue value = default, bool condition = true) where TModel : class
        {
            return condition ? query.FieldNotEquals(objectPath, value) : query;
        }

        public static T FieldHasValue<T>(this T query, Field field) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Operator = ComparisonOperator.HasValue });
        }

        public static IRepositoryQuery<T> FieldHasValue<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Operator = ComparisonOperator.HasValue });
        }

        public static T FieldEmpty<T>(this T query, Field field) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Operator = ComparisonOperator.IsEmpty });
        }

        public static IRepositoryQuery<T> FieldEmpty<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Operator = ComparisonOperator.IsEmpty });
        }
    }
}

namespace Foundatio.Repositories.Options
{
    public static class ReadFieldConditionQueryExtensions
    {
        public static ICollection<FieldCondition> GetFieldConditions(this IRepositoryQuery query)
        {
            return query.SafeGetCollection<FieldCondition>(FieldConditionQueryExtensions.FieldConditionsKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders
{
    public class FieldConditionsQueryBuilder : IElasticQueryBuilder
    {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            var resolver = ctx.GetMappingResolver();

            var fieldConditions = ctx.Source.SafeGetCollection<FieldCondition>(FieldConditionQueryExtensions.FieldConditionsKey);
            if (fieldConditions == null || fieldConditions.Count <= 0)
                return Task.CompletedTask;

            foreach (var fieldValue in fieldConditions)
            {
                QueryBase query;
                if (fieldValue.Value == null && fieldValue.Operator == ComparisonOperator.Equals)
                    fieldValue.Operator = ComparisonOperator.IsEmpty;
                else if (fieldValue.Value == null && fieldValue.Operator == ComparisonOperator.NotEquals)
                    fieldValue.Operator = ComparisonOperator.HasValue;

                switch (fieldValue.Operator)
                {
                    case ComparisonOperator.Equals:
                        if (fieldValue.Value is IEnumerable && fieldValue.Value is not string)
                        {
                            var values = new List<object>();
                            foreach (var value in (IEnumerable)fieldValue.Value)
                                values.Add(value);
                            query = new TermsQuery { Field = resolver.GetNonAnalyzedFieldName(fieldValue.Field), Terms = values };
                        }
                        else
                            query = new TermQuery { Field = resolver.GetNonAnalyzedFieldName(fieldValue.Field), Value = fieldValue.Value };
                        ctx.Filter &= query;

                        break;
                    case ComparisonOperator.NotEquals:
                        if (fieldValue.Value is IEnumerable && fieldValue.Value is not string)
                        {
                            var values = new List<object>();
                            foreach (var value in (IEnumerable)fieldValue.Value)
                                values.Add(value);
                            query = new TermsQuery { Field = resolver.GetNonAnalyzedFieldName(fieldValue.Field), Terms = values };
                        }
                        else
                            query = new TermQuery { Field = resolver.GetNonAnalyzedFieldName(fieldValue.Field), Value = fieldValue.Value };

                        ctx.Filter &= new BoolQuery { MustNot = new QueryContainer[] { query } };
                        break;
                    case ComparisonOperator.Contains:
                        var fieldContains = resolver.GetResolvedField(fieldValue.Field);
                        if (!resolver.IsPropertyAnalyzed(fieldContains))
                            throw new InvalidOperationException($"Contains operator can't be used on non-analyzed field {fieldContains}");

                        if (fieldValue.Value is IEnumerable && fieldValue.Value is not string)
                        {
                            var sb = new StringBuilder();
                            foreach (var value in (IEnumerable)fieldValue.Value)
                                sb.Append(value.ToString()).Append(" ");
                            query = new MatchQuery { Field = fieldContains, Query = sb.ToString() };
                        }
                        else
                            query = new MatchQuery { Field = fieldContains, Query = fieldValue.Value.ToString() };
                        ctx.Filter &= query;

                        break;
                    case ComparisonOperator.NotContains:
                        var fieldNotContains = resolver.GetResolvedField(fieldValue.Field);
                        if (!resolver.IsPropertyAnalyzed(fieldNotContains))
                            throw new InvalidOperationException($"NotContains operator can't be used on non-analyzed field {fieldNotContains}");

                        if (fieldValue.Value is IEnumerable && fieldValue.Value is not string)
                        {
                            var sb = new StringBuilder();
                            foreach (var value in (IEnumerable)fieldValue.Value)
                                sb.Append(value.ToString()).Append(" ");
                            query = new MatchQuery { Field = fieldNotContains, Query = sb.ToString() };
                        }
                        else
                            query = new MatchQuery { Field = fieldNotContains, Query = fieldValue.Value.ToString() };

                        ctx.Filter &= new BoolQuery { MustNot = new QueryContainer[] { query } };
                        break;
                    case ComparisonOperator.IsEmpty:
                        ctx.Filter &= new BoolQuery { MustNot = new QueryContainer[] { new ExistsQuery { Field = resolver.GetResolvedField(fieldValue.Field) } } };
                        break;
                    case ComparisonOperator.HasValue:
                        ctx.Filter &= new ExistsQuery { Field = resolver.GetResolvedField(fieldValue.Field) };
                        break;
                }
            }

            return Task.CompletedTask;
        }
    }
}
