using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories
{
    /// <summary>
    /// Extension methods for adding field conditions to repository queries.
    /// These are the primary API for building type-safe Elasticsearch filter clauses without
    /// dropping to raw Elasticsearch <c>Query</c> objects.
    /// </summary>
    public static class FieldConditionQueryExtensions
    {
        internal const string FieldConditionsKey = "@FieldConditionsKey";
        internal const string FieldConditionGroupsKey = "@FieldConditionGroupsKey";

        public static T FieldCondition<T>(this T query, Field field, ComparisonOperator op, object? value) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = value, Operator = op });
        }

        public static T FieldCondition<T>(this T query, Field field, ComparisonOperator op, params object[] values) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = values, Operator = op });
        }

        public static TQuery FieldConditionIf<TQuery, TValue>(this TQuery query, Field field, ComparisonOperator op, TValue? value = default, Func<TValue?, bool>? condition = null) where TQuery : IRepositoryQuery
        {
            bool result = condition is null || condition(value);
            return result ? query.FieldCondition(field, op, value) : query;
        }

        public static TQuery FieldConditionIf<TQuery, TValue>(this TQuery query, Field field, ComparisonOperator op, TValue? value = default, bool condition = true) where TQuery : IRepositoryQuery
        {
            return condition ? query.FieldCondition(field, op, value) : query;
        }

        public static IRepositoryQuery<T> FieldCondition<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath, ComparisonOperator op, object? value) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = op });
        }

        public static IRepositoryQuery<T> FieldCondition<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath, ComparisonOperator op, params object[] values) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = values, Operator = op });
        }

        public static IRepositoryQuery<TModel> FieldConditionIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, ComparisonOperator op, TValue? value = default, Func<TValue?, bool>? condition = null) where TModel : class
        {
            bool result = condition is null || condition(value);
            return result ? query.FieldCondition(objectPath, op, value) : query;
        }

        public static IRepositoryQuery<TModel> FieldConditionIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, ComparisonOperator op, TValue? value = default, bool condition = true) where TModel : class
        {
            return condition ? query.FieldCondition(objectPath, op, value) : query;
        }

        /// <summary>
        /// Full-text token match on an analyzed field. Generates <c>MatchQuery { Operator = And }</c>.
        /// All tokens in the value must be present in the field (order-independent).
        /// </summary>
        /// <remarks>
        /// <para>This is NOT wildcard or substring matching. <c>FieldContains(f =&gt; f.Name, "Er")</c>
        /// will NOT match "Eric" because "er" is not a complete token.</para>
        /// <para>Throws <see cref="Foundatio.Repositories.Exceptions.QueryValidationException"/> at build time if the field
        /// is a non-analyzed (keyword) field.</para>
        /// </remarks>
        public static IRepositoryQuery<T> FieldContains<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath, object? value) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.Contains });
        }

        /// <summary>
        /// Full-text token match with multiple values joined as space-separated tokens.
        /// All tokens must be present (AND semantics, order-independent).
        /// </summary>
        public static IRepositoryQuery<T> FieldContains<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath, params object[] values) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = values, Operator = ComparisonOperator.Contains });
        }

        public static IRepositoryQuery<TModel> FieldContainsIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, TValue? value = default, Func<TValue?, bool>? condition = null) where TModel : class
        {
            bool result = condition is null || condition(value);
            return result ? query.FieldContains(objectPath, value) : query;
        }

        public static IRepositoryQuery<TModel> FieldContainsIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, TValue? value = default, bool condition = true) where TModel : class
        {
            return condition ? query.FieldContains(objectPath, value) : query;
        }

        /// <summary>
        /// Negated full-text token match. Generates <c>BoolQuery { MustNot = MatchQuery }</c>.
        /// </summary>
        public static IRepositoryQuery<T> FieldNotContains<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath, object? value) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.NotContains });
        }

        public static IRepositoryQuery<T> FieldNotContains<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath, params object[] values) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = values, Operator = ComparisonOperator.NotContains });
        }

        public static IRepositoryQuery<TModel> FieldNotContainsIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, TValue? value = default, Func<TValue?, bool>? condition = null) where TModel : class
        {
            bool result = condition is null || condition(value);
            return result ? query.FieldNotContains(objectPath, value) : query;
        }

        public static IRepositoryQuery<TModel> FieldNotContainsIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, TValue? value = default, bool condition = true) where TModel : class
        {
            return condition ? query.FieldNotContains(objectPath, value) : query;
        }

        public static T FieldEmpty<T>(this T query, Field field) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Operator = ComparisonOperator.IsEmpty });
        }

        public static TQuery FieldEmptyIf<TQuery>(this TQuery query, Field field, bool condition) where TQuery : IRepositoryQuery
        {
            return condition ? query.FieldEmpty(field) : query;
        }

        public static IRepositoryQuery<T> FieldEmpty<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Operator = ComparisonOperator.IsEmpty });
        }

        public static IRepositoryQuery<T> FieldEmptyIf<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath, bool condition) where T : class
        {
            return condition ? query.FieldEmpty(objectPath) : query;
        }

        /// <summary>
        /// Filters to documents where the field exists and has a value. Generates <c>ExistsQuery</c>.
        /// </summary>
        public static T FieldHasValue<T>(this T query, Field field) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Operator = ComparisonOperator.HasValue });
        }

        public static TQuery FieldHasValueIf<TQuery>(this TQuery query, Field field, bool condition) where TQuery : IRepositoryQuery
        {
            return condition ? query.FieldHasValue(field) : query;
        }

        public static IRepositoryQuery<T> FieldHasValue<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Operator = ComparisonOperator.HasValue });
        }

        public static IRepositoryQuery<T> FieldHasValueIf<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath, bool condition) where T : class
        {
            return condition ? query.FieldHasValue(objectPath) : query;
        }

        /// <summary>
        /// Exact match filter on the non-analyzed (<c>.keyword</c>) field.
        /// Generates <c>TermQuery</c> (single value) or <c>TermsQuery</c> (multiple values).
        /// Null values are rewritten to <see cref="ComparisonOperator.IsEmpty"/>.
        /// </summary>
        /// <remarks>
        /// <para><b>Target field type:</b> keyword, numeric, boolean, date. For full-text search
        /// on analyzed fields, use <c>FieldContains</c> instead.</para>
        /// <para><b>Throws</b> <see cref="Foundatio.Repositories.Exceptions.QueryValidationException"/> at build time if the
        /// field is an analyzed text field with no <c>.keyword</c> sub-field.</para>
        /// </remarks>
        public static T FieldEquals<T>(this T query, Field field, object? value) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.Equals });
        }

        public static T FieldEquals<T>(this T query, Field field, params object[] values) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = values, Operator = ComparisonOperator.Equals });
        }

        public static TQuery FieldEqualsIf<TQuery, TValue>(this TQuery query, Field field, TValue? value = default, Func<TValue?, bool>? condition = null) where TQuery : IRepositoryQuery
        {
            bool result = condition is null || condition(value);
            return result ? query.FieldEquals(field, value) : query;
        }

        public static TQuery FieldEqualsIf<TQuery, TValue>(this TQuery query, Field field, TValue? value = default, bool condition = true) where TQuery : IRepositoryQuery
        {
            return condition ? query.FieldEquals(field, value) : query;
        }

        public static IRepositoryQuery<T> FieldEquals<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath, object? value) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.Equals });
        }

        public static IRepositoryQuery<T> FieldEquals<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath, params object[] values) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = values, Operator = ComparisonOperator.Equals });
        }

        public static IRepositoryQuery<TModel> FieldEqualsIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, TValue? value = default, Func<TValue?, bool>? condition = null) where TModel : class
        {
            bool result = condition is null || condition(value);
            return result ? query.FieldEquals(objectPath, value) : query;
        }

        public static IRepositoryQuery<TModel> FieldEqualsIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, TValue? value = default, bool condition = true) where TModel : class
        {
            return condition ? query.FieldEquals(objectPath, value) : query;
        }

        /// <summary>
        /// Negated exact match. Generates <c>BoolQuery { MustNot = TermQuery/TermsQuery }</c>
        /// on the non-analyzed (<c>.keyword</c>) field.
        /// Null values are rewritten to <see cref="ComparisonOperator.HasValue"/>.
        /// </summary>
        public static T FieldNotEquals<T>(this T query, Field field, object? value) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.NotEquals });
        }

        public static T FieldNotEquals<T>(this T query, Field field, params object[] values) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = field, Value = values, Operator = ComparisonOperator.NotEquals });
        }

        public static TQuery FieldNotEqualsIf<TQuery, TValue>(this TQuery query, Field field, TValue? value = default, Func<TValue?, bool>? condition = null) where TQuery : IRepositoryQuery
        {
            bool result = condition is null || condition(value);
            return result ? query.FieldNotEquals(field, value) : query;
        }

        public static TQuery FieldNotEqualsIf<TQuery, TValue>(this TQuery query, Field field, TValue? value = default, bool condition = true) where TQuery : IRepositoryQuery
        {
            return condition ? query.FieldNotEquals(field, value) : query;
        }

        public static IRepositoryQuery<T> FieldNotEquals<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath, object? value) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.NotEquals });
        }

        public static IRepositoryQuery<T> FieldNotEquals<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath, params object[] values) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = values, Operator = ComparisonOperator.NotEquals });
        }

        public static IRepositoryQuery<TModel> FieldNotEqualsIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, TValue? value = default, Func<TValue?, bool>? condition = null) where TModel : class
        {
            bool result = condition is null || condition(value);
            return result ? query.FieldNotEquals(objectPath, value) : query;
        }

        public static IRepositoryQuery<TModel> FieldNotEqualsIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, TValue? value = default, bool condition = true) where TModel : class
        {
            return condition ? query.FieldNotEquals(objectPath, value) : query;
        }

        /// <summary>
        /// Strictly greater than comparison. Generates <c>DateRangeQuery</c> (DateTime/DateTimeOffset),
        /// <c>NumericRangeQuery</c> (numeric types), or <c>TermRangeQuery</c> (string).
        /// </summary>
        public static IRepositoryQuery<T> FieldGreaterThan<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath, object? value) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.GreaterThan });
        }

        public static IRepositoryQuery<TModel> FieldGreaterThanIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, TValue? value = default, Func<TValue?, bool>? condition = null) where TModel : class
        {
            bool result = condition is null || condition(value);
            return result ? query.FieldGreaterThan(objectPath, value) : query;
        }

        public static IRepositoryQuery<TModel> FieldGreaterThanIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, TValue? value = default, bool condition = true) where TModel : class
        {
            return condition ? query.FieldGreaterThan(objectPath, value) : query;
        }

        /// <summary>
        /// Greater than or equal comparison.
        /// </summary>
        public static IRepositoryQuery<T> FieldGreaterThanOrEqual<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath, object? value) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.GreaterThanOrEqual });
        }

        public static IRepositoryQuery<TModel> FieldGreaterThanOrEqualIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, TValue? value = default, Func<TValue?, bool>? condition = null) where TModel : class
        {
            bool result = condition is null || condition(value);
            return result ? query.FieldGreaterThanOrEqual(objectPath, value) : query;
        }

        public static IRepositoryQuery<TModel> FieldGreaterThanOrEqualIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, TValue? value = default, bool condition = true) where TModel : class
        {
            return condition ? query.FieldGreaterThanOrEqual(objectPath, value) : query;
        }

        /// <summary>
        /// Strictly less than comparison.
        /// </summary>
        public static IRepositoryQuery<T> FieldLessThan<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath, object? value) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.LessThan });
        }

        public static IRepositoryQuery<TModel> FieldLessThanIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, TValue? value = default, Func<TValue?, bool>? condition = null) where TModel : class
        {
            bool result = condition is null || condition(value);
            return result ? query.FieldLessThan(objectPath, value) : query;
        }

        public static IRepositoryQuery<TModel> FieldLessThanIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, TValue? value = default, bool condition = true) where TModel : class
        {
            return condition ? query.FieldLessThan(objectPath, value) : query;
        }

        /// <summary>
        /// Less than or equal comparison.
        /// </summary>
        public static IRepositoryQuery<T> FieldLessThanOrEqual<T>(this IRepositoryQuery<T> query, Expression<Func<T, object?>> objectPath, object? value) where T : class
        {
            return query.AddCollectionOptionValue(FieldConditionsKey, new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.LessThanOrEqual });
        }

        public static IRepositoryQuery<TModel> FieldLessThanOrEqualIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, TValue? value = default, Func<TValue?, bool>? condition = null) where TModel : class
        {
            bool result = condition is null || condition(value);
            return result ? query.FieldLessThanOrEqual(objectPath, value) : query;
        }

        public static IRepositoryQuery<TModel> FieldLessThanOrEqualIf<TModel, TValue>(this IRepositoryQuery<TModel> query, Expression<Func<TModel, object?>> objectPath, TValue? value = default, bool condition = true) where TModel : class
        {
            return condition ? query.FieldLessThanOrEqual(objectPath, value) : query;
        }

        /// <summary>
        /// Adds an AND group. All child conditions must match.
        /// Generates <c>BoolQuery { Must = [...] }</c>.
        /// </summary>
        /// <remarks>
        /// At the top query level, AND is the default behavior. This method is available
        /// for explicitness and for nesting inside OR/NOT groups.
        /// </remarks>
        public static IRepositoryQuery<T> FieldAnd<T>(this IRepositoryQuery<T> query, Action<FieldConditionGroup<T>> configure) where T : class
        {
            ArgumentNullException.ThrowIfNull(configure);
            var group = new FieldConditionGroup<T>(FieldConditionGroupOperator.And);
            configure(group);
            return query.AddCollectionOptionValue(FieldConditionGroupsKey, (FieldConditionGroup)group);
        }

        public static IRepositoryQuery<T> FieldAnd<T>(this IRepositoryQuery<T> query, FieldConditionGroup<T> group) where T : class
        {
            ArgumentNullException.ThrowIfNull(group);
            return query.AddCollectionOptionValue(FieldConditionGroupsKey, (FieldConditionGroup)group);
        }

        /// <summary>
        /// Adds a NOT group. No child condition must match (AND-NOT semantics).
        /// Generates <c>BoolQuery { MustNot = [...] }</c>.
        /// </summary>
        /// <remarks>
        /// Multiple conditions inside NOT produce NOT A AND NOT B (exclude documents matching ANY clause).
        /// For NOT (A AND B), nest an explicit AND group inside NOT.
        /// </remarks>
        public static IRepositoryQuery<T> FieldNot<T>(this IRepositoryQuery<T> query, Action<FieldConditionGroup<T>> configure) where T : class
        {
            ArgumentNullException.ThrowIfNull(configure);
            var group = new FieldConditionGroup<T>(FieldConditionGroupOperator.Not);
            configure(group);
            return query.AddCollectionOptionValue(FieldConditionGroupsKey, (FieldConditionGroup)group);
        }

        public static IRepositoryQuery<T> FieldNot<T>(this IRepositoryQuery<T> query, FieldConditionGroup<T> group) where T : class
        {
            ArgumentNullException.ThrowIfNull(group);
            return query.AddCollectionOptionValue(FieldConditionGroupsKey, (FieldConditionGroup)group);
        }

        /// <summary>
        /// Adds an OR group where at least one child condition must match.
        /// Generates <c>BoolQuery { Should = [...] }</c>.
        /// </summary>
        public static IRepositoryQuery<T> FieldOr<T>(this IRepositoryQuery<T> query, Action<FieldConditionGroup<T>> configure) where T : class
        {
            ArgumentNullException.ThrowIfNull(configure);
            var group = new FieldConditionGroup<T>(FieldConditionGroupOperator.Or);
            configure(group);
            return query.AddCollectionOptionValue(FieldConditionGroupsKey, (FieldConditionGroup)group);
        }

        /// <summary>
        /// Adds a pre-built OR group. Use for dynamic/conditional group building.
        /// </summary>
        public static IRepositoryQuery<T> FieldOr<T>(this IRepositoryQuery<T> query, FieldConditionGroup<T> group) where T : class
        {
            ArgumentNullException.ThrowIfNull(group);
            return query.AddCollectionOptionValue(FieldConditionGroupsKey, (FieldConditionGroup)group);
        }

        // Untyped IRepositoryQuery overloads

        public static T FieldAnd<T>(this T query, Action<FieldConditionGroup> configure) where T : IRepositoryQuery
        {
            ArgumentNullException.ThrowIfNull(configure);
            var group = new FieldConditionGroup(FieldConditionGroupOperator.And);
            configure(group);
            return query.AddCollectionOptionValue(FieldConditionGroupsKey, group);
        }

        public static T FieldAnd<T>(this T query, FieldConditionGroup group) where T : IRepositoryQuery
        {
            ArgumentNullException.ThrowIfNull(group);
            return query.AddCollectionOptionValue(FieldConditionGroupsKey, group);
        }

        public static T FieldNot<T>(this T query, Action<FieldConditionGroup> configure) where T : IRepositoryQuery
        {
            ArgumentNullException.ThrowIfNull(configure);
            var group = new FieldConditionGroup(FieldConditionGroupOperator.Not);
            configure(group);
            return query.AddCollectionOptionValue(FieldConditionGroupsKey, group);
        }

        public static T FieldNot<T>(this T query, FieldConditionGroup group) where T : IRepositoryQuery
        {
            ArgumentNullException.ThrowIfNull(group);
            return query.AddCollectionOptionValue(FieldConditionGroupsKey, group);
        }

        public static T FieldOr<T>(this T query, Action<FieldConditionGroup> configure) where T : IRepositoryQuery
        {
            ArgumentNullException.ThrowIfNull(configure);
            var group = new FieldConditionGroup(FieldConditionGroupOperator.Or);
            configure(group);
            return query.AddCollectionOptionValue(FieldConditionGroupsKey, group);
        }

        public static T FieldOr<T>(this T query, FieldConditionGroup group) where T : IRepositoryQuery
        {
            ArgumentNullException.ThrowIfNull(group);
            return query.AddCollectionOptionValue(FieldConditionGroupsKey, group);
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

        public static ICollection<FieldConditionGroup> GetFieldConditionGroups(this IRepositoryQuery query)
        {
            return query.SafeGetCollection<FieldConditionGroup>(FieldConditionQueryExtensions.FieldConditionGroupsKey);
        }
    }
}
