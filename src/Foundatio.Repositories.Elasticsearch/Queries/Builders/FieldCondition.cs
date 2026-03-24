using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Nest;

namespace Foundatio.Repositories;

/// <summary>
/// Represents a single field-level comparison condition used to build
/// Elasticsearch filter clauses via the repository query pipeline.
/// </summary>
/// <remarks>
/// Each condition specifies a <see cref="Field"/>, an <see cref="Operator"/>,
/// and an optional <see cref="Value"/>. The Elasticsearch query builder
/// translates these into the appropriate Elasticsearch query (TermQuery, TermsQuery,
/// MatchQuery, ExistsQuery, DateRangeQuery, NumericRangeQuery, etc.) at build time.
/// </remarks>
public class FieldCondition
{
    public Field Field { get; set; }
    public object Value { get; set; }
    public ComparisonOperator Operator { get; set; }
}

/// <summary>
/// Defines the type of comparison a <see cref="FieldCondition"/> performs.
/// </summary>
/// <remarks>
/// <para><b>Operator decision tree:</b></para>
/// <list type="bullet">
///   <item><b>Exact match on a specific value?</b> → <see cref="Equals"/> (keyword field, case-sensitive)</item>
///   <item><b>Full-text search across words?</b> → <see cref="Contains"/> (analyzed field, tokenized, order-independent)</item>
///   <item><b>Check if a field has any value?</b> → <see cref="HasValue"/> / <see cref="IsEmpty"/></item>
///   <item><b>Comparison (greater/less)?</b> → <see cref="GreaterThan"/> / <see cref="LessThan"/> / etc.</item>
///   <item><b>Exclude specific values?</b> → <see cref="NotEquals"/> (keyword field)</item>
///   <item><b>Exclude documents containing words?</b> → <see cref="NotContains"/> (analyzed field)</item>
/// </list>
/// </remarks>
public enum ComparisonOperator
{
    /// <summary>
    /// Exact match. Generates <c>TermQuery</c> (single value) or <c>TermsQuery</c> (multiple values)
    /// on the non-analyzed (<c>.keyword</c>) field. Null values are rewritten to <see cref="IsEmpty"/>.
    /// </summary>
    Equals,

    /// <summary>
    /// Negated exact match. Generates <c>BoolQuery { MustNot = TermQuery/TermsQuery }</c>
    /// on the non-analyzed field. Null values are rewritten to <see cref="HasValue"/>.
    /// </summary>
    NotEquals,

    /// <summary>
    /// Field does not exist or has no value. Generates <c>BoolQuery { MustNot = ExistsQuery }</c>.
    /// </summary>
    IsEmpty,

    /// <summary>
    /// Field exists and has a value. Generates <c>ExistsQuery</c>.
    /// </summary>
    HasValue,

    /// <summary>
    /// Full-text token matching on analyzed fields. Generates <c>MatchQuery { Operator = And }</c>.
    /// All input tokens must be present (order-independent). Does NOT support wildcard or substring matching.
    /// Null values are rewritten to <see cref="IsEmpty"/>.
    /// </summary>
    Contains,

    /// <summary>
    /// Negated full-text token matching. Generates <c>BoolQuery { MustNot = MatchQuery }</c>.
    /// Null values are rewritten to <see cref="HasValue"/>.
    /// </summary>
    NotContains,

    /// <summary>
    /// Strictly greater than. Generates <c>DateRangeQuery</c> (DateTime/DateTimeOffset),
    /// <c>NumericRangeQuery</c> (int/long/double/float/decimal), or <c>TermRangeQuery</c> (string).
    /// </summary>
    GreaterThan,

    /// <summary>
    /// Greater than or equal. Generates the same range query types as <see cref="GreaterThan"/>
    /// with an inclusive lower bound.
    /// </summary>
    GreaterThanOrEqual,

    /// <summary>
    /// Strictly less than. Generates the same range query types as <see cref="GreaterThan"/>
    /// with an exclusive upper bound.
    /// </summary>
    LessThan,

    /// <summary>
    /// Less than or equal. Generates the same range query types as <see cref="GreaterThan"/>
    /// with an inclusive upper bound.
    /// </summary>
    LessThanOrEqual
}

/// <summary>
/// Defines the boolean operator used to combine conditions within a <see cref="FieldConditionGroup"/>.
/// Named <c>FieldConditionGroupOperator</c> to avoid conflict with
/// <c>Foundatio.Parsers.LuceneQueries.Nodes.GroupOperator</c>.
/// </summary>
public enum FieldConditionGroupOperator
{
    /// <summary>All conditions must match. Generates <c>BoolQuery { Must = [...] }</c>.</summary>
    And,

    /// <summary>At least one condition must match. Generates <c>BoolQuery { Should = [...], MinimumShouldMatch = 1 }</c>.</summary>
    Or,

    /// <summary>
    /// No condition must match (AND-NOT semantics). Generates <c>BoolQuery { MustNot = [...] }</c>.
    /// Multiple conditions mean NOT A AND NOT B (exclude documents matching any clause).
    /// For NOT (A AND B), nest an explicit <see cref="And"/> group inside <see cref="Not"/>.
    /// </summary>
    Not
}

/// <summary>
/// Groups multiple field conditions under a boolean operator (AND, OR, NOT).
/// </summary>
/// <remarks>
/// <para>Use <see cref="FieldConditionGroup{T}"/> for strongly-typed expression-based overloads.</para>
/// <para>Empty groups (no conditions and no children) are silently skipped at build time.</para>
/// <para>Single-item groups are unwrapped to avoid unnecessary BoolQuery nesting.</para>
/// </remarks>
public class FieldConditionGroup
{
    public FieldConditionGroup(FieldConditionGroupOperator op)
    {
        Operator = op;
    }

    public FieldConditionGroupOperator Operator { get; }
    public List<FieldCondition> Conditions { get; } = new();
    public List<FieldConditionGroup> Children { get; } = new();

    public FieldConditionGroup FieldCondition(Field field, ComparisonOperator op, object value)
    {
        Conditions.Add(new FieldCondition { Field = field, Value = value, Operator = op });
        return this;
    }

    public FieldConditionGroup FieldCondition(Field field, ComparisonOperator op, params object[] values)
    {
        Conditions.Add(new FieldCondition { Field = field, Value = values, Operator = op });
        return this;
    }

    public FieldConditionGroup FieldContains(Field field, object value)
    {
        Conditions.Add(new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.Contains });
        return this;
    }

    public FieldConditionGroup FieldContains(Field field, params object[] values)
    {
        Conditions.Add(new FieldCondition { Field = field, Value = values, Operator = ComparisonOperator.Contains });
        return this;
    }

    public FieldConditionGroup FieldNotContains(Field field, object value)
    {
        Conditions.Add(new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.NotContains });
        return this;
    }

    public FieldConditionGroup FieldNotContains(Field field, params object[] values)
    {
        Conditions.Add(new FieldCondition { Field = field, Value = values, Operator = ComparisonOperator.NotContains });
        return this;
    }

    public FieldConditionGroup FieldEmpty(Field field)
    {
        Conditions.Add(new FieldCondition { Field = field, Operator = ComparisonOperator.IsEmpty });
        return this;
    }

    public FieldConditionGroup FieldHasValue(Field field)
    {
        Conditions.Add(new FieldCondition { Field = field, Operator = ComparisonOperator.HasValue });
        return this;
    }

    public FieldConditionGroup FieldEquals(Field field, object value)
    {
        Conditions.Add(new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.Equals });
        return this;
    }

    public FieldConditionGroup FieldEquals(Field field, params object[] values)
    {
        Conditions.Add(new FieldCondition { Field = field, Value = values, Operator = ComparisonOperator.Equals });
        return this;
    }

    public FieldConditionGroup FieldNotEquals(Field field, object value)
    {
        Conditions.Add(new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.NotEquals });
        return this;
    }

    public FieldConditionGroup FieldNotEquals(Field field, params object[] values)
    {
        Conditions.Add(new FieldCondition { Field = field, Value = values, Operator = ComparisonOperator.NotEquals });
        return this;
    }

    public FieldConditionGroup FieldGreaterThan(Field field, object value)
    {
        Conditions.Add(new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.GreaterThan });
        return this;
    }

    public FieldConditionGroup FieldGreaterThanOrEqual(Field field, object value)
    {
        Conditions.Add(new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.GreaterThanOrEqual });
        return this;
    }

    public FieldConditionGroup FieldLessThan(Field field, object value)
    {
        Conditions.Add(new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.LessThan });
        return this;
    }

    public FieldConditionGroup FieldLessThanOrEqual(Field field, object value)
    {
        Conditions.Add(new FieldCondition { Field = field, Value = value, Operator = ComparisonOperator.LessThanOrEqual });
        return this;
    }

    public FieldConditionGroup FieldAnd(Action<FieldConditionGroup> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var child = new FieldConditionGroup(FieldConditionGroupOperator.And);
        configure(child);
        Children.Add(child);
        return this;
    }

    public FieldConditionGroup FieldNot(Action<FieldConditionGroup> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var child = new FieldConditionGroup(FieldConditionGroupOperator.Not);
        configure(child);
        Children.Add(child);
        return this;
    }

    public FieldConditionGroup FieldOr(Action<FieldConditionGroup> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var child = new FieldConditionGroup(FieldConditionGroupOperator.Or);
        configure(child);
        Children.Add(child);
        return this;
    }
}

/// <summary>
/// Strongly-typed version of <see cref="FieldConditionGroup"/> that provides
/// <see cref="Expression{TDelegate}"/>-based overloads for compile-time safety and
/// IntelliSense support.
/// </summary>
/// <typeparam name="T">The document type being queried.</typeparam>
/// <remarks>
/// <para>Create instances via static factories:</para>
/// <code>
/// var group = FieldConditionGroup&lt;Employee&gt;.Or();
/// group.FieldEquals(f =&gt; f.CompanyId, "abc");
/// </code>
/// <para>Or use the lambda-based API on <c>IRepositoryQuery&lt;T&gt;</c>:</para>
/// <code>
/// query.FieldOr(g =&gt; g
///     .FieldEquals(f =&gt; f.IsPrivate, false)
///     .FieldEquals(f =&gt; f.CompanyId, companyIds)
/// );
/// </code>
/// </remarks>
public class FieldConditionGroup<T> : FieldConditionGroup where T : class
{
    public FieldConditionGroup(FieldConditionGroupOperator op) : base(op) { }

    /// <summary>Creates an OR group. At least one child condition must match.</summary>
    public static FieldConditionGroup<T> Or() => new(FieldConditionGroupOperator.Or);

    /// <summary>Creates an AND group. All child conditions must match.</summary>
    public static FieldConditionGroup<T> And() => new(FieldConditionGroupOperator.And);

    /// <summary>Creates a NOT group. No child condition must match (AND-NOT semantics).</summary>
    public static FieldConditionGroup<T> Not() => new(FieldConditionGroupOperator.Not);

    public new FieldConditionGroup<T> FieldCondition(Field field, ComparisonOperator op, object value)
    {
        base.FieldCondition(field, op, value);
        return this;
    }

    public new FieldConditionGroup<T> FieldCondition(Field field, ComparisonOperator op, params object[] values)
    {
        base.FieldCondition(field, op, values);
        return this;
    }

    public FieldConditionGroup<T> FieldCondition(Expression<Func<T, object>> objectPath, ComparisonOperator op, object value)
    {
        Conditions.Add(new FieldCondition { Field = objectPath, Value = value, Operator = op });
        return this;
    }

    public FieldConditionGroup<T> FieldCondition(Expression<Func<T, object>> objectPath, ComparisonOperator op, params object[] values)
    {
        Conditions.Add(new FieldCondition { Field = objectPath, Value = values, Operator = op });
        return this;
    }

    public new FieldConditionGroup<T> FieldContains(Field field, object value)
    {
        base.FieldContains(field, value);
        return this;
    }

    public new FieldConditionGroup<T> FieldContains(Field field, params object[] values)
    {
        base.FieldContains(field, values);
        return this;
    }

    public FieldConditionGroup<T> FieldContains(Expression<Func<T, object>> objectPath, object value)
    {
        Conditions.Add(new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.Contains });
        return this;
    }

    public FieldConditionGroup<T> FieldContains(Expression<Func<T, object>> objectPath, params object[] values)
    {
        Conditions.Add(new FieldCondition { Field = objectPath, Value = values, Operator = ComparisonOperator.Contains });
        return this;
    }

    public new FieldConditionGroup<T> FieldNotContains(Field field, object value)
    {
        base.FieldNotContains(field, value);
        return this;
    }

    public new FieldConditionGroup<T> FieldNotContains(Field field, params object[] values)
    {
        base.FieldNotContains(field, values);
        return this;
    }

    public FieldConditionGroup<T> FieldNotContains(Expression<Func<T, object>> objectPath, object value)
    {
        Conditions.Add(new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.NotContains });
        return this;
    }

    public FieldConditionGroup<T> FieldNotContains(Expression<Func<T, object>> objectPath, params object[] values)
    {
        Conditions.Add(new FieldCondition { Field = objectPath, Value = values, Operator = ComparisonOperator.NotContains });
        return this;
    }

    public new FieldConditionGroup<T> FieldEmpty(Field field)
    {
        base.FieldEmpty(field);
        return this;
    }

    public FieldConditionGroup<T> FieldEmpty(Expression<Func<T, object>> objectPath)
    {
        Conditions.Add(new FieldCondition { Field = objectPath, Operator = ComparisonOperator.IsEmpty });
        return this;
    }

    public new FieldConditionGroup<T> FieldHasValue(Field field)
    {
        base.FieldHasValue(field);
        return this;
    }

    public FieldConditionGroup<T> FieldHasValue(Expression<Func<T, object>> objectPath)
    {
        Conditions.Add(new FieldCondition { Field = objectPath, Operator = ComparisonOperator.HasValue });
        return this;
    }

    public new FieldConditionGroup<T> FieldEquals(Field field, object value)
    {
        base.FieldEquals(field, value);
        return this;
    }

    public new FieldConditionGroup<T> FieldEquals(Field field, params object[] values)
    {
        base.FieldEquals(field, values);
        return this;
    }

    public FieldConditionGroup<T> FieldEquals(Expression<Func<T, object>> objectPath, object value)
    {
        Conditions.Add(new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.Equals });
        return this;
    }

    public FieldConditionGroup<T> FieldEquals(Expression<Func<T, object>> objectPath, params object[] values)
    {
        Conditions.Add(new FieldCondition { Field = objectPath, Value = values, Operator = ComparisonOperator.Equals });
        return this;
    }

    public new FieldConditionGroup<T> FieldNotEquals(Field field, object value)
    {
        base.FieldNotEquals(field, value);
        return this;
    }

    public new FieldConditionGroup<T> FieldNotEquals(Field field, params object[] values)
    {
        base.FieldNotEquals(field, values);
        return this;
    }

    public FieldConditionGroup<T> FieldNotEquals(Expression<Func<T, object>> objectPath, object value)
    {
        Conditions.Add(new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.NotEquals });
        return this;
    }

    public FieldConditionGroup<T> FieldNotEquals(Expression<Func<T, object>> objectPath, params object[] values)
    {
        Conditions.Add(new FieldCondition { Field = objectPath, Value = values, Operator = ComparisonOperator.NotEquals });
        return this;
    }

    public new FieldConditionGroup<T> FieldGreaterThan(Field field, object value)
    {
        base.FieldGreaterThan(field, value);
        return this;
    }

    public FieldConditionGroup<T> FieldGreaterThan(Expression<Func<T, object>> objectPath, object value)
    {
        Conditions.Add(new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.GreaterThan });
        return this;
    }

    public new FieldConditionGroup<T> FieldGreaterThanOrEqual(Field field, object value)
    {
        base.FieldGreaterThanOrEqual(field, value);
        return this;
    }

    public FieldConditionGroup<T> FieldGreaterThanOrEqual(Expression<Func<T, object>> objectPath, object value)
    {
        Conditions.Add(new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.GreaterThanOrEqual });
        return this;
    }

    public new FieldConditionGroup<T> FieldLessThan(Field field, object value)
    {
        base.FieldLessThan(field, value);
        return this;
    }

    public FieldConditionGroup<T> FieldLessThan(Expression<Func<T, object>> objectPath, object value)
    {
        Conditions.Add(new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.LessThan });
        return this;
    }

    public new FieldConditionGroup<T> FieldLessThanOrEqual(Field field, object value)
    {
        base.FieldLessThanOrEqual(field, value);
        return this;
    }

    public FieldConditionGroup<T> FieldLessThanOrEqual(Expression<Func<T, object>> objectPath, object value)
    {
        Conditions.Add(new FieldCondition { Field = objectPath, Value = value, Operator = ComparisonOperator.LessThanOrEqual });
        return this;
    }

    public new FieldConditionGroup<T> FieldAnd(Action<FieldConditionGroup> configure)
    {
        base.FieldAnd(configure);
        return this;
    }

    public FieldConditionGroup<T> FieldAnd(Action<FieldConditionGroup<T>> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var child = new FieldConditionGroup<T>(FieldConditionGroupOperator.And);
        configure(child);
        Children.Add(child);
        return this;
    }

    public new FieldConditionGroup<T> FieldNot(Action<FieldConditionGroup> configure)
    {
        base.FieldNot(configure);
        return this;
    }

    public FieldConditionGroup<T> FieldNot(Action<FieldConditionGroup<T>> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var child = new FieldConditionGroup<T>(FieldConditionGroupOperator.Not);
        configure(child);
        Children.Add(child);
        return this;
    }

    public new FieldConditionGroup<T> FieldOr(Action<FieldConditionGroup> configure)
    {
        base.FieldOr(configure);
        return this;
    }

    public FieldConditionGroup<T> FieldOr(Action<FieldConditionGroup<T>> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var child = new FieldConditionGroup<T>(FieldConditionGroupOperator.Or);
        configure(child);
        Children.Add(child);
        return this;
    }
}
