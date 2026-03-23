using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders;

/// <summary>
/// Translates <see cref="FieldCondition"/> objects and <see cref="FieldConditionGroup"/> trees
/// stored on the query options bag into Elasticsearch filter clauses.
/// </summary>
/// <remarks>
/// <para>Registered as a default query builder in <see cref="ElasticQueryBuilder.RegisterDefaults"/>.
/// Runs after <c>SoftDeletesQueryBuilder</c> and <c>DateRangeQueryBuilder</c>.</para>
/// <para>Performs runtime validation and throws <see cref="QueryValidationException"/>
/// for detectable misuse such as TermQuery on analyzed-only fields, Contains on keyword fields,
/// and contradictory soft-delete conditions.</para>
/// </remarks>
public class FieldConditionsQueryBuilder : IElasticQueryBuilder
{
    public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
    {
        var resolver = ctx.GetMappingResolver();

        var fieldConditions = ctx.Source.SafeGetCollection<FieldCondition>(FieldConditionQueryExtensions.FieldConditionsKey);
        if (fieldConditions is { Count: > 0 })
        {
            foreach (var fieldValue in fieldConditions)
            {
                ValidateCondition(fieldValue, ctx);
                ctx.Filter &= await TranslateConditionAsync(fieldValue, resolver, ctx).AnyContext();
            }
        }

        var groups = ctx.Source.SafeGetCollection<FieldConditionGroup>(FieldConditionQueryExtensions.FieldConditionGroupsKey);
        if (groups is { Count: > 0 })
        {
            foreach (var group in groups)
            {
                var translated = await TranslateGroupAsync(group, resolver, ctx).AnyContext();
                if (translated is not null)
                    ctx.Filter &= translated;
            }
        }
    }

    private static void ValidateCondition<T>(FieldCondition condition, QueryBuilderContext<T> ctx) where T : class, new()
    {
        if (condition.Field is null)
        {
            throw new QueryValidationException(
                "A FieldCondition was added with a null field. Every condition must specify which field it applies to. This is likely a bug in the query construction code.");
        }

        bool isRange = condition.Operator is ComparisonOperator.GreaterThan
            or ComparisonOperator.GreaterThanOrEqual
            or ComparisonOperator.LessThan
            or ComparisonOperator.LessThanOrEqual;

        if (isRange)
        {
            if (condition.Value is null)
            {
                var resolver = ctx.GetMappingResolver();
                string fieldName = resolver.GetResolvedField(condition.Field);
                throw new QueryValidationException(
                    $"""
                    Range operator '{condition.Operator}' cannot be used with a null value on field '{fieldName}'.
                    A null range bound is meaningless — there is no "greater than nothing".

                    To fix this:
                      - Use FieldHasValue() to check if the field exists
                      - Use FieldEmpty() to check if the field is missing
                      - Use the *If variant to conditionally add the range:
                        .Field{condition.Operator}If(f => f.Property, value, value.HasValue)
                    """, fieldName, condition.Operator.ToString());
            }

            if (condition.Value is IEnumerable and not string)
            {
                var resolver = ctx.GetMappingResolver();
                string fieldName = resolver.GetResolvedField(condition.Field);
                throw new QueryValidationException(
                    $"""
                    Range operator '{condition.Operator}' cannot be used with a collection value on field '{fieldName}'.
                    Range operators compare a single scalar value against the field.

                    To fix this:
                      - Use a single scalar value: .Field{condition.Operator}(f => f.Property, singleValue)
                      - For multiple value matching: .FieldEquals(f => f.Property, value1, value2, value3)
                    """, fieldName, condition.Operator.ToString());
            }
        }
    }

    private static void ValidateSoftDeleteCondition(string resolvedField, FieldCondition condition, ICommandOptions options)
    {
        if (!options.SupportsSoftDeletes() || options.GetSoftDeleteMode() is not SoftDeleteQueryMode.ActiveOnly)
            return;

        if (!String.Equals(resolvedField, "isDeleted", StringComparison.OrdinalIgnoreCase))
            return;

        if (condition.Value is not bool boolValue)
            return;

        bool isContradictory =
            (condition.Operator is ComparisonOperator.Equals && boolValue) ||
            (condition.Operator is ComparisonOperator.NotEquals && !boolValue);

        if (isContradictory)
        {
            throw new QueryValidationException(
                $"""
                Field condition targets 'isDeleted' with operator '{condition.Operator}' and value '{boolValue}', but the current query uses ActiveOnly mode (the default).
                The SoftDeletesQueryBuilder automatically adds an 'isDeleted: false' filter, so this condition creates a contradictory filter that returns zero results.

                To fix this, add one of these to your query:
                  - .IncludeSoftDeletes()        — returns both active and deleted documents
                  - .SoftDeleteMode(DeletedOnly)  — returns only deleted documents
                  - .SoftDeleteMode(All)          — returns all documents regardless of delete status
                """, resolvedField, condition.Operator.ToString());
        }
    }

    internal static async Task<QueryContainer> TranslateConditionAsync<T>(
        FieldCondition condition, ElasticMappingResolver resolver, QueryBuilderContext<T> ctx) where T : class, new()
    {
        if (condition.Value is null && condition.Operator is ComparisonOperator.Equals or ComparisonOperator.Contains)
            condition.Operator = ComparisonOperator.IsEmpty;
        else if (condition.Value is null && condition.Operator is ComparisonOperator.NotEquals or ComparisonOperator.NotContains)
            condition.Operator = ComparisonOperator.HasValue;

        bool nonAnalyzed = condition.Operator is ComparisonOperator.Equals or ComparisonOperator.NotEquals;

        string resolvedField = await ResolveFieldAsync(ctx, resolver, condition.Field, nonAnalyzed).AnyContext();

        ValidateSoftDeleteCondition(resolvedField, condition, ctx.Options);

        if (nonAnalyzed)
        {
            ValidateNonAnalyzedField(resolver, resolvedField, condition);
        }

        switch (condition.Operator)
        {
            case ComparisonOperator.Equals:
                {
                    QueryBase eqQuery;
                    if (condition.Value is IEnumerable and not string)
                    {
                        var values = new List<object>();
                        foreach (var value in (IEnumerable)condition.Value)
                            values.Add(value);
                        eqQuery = new TermsQuery { Field = resolvedField, Terms = values };
                    }
                    else
                    {
                        eqQuery = new TermQuery { Field = resolvedField, Value = condition.Value };
                    }
                    return eqQuery;
                }
            case ComparisonOperator.NotEquals:
                {
                    QueryBase neQuery;
                    if (condition.Value is IEnumerable and not string)
                    {
                        var values = new List<object>();
                        foreach (var value in (IEnumerable)condition.Value)
                            values.Add(value);
                        neQuery = new TermsQuery { Field = resolvedField, Terms = values };
                    }
                    else
                    {
                        neQuery = new TermQuery { Field = resolvedField, Value = condition.Value };
                    }
                    return new BoolQuery { MustNot = new QueryContainer[] { neQuery } };
                }
            case ComparisonOperator.Contains:
                {
                    if (!resolver.IsPropertyAnalyzed(resolvedField))
                    {
                        throw new QueryValidationException(
                            $"""
                        FieldContains cannot be used on field '{resolvedField}' because it is a non-analyzed (keyword) field.
                        FieldContains generates a MatchQuery which requires an analyzed text field to tokenize the input.

                        To fix this, either:
                          - Use FieldEquals() for exact matching on keyword fields
                          - Change the field mapping to a text type if you need full-text search
                        """, resolvedField, nameof(ComparisonOperator.Contains));
                    }

                    string containsText = condition.Value is IEnumerable and not string
                        ? String.Join(" ", ((IEnumerable)condition.Value).Cast<object>())
                        : condition.Value?.ToString() ?? String.Empty;
                    return new MatchQuery { Field = resolvedField, Query = containsText, Operator = Operator.And };
                }
            case ComparisonOperator.NotContains:
                {
                    if (!resolver.IsPropertyAnalyzed(resolvedField))
                    {
                        throw new QueryValidationException(
                            $"""
                        FieldNotContains cannot be used on field '{resolvedField}' because it is a non-analyzed (keyword) field.
                        FieldNotContains generates a MatchQuery which requires an analyzed text field to tokenize the input.

                        To fix this, either:
                          - Use FieldNotEquals() for exact negation on keyword fields
                          - Change the field mapping to a text type if you need full-text search
                        """, resolvedField, nameof(ComparisonOperator.NotContains));
                    }

                    string notContainsText = condition.Value is IEnumerable and not string
                        ? String.Join(" ", ((IEnumerable)condition.Value).Cast<object>())
                        : condition.Value?.ToString() ?? String.Empty;
                    return new BoolQuery { MustNot = new QueryContainer[] { new MatchQuery { Field = resolvedField, Query = notContainsText, Operator = Operator.And } } };
                }
            case ComparisonOperator.IsEmpty:
                return new BoolQuery { MustNot = new QueryContainer[] { new ExistsQuery { Field = resolvedField } } };
            case ComparisonOperator.HasValue:
                return new ExistsQuery { Field = resolvedField };
            case ComparisonOperator.GreaterThan:
            case ComparisonOperator.GreaterThanOrEqual:
            case ComparisonOperator.LessThan:
            case ComparisonOperator.LessThanOrEqual:
                return BuildRangeQuery(resolvedField, condition.Operator, condition.Value);
            default:
                throw new ArgumentOutOfRangeException(nameof(condition.Operator), condition.Operator, "Unknown comparison operator.");
        }
    }

    private static void ValidateNonAnalyzedField(ElasticMappingResolver resolver, string resolvedField, FieldCondition condition)
    {
        string originalField = resolver.GetResolvedField(condition.Field);
        if (String.Equals(originalField, resolvedField, StringComparison.Ordinal) && resolver.IsPropertyAnalyzed(resolvedField))
        {
            throw new QueryValidationException(
                $"""
                Field{condition.Operator} cannot be used on field '{resolvedField}' because it is an analyzed text field with no .keyword sub-field.
                A TermQuery on an analyzed text field almost never matches because Elasticsearch stores lowercased tokens (e.g., "eric") but TermQuery matches the exact input (e.g., "Eric").

                To fix this, either:
                  - Use FieldContains() for full-text token matching
                  - Add a .keyword sub-field to the field mapping in your index configuration:
                    m.Text(p => p.Name(f => f.Property).Fields(f => f.Keyword(k => k.Name("keyword"))))
                """, resolvedField, condition.Operator.ToString());
        }
    }

    private static QueryContainer BuildRangeQuery(string field, ComparisonOperator op, object value)
    {
        switch (value)
        {
            case DateTime dtValue:
                {
                    var rangeQuery = new DateRangeQuery { Field = field };
                    switch (op)
                    {
                        case ComparisonOperator.GreaterThan: rangeQuery.GreaterThan = dtValue; break;
                        case ComparisonOperator.GreaterThanOrEqual: rangeQuery.GreaterThanOrEqualTo = dtValue; break;
                        case ComparisonOperator.LessThan: rangeQuery.LessThan = dtValue; break;
                        case ComparisonOperator.LessThanOrEqual: rangeQuery.LessThanOrEqualTo = dtValue; break;
                    }
                    return rangeQuery;
                }
            case DateTimeOffset dtoValue:
                {
                    var rangeQuery = new DateRangeQuery { Field = field };
                    switch (op)
                    {
                        case ComparisonOperator.GreaterThan: rangeQuery.GreaterThan = dtoValue.UtcDateTime; break;
                        case ComparisonOperator.GreaterThanOrEqual: rangeQuery.GreaterThanOrEqualTo = dtoValue.UtcDateTime; break;
                        case ComparisonOperator.LessThan: rangeQuery.LessThan = dtoValue.UtcDateTime; break;
                        case ComparisonOperator.LessThanOrEqual: rangeQuery.LessThanOrEqualTo = dtoValue.UtcDateTime; break;
                    }
                    return rangeQuery;
                }
            case int intValue:
                {
                    var rangeQuery = new NumericRangeQuery { Field = field };
                    switch (op)
                    {
                        case ComparisonOperator.GreaterThan: rangeQuery.GreaterThan = intValue; break;
                        case ComparisonOperator.GreaterThanOrEqual: rangeQuery.GreaterThanOrEqualTo = intValue; break;
                        case ComparisonOperator.LessThan: rangeQuery.LessThan = intValue; break;
                        case ComparisonOperator.LessThanOrEqual: rangeQuery.LessThanOrEqualTo = intValue; break;
                    }
                    return rangeQuery;
                }
            case long longValue:
                {
                    var rangeQuery = new NumericRangeQuery { Field = field };
                    switch (op)
                    {
                        case ComparisonOperator.GreaterThan: rangeQuery.GreaterThan = longValue; break;
                        case ComparisonOperator.GreaterThanOrEqual: rangeQuery.GreaterThanOrEqualTo = longValue; break;
                        case ComparisonOperator.LessThan: rangeQuery.LessThan = longValue; break;
                        case ComparisonOperator.LessThanOrEqual: rangeQuery.LessThanOrEqualTo = longValue; break;
                    }
                    return rangeQuery;
                }
            case double doubleValue:
                {
                    var rangeQuery = new NumericRangeQuery { Field = field };
                    switch (op)
                    {
                        case ComparisonOperator.GreaterThan: rangeQuery.GreaterThan = doubleValue; break;
                        case ComparisonOperator.GreaterThanOrEqual: rangeQuery.GreaterThanOrEqualTo = doubleValue; break;
                        case ComparisonOperator.LessThan: rangeQuery.LessThan = doubleValue; break;
                        case ComparisonOperator.LessThanOrEqual: rangeQuery.LessThanOrEqualTo = doubleValue; break;
                    }
                    return rangeQuery;
                }
            case float floatValue:
                {
                    var rangeQuery = new NumericRangeQuery { Field = field };
                    switch (op)
                    {
                        case ComparisonOperator.GreaterThan: rangeQuery.GreaterThan = floatValue; break;
                        case ComparisonOperator.GreaterThanOrEqual: rangeQuery.GreaterThanOrEqualTo = floatValue; break;
                        case ComparisonOperator.LessThan: rangeQuery.LessThan = floatValue; break;
                        case ComparisonOperator.LessThanOrEqual: rangeQuery.LessThanOrEqualTo = floatValue; break;
                    }
                    return rangeQuery;
                }
            case decimal decValue:
                {
                    var rangeQuery = new NumericRangeQuery { Field = field };
                    switch (op)
                    {
                        case ComparisonOperator.GreaterThan: rangeQuery.GreaterThan = (double)decValue; break;
                        case ComparisonOperator.GreaterThanOrEqual: rangeQuery.GreaterThanOrEqualTo = (double)decValue; break;
                        case ComparisonOperator.LessThan: rangeQuery.LessThan = (double)decValue; break;
                        case ComparisonOperator.LessThanOrEqual: rangeQuery.LessThanOrEqualTo = (double)decValue; break;
                    }
                    return rangeQuery;
                }
            case string strValue:
                {
                    var rangeQuery = new TermRangeQuery { Field = field };
                    switch (op)
                    {
                        case ComparisonOperator.GreaterThan: rangeQuery.GreaterThan = strValue; break;
                        case ComparisonOperator.GreaterThanOrEqual: rangeQuery.GreaterThanOrEqualTo = strValue; break;
                        case ComparisonOperator.LessThan: rangeQuery.LessThan = strValue; break;
                        case ComparisonOperator.LessThanOrEqual: rangeQuery.LessThanOrEqualTo = strValue; break;
                    }
                    return rangeQuery;
                }
            default:
                throw new QueryValidationException(
                    $"""
                    Range operator '{op}' received a value of unsupported type '{value.GetType().Name}' on field '{field}'.
                    Range operators support DateTime, DateTimeOffset, numeric types (int, long, double, float, decimal), and string.
                    """, field, op.ToString());
        }
    }

    private static async Task<QueryContainer> TranslateGroupAsync<T>(
        FieldConditionGroup group, ElasticMappingResolver resolver, QueryBuilderContext<T> ctx) where T : class, new()
    {
        var clauses = new List<QueryContainer>();

        foreach (var condition in group.Conditions)
        {
            ValidateCondition(condition, ctx);
            clauses.Add(await TranslateConditionAsync(condition, resolver, ctx).AnyContext());
        }

        foreach (var child in group.Children)
        {
            var childQuery = await TranslateGroupAsync(child, resolver, ctx).AnyContext();
            if (childQuery is not null)
                clauses.Add(childQuery);
        }

        if (clauses.Count is 0)
            return null;

        if (clauses.Count is 1 && group.Operator is not FieldConditionGroupOperator.Not)
            return clauses[0];

        return group.Operator switch
        {
            FieldConditionGroupOperator.Or => new BoolQuery { Should = clauses.ToArray(), MinimumShouldMatch = 1 },
            FieldConditionGroupOperator.And => new BoolQuery { Must = clauses.ToArray() },
            FieldConditionGroupOperator.Not => new BoolQuery { MustNot = clauses.ToArray() },
            _ => throw new ArgumentOutOfRangeException(nameof(group.Operator), group.Operator, "Unknown group operator.")
        };
    }

    private static async Task<string> ResolveFieldAsync<T>(QueryBuilderContext<T> ctx, ElasticMappingResolver resolver, Field field, bool nonAnalyzed) where T : class, new()
    {
        string resolved = resolver.GetResolvedField(field);

        if (ctx is IQueryVisitorContextWithFieldResolver { FieldResolver: not null } fieldResolverCtx)
        {
            string customResolved = await fieldResolverCtx.FieldResolver(resolved, ctx).AnyContext();
            if (!String.IsNullOrWhiteSpace(customResolved))
                resolved = customResolved;
        }

        if (nonAnalyzed)
            resolved = resolver.GetNonAnalyzedFieldName(resolved);

        return resolved;
    }
}
