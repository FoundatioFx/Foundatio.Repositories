using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Utility;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Options;

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
                ValidateCondition(fieldValue);
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

    private static FieldValue ToFieldValue(object value) => FieldValueHelper.ToFieldValue(value);

    private static void ValidateCondition(FieldCondition condition)
    {
        if (condition.Field is null)
        {
            throw new QueryValidationException(
                "A FieldCondition was added with a null field. Every condition must specify which field it applies to. This is likely a bug in the query construction code.");
        }
    }

    private static void ValidateRangeCondition(string resolvedField, FieldCondition condition)
    {
        bool isRange = condition.Operator is ComparisonOperator.GreaterThan
            or ComparisonOperator.GreaterThanOrEqual
            or ComparisonOperator.LessThan
            or ComparisonOperator.LessThanOrEqual;

        if (!isRange)
            return;

        if (condition.Value is null)
        {
            throw new QueryValidationException(
                $"""
                Range operator '{condition.Operator}' cannot be used with a null value on field '{resolvedField}'.
                A null range bound is meaningless — there is no "greater than nothing".

                To fix this:
                  - Use FieldHasValue() to check if the field exists
                  - Use FieldEmpty() to check if the field is missing
                  - Use the *If variant to conditionally add the range, for example:
                    .Field{condition.Operator}If(f => f.Property, value, value is not null)
                """, resolvedField, condition.Operator.ToString());
        }

        if (condition.Value is IEnumerable and not string)
        {
            throw new QueryValidationException(
                $"""
                Range operator '{condition.Operator}' cannot be used with a collection value on field '{resolvedField}'.
                Range operators compare a single scalar value against the field.

                To fix this:
                  - Use a single scalar value: .Field{condition.Operator}(f => f.Property, singleValue)
                  - For multiple value matching: .FieldEquals(f => f.Property, value1, value2, value3)
                """, resolvedField, condition.Operator.ToString());
        }
    }

    private static void ValidateSoftDeleteCondition(string resolvedField, FieldCondition condition, ICommandOptions options)
    {
        if (!options.SupportsSoftDeletes() || options.GetSoftDeleteMode() is not SoftDeleteQueryMode.ActiveOnly)
            return;

        string softDeleteField = ResolveSoftDeleteFieldName(options);
        if (!String.Equals(resolvedField, softDeleteField, StringComparison.OrdinalIgnoreCase))
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
                Field condition targets '{softDeleteField}' with operator '{condition.Operator}' and value '{boolValue}', but the current query uses ActiveOnly mode (the default).
                The SoftDeletesQueryBuilder automatically adds an '{softDeleteField}: false' filter, so this condition creates a contradictory filter that returns zero results.

                To fix this, add one of these to your query:
                  - .IncludeSoftDeletes()                            — returns both active and deleted documents
                  - .SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly) — returns only deleted documents
                  - .SoftDeleteMode(SoftDeleteQueryMode.All)         — returns all documents regardless of delete status
                """, resolvedField, condition.Operator.ToString());
        }
    }

    private static string ResolveSoftDeleteFieldName(ICommandOptions options)
    {
        var documentType = options.DocumentType();
        var property = documentType?.GetProperty(nameof(Foundatio.Repositories.Models.ISupportSoftDeletes.IsDeleted));
        var index = options.GetElasticIndex();

        if (property is not null && index is not null)
        {
            string inferred = index.Configuration.Client.Infer.Field(new Field(property));
            if (!String.IsNullOrEmpty(inferred))
                return inferred;
        }

        return "isDeleted";
    }

    internal static async Task<Query> TranslateConditionAsync<T>(
        FieldCondition condition, ElasticMappingResolver resolver, QueryBuilderContext<T> ctx) where T : class, new()
    {
        if (condition.Value is null && condition.Operator is ComparisonOperator.Equals or ComparisonOperator.Contains)
            condition.Operator = ComparisonOperator.IsEmpty;
        else if (condition.Value is null && condition.Operator is ComparisonOperator.NotEquals or ComparisonOperator.NotContains)
            condition.Operator = ComparisonOperator.HasValue;

        bool nonAnalyzed = condition.Operator is ComparisonOperator.Equals or ComparisonOperator.NotEquals;
        bool isStringRange = condition.Value is string
            && condition.Operator is ComparisonOperator.GreaterThan
                or ComparisonOperator.GreaterThanOrEqual
                or ComparisonOperator.LessThan
                or ComparisonOperator.LessThanOrEqual;

        string resolvedField = await ResolveFieldAsync(ctx, resolver, condition.Field, nonAnalyzed || isStringRange).AnyContext();

        ValidateSoftDeleteCondition(resolvedField, condition, ctx.Options);
        ValidateRangeCondition(resolvedField, condition);

        if (nonAnalyzed || isStringRange)
        {
            ValidateNonAnalyzedField(resolver, resolvedField, condition);
        }

        switch (condition.Operator)
        {
            case ComparisonOperator.Equals:
                {
                    var conditionValue = condition.Value ?? throw new QueryValidationException($"Condition value for field '{resolvedField}' cannot be null for Equals operator.");
                    Query eqQuery;
                    if (conditionValue is IEnumerable and not string)
                    {
                        var values = new List<FieldValue>();
                        foreach (var value in (IEnumerable)conditionValue)
                            values.Add(ToFieldValue(value));
                        eqQuery = new TermsQuery { Field = resolvedField, Terms = new TermsQueryField(values) };
                    }
                    else
                    {
                        eqQuery = new TermQuery { Field = resolvedField, Value = ToFieldValue(conditionValue) };
                    }
                    return eqQuery;
                }
            case ComparisonOperator.NotEquals:
                {
                    var conditionValue = condition.Value ?? throw new QueryValidationException($"Condition value for field '{resolvedField}' cannot be null for NotEquals operator.");
                    Query neQuery;
                    if (conditionValue is IEnumerable and not string)
                    {
                        var values = new List<FieldValue>();
                        foreach (var value in (IEnumerable)conditionValue)
                            values.Add(ToFieldValue(value));
                        neQuery = new TermsQuery { Field = resolvedField, Terms = new TermsQueryField(values) };
                    }
                    else
                    {
                        neQuery = new TermQuery { Field = resolvedField, Value = ToFieldValue(conditionValue) };
                    }
                    return new BoolQuery { MustNot = new Query[] { neQuery } };
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
                    return new BoolQuery { MustNot = new Query[] { new MatchQuery { Field = resolvedField, Query = notContainsText, Operator = Operator.And } } };
                }
            case ComparisonOperator.IsEmpty:
                return new BoolQuery { MustNot = new Query[] { new ExistsQuery { Field = resolvedField } } };
            case ComparisonOperator.HasValue:
                return new ExistsQuery { Field = resolvedField };
            case ComparisonOperator.GreaterThan:
            case ComparisonOperator.GreaterThanOrEqual:
            case ComparisonOperator.LessThan:
            case ComparisonOperator.LessThanOrEqual:
                var rangeValue = condition.Value ?? throw new QueryValidationException($"Condition value for field '{resolvedField}' cannot be null for range operator.");
                return BuildRangeQuery(resolvedField, condition.Operator, rangeValue);
            default:
                throw new ArgumentOutOfRangeException(nameof(condition.Operator), condition.Operator, "Unknown comparison operator.");
        }
    }

    private static void ValidateNonAnalyzedField(ElasticMappingResolver resolver, string resolvedField, FieldCondition condition)
    {
        if (resolver.IsPropertyAnalyzed(resolvedField))
        {
            bool isRange = condition.Operator is ComparisonOperator.GreaterThan
                or ComparisonOperator.GreaterThanOrEqual
                or ComparisonOperator.LessThan
                or ComparisonOperator.LessThanOrEqual;

            string queryType = isRange ? "TermRangeQuery" : "TermQuery";
            string suggestion = isRange
                ? "Use FieldContains() for full-text token matching, or target a keyword field for lexicographic range comparison"
                : "Use FieldContains() for full-text token matching";

            throw new QueryValidationException(
                $"""
                Field{condition.Operator} cannot be used on field '{resolvedField}' because it is an analyzed text field with no .keyword sub-field.
                A {queryType} on an analyzed text field produces unexpected results because Elasticsearch stores lowercased tokens, not the original text.

                To fix this, either:
                  - {suggestion}
                  - Add a .keyword sub-field to the field mapping in your index configuration:
                    m.Text(p => p.Name(f => f.Property).Fields(f => f.Keyword(k => k.Name("keyword"))))
                """, resolvedField, condition.Operator.ToString());
        }
    }

    private static Query BuildRangeQuery(string field, ComparisonOperator op, object value)
    {
        switch (value)
        {
            case DateTime dtValue:
                return BuildDateRange(field, op, dtValue);
            case DateTimeOffset dtoValue:
                return BuildDateRange(field, op, dtoValue.UtcDateTime);
            case int intValue:
                return BuildNumberRange(field, op, intValue);
            case long longValue:
                return BuildNumberRange(field, op, longValue);
            case double doubleValue:
                return BuildNumberRange(field, op, doubleValue);
            case float floatValue:
                return BuildNumberRange(field, op, (double)floatValue);
            case decimal decValue:
                return BuildNumberRange(field, op, (double)decValue);
            case string strValue:
                return BuildTermRange(field, op, strValue);
            default:
                throw new QueryValidationException(
                    $"""
                    Range operator '{op}' received a value of unsupported type '{value.GetType().Name}' on field '{field}'.
                    Range operators support DateTime, DateTimeOffset, numeric types (int, long, double, float, decimal), and string.
                    """, field, op.ToString());
        }
    }

    private static DateRangeQuery BuildDateRange(string field, ComparisonOperator op, DateTime value)
    {
        var query = new DateRangeQuery { Field = field };
        switch (op)
        {
            case ComparisonOperator.GreaterThan:
                query.Gt = value;
                break;
            case ComparisonOperator.GreaterThanOrEqual:
                query.Gte = value;
                break;
            case ComparisonOperator.LessThan:
                query.Lt = value;
                break;
            case ComparisonOperator.LessThanOrEqual:
                query.Lte = value;
                break;
        }
        return query;
    }

    private static NumberRangeQuery BuildNumberRange(string field, ComparisonOperator op, double value)
    {
        var query = new NumberRangeQuery { Field = field };
        switch (op)
        {
            case ComparisonOperator.GreaterThan:
                query.Gt = value;
                break;
            case ComparisonOperator.GreaterThanOrEqual:
                query.Gte = value;
                break;
            case ComparisonOperator.LessThan:
                query.Lt = value;
                break;
            case ComparisonOperator.LessThanOrEqual:
                query.Lte = value;
                break;
        }
        return query;
    }

    private static TermRangeQuery BuildTermRange(string field, ComparisonOperator op, string value)
    {
        var query = new TermRangeQuery { Field = field };
        switch (op)
        {
            case ComparisonOperator.GreaterThan:
                query.Gt = value;
                break;
            case ComparisonOperator.GreaterThanOrEqual:
                query.Gte = value;
                break;
            case ComparisonOperator.LessThan:
                query.Lt = value;
                break;
            case ComparisonOperator.LessThanOrEqual:
                query.Lte = value;
                break;
        }
        return query;
    }

    private static async Task<Query?> TranslateGroupAsync<T>(
        FieldConditionGroup group, ElasticMappingResolver resolver, QueryBuilderContext<T> ctx) where T : class, new()
    {
        var clauses = new List<Query>();

        foreach (var condition in group.Conditions)
        {
            ValidateCondition(condition);
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
            string? customResolved = await fieldResolverCtx.FieldResolver(resolved, ctx).AnyContext();
            if (!String.IsNullOrWhiteSpace(customResolved))
                resolved = customResolved;
        }

        if (nonAnalyzed)
        {
            string? nonAnalyzedField = resolver.GetNonAnalyzedFieldName(resolved);
            if (nonAnalyzedField is not null)
                resolved = nonAnalyzedField;
        }

        return resolved;
    }
}
