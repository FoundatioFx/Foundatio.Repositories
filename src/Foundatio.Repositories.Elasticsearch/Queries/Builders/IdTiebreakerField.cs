using System;
using System.Collections.Generic;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders;

/// <summary>
/// Resolves the model's <c>id</c> field for use as a default sort or search_after tiebreaker.
/// Shared by <see cref="DefaultSortQueryBuilder"/> and <see cref="SearchAfterQueryBuilder"/> so both
/// agree on when the tiebreaker is safe to add, how it resolves, and whether it is already present.
/// </summary>
internal static class IdTiebreakerField
{
    private const string Id = nameof(IIdentity.Id);

    /// <summary>
    /// Ensures the id field's sort-safe mapped name is present in <paramref name="sortFields"/>.
    /// </summary>
    /// <returns>
    /// <c>true</c> when a sortable id is available and is present after this call; otherwise,
    /// <c>false</c> when no id tiebreaker should be added.
    /// </returns>
    public static bool TryEnsure<T>(QueryBuilderContext<T> ctx, ICollection<SortOptions> sortFields) where T : class, new()
    {
        if (!TryResolve(ctx, out string idSortFieldName))
            return false;

        var resolver = ctx.GetMappingResolver();
        foreach (var sort in sortFields)
        {
            if (sort?.Field?.Field is not { } sortField)
                continue;

            string? fieldName = resolver.GetSortFieldName(sortField);
            if (String.Equals(fieldName, idSortFieldName, StringComparison.Ordinal))
                return true;
        }

        sortFields.Add(new FieldSort { Field = idSortFieldName });
        return true;
    }

    private static bool TryResolve<T>(QueryBuilderContext<T> ctx, out string idSortFieldName) where T : class, new()
    {
        idSortFieldName = String.Empty;

        if (!typeof(IIdentity).IsAssignableFrom(typeof(T)))
            return false;

        if (ctx.Options.GetElasticIndex()?.HasSortableIdField is false)
            return false;

        var resolver = ctx.GetMappingResolver();
        string? resolvedField = resolver.GetResolvedField(Id);
        if (String.IsNullOrEmpty(resolvedField))
            return false;

        idSortFieldName = resolver.GetSortFieldName(resolvedField) ?? resolvedField;
        return true;
    }
}
