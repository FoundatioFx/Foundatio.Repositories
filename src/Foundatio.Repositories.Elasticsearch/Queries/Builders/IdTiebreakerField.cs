using System;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders;

/// <summary>
/// Resolves the model's <c>id</c> field for use as a default sort or search_after tiebreaker.
/// Shared by <see cref="DefaultSortQueryBuilder"/> and <see cref="SearchAfterQueryBuilder"/> so both
/// agree on when the tiebreaker is safe to add and how it resolves, instead of each maintaining its
/// own copy of this logic.
/// </summary>
internal static class IdTiebreakerField
{
    private const string Id = nameof(IIdentity.Id);

    /// <summary>
    /// Attempts to resolve the id field's mapped name and sort-safe name for <typeparamref name="T"/>.
    /// </summary>
    /// <returns>
    /// <c>false</c> -- meaning no id tiebreaker should be added at all -- when <typeparamref name="T"/>
    /// doesn't implement <see cref="IIdentity"/> (there is no <c>Id</c> property to sort by), when
    /// <see cref="Foundatio.Repositories.Elasticsearch.Configuration.IIndex.HasSortableIdField"/> is
    /// <c>false</c> on the target index, or when the resolver can't produce a usable field name.
    /// </returns>
    public static bool TryResolve<T>(QueryBuilderContext<T> ctx, out string idField, out string idSortFieldName) where T : class, new()
    {
        idField = String.Empty;
        idSortFieldName = String.Empty;

        if (!typeof(IIdentity).IsAssignableFrom(typeof(T)))
            return false;

        if (ctx.Options.GetElasticIndex()?.HasSortableIdField == false)
            return false;

        var resolver = ctx.GetMappingResolver();
        string? resolvedField = resolver.GetResolvedField(Id);
        if (String.IsNullOrEmpty(resolvedField))
            return false;

        idField = resolvedField;
        idSortFieldName = resolver.GetSortFieldName(resolvedField) ?? resolvedField;
        return true;
    }
}
