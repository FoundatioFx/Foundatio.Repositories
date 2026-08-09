using System.Collections.Generic;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders;

/// <summary>
/// Ensures the model's <c>id</c> field is always present in the sort list, either as the default
/// sort when no other sorts exist or as a tiebreaker appended after existing sorts.
/// </summary>
/// <remarks>
/// See <see cref="IdTiebreakerField.TryEnsure{T}"/> for when the id tiebreaker is skipped entirely
/// (models without <see cref="IIdentity"/>, or indexes that opt out via
/// <see cref="Foundatio.Repositories.Elasticsearch.Configuration.IIndex.HasSortableIdField"/>).
/// </remarks>
public class DefaultSortQueryBuilder : IElasticQueryBuilder
{
    public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
    {
        // Get existing sorts from context data (set by SortQueryBuilder or ExpressionQueryBuilder)
        List<SortOptions>? sortFields = null;
        if (ctx.Data.TryGetValue(SortQueryBuilder.SortFieldsKey, out var sortsObj) && sortsObj is List<SortOptions> sorts)
        {
            sortFields = sorts;
        }

        sortFields ??= new List<SortOptions>();

        if (!IdTiebreakerField.TryEnsure(ctx, sortFields))
            return Task.CompletedTask;

        ctx.Data[SortQueryBuilder.SortFieldsKey] = sortFields;

        return Task.CompletedTask;
    }
}
