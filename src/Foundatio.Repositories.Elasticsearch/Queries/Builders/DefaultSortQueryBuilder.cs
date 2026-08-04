using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders;

/// <summary>
/// Ensures the model's <c>id</c> field is always present in the sort list, either as the default
/// sort when no other sorts exist or as a tiebreaker appended after existing sorts.
/// </summary>
/// <remarks>
/// See <see cref="IdTiebreakerField.TryResolve{T}"/> for when the id tiebreaker is skipped entirely
/// (models without <see cref="IIdentity"/>, or indexes that opt out via
/// <see cref="Foundatio.Repositories.Elasticsearch.Configuration.IIndex.HasSortableIdField"/>).
/// </remarks>
public class DefaultSortQueryBuilder : IElasticQueryBuilder
{
    public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
    {
        if (!IdTiebreakerField.TryResolve(ctx, out string idField, out string idSortFieldName))
            return Task.CompletedTask;

        // Get existing sorts from context data (set by SortQueryBuilder or ExpressionQueryBuilder)
        List<SortOptions>? sortFields = null;
        if (ctx.Data.TryGetValue(SortQueryBuilder.SortFieldsKey, out var sortsObj) && sortsObj is List<SortOptions> sorts)
        {
            sortFields = sorts;
        }

        sortFields ??= new List<SortOptions>();

        var resolver = ctx.GetMappingResolver();

        // ensure id field is always present as a sort (default or tiebreaker)
        bool hasIdField = sortFields.Any(s =>
        {
            if (s?.Field?.Field == null)
                return false;
            string? fieldName = resolver.GetSortFieldName(s.Field.Field);
            return fieldName?.Equals(idSortFieldName) == true;
        });

        if (!hasIdField)
        {
            sortFields.Add(new FieldSort { Field = idField });
        }

        ctx.Data[SortQueryBuilder.SortFieldsKey] = sortFields;

        return Task.CompletedTask;
    }
}

