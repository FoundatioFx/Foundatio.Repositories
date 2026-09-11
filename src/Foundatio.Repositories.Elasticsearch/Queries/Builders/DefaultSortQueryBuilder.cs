using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders;

public class DefaultSortQueryBuilder : IElasticQueryBuilder
{
    private const string Id = nameof(IIdentity.Id);

    public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
    {
        // Get existing sorts from context data (set by SortQueryBuilder or ExpressionQueryBuilder)
        List<SortOptions>? sortFields = null;
        if (ctx.Data.TryGetValue(SortQueryBuilder.SortFieldsKey, out var sortsObj) && sortsObj is List<SortOptions> sorts)
        {
            sortFields = sorts;
        }

        sortFields ??= new List<SortOptions>();

        var resolver = ctx.GetMappingResolver();
        string idField = await resolver.GetResolvedFieldAsync(Id).AnyContext() ?? "_id";

        // ensure id field is always present as a sort (default or tiebreaker)
        bool hasIdField = false;
        foreach (var sort in sortFields)
        {
            if (sort?.Field?.Field is not { } field)
                continue;

            string fieldName = await resolver.GetSortFieldNameAsync(field).AnyContext();
            if (fieldName?.Equals(idField) == true)
            {
                hasIdField = true;
                break;
            }
        }

        if (!hasIdField)
        {
            sortFields.Add(new FieldSort { Field = idField });
        }

        ctx.Data[SortQueryBuilder.SortFieldsKey] = sortFields;
    }
}
