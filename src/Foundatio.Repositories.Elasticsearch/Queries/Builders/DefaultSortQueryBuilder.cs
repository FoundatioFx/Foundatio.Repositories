using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders;

public class DefaultSortQueryBuilder : IElasticQueryBuilder
{
    private const string Id = nameof(IIdentity.Id);

    public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
    {
        // Get existing sorts from context data (set by SortQueryBuilder or ExpressionQueryBuilder)
        List<SortOptions> sortFields = null;
        if (ctx.Data.TryGetValue(SortQueryBuilder.SortFieldsKey, out var sortsObj) && sortsObj is List<SortOptions> sorts)
        {
            sortFields = sorts;
        }

        sortFields ??= new List<SortOptions>();

        var resolver = ctx.GetMappingResolver();
        string idField = resolver.GetResolvedField(Id) ?? "_id";

        // ensure id field is always present as a sort (default or tiebreaker)
        bool hasIdField = sortFields.Any(s =>
        {
            if (s?.Field?.Field == null)
                return false;
            string fieldName = resolver.GetSortFieldName(s.Field.Field);
            return fieldName?.Equals(idField) == true;
        });

        if (!hasIdField)
        {
            sortFields.Add(new FieldSort { Field = idField });
        }

        ctx.Data[SortQueryBuilder.SortFieldsKey] = sortFields;

        return Task.CompletedTask;
    }
}
