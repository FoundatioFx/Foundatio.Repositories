using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders;

public class DefaultSortQueryBuilder : IElasticQueryBuilder
{
    private const string Id = nameof(IIdentity.Id);

    public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
    {
        if (ctx.Search is not ISearchRequest searchRequest)
            return Task.CompletedTask;

        var resolver = ctx.GetMappingResolver();
        string idField = resolver.GetResolvedField(Id) ?? "_id";

        searchRequest.Sort ??= new List<ISort>();
        var sortFields = searchRequest.Sort;

        // ensure id field is always present as a sort (default or tiebreaker)
        if (!sortFields.Any(s => idField.Equals(resolver.GetResolvedField(s.SortKey))))
            sortFields.Add(new FieldSort { Field = idField });

        return Task.CompletedTask;
    }
}
