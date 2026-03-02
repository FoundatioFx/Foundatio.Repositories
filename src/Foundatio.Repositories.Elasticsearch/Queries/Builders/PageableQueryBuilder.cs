using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders;

public class PageableQueryBuilder : IElasticQueryBuilder
{
    public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
    {
        int limit = ctx.Options.GetLimit();
        if (limit >= ctx.Options.GetMaxLimit() || ctx.Options.ShouldUseSnapshotPaging())
        {
            ctx.Search.Size(limit);
        }
        else
        {
            // add 1 to limit if not snapshot paging so we can know if we have more results
            ctx.Search.Size(limit + 1);
        }

        // can only use search_after or skip
        // Note: skip (from) is not allowed in scroll context, so only apply if not snapshot paging
        if (ctx.Options.HasSearchAfter())
            ctx.Search.SearchAfter(ctx.Options.GetSearchAfter().Select(ToFieldValue).ToList());
        else if (ctx.Options.HasSearchBefore())
            ctx.Search.SearchAfter(ctx.Options.GetSearchBefore().Select(ToFieldValue).ToList());
        else if (ctx.Options.ShouldUseSkip() && !ctx.Options.ShouldUseSnapshotPaging())
            ctx.Search.From(ctx.Options.GetSkip());

        return Task.CompletedTask;
    }

    private static FieldValue ToFieldValue(object value)
    {
        return value switch
        {
            null => FieldValue.Null,
            string s => FieldValue.String(s),
            bool b => FieldValue.Boolean(b),
            long l => FieldValue.Long(l),
            int i => FieldValue.Long(i),
            double d => FieldValue.Double(d),
            float f => FieldValue.Double(f),
            decimal m => FieldValue.Double((double)m),
            _ => FieldValue.String(value.ToString())
        };
    }
}
