using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Utility;
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
            ctx.Search.SearchAfter(ctx.Options.GetSearchAfter().Select(FieldValueHelper.ToFieldValue).ToList());
        else if (ctx.Options.HasSearchBefore())
            ctx.Search.SearchAfter(ctx.Options.GetSearchBefore().Select(FieldValueHelper.ToFieldValue).ToList());
        // Skip (from) is intentionally ignored during snapshot paging because Elasticsearch
        // does not support the 'from' parameter in a point-in-time / scroll context.
        else if (ctx.Options.ShouldUseSkip() && !ctx.Options.ShouldUseSnapshotPaging())
            ctx.Search.From(ctx.Options.GetSkip());

        return Task.CompletedTask;
    }
}
