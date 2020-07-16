using System.Threading.Tasks;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class PageableQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            if (ctx.Options.HasPageLimit()) {
                int limit = ctx.Options.GetLimit();
                if (limit >= ctx.Options.GetMaxLimit() || ctx.Options.ShouldUseSnapshotPaging()) {
                    ctx.Search.Size(limit);
                } else {
                    // add 1 to limit if not snapshot paging so we can know if we have more results
                    ctx.Search.Size(limit + 1);
                }
            }

            // can only use search_after or skip
            if (ctx.Options.HasSearchAfter())
                ctx.Search.SearchAfter(ctx.Options.GetSearchAfter());
            else if (ctx.Options.ShouldUseSkip())
                ctx.Search.Skip(ctx.Options.GetSkip());

            return Task.CompletedTask;
        }
    }
}
