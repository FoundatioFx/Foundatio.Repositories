using System.Threading.Tasks;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class PagableQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            // add 1 to limit if not snapshot paging so we can know if we have more results
            if (ctx.Options.HasPageLimit())
                ctx.Search.Size(ctx.Options.GetLimit() + (ctx.Options.ShouldUseSnapshotPaging() == false ? 1 : 0));
            
            // can only use search_after or skip
            if (ctx.Options.HasSearchAfter())
                ctx.Search.SearchAfter(ctx.Options.GetSearchAfter());
            else if (ctx.Options.ShouldUseSkip())
                ctx.Search.Skip(ctx.Options.GetSkip());

            return Task.CompletedTask;
        }
    }
}
