using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Models;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class PagableQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var pagingOptions = ctx.GetOptionsAs<IPagingOptions>();
            var elasticPagingOptions = ctx.GetOptionsAs<IElasticPagingOptions>();
            if (pagingOptions == null)
                return Task.CompletedTask;
            
            // add 1 to limit if not auto paging so we can know if we have more results
            if (pagingOptions.ShouldUseLimit())
                ctx.Search.Size(pagingOptions.GetLimit() + (elasticPagingOptions.ShouldUseSnapshotPaging() == false ? 1 : 0));

            if (pagingOptions.ShouldUseSkip())
                ctx.Search.Skip(pagingOptions.GetSkip());

            return Task.CompletedTask;
        }
    }
}
