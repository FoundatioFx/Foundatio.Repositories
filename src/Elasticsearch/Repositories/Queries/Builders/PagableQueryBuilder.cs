using System;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class PagableQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var pagableQuery = ctx.GetQueryAs<IPagableQuery>();
            if (pagableQuery == null)
                return;

            // add 1 to limit if not auto paging so we can know if we have more results
            if (pagableQuery.ShouldUseLimit())
                ctx.Search.Size(pagableQuery.GetLimit() + (!pagableQuery.UseSnapshotPaging ? 1 : 0));
            if (pagableQuery.ShouldUseSkip())
                ctx.Search.Skip(pagableQuery.GetSkip());
        }
    }
}
