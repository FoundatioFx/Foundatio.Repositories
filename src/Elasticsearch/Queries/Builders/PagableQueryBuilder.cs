using System;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class PagableQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var pagableQuery = ctx.GetSourceAs<IPagableQuery>();
            if (pagableQuery == null)
                return;

            if (pagableQuery.ShouldUseLimit())
                ctx.Search.Size(pagableQuery.GetLimit());
            if (pagableQuery.ShouldUseSkip())
                ctx.Search.Skip(pagableQuery.GetSkip());
        }
    }
}
