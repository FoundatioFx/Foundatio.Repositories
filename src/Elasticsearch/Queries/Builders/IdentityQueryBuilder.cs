using System;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class IdentityQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var identityQuery = ctx.GetSourceAs<IIdentityQuery>();

            if (identityQuery.Ids != null && identityQuery.Ids.Count > 0)
                ctx.Filter &= new IdsFilter { Values = identityQuery.Ids };

            if (identityQuery.ExcludedIds != null && identityQuery.ExcludedIds.Count > 0)
                ctx.Filter &= !new IdsFilter { Values = identityQuery.ExcludedIds }.ToContainer();
        }
    }
}