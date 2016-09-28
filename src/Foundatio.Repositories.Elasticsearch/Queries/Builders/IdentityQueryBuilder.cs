using System;
using System.Linq;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class IdentityQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var identityQuery = ctx.GetSourceAs<IIdentityQuery>();
            if (identityQuery == null)
                return; 

            if (identityQuery.Ids != null && identityQuery.Ids.Count > 0)
                ctx.Filter &= new IdsQuery { Values = identityQuery.Ids.Select(id => new Id(id)) };

            if (identityQuery.ExcludedIds != null && identityQuery.ExcludedIds.Count > 0)
                ctx.Filter &= !new IdsQuery { Values = identityQuery.ExcludedIds.Select(id => new Id(id)) };
        }
    }
}