using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class IdentityQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var identityQuery = ctx.GetSourceAs<IIdentityQuery>();
            if (identityQuery == null)
                return Task.CompletedTask;

            if (identityQuery.Ids != null && identityQuery.Ids.Count > 0)
                ctx.Filter &= new IdsQuery { Values = identityQuery.Ids.Select(id => new Nest.Id(id)) };

            if (identityQuery.ExcludedIds != null && identityQuery.ExcludedIds.Count > 0)
                ctx.Filter &= !new IdsQuery { Values = identityQuery.ExcludedIds.Select(id => new Nest.Id(id)) };

            return Task.CompletedTask;
        }
    }
}