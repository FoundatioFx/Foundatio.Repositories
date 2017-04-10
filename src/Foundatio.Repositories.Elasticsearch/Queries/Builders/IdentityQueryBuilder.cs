using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class IdentityQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var ids = ctx.Source.GetIds();
            if (ids.Count > 0)
                ctx.Filter &= new IdsQuery { Values = ids.Select(id => new Nest.Id(id)) };

            var excludesIds = ctx.Source.GetExcludedIds();
            if (excludesIds.Count > 0)
                ctx.Filter &= !new IdsQuery { Values = excludesIds.Select(id => new Nest.Id(id)) };

            return Task.CompletedTask;
        }
    }
}