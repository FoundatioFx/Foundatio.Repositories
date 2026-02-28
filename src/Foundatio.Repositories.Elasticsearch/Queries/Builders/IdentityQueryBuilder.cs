using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Repositories.Queries;
using ElasticId = Elastic.Clients.Elasticsearch.Id;
using ElasticIds = Elastic.Clients.Elasticsearch.Ids;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders;

public class IdentityQueryBuilder : IElasticQueryBuilder
{
    public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
    {
        var ids = ctx.Source.GetIds();
        if (ids.Count > 0)
            ctx.Filter &= new IdsQuery { Values = new ElasticIds(ids.Select(id => new ElasticId(id))) };

        var excludesIds = ctx.Source.GetExcludedIds();
        if (excludesIds.Count > 0)
            ctx.Filter &= new BoolQuery { MustNot = new Query[] { new IdsQuery { Values = new ElasticIds(excludesIds.Select(id => new ElasticId(id))) } } };

        return Task.CompletedTask;
    }
}
