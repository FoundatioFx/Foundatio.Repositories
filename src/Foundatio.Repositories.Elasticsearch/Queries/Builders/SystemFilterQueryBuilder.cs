using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class SystemFilterQueryBuilder : IElasticQueryBuilder {
        private readonly ElasticQueryBuilder _queryBuilder;

        public SystemFilterQueryBuilder(ElasticQueryBuilder queryBuilder) {
            _queryBuilder = queryBuilder;
        }

        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var systemFilter = ctx.GetSourceAs<ISystemFilterQuery>();

            if (systemFilter?.SystemFilter == null)
                return;

            var innerContext = new QueryBuilderContext<T>(systemFilter.SystemFilter, ctx.Options, ctx.Search) {
                Filter = ctx.Filter,
                Query = ctx.Query
            };
            await _queryBuilder.BuildAsync<T>(innerContext).AnyContext();

            ctx.Filter = innerContext.Filter;
            ctx.Query = innerContext.Query;
        }
    }
}
