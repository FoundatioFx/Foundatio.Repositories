using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Repositories.Queries.Builders {
    public class SystemFilterQueryBuilder : IElasticQueryBuilder {
        private readonly ElasticQueryBuilder _queryBuilder;

        public SystemFilterQueryBuilder(ElasticQueryBuilder queryBuilder) {
            _queryBuilder = queryBuilder;
        }

        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var systemFilter = ctx.GetSourceAs<ISystemFilterQuery>();

            if (systemFilter?.SystemFilter == null)
                return;

            var innerContext = new QueryBuilderContext<T>(systemFilter.SystemFilter, ctx.Options, ctx.Search) {
                Filter = ctx.Filter,
                Query = ctx.Query
            };
            _queryBuilder.Build<T>(innerContext);

            ctx.Filter = innerContext.Filter;
            ctx.Query = innerContext.Query;
        }
    }
}
