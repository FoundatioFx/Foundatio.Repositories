using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Repositories.Queries.Builders {
    public class SystemFilterQueryBuilder : IElasticQueryBuilder {
        private readonly ElasticQueryBuilder _queryBuilder;

        public SystemFilterQueryBuilder(ElasticQueryBuilder queryBuilder) {
            _queryBuilder = queryBuilder;
        }

        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var systemFilter = ctx.GetSourceAs<ISystemFilterQuery>();
            var opt = ctx.GetOptionsAs<IQueryOptions>();

            if (systemFilter?.SystemFilter != null)
                ctx.Filter &= _queryBuilder.BuildFilter<T>(systemFilter.SystemFilter, opt != null && opt.SupportsSoftDeletes);
        }
    }
}
