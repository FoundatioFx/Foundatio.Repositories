using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Repositories.Queries.Builders {
    public class SystemFilterQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var systemFilter = ctx.GetQueryAs<ISystemFilterQuery>();
            var opt = ctx.GetOptionsAs<IQueryOptions>();

            //if (systemFilter?.SystemFilter != null)
            //    container &= BuildFilterFromQuery(systemFilter.SystemFilter, opt != null && opt.SupportsSoftDeletes);
        }
    }
}
