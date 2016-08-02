using System;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class SoftDeletesQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var softDeletesQuery = ctx.GetSourceAs<ISoftDeletesQuery>();
            if (softDeletesQuery == null)
                return;

            var opt = ctx.GetOptionsAs<IQueryOptions>();
            if (opt == null || !opt.SupportsSoftDeletes)
                return;
            
            ctx.Filter &= new TermFilter { Field = "deleted", Value = softDeletesQuery.IncludeSoftDeletes };
        }
    }
}