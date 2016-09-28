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

            var idsQuery = ctx.GetSourceAs<IIdentityQuery>();

            var opt = ctx.GetOptionsAs<IElasticQueryOptions>();
            if (opt == null || !opt.SupportsSoftDeletes || (idsQuery != null && idsQuery.Ids.Count > 0))
                return;

            ctx.Filter &= new TermQuery { Field = Fields.Deleted, Value = softDeletesQuery.IncludeSoftDeletes };
        }

        internal class Fields {
            public const string Deleted = "deleted";
        }
    }
}