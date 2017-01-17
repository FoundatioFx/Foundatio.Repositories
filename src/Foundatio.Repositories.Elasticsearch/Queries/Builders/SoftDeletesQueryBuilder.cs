using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class SoftDeletesQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var softDeletesQuery = ctx.GetSourceAs<ISoftDeletesQuery>();
            if (softDeletesQuery == null || softDeletesQuery.IncludeSoftDeletes)
                return Task.CompletedTask;

            var idsQuery = ctx.GetSourceAs<IIdentityQuery>();

            var opt = ctx.GetOptionsAs<IElasticQueryOptions>();
            if (opt == null || !opt.SupportsSoftDeletes || (idsQuery != null && idsQuery.Ids.Count > 0))
                return Task.CompletedTask;

            ctx.Filter &= new TermFilter { Field = Fields.Deleted, Value = softDeletesQuery.IncludeSoftDeletes };

            return Task.CompletedTask;
        }

        internal class Fields {
            public const string Deleted = "deleted";
        }
    }
}

