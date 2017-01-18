using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class SoftDeletesQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var softDeletesQuery = ctx.GetSourceAs<ISoftDeletesQuery>();
            if (softDeletesQuery == null)
                return Task.CompletedTask;

            var mode = softDeletesQuery.SoftDeleteMode;
            var systemFilterContext = ctx as ISystemFilterQueryBuilderContext;

            // only automatically apply this to system filters
            if (systemFilterContext != null && mode.HasValue == false)
                mode = SoftDeleteQueryMode.ActiveOnly;

            if (mode.HasValue == false || mode.Value == SoftDeleteQueryMode.All)
                return Task.CompletedTask;

            var idsQuery = ctx.GetSourceAs<IIdentityQuery>();
            var opt = ctx.GetOptionsAs<IElasticQueryOptions>();
            if (opt == null || !opt.SupportsSoftDeletes || (idsQuery != null && idsQuery.Ids.Count > 0))
                return Task.CompletedTask;

            if (mode.Value == SoftDeleteQueryMode.ActiveOnly)
                ctx.Filter &= new TermFilter { Field = Fields.Deleted, Value = false };
            else if (mode.Value == SoftDeleteQueryMode.DeletedOnly)
                ctx.Filter &= new TermFilter { Field = Fields.Deleted, Value = true };

            return Task.CompletedTask;
        }

        internal class Fields {
            public const string Deleted = "deleted";
        }
    }
}

