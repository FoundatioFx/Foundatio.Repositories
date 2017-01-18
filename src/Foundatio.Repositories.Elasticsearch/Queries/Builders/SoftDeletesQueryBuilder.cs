using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class SoftDeletesQueryBuilder : IElasticQueryBuilder {
        private const string IsDeleted = nameof(ISupportSoftDeletes.IsDeleted);
        
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            // wait until the system filter query is being built if the query supports it
            if (ctx.Type != ContextType.SystemFilter && ctx.Source is ISystemFilterQuery)
                return Task.CompletedTask;

            var mode = ctx.GetSourceAs<ISoftDeletesQuery>()?.SoftDeleteMode;
            // if no mode was specified, then try using the parent query mode
            if (mode == null && ctx.Parent != null)
                mode = ctx.Parent.GetSourceAs<ISoftDeletesQuery>()?.SoftDeleteMode;

            // default to active only if no mode has been specified
            if (mode.HasValue == false)
                mode = SoftDeleteQueryMode.ActiveOnly;

            // no filter needed if we want all
            if (mode.Value == SoftDeleteQueryMode.All)
                return Task.CompletedTask;

            // if we are querying for specific ids then we don't need a deleted filter
            var idsQuery = ctx.GetSourceAs<IIdentityQuery>();
            var options = ctx.GetOptionsAs<IElasticQueryOptions>();
            if (options == null || !options.SupportsSoftDeletes || (idsQuery != null && idsQuery.Ids.Count > 0))
                return Task.CompletedTask;

            string fieldName = options.IndexType?.GetFieldName(IsDeleted) ?? IsDeleted;
            if (mode.Value == SoftDeleteQueryMode.ActiveOnly)
                ctx.Filter &= new TermQuery { Field = fieldName, Value = false };
            else if (mode.Value == SoftDeleteQueryMode.DeletedOnly)
                ctx.Filter &= new TermQuery { Field = fieldName, Value = true };

            return Task.CompletedTask;
        }
    }
}