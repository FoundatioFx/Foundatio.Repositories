using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class SoftDeletesQueryBuilder : IElasticQueryBuilder {
        private const string IsDeleted = nameof(ISupportSoftDeletes.IsDeleted);
        
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            // dont add filter to child query system filters
            if (ctx.Type == ContextType.Child)
                return Task.CompletedTask;

            // get soft delete mode, use parent query as default if it exists
            var mode = ctx.Source.GetSoftDeleteMode(ctx.Parent?.Source?.GetSoftDeleteMode() ?? SoftDeleteQueryMode.ActiveOnly);

            // no filter needed if we want all
            if (mode == SoftDeleteQueryMode.All)
                return Task.CompletedTask;

            // check to see if the model supports soft deletes
            var options = ctx.Options.GetElasticTypeSettings();
            if (options == null || !options.SupportsSoftDeletes)
                return Task.CompletedTask;

            // if we are querying for specific ids then we don't need a deleted filter
            var ids = ctx.Source.GetIds();
            if (ids.Count > 0)
                return Task.CompletedTask;

            string fieldName = options.IndexType?.GetFieldName(IsDeleted) ?? IsDeleted;
            if (mode == SoftDeleteQueryMode.ActiveOnly)
                ctx.Filter &= new TermQuery { Field = fieldName, Value = false };
            else if (mode == SoftDeleteQueryMode.DeletedOnly)
                ctx.Filter &= new TermQuery { Field = fieldName, Value = true };

            return Task.CompletedTask;
        }
    }
}