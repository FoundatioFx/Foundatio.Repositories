using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class SoftDeletesQueryBuilder : IElasticQueryBuilder {
        private const string IsDeleted = nameof(ISupportSoftDeletes.IsDeleted);
        
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            // TODO: Figure out how to automatically add parent filter for soft deletes on queries that have a parent document type

            // dont add filter to child query system filters
            if (ctx.Parent != null)
                return Task.CompletedTask;

            // get soft delete mode, use parent query as default if it exists
            var mode = ctx.Options.GetSoftDeleteMode(ctx.Parent?.Options?.GetSoftDeleteMode() ?? SoftDeleteQueryMode.ActiveOnly);

            // no filter needed if we want all
            if (mode == SoftDeleteQueryMode.All)
                return Task.CompletedTask;

            // check to see if the model supports soft deletes
            if (!ctx.Options.SupportsSoftDeletes())
                return Task.CompletedTask;

            // if we are querying for specific ids then we don't need a deleted filter
            var ids = ctx.Source.GetIds();
            if (ids.Count > 0)
                return Task.CompletedTask;

            var documentType = ctx.Options.DocumentType();
            var property = documentType.GetProperty(nameof(ISupportSoftDeletes.IsDeleted));
            var index = ctx.Options.GetElasticIndex();
            
            string fieldName = index?.Configuration.Client.Infer.Field(new Field(property)) ?? "deleted";
            if (mode == SoftDeleteQueryMode.ActiveOnly)
                ctx.Filter &= new TermQuery { Field = fieldName, Value = false };
            else if (mode == SoftDeleteQueryMode.DeletedOnly)
                ctx.Filter &= new TermQuery { Field = fieldName, Value = true };

            var parentType = ctx.Options.ParentDocumentType();
            if (parentType != null && parentType != typeof(object))
                ctx.Filter &= new HasParentQuery {
                    ParentType = parentType,
                    Query = new BoolQuery {
                        Filter = new[] { new QueryContainer(new TermQuery { Field = fieldName, Value = false }) }
                    }
                };

            return Task.CompletedTask;
        }
    }
}