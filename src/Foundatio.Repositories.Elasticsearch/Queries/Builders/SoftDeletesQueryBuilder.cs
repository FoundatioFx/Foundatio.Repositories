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
            var softDeletesQuery = ctx.GetSourceAs<ISoftDeletesQuery>();
            if (softDeletesQuery == null || softDeletesQuery.IncludeSoftDeletes)
                return Task.CompletedTask;

            var options = ctx.GetOptionsAs<IElasticQueryOptions>();
            if (options == null || !options.SupportsSoftDeletes)
                return Task.CompletedTask;

            var idsQuery = ctx.GetSourceAs<IIdentityQuery>();
            if (idsQuery != null && idsQuery.Ids.Count > 0)
                return Task.CompletedTask;

            string fieldName = options.IndexType?.GetFieldName(IsDeleted) ?? IsDeleted;
            ctx.Filter &= new TermQuery {  Field = fieldName, Value = softDeletesQuery.IncludeSoftDeletes };
            return Task.CompletedTask;
        }
    }
}