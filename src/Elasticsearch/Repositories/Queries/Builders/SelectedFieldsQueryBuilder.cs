using System;
using Foundatio.Repositories.Elasticsearch.Queries.Options;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class SelectedFieldsQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var selectedFieldsQuery = ctx.GetQueryAs<ISelectedFieldsQuery>();
            if (selectedFieldsQuery?.SelectedFields?.Count > 0) {
                ctx.Search.Source(s => s.Include(selectedFieldsQuery.SelectedFields.ToArray()));
                return;
            }

            var opt = ctx.GetOptionsAs<IQueryOptions>();
            if (opt?.DefaultExcludes?.Length > 0)
                ctx.Search.Source(s => s.Exclude(opt.DefaultExcludes));
        }
    }
}