using System;
using Foundatio.Elasticsearch.Repositories.Queries.Options;
using Nest;

namespace Foundatio.Elasticsearch.Repositories.Queries.Builders {
    public class SelectedFieldsQueryBuilder : QueryBuilderBase {
        public override void BuildSearch<T>(object query, object options, ref SearchDescriptor<T> descriptor) {
            var selectedFieldsQuery = query as ISelectedFieldsQuery;
            if (selectedFieldsQuery?.SelectedFields?.Count > 0) {
                descriptor.Source(s => s.Include(f => f.Fields(selectedFieldsQuery.SelectedFields.ToArray())));
                return;
            }

            var opt = options as IQueryOptions;
            if (opt?.DefaultExcludes?.Length > 0)
                descriptor.Source(s => s.Exclude(f => f.Fields(opt.DefaultExcludes)));
        }
    }
}