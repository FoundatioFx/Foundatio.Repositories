using System;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class SelectedFieldsQueryBuilder : ElasticQueryBuilderBase {
        public override void BuildSearch<T>(object query, object options, ref SearchDescriptor<T> descriptor) {
            var selectedFieldsQuery = query as ISelectedFieldsQuery;
            if (selectedFieldsQuery?.SelectedFields?.Count > 0) {
                descriptor.Source(s => s.Include(selectedFieldsQuery.SelectedFields.ToArray()));
                return;
            }

            var opt = options as IQueryOptions;
            if (opt?.DefaultExcludes?.Length > 0)
                descriptor.Source(s => s.Exclude(opt.DefaultExcludes));
        }
    }
}