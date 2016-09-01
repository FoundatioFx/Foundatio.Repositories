using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface ISelectedFieldsQuery {
        ICollection<string> SelectedFields { get; }
    }

    public class SelectedFieldsQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var selectedFieldsQuery = ctx.GetSourceAs<ISelectedFieldsQuery>();
            if (selectedFieldsQuery?.SelectedFields?.Count > 0) {
                ctx.Search.Source(s => s.Include(selectedFieldsQuery.SelectedFields.ToArray()));
                return;
            }

            var opt = ctx.GetOptionsAs<IElasticQueryOptions>();
            if (opt?.DefaultExcludes?.Length > 0)
                ctx.Search.Source(s => s.Exclude(opt.DefaultExcludes));
        }
    }

    public static class SelectedFieldsQueryExtensions {
        public static T WithSelectedField<T>(this T query, string field) where T : ISelectedFieldsQuery {
            query.SelectedFields?.Add(field);
            return query;
        }

        public static T WithSelectedFields<T>(this T query, params string[] fields) where T : ISelectedFieldsQuery {
            query.SelectedFields?.AddRange(fields.Distinct());
            return query;
        }
    }
}