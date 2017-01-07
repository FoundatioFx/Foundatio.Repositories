using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Extensions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IFieldIncludesQuery {
        ICollection<Field> FieldIncludes { get; }
        ICollection<Field> FieldExcludes { get; }
    }
    
    public class FieldIncludesQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var fieldIncludesQuery = ctx.GetSourceAs<IFieldIncludesQuery>();
            if (fieldIncludesQuery?.FieldIncludes?.Count > 0) {
                ctx.Search.Source(s => s.Includes(i => i.Fields(fieldIncludesQuery.FieldIncludes.ToArray())));
                return Task.CompletedTask;
            }

            var opt = ctx.GetOptionsAs<IElasticQueryOptions>();
            if (opt?.DefaultExcludes?.Count > 0)
                ctx.Search.Source(s => s.Excludes(i => i.Fields(opt.DefaultExcludes.ToArray())));

            if (fieldIncludesQuery?.FieldExcludes?.Count > 0) {
                ctx.Search.Source(s => s.Excludes(i => i.Fields(fieldIncludesQuery.FieldExcludes.ToArray())));
                return Task.CompletedTask;
            }

            return Task.CompletedTask;
        }
    }

    public static class FieldIncludesQueryExtensions {
        public static T IncludeField<T>(this T query, Field field) where T : IFieldIncludesQuery {
            query.FieldIncludes?.Add(field);
            return query;
        }

        public static T IncludeField<T, TModel>(this T query, Expression<Func<TModel, object>> objectPath) where T : IFieldIncludesQuery {
            query.FieldIncludes?.Add(objectPath);
            return query;
        }

        public static T IncludeFields<T>(this T query, params Field[] fields) where T : IFieldIncludesQuery {
            query.FieldIncludes?.AddRange(fields);
            return query;
        }

        public static T IncludeFields<T, TModel>(this T query, params Expression<Func<TModel, object>>[] objectPaths) where T : IFieldIncludesQuery {
            foreach (var objectPath in objectPaths)
                query.FieldIncludes?.Add(objectPath);

            return query;
        }

        public static T ExcludeField<T>(this T query, Field field) where T : IFieldIncludesQuery {
            query.FieldExcludes?.Add(field);
            return query;
        }

        public static T ExcludeField<T, TModel>(this T query, Expression<Func<TModel, object>> objectPath) where T : IFieldIncludesQuery {
            query.FieldExcludes?.Add(objectPath);
            return query;
        }

        public static T ExcludeFields<T>(this T query, params Field[] fields) where T : IFieldIncludesQuery {
            query.FieldExcludes?.AddRange(fields);
            return query;
        }

        public static T ExcludeFields<T, TModel>(this T query, params Expression<Func<TModel, object>>[] fields) where T : IFieldIncludesQuery {
            foreach (var field in fields)
                query.FieldExcludes?.Add(field);

            return query;
        }
    }
}