using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories {
    public static class FieldIncludesQueryExtensions {
        internal const string IncludesKey = "@Includes";
        public static T Include<T>(this T query, Field field) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(IncludesKey, field);
        }

        public static T Include<T>(this T query, IEnumerable<Field> fields) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(IncludesKey, fields);
        }

        public static IRepositoryQuery Include<T>(this IRepositoryQuery query, Expression<Func<T, object>> objectPath) {
            return query.AddCollectionOptionValue<IRepositoryQuery, Field>(IncludesKey, objectPath);
        }

        public static IRepositoryQuery<T> Include<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath) where T: class {
            return query.AddCollectionOptionValue<IRepositoryQuery<T>, Field>(IncludesKey, objectPath);
        }

        public static IRepositoryQuery<T> Include<T>(this IRepositoryQuery<T> query, params Expression<Func<T, object>>[] objectPaths) where T : class {
            foreach (var objectPath in objectPaths)
                query.Include(objectPath);

            return query;
        }

        internal const string ExcludesKey = "@Excludes";
        public static T Exclude<T>(this T query, Field field) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(ExcludesKey, field);
        }

        public static T Exclude<T>(this T query, IEnumerable<Field> fields) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(ExcludesKey, fields);
        }

        public static IRepositoryQuery<T> Exclude<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath) where T : class {
            return query.AddCollectionOptionValue<IRepositoryQuery<T>, Field>(ExcludesKey, objectPath);
        }

        public static IRepositoryQuery<T> Exclude<T>(this IRepositoryQuery<T> query, params Expression<Func<T, object>>[] objectPaths) where T : class {
            foreach (var objectPath in objectPaths)
                query.Exclude(objectPath);

            return query;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadFieldIncludesQueryExtensions {
        public static ICollection<Field> GetIncludes(this IRepositoryQuery options) {
            return options.SafeGetCollection<Field>(FieldIncludesQueryExtensions.IncludesKey);
        }

        public static ICollection<Field> GetExcludes(this IRepositoryQuery options) {
            return options.SafeGetCollection<Field>(FieldIncludesQueryExtensions.ExcludesKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class FieldIncludesQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var includes = ctx.Source.GetIncludes();
            if (includes.Count > 0)
                ctx.Search.Source(s => s.Includes(i => i.Fields(includes.ToArray())));

            var excludes = ctx.Source.GetExcludes();
            if (excludes.Count > 0)
                ctx.Search.Source(s => s.Excludes(i => i.Fields(excludes.ToArray())));

            return Task.CompletedTask;
        }
    }
}