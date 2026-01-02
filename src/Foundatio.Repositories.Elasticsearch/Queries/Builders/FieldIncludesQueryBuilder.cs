using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Search;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Utility;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories
{
    public static class FieldIncludesQueryExtensions
    {
        internal const string IncludesKey = "@Includes";
        public static T Include<T>(this T query, Field field) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(IncludesKey, field);
        }

        public static T Include<T>(this T query, IEnumerable<Field> fields) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(IncludesKey, fields);
        }

        public static IRepositoryQuery Include<T>(this IRepositoryQuery query, Expression<Func<T, object>> objectPath)
        {
            return query.AddCollectionOptionValue<IRepositoryQuery, Field>(IncludesKey, objectPath);
        }

        public static IRepositoryQuery<T> Include<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath) where T : class
        {
            return query.AddCollectionOptionValue<IRepositoryQuery<T>, Field>(IncludesKey, objectPath);
        }

        public static IRepositoryQuery<T> Include<T>(this IRepositoryQuery<T> query, params Expression<Func<T, object>>[] objectPaths) where T : class
        {
            foreach (var objectPath in objectPaths)
                query.Include(objectPath);

            return query;
        }

        internal const string IncludesMaskKey = "@IncludesMask";
        public static T IncludeMask<T>(this T options, string maskExpression) where T : IRepositoryQuery
        {
            return options.BuildOption(IncludesMaskKey, maskExpression);
        }

        internal const string ExcludesKey = "@Excludes";
        public static T Exclude<T>(this T query, Field field) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(ExcludesKey, field);
        }

        public static T Exclude<T>(this T query, IEnumerable<Field> fields) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(ExcludesKey, fields);
        }

        public static IRepositoryQuery<T> Exclude<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath) where T : class
        {
            return query.AddCollectionOptionValue<IRepositoryQuery<T>, Field>(ExcludesKey, objectPath);
        }

        public static IRepositoryQuery<T> Exclude<T>(this IRepositoryQuery<T> query, params Expression<Func<T, object>>[] objectPaths) where T : class
        {
            foreach (var objectPath in objectPaths)
                query.Exclude(objectPath);

            return query;
        }

        internal const string ExcludesMaskKey = "@ExcludesMask";
        public static T ExcludeMask<T>(this T options, string maskExpression) where T : IRepositoryQuery
        {
            return options.BuildOption(ExcludesMaskKey, maskExpression);
        }
    }

    public static class FieldIncludesCommandExtensions
    {
        internal const string IncludesKey = "@Includes";
        public static T Include<T>(this T options, Field field) where T : ICommandOptions
        {
            return options.AddCollectionOptionValue(IncludesKey, field);
        }

        public static T Include<T>(this T options, IEnumerable<Field> fields) where T : ICommandOptions
        {
            return options.AddCollectionOptionValue(IncludesKey, fields);
        }

        public static ICommandOptions Include<T>(this ICommandOptions options, Expression<Func<T, object>> objectPath)
        {
            return options.AddCollectionOptionValue<ICommandOptions, Field>(IncludesKey, objectPath);
        }

        public static ICommandOptions<T> Include<T>(this ICommandOptions<T> options, Expression<Func<T, object>> objectPath) where T : class
        {
            return options.AddCollectionOptionValue<ICommandOptions<T>, Field>(IncludesKey, objectPath);
        }

        public static ICommandOptions<T> Include<T>(this ICommandOptions<T> options, params Expression<Func<T, object>>[] objectPaths) where T : class
        {
            foreach (var objectPath in objectPaths)
                options.Include(objectPath);

            return options;
        }

        internal const string IncludesMaskKey = "@IncludesMask";
        public static T IncludeMask<T>(this T options, string maskExpression) where T : ICommandOptions
        {
            return options.BuildOption(IncludesMaskKey, maskExpression);
        }

        internal const string ExcludesKey = "@Excludes";
        public static T Exclude<T>(this T options, Field field) where T : ICommandOptions
        {
            return options.AddCollectionOptionValue(ExcludesKey, field);
        }

        public static T Exclude<T>(this T options, IEnumerable<Field> fields) where T : ICommandOptions
        {
            return options.AddCollectionOptionValue(ExcludesKey, fields);
        }

        public static ICommandOptions<T> Exclude<T>(this ICommandOptions<T> options, Expression<Func<T, object>> objectPath) where T : class
        {
            return options.AddCollectionOptionValue<ICommandOptions<T>, Field>(ExcludesKey, objectPath);
        }

        public static ICommandOptions<T> Exclude<T>(this ICommandOptions<T> options, params Expression<Func<T, object>>[] objectPaths) where T : class
        {
            foreach (var objectPath in objectPaths)
                options.Exclude(objectPath);

            return options;
        }

        internal const string ExcludesMaskKey = "@ExcludesMask";
        public static T ExcludeMask<T>(this T options, string maskExpression) where T : ICommandOptions
        {
            return options.BuildOption(ExcludesMaskKey, maskExpression);
        }
    }
}

namespace Foundatio.Repositories.Options
{
    public static class ReadFieldIncludesQueryExtensions
    {
        public static ICollection<Field> GetIncludes(this IRepositoryQuery options)
        {
            return options.SafeGetCollection<Field>(FieldIncludesQueryExtensions.IncludesKey);
        }

        public static string GetIncludeMask(this IRepositoryQuery options)
        {
            return options.SafeGetOption<string>(FieldIncludesQueryExtensions.IncludesMaskKey);
        }

        public static ICollection<Field> GetExcludes(this IRepositoryQuery options)
        {
            return options.SafeGetCollection<Field>(FieldIncludesQueryExtensions.ExcludesKey);
        }

        public static string GetExcludeMask(this IRepositoryQuery options)
        {
            return options.SafeGetOption<string>(FieldIncludesQueryExtensions.ExcludesMaskKey);
        }
    }

    public static class ReadFieldIncludesCommandExtensions
    {
        public static ICollection<Field> GetIncludes(this ICommandOptions options)
        {
            return options.SafeGetCollection<Field>(FieldIncludesCommandExtensions.IncludesKey);
        }

        public static string GetIncludeMask(this ICommandOptions options)
        {
            return options.SafeGetOption<string>(FieldIncludesCommandExtensions.IncludesMaskKey);
        }

        public static ICollection<Field> GetExcludes(this ICommandOptions options)
        {
            return options.SafeGetCollection<Field>(FieldIncludesCommandExtensions.ExcludesKey);
        }

        public static string GetExcludeMask(this ICommandOptions options)
        {
            return options.SafeGetOption<string>(FieldIncludesCommandExtensions.ExcludesMaskKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders
{
    public class FieldIncludesQueryBuilder : IElasticQueryBuilder
    {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            var resolver = ctx.GetMappingResolver();

            var includes = new HashSet<Field>();
            includes.AddRange(ctx.Source.GetIncludes());
            includes.AddRange(ctx.Options.GetIncludes());
            if (includes.Count > 0 && typeof(Models.IIdentity).IsAssignableFrom(typeof(T)))
                includes.Add(nameof(Models.IIdentity.Id));

            string queryIncludeMask = ctx.Source.GetIncludeMask();
            if (!String.IsNullOrEmpty(queryIncludeMask))
                includes.AddRange(FieldIncludeParser.ParseFieldPaths(queryIncludeMask).Select(f => (Field)f));

            string optionIncludeMask = ctx.Options.GetIncludeMask();
            if (!String.IsNullOrEmpty(optionIncludeMask))
                includes.AddRange(FieldIncludeParser.ParseFieldPaths(optionIncludeMask).Select(f => (Field)f));

            var resolvedIncludes = resolver.GetResolvedFields(includes).ToArray();

            var excludes = new HashSet<Field>();
            excludes.AddRange(ctx.Source.GetExcludes());
            excludes.AddRange(ctx.Options.GetExcludes());

            string queryExcludeMask = ctx.Source.GetExcludeMask();
            if (!String.IsNullOrEmpty(queryExcludeMask))
                excludes.AddRange(FieldIncludeParser.ParseFieldPaths(queryExcludeMask).Select(f => (Field)f));

            string optionExcludeMask = ctx.Options.GetExcludeMask();
            if (!String.IsNullOrEmpty(optionExcludeMask))
                excludes.AddRange(FieldIncludeParser.ParseFieldPaths(optionExcludeMask).Select(f => (Field)f));

            var resolvedExcludes = resolver.GetResolvedFields(excludes)
                .Where(f => !resolvedIncludes.Contains(f))
                .ToArray();

            if (resolvedIncludes.Length > 0 || resolvedExcludes.Length > 0)
            {
                var filter = new SourceFilter();
                if (resolvedIncludes.Length > 0)
                    filter.Includes = resolvedIncludes;
                if (resolvedExcludes.Length > 0)
                    filter.Excludes = resolvedExcludes;
                ctx.Search.Source(new SourceConfig(filter));
            }

            return Task.CompletedTask;
        }
    }
}
