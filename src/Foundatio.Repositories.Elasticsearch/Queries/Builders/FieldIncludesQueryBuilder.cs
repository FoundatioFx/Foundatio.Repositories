using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Utility;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories
{
    /// <summary>
    /// Extension methods for configuring field includes and excludes on <see cref="IRepositoryQuery"/>.
    /// These control Elasticsearch <c>_source</c> filtering to limit which fields are returned.
    /// Includes and excludes from the query are merged with those from <see cref="ICommandOptions"/> at execution time.
    /// If the same field appears in both includes and excludes, the include takes precedence.
    /// When any includes are specified on an <see cref="Models.IIdentity"/> type, the <c>Id</c> field is automatically included.
    /// </summary>
    public static class FieldIncludesQueryExtensions
    {
        internal const string IncludesKey = "@Includes";

        /// <summary>
        /// Adds a field to the set of included fields returned from Elasticsearch <c>_source</c> filtering.
        /// Multiple calls are additive. These are merged with any includes set via <see cref="IncludeMask{T}"/>
        /// and with includes from <see cref="ICommandOptions"/>.
        /// </summary>
        public static T Include<T>(this T query, Field field) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(IncludesKey, field);
        }

        /// <inheritdoc cref="Include{T}(T, Field)"/>
        public static T Include<T>(this T query, IEnumerable<Field> fields) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(IncludesKey, fields);
        }

        /// <inheritdoc cref="Include{T}(T, Field)"/>
        public static IRepositoryQuery Include<T>(this IRepositoryQuery query, Expression<Func<T, object>> objectPath)
        {
            return query.AddCollectionOptionValue<IRepositoryQuery, Field>(IncludesKey, objectPath);
        }

        /// <inheritdoc cref="Include{T}(T, Field)"/>
        public static IRepositoryQuery<T> Include<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath) where T : class
        {
            return query.AddCollectionOptionValue<IRepositoryQuery<T>, Field>(IncludesKey, objectPath);
        }

        /// <inheritdoc cref="Include{T}(T, Field)"/>
        public static IRepositoryQuery<T> Include<T>(this IRepositoryQuery<T> query, params Expression<Func<T, object>>[] objectPaths) where T : class
        {
            foreach (var objectPath in objectPaths)
                query.Include(objectPath);

            return query;
        }

        internal const string IncludesMaskKey = "@IncludesMask";

        /// <summary>
        /// Sets a field mask expression that specifies which fields to include. The expression uses a
        /// Google FieldMask-style syntax where nested fields are grouped with parentheses
        /// (e.g., <c>"id,address(street,city)"</c> expands to <c>id</c>, <c>address.street</c>, <c>address.city</c>).
        /// The parsed fields are merged with any individually added via <see cref="Include{T}(T, Field)"/>.
        /// </summary>
        public static T IncludeMask<T>(this T options, string maskExpression) where T : IRepositoryQuery
        {
            return options.BuildOption(IncludesMaskKey, maskExpression);
        }

        internal const string ExcludesKey = "@Excludes";

        /// <summary>
        /// Adds a field to the set of excluded fields omitted from Elasticsearch <c>_source</c> filtering.
        /// Multiple calls are additive. These are merged with any excludes set via <see cref="ExcludeMask{T}"/>
        /// and with excludes from <see cref="ICommandOptions"/>.
        /// If the same field appears in both includes and excludes, the include takes precedence.
        /// </summary>
        /// <remarks>
        /// Setting any explicit excludes on a query will cause default excludes (registered via
        /// <c>AddDefaultExclude</c> on the repository) to be skipped.
        /// </remarks>
        public static T Exclude<T>(this T query, Field field) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(ExcludesKey, field);
        }

        /// <inheritdoc cref="Exclude{T}(T, Field)"/>
        public static T Exclude<T>(this T query, IEnumerable<Field> fields) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(ExcludesKey, fields);
        }

        /// <inheritdoc cref="Exclude{T}(T, Field)"/>
        public static IRepositoryQuery<T> Exclude<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath) where T : class
        {
            return query.AddCollectionOptionValue<IRepositoryQuery<T>, Field>(ExcludesKey, objectPath);
        }

        /// <inheritdoc cref="Exclude{T}(T, Field)"/>
        public static IRepositoryQuery<T> Exclude<T>(this IRepositoryQuery<T> query, params Expression<Func<T, object>>[] objectPaths) where T : class
        {
            foreach (var objectPath in objectPaths)
                query.Exclude(objectPath);

            return query;
        }

        internal const string ExcludesMaskKey = "@ExcludesMask";

        /// <summary>
        /// Sets a field mask expression that specifies which fields to exclude. The expression uses a
        /// Google FieldMask-style syntax where nested fields are grouped with parentheses
        /// (e.g., <c>"internal(data,logs)"</c> expands to <c>internal.data</c>, <c>internal.logs</c>).
        /// The parsed fields are merged with any individually added via <see cref="Exclude{T}(T, Field)"/>.
        /// </summary>
        public static T ExcludeMask<T>(this T options, string maskExpression) where T : IRepositoryQuery
        {
            return options.BuildOption(ExcludesMaskKey, maskExpression);
        }
    }

    /// <summary>
    /// Extension methods for configuring field includes and excludes on <see cref="ICommandOptions"/>.
    /// These control Elasticsearch <c>_source</c> filtering to limit which fields are returned.
    /// Includes and excludes from command options are merged with those from <see cref="IRepositoryQuery"/> at execution time.
    /// If the same field appears in both includes and excludes, the include takes precedence.
    /// When any includes are specified on an <see cref="Models.IIdentity"/> type, the <c>Id</c> field is automatically included.
    /// </summary>
    /// <remarks>
    /// ID-based caching is skipped when includes or excludes are active to avoid storing incomplete documents in the cache.
    /// </remarks>
    public static class FieldIncludesCommandExtensions
    {
        internal const string IncludesKey = "@Includes";

        /// <summary>
        /// Adds a field to the set of included fields returned from Elasticsearch <c>_source</c> filtering.
        /// Multiple calls are additive. These are merged with any includes set via <see cref="IncludeMask{T}"/>
        /// and with includes from <see cref="IRepositoryQuery"/>.
        /// </summary>
        public static T Include<T>(this T options, Field field) where T : ICommandOptions
        {
            return options.AddCollectionOptionValue(IncludesKey, field);
        }

        /// <inheritdoc cref="Include{T}(T, Field)"/>
        public static T Include<T>(this T options, IEnumerable<Field> fields) where T : ICommandOptions
        {
            return options.AddCollectionOptionValue(IncludesKey, fields);
        }

        /// <inheritdoc cref="Include{T}(T, Field)"/>
        public static ICommandOptions Include<T>(this ICommandOptions options, Expression<Func<T, object>> objectPath)
        {
            return options.AddCollectionOptionValue<ICommandOptions, Field>(IncludesKey, objectPath);
        }

        /// <inheritdoc cref="Include{T}(T, Field)"/>
        public static ICommandOptions<T> Include<T>(this ICommandOptions<T> options, Expression<Func<T, object>> objectPath) where T : class
        {
            return options.AddCollectionOptionValue<ICommandOptions<T>, Field>(IncludesKey, objectPath);
        }

        /// <inheritdoc cref="Include{T}(T, Field)"/>
        public static ICommandOptions<T> Include<T>(this ICommandOptions<T> options, params Expression<Func<T, object>>[] objectPaths) where T : class
        {
            foreach (var objectPath in objectPaths)
                options.Include(objectPath);

            return options;
        }

        internal const string IncludesMaskKey = "@IncludesMask";

        /// <summary>
        /// Sets a field mask expression that specifies which fields to include. The expression uses a
        /// Google FieldMask-style syntax where nested fields are grouped with parentheses
        /// (e.g., <c>"id,address(street,city)"</c> expands to <c>id</c>, <c>address.street</c>, <c>address.city</c>).
        /// The parsed fields are merged with any individually added via <see cref="Include{T}(T, Field)"/>.
        /// </summary>
        public static T IncludeMask<T>(this T options, string maskExpression) where T : ICommandOptions
        {
            return options.BuildOption(IncludesMaskKey, maskExpression);
        }

        internal const string ExcludesKey = "@Excludes";

        /// <summary>
        /// Adds a field to the set of excluded fields omitted from Elasticsearch <c>_source</c> filtering.
        /// Multiple calls are additive. These are merged with any excludes set via <see cref="ExcludeMask{T}"/>
        /// and with excludes from <see cref="IRepositoryQuery"/>.
        /// If the same field appears in both includes and excludes, the include takes precedence.
        /// </summary>
        public static T Exclude<T>(this T options, Field field) where T : ICommandOptions
        {
            return options.AddCollectionOptionValue(ExcludesKey, field);
        }

        /// <inheritdoc cref="Exclude{T}(T, Field)"/>
        public static T Exclude<T>(this T options, IEnumerable<Field> fields) where T : ICommandOptions
        {
            return options.AddCollectionOptionValue(ExcludesKey, fields);
        }

        /// <inheritdoc cref="Exclude{T}(T, Field)"/>
        public static ICommandOptions<T> Exclude<T>(this ICommandOptions<T> options, Expression<Func<T, object>> objectPath) where T : class
        {
            return options.AddCollectionOptionValue<ICommandOptions<T>, Field>(ExcludesKey, objectPath);
        }

        /// <inheritdoc cref="Exclude{T}(T, Field)"/>
        public static ICommandOptions<T> Exclude<T>(this ICommandOptions<T> options, params Expression<Func<T, object>>[] objectPaths) where T : class
        {
            foreach (var objectPath in objectPaths)
                options.Exclude(objectPath);

            return options;
        }

        internal const string ExcludesMaskKey = "@ExcludesMask";

        /// <summary>
        /// Sets a field mask expression that specifies which fields to exclude. The expression uses a
        /// Google FieldMask-style syntax where nested fields are grouped with parentheses
        /// (e.g., <c>"internal(data,logs)"</c> expands to <c>internal.data</c>, <c>internal.logs</c>).
        /// The parsed fields are merged with any individually added via <see cref="Exclude{T}(T, Field)"/>.
        /// </summary>
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
    /// <summary>
    /// Elasticsearch query builder that applies <c>_source</c> filtering based on field includes and excludes.
    /// </summary>
    /// <remarks>
    /// <para>
    /// At execution time, this builder merges field includes and excludes from all sources into a single set:
    /// </para>
    /// <list type="bullet">
    ///   <item>Individual fields from <see cref="IRepositoryQuery"/> (<c>.Include()</c> / <c>.Exclude()</c>)</item>
    ///   <item>Individual fields from <see cref="ICommandOptions"/> (<c>.Include()</c> / <c>.Exclude()</c>)</item>
    ///   <item>Parsed mask expressions from <see cref="IRepositoryQuery"/> (<c>.IncludeMask()</c> / <c>.ExcludeMask()</c>)</item>
    ///   <item>Parsed mask expressions from <see cref="ICommandOptions"/> (<c>.IncludeMask()</c> / <c>.ExcludeMask()</c>)</item>
    /// </list>
    /// <para>
    /// When any includes are present and the entity type implements <see cref="Models.IIdentity"/>, the <c>Id</c>
    /// field is automatically added to the include set.
    /// If a field appears in both includes and excludes, the include takes precedence (the exclude is dropped).
    /// </para>
    /// </remarks>
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

            if (resolvedIncludes.Length > 0 && resolvedExcludes.Length > 0)
                ctx.Search.Source(s => s.Includes(i => i.Fields(resolvedIncludes)).Excludes(i => i.Fields(resolvedExcludes)));
            else if (resolvedIncludes.Length > 0)
                ctx.Search.Source(s => s.Includes(i => i.Fields(resolvedIncludes)));
            else if (resolvedExcludes.Length > 0)
                ctx.Search.Source(s => s.Excludes(i => i.Fields(resolvedExcludes)));

            return Task.CompletedTask;
        }
    }
}
