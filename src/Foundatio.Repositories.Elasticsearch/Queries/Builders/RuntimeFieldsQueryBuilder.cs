using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories
{
    public static class RuntimeFieldsQueryExtensions
    {
        internal const string RuntimeFieldsKey = "@RuntimeFields";
        public static T RuntimeField<T>(this T query, string name, ElasticRuntimeFieldType fieldType = ElasticRuntimeFieldType.Keyword) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(RuntimeFieldsKey, new ElasticRuntimeField { Name = name, FieldType = fieldType });
        }

        public static T RuntimeField<T>(this T query, ElasticRuntimeField field) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(RuntimeFieldsKey, field);
        }

        public static T RuntimeField<T>(this T query, IEnumerable<ElasticRuntimeField> fields) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue(RuntimeFieldsKey, fields);
        }
    }

    public static class RuntimeFieldsOptionsExtensions
    {
        internal const string EnableRuntimeFieldResolverKey = "@EnableRuntimeFieldResolver";

        public static T EnableRuntimeFieldResolver<T>(this T options, bool enabled = true) where T : ICommandOptions
        {
            return options.BuildOption(EnableRuntimeFieldResolverKey, enabled);
        }

        internal const string RuntimeFieldResolverKey = "@RuntimeFieldResolver";
        public static T RuntimeFieldResolver<T>(this T options, RuntimeFieldResolver fieldResolver) where T : ICommandOptions
        {
            return options.BuildOption(RuntimeFieldResolverKey, fieldResolver).BuildOption(EnableRuntimeFieldResolverKey, true);
        }
    }
}

namespace Foundatio.Repositories.Queries
{
    public static class ReadRuntimeFieldsQueryExtensions
    {
        public static ICollection<ElasticRuntimeField> GetRuntimeFields(this IRepositoryQuery options)
        {
            return options.SafeGetCollection<ElasticRuntimeField>(RuntimeFieldsQueryExtensions.RuntimeFieldsKey);
        }
    }
}

namespace Foundatio.Repositories.Options
{
    public static class ReadRuntimeFieldsOptionsExtensions
    {
        public static bool? IsRuntimeFieldResolvingEnabled(this ICommandOptions options)
        {
            return options.SafeGetOption<bool?>(RuntimeFieldsOptionsExtensions.EnableRuntimeFieldResolverKey);
        }

        public static RuntimeFieldResolver GetRuntimeFieldResolver(this ICommandOptions options)
        {
            return options.SafeGetOption<RuntimeFieldResolver>(RuntimeFieldsOptionsExtensions.RuntimeFieldResolverKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders
{
    public class AddRuntimeFieldsToContextQueryBuilder : IElasticQueryBuilder
    {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            if (ctx is not IElasticQueryVisitorContext elasticContext)
                return Task.CompletedTask;

            foreach (var field in ctx.Source.GetRuntimeFields())
                elasticContext.RuntimeFields.Add(field);

            var enabled = ctx.Options.IsRuntimeFieldResolvingEnabled();
            if (enabled.HasValue)
                elasticContext.EnableRuntimeFieldResolver = enabled;

            var fieldResolver = ctx.Options.GetRuntimeFieldResolver();
            if (fieldResolver != null)
                elasticContext.RuntimeFieldResolver = elasticContext.RuntimeFieldResolver != null ? f => fieldResolver(f) ?? elasticContext.RuntimeFieldResolver(f) : fieldResolver;

            return Task.CompletedTask;
        }
    }

    public class RuntimeFieldsQueryBuilder : IElasticQueryBuilder
    {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            if (ctx is not IElasticQueryVisitorContext elasticContext)
                return Task.CompletedTask;

            // fields need to be added to the context from the query before this
            if (elasticContext.RuntimeFields.Count > 0)
                ctx.Search.RuntimeFields<T>(f =>
                {
                    foreach (var field in elasticContext.RuntimeFields)
                        f.RuntimeField(field.Name, GetFieldType(field.FieldType), d =>
                        {
                            if (!String.IsNullOrEmpty(field.Script))
                                d.Script(field.Script);

                            return d;
                        });
                    return f;
                });

            return Task.CompletedTask;
        }

        private FieldType GetFieldType(ElasticRuntimeFieldType fieldType)
        {
            return fieldType switch
            {
                ElasticRuntimeFieldType.Boolean => FieldType.Boolean,
                ElasticRuntimeFieldType.Date => FieldType.Date,
                ElasticRuntimeFieldType.Double => FieldType.Double,
                ElasticRuntimeFieldType.GeoPoint => FieldType.GeoPoint,
                ElasticRuntimeFieldType.Ip => FieldType.Ip,
                ElasticRuntimeFieldType.Keyword => FieldType.Keyword,
                ElasticRuntimeFieldType.Long => FieldType.Long,
                _ => FieldType.Keyword,
            };
        }
    }
}
