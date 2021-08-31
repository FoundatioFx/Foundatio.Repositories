using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories {
    public static class RuntimeFieldsQueryExtensions {
        internal const string RuntimeFieldsKey = "@RuntimeFields";
        public static T RuntimeField<T>(this T query, string name, ElasticRuntimeFieldType fieldType = ElasticRuntimeFieldType.Keyword) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(RuntimeFieldsKey, new ElasticRuntimeField { Name = name, FieldType = fieldType });
        }

        public static T RuntimeField<T>(this T query, ElasticRuntimeField field) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(RuntimeFieldsKey, field);
        }

        public static T RuntimeField<T>(this T query, IEnumerable<ElasticRuntimeField> fields) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(RuntimeFieldsKey, fields);
        }

        internal const string RuntimeFieldResolverKey = "@RuntimeFieldResolver";
        public static T RuntimeFieldResolver<T>(this T query, RuntimeFieldResolver fieldResolver) where T : IRepositoryQuery {
            return query.BuildOption(RuntimeFieldResolverKey, fieldResolver);
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadRuntimeFieldsQueryExtensions {
        public static ICollection<ElasticRuntimeField> GetRuntimeFields(this IRepositoryQuery options) {
            return options.SafeGetCollection<ElasticRuntimeField>(RuntimeFieldsQueryExtensions.RuntimeFieldsKey);
        }

        public static RuntimeFieldResolver GetRuntimeFieldResolver(this IRepositoryQuery options) {
            return options.SafeGetOption<RuntimeFieldResolver>(RuntimeFieldsQueryExtensions.RuntimeFieldResolverKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class AddRuntimeFieldsToContextQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            if (ctx is not IElasticQueryVisitorContext elasticContext)
                return Task.CompletedTask;

            foreach (var field in ctx.Source.GetRuntimeFields())
                elasticContext.RuntimeFields.Add(field);

            var fieldResolver = ctx.Source.GetRuntimeFieldResolver();
            if (fieldResolver != null)
                elasticContext.RuntimeFieldResolver = elasticContext.RuntimeFieldResolver != null ? f => fieldResolver(f) ?? elasticContext.RuntimeFieldResolver(f) : fieldResolver;

            return Task.CompletedTask;
        }
    }

    public class RuntimeFieldsQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            if (ctx is not IElasticQueryVisitorContext elasticContext)
                return Task.CompletedTask;

            // fields need to be added to the context from the query before this
            foreach (var field in elasticContext.RuntimeFields)
                ctx.Search.RuntimeFields<T>(f => f.RuntimeField(field.Name, GetFieldType(field.FieldType), d => {
                    if (!String.IsNullOrEmpty(field.Script))
                        d.Script(field.Script);

                    return d;
                }));

            return Task.CompletedTask;
        }

        private FieldType GetFieldType(ElasticRuntimeFieldType fieldType) {
            return fieldType switch {
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