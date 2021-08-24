using System;
using System.Collections.Generic;
using System.Threading.Tasks;
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
    }

    public class ElasticRuntimeField {
        public string Name { get; set; }
        public ElasticRuntimeFieldType FieldType { get; set; }
        public string Script { get; set; }
    }

    // This is the list of supported field types for runtime fields:
    // https://www.elastic.co/guide/en/elasticsearch/reference/master/runtime-mapping-fields.html
    public enum ElasticRuntimeFieldType {
        Boolean,
        Date,
        Double,
        GeoPoint,
        Ip,
        Keyword,
        Long
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadRuntimeFieldsQueryExtensions {
        public static ICollection<ElasticRuntimeField> GetRuntimeFields(this IRepositoryQuery options) {
            return options.SafeGetCollection<ElasticRuntimeField>(RuntimeFieldsQueryExtensions.RuntimeFieldsKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class RuntimeFieldsQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var runtimeFields = ctx.Source.GetRuntimeFields();
            foreach (var field in runtimeFields)
                ctx.Search.RuntimeFields<T>(f => f.RuntimeField(field.Name, GetFieldType(field.FieldType), d => {
                    if (!String.IsNullOrEmpty(field.Script))
                        d.Script(field.Script);

                    return d;
                }));

            return Task.CompletedTask;
        }

        private FieldType GetFieldType(ElasticRuntimeFieldType fieldType) {
            switch (fieldType) {
                case ElasticRuntimeFieldType.Boolean: return FieldType.Boolean;
                case ElasticRuntimeFieldType.Date: return FieldType.Date;
                case ElasticRuntimeFieldType.Double: return FieldType.Double;
                case ElasticRuntimeFieldType.GeoPoint: return FieldType.GeoPoint;
                case ElasticRuntimeFieldType.Ip: return FieldType.Ip;
                case ElasticRuntimeFieldType.Keyword: return FieldType.Keyword;
                case ElasticRuntimeFieldType.Long: return FieldType.Long;
                default: return FieldType.Keyword;
            }
        }
    }
}