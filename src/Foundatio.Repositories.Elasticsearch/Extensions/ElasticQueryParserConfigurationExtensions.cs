using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Microsoft.Extensions.Logging;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public static class ElasticQueryParserConfigurationExtensions {
        public static ElasticQueryParserConfiguration UseMappings<T>(this ElasticQueryParserConfiguration config, IndexTypeBase<T> indexType) where T : class {
            var logger = indexType.Configuration.LoggerFactory.CreateLogger(typeof(ElasticQueryParserConfiguration));
            var descriptor = indexType.BuildMapping(new TypeMappingDescriptor<T>());

            return config
                .UseAliases(indexType.AliasMap)
                .UseMappings<T>(d => descriptor, () => {
                    var response = indexType.Configuration.Client.GetMapping(new GetMappingRequest(indexType.Index.Name, indexType.Name));
                    if (response.IsValid)
                        logger.LogTraceRequest(response);
                    else
                        logger.LogErrorRequest(response, "Error getting mapping for index {Name}", indexType.Index.Name);

                    return (ITypeMapping) response.Mapping ?? descriptor;
                });
        }
    }
}