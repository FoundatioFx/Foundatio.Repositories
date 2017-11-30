using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
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
                    logger.LogTrace(response.GetRequest());
                    if (!response.IsValid) 
                        logger.LogError(response.OriginalException, response.GetErrorMessage());

                    return (ITypeMapping) response.Mapping ?? descriptor;
                });
        }
    }
}