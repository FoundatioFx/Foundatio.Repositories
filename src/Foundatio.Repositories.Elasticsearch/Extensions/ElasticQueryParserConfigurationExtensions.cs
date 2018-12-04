using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Microsoft.Extensions.Logging;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public static class ElasticQueryParserConfigurationExtensions {
        public static ElasticQueryParserConfiguration UseMappings<T>(this ElasticQueryParserConfiguration config, Index<T> index) where T : class {
            var logger = index.Configuration.LoggerFactory.CreateLogger(typeof(ElasticQueryParserConfiguration));
            var descriptor = index.BuildMapping(new TypeMappingDescriptor<T>());

            return config
                .UseAliases(index.AliasMap)
                .UseMappings<T>(d => descriptor, () => {
                    var response = index.Configuration.Client.GetMapping(new GetMappingRequest(index.Name, ElasticConfiguration.DocType));
                    if (response.IsValid)
                        logger.LogTraceRequest(response);
                    else
                        logger.LogErrorRequest(response, "Error getting mapping for index {Name}", index.Name);

                    return (ITypeMapping) response.Mapping ?? descriptor;
                });
        }
    }
}