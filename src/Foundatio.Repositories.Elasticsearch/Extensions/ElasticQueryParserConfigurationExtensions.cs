using System;
using Foundatio.Logging;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Nest;
using Foundatio.Repositories.Elasticsearch.Extensions;

namespace Foundatio.Parsers.ElasticQueries {
    public static class ElasticQueryParserConfigurationExtensions {
        public static ElasticQueryParserConfiguration UseMappings<T>(this ElasticQueryParserConfiguration config, IndexTypeBase<T> indexType) where T : class {
            var logger = indexType.Configuration.LoggerFactory.CreateLogger(typeof(ElasticQueryParserConfiguration));

            var visitor = new AliasMappingVisitor(indexType.Configuration.Client.Infer);
            var walker = new MappingWalker(visitor);

            var descriptor = indexType.BuildMapping(new TypeMappingDescriptor<T>());
            walker.Accept(descriptor);

            return config
                .UseAliases(visitor.RootAliasMap)
                .UseMappings<T>(d => descriptor, () => {
                    var response = indexType.Configuration.Client.GetMapping(new GetMappingRequest(indexType.Index.Name, indexType.Name));
                    logger.Trace(() => response.GetRequest());
                    if (!response.IsValid) 
                        logger.Error(response.OriginalException, response.GetErrorMessage());

                    return (ITypeMapping) response.Mapping ?? descriptor;
                });
        }
    }
}