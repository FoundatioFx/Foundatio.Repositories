using System;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public static class ElasticQueryParserConfigurationExtensions {
        public static ElasticQueryParserConfiguration UseMappings<T>(this ElasticQueryParserConfiguration config, IndexTypeBase<T> indexType) where T : class {
            var visitor = new AliasMappingVisitor(indexType.Configuration.Client.Infer);
            var walker = new MappingWalker(visitor);

            var descriptor = indexType.BuildMapping(new TypeMappingDescriptor<T>());
            walker.Accept(descriptor);

            return config
                .UseAliases(visitor.RootAliasMap)
                .UseMappings<T>(d => descriptor, () => indexType.Configuration.Client.GetMapping(new GetMappingRequest(indexType.Index.Name, indexType.Name)).Mapping);
        }
    }
}