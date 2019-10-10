using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class DynamicIndex<T> : Index<T> where T : class {
        public DynamicIndex(IElasticConfiguration configuration, string name = null): base(configuration, name) {}
        
        protected override ElasticQueryParser CreateQueryParser() {
            var parser = base.CreateQueryParser();
            parser.Configuration.UseMappings<T>(ConfigureIndexMapping, Configuration.Client, Name);
            return parser;
        }

        public override TypeMappingDescriptor<T> ConfigureIndexMapping(TypeMappingDescriptor<T> map) {
            return map.Dynamic().AutoMap<T>().Properties(p => p.SetupDefaults());
        }
    }
}