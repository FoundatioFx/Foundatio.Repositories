using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Elastic.Clients.Elasticsearch.Mapping;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

public class DynamicIndex<T> : Index<T> where T : class
{
    public DynamicIndex(IElasticConfiguration configuration, string name = null) : base(configuration, name) { }

    protected override ElasticMappingResolver CreateMappingResolver()
    {
        return ElasticMappingResolver.Create<T>(ConfigureIndexMapping, Configuration.Client, Name, _logger);
    }

    public override void ConfigureIndexMapping(TypeMappingDescriptor<T> map)
    {
        map.Dynamic(DynamicMapping.True).Properties(p => p.SetupDefaults());
    }
}
