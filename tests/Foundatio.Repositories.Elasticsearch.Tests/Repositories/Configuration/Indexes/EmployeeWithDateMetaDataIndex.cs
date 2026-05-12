using System.Text.Json;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;

public sealed class EmployeeWithDateMetaDataIndex : Index<EmployeeWithDateMetaData>
{
    private static string CamelCase(string name) => JsonNamingPolicy.CamelCase.ConvertName(name);

    public EmployeeWithDateMetaDataIndex(IElasticConfiguration configuration) : base(configuration, "employees-metadata") { }

    public override void ConfigureIndex(Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexRequestDescriptor idx)
    {
        base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
    }

    public override void ConfigureIndexMapping(TypeMappingDescriptor<EmployeeWithDateMetaData> map)
    {
        map
            .Dynamic(DynamicMapping.False)
            .Properties(p => p
                .SetupDefaults()
                .Text(e => e.Name)
                .IntegerNumber(e => e.Age)
                .Keyword(e => e.CompanyName)
                .Keyword(e => e.CompanyId)
                .Object(e => e.MetaData, mp => mp
                    .Properties(p2 => p2
                        .Date(CamelCase(nameof(DateMetaData.DateCreatedUtc)))
                        .Date(CamelCase(nameof(DateMetaData.DateUpdatedUtc)))
                    ))
            );
    }
}
