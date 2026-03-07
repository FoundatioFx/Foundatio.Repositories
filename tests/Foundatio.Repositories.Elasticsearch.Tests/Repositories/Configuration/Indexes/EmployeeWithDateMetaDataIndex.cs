using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;

public sealed class EmployeeWithDateMetaDataIndex : Index<EmployeeWithDateMetaData>
{
    public EmployeeWithDateMetaDataIndex(IElasticConfiguration configuration) : base(configuration, "employees-metadata") { }

    public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx)
    {
        return base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
    }

    public override TypeMappingDescriptor<EmployeeWithDateMetaData> ConfigureIndexMapping(TypeMappingDescriptor<EmployeeWithDateMetaData> map)
    {
        return map
            .Dynamic(false)
            .Properties(p => p
                .SetupDefaults()
                .Keyword(f => f.Name(e => e.Id))
                .Text(f => f.Name(e => e.Name))
                .Scalar(f => f.Age, f => f.Name(e => e.Age))
                .Keyword(f => f.Name(e => e.CompanyName))
                .Keyword(f => f.Name(e => e.CompanyId))
                .Object<DateMetaData>(o => o.Name(e => e.MetaData).Properties(mp => mp
                    .Date(d => d.Name(m => m.DateCreatedUtc))
                    .Date(d => d.Name(m => m.DateUpdatedUtc))
                ))
            );
    }
}
