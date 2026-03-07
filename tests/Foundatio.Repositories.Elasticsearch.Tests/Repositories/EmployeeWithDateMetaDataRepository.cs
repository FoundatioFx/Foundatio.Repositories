using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public interface IEmployeeWithDateMetaDataRepository : ISearchableRepository<EmployeeWithDateMetaData>
{
    TimeProvider TimeProvider { get; set; }
}

public class EmployeeWithDateMetaDataRepository : ElasticRepositoryBase<EmployeeWithDateMetaData>, IEmployeeWithDateMetaDataRepository
{
    public EmployeeWithDateMetaDataRepository(MyAppElasticConfiguration elasticConfiguration) : base(elasticConfiguration.EmployeesWithDateMetaData) { }

    public EmployeeWithDateMetaDataRepository(IIndex index) : base(index) { }

    public TimeProvider TimeProvider
    {
        get => ElasticIndex.Configuration.TimeProvider;
        set => ElasticIndex.Configuration.TimeProvider = value;
    }

    protected override bool HasDateTracking => true;

    protected override string GetUpdatedUtcFieldPath()
    {
        return InferField(d => ((IHaveDateMetaData)d).MetaData.DateUpdatedUtc);
    }

    protected override void SetDocumentDates(EmployeeWithDateMetaData document, TimeProvider timeProvider)
    {
        base.SetDocumentDates(document, timeProvider);

        if (document is IHaveDateMetaData metaDoc)
        {
            var utcNow = timeProvider.GetUtcNow().UtcDateTime;
            metaDoc.MetaData ??= new DateMetaData();

            if (metaDoc.MetaData.DateCreatedUtc is null
                || metaDoc.MetaData.DateCreatedUtc == DateTime.MinValue
                || metaDoc.MetaData.DateCreatedUtc > utcNow)
                metaDoc.MetaData.DateCreatedUtc = utcNow;

            metaDoc.MetaData.DateUpdatedUtc = utcNow;
        }
    }
}
