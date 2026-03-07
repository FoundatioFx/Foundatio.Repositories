using System;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

public interface IDateMetaData
{
    DateTime? DateCreatedUtc { get; set; }
    DateTime? DateUpdatedUtc { get; set; }
}

public class DateMetaData : IDateMetaData
{
    public DateTime? DateCreatedUtc { get; set; }
    public DateTime? DateUpdatedUtc { get; set; }
}

public interface IHaveDateMetaData
{
    IDateMetaData MetaData { get; set; }
}

public class EmployeeWithDateMetaData : IIdentity, IVersioned, IHaveDateMetaData
{
    public string Id { get; set; }
    public string Name { get; set; }
    public int Age { get; set; }
    public string CompanyName { get; set; }
    public string CompanyId { get; set; }
    public string Version { get; set; }
    public DateMetaData MetaData { get; set; } = new DateMetaData();
    IDateMetaData IHaveDateMetaData.MetaData { get => MetaData; set => MetaData = value as DateMetaData ?? new DateMetaData { DateCreatedUtc = value?.DateCreatedUtc, DateUpdatedUtc = value?.DateUpdatedUtc }; }
}

public static class EmployeeWithDateMetaDataGenerator
{
    public static EmployeeWithDateMetaData Default => new()
    {
        Name = "Blake",
        Age = 29,
        CompanyName = "Exceptionless",
        CompanyId = "default-company"
    };

    public static EmployeeWithDateMetaData Generate(string id = null, string name = null, int? age = null, string companyName = null)
    {
        return new EmployeeWithDateMetaData
        {
            Id = id,
            Name = name ?? "Test",
            Age = age ?? 25,
            CompanyName = companyName ?? "TestCo",
            CompanyId = "test-company"
        };
    }
}
