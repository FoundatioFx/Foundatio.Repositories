using System;
using Exceptionless;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Elasticsearch.Tests.Models {
    public class Employee : IIdentity, IHaveDates {
        public string Id { get; set; }
        public string CompanyId { get; set; }
        public string CompanyName { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime UpdatedUtc { get; set; }
        public DateTimeOffset StartDate { get; set; }
    }

    public static class EmployeeGenerator {
        public static readonly string DefaultCompanyId = ObjectId.GenerateNewId().ToString();

        public static Employee Default => new Employee {
            Name = "Blake",
            Age = 29,
            CompanyName = "Exceptionless",
            CompanyId = DefaultCompanyId,
            StartDate = DateTimeOffset.Now
        };

        public static Employee Generate(string id = null, string name = null, int? age = null, string companyName = null, string companyId = null, DateTime? createdUtc = null, DateTime? updatedUtc = null, DateTimeOffset? startDate = null) {
            return new Employee {
                Id = id,
                Name = name ?? RandomData.GetAlphaString(),
                Age = age ?? RandomData.GetInt(18, 100),
                CompanyName = companyName ?? RandomData.GetAlphaString(),
                CompanyId = companyId ?? ObjectId.GenerateNewId().ToString(),
                CreatedUtc = createdUtc.GetValueOrDefault(),
                UpdatedUtc = updatedUtc.GetValueOrDefault(),
                StartDate = startDate ?? RandomData.GetDateTimeOffset(DateTimeOffset.Now.StartOfMonth(), DateTimeOffset.Now)
            };
        }
    }
}

