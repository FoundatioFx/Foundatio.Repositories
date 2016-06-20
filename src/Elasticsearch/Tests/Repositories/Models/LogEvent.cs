using System;
using Exceptionless;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Elasticsearch.Tests.Models {
    public class LogEvent : IIdentity, IHaveCreatedDate {
        public string Id { get; set; }
        public string CompanyId { get; set; }
        public string Message { get; set; }
        public DateTime CreatedUtc { get; set; }
    }

    public static class LogEventGenerator {
        public static readonly string DefaultCompanyId = ObjectId.GenerateNewId().ToString();

        public static LogEvent Default => new LogEvent {
            Message = "Hello world",
            CompanyId = DefaultCompanyId,
            CreatedUtc = DateTime.Now
        };

        public static LogEvent Generate(string id = null, string companyId = null, string message = null, DateTime? createdUtc = null) {
            return new LogEvent {
                Id = id,
                Message = message ?? RandomData.GetAlphaString(),
                CompanyId = companyId ?? ObjectId.GenerateNewId().ToString(),
                CreatedUtc = createdUtc ?? RandomData.GetDateTime(DateTime.Now.StartOfMonth(), DateTime.Now)
            };
        }
    }
}

