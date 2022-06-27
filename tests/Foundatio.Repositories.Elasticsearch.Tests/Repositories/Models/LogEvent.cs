using System;
using System.Collections.Generic;
using Exceptionless;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

public record LogEvent(
    string Id,
    string CompanyId,
    string Message,
    int Value,
    LogEventMeta Meta,
    DateTimeOffset Date,
    DateTime CreatedUtc
) : IIdentity, IHaveCreatedDate {
    public string Id { get; set; } = Id;
    public DateTime CreatedUtc { get; set; } = CreatedUtc;
}

public record LogEventMeta {
    public string Stuff { get; set; }
}

public static class LogEventGenerator {
    public static readonly string DefaultCompanyId = ObjectId.GenerateNewId().ToString();

    public static LogEvent Default {
        get {
            return new LogEvent(null, DefaultCompanyId, "Hello world", 0, new LogEventMeta(), SystemClock.OffsetNow, SystemClock.UtcNow);
        }
    }

    public static LogEvent Generate(string id = null, string companyId = null, string message = null, DateTime? createdUtc = null, DateTimeOffset? date = null, string stuff = null) {
        var created = createdUtc ?? RandomData.GetDateTime(SystemClock.UtcNow.StartOfMonth(), SystemClock.UtcNow);
        return new LogEvent(
            id,
            companyId ?? ObjectId.GenerateNewId().ToString(),
            message ?? RandomData.GetAlphaString(),
            0,
            new LogEventMeta { Stuff = stuff, },
            date ?? created,
            created
        );
    }
    
    public static List<LogEvent> GenerateLogs(int count = 10) {
        var results = new List<LogEvent>(count);
        for (int index = 0; index < count; index++)
            results.Add(Generate());

        return results;
    }
}

