using System;
using System.Collections.Generic;
using Exceptionless;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

public class LogEvent : IIdentity, IHaveCreatedDate
{
    public string Id { get; set; } = null!;

    protected bool Equals(LogEvent other)
    {
        return String.Equals(Id, other.Id, StringComparison.InvariantCultureIgnoreCase) &&
            String.Equals(CompanyId, other.CompanyId, StringComparison.InvariantCultureIgnoreCase) &&
            String.Equals(Message, other.Message, StringComparison.InvariantCultureIgnoreCase) &&
            Value == other.Value &&
            Date.Equals(other.Date) &&
            CreatedUtc.Equals(other.CreatedUtc);
    }

    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(null, obj))
            return false;
        if (ReferenceEquals(this, obj))
            return true;
        if (obj.GetType() != GetType())
            return false;
        return Equals((LogEvent)obj);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            int hashCode = (Id != null ? StringComparer.InvariantCultureIgnoreCase.GetHashCode(Id) : 0);
            hashCode = (hashCode * 397) ^ (CompanyId != null ? StringComparer.InvariantCultureIgnoreCase.GetHashCode(CompanyId) : 0);
            hashCode = (hashCode * 397) ^ (Message != null ? StringComparer.InvariantCultureIgnoreCase.GetHashCode(Message) : 0);
            hashCode = (hashCode * 397) ^ Value;
            hashCode = (hashCode * 397) ^ Date.GetHashCode();
            hashCode = (hashCode * 397) ^ CreatedUtc.GetHashCode();
            return hashCode;
        }
    }

    public static bool operator ==(LogEvent left, LogEvent right)
    {
        return Equals(left, right);
    }

    public static bool operator !=(LogEvent left, LogEvent right)
    {
        return !Equals(left, right);
    }

    public string CompanyId { get; set; } = null!;
    public string Message { get; set; } = null!;
    public int Value { get; set; }
    public LogEventMeta Meta { get; set; } = null!;
    public DateTimeOffset Date { get; set; }
    public DateTime CreatedUtc { get; set; }
}

public class LogEventMeta
{
    public string Stuff { get; set; } = null!;
}

public static class LogEventGenerator
{
    public static readonly string DefaultCompanyId = ObjectId.GenerateNewId().ToString();

    public static LogEvent Default => new()
    {
        Message = "Hello world",
        CompanyId = DefaultCompanyId,
        CreatedUtc = DateTime.UtcNow
    };

    public static LogEvent Generate(string? id = null, string? companyId = null, string? message = null, DateTime? createdUtc = null, DateTimeOffset? date = null, string? stuff = null)
    {
        var created = createdUtc ?? RandomData.GetDateTime(DateTime.UtcNow.StartOfMonth(), DateTime.UtcNow);
        return new LogEvent
        {
            Id = id ?? String.Empty,
            Message = message ?? RandomData.GetAlphaString(),
            CompanyId = companyId ?? ObjectId.GenerateNewId().ToString(),
            CreatedUtc = created,
            Meta = new LogEventMeta
            {
                Stuff = stuff ?? String.Empty,
            },
            Date = date ?? created
        };
    }

    public static List<LogEvent> GenerateLogs(int count = 10)
    {
        var results = new List<LogEvent>(count);
        for (int index = 0; index < count; index++)
            results.Add(Generate());

        return results;
    }
}

