using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Foundatio.Serializer;

namespace Foundatio.Repositories.Elasticsearch;

internal static class ElasticReindexTaskResponseReader
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    public static ElasticReindexTaskResponse? ReadCompleted(object? response, ITextSerializer? serializer = null)
    {
        if (response is JsonElement element)
        {
            if (!HasCompletedResponseShape(element))
                return null;

            return serializer is null
                ? element.Deserialize<ElasticReindexTaskResponse>(SerializerOptions)
                : serializer.Deserialize<ElasticReindexTaskResponse>(element.GetRawText());
        }

        if (response is not IDictionary<string, object> values)
            return null;

        if (!TryReadInt64(values, "total", out long total)
            || !TryReadInt64(values, "created", out long created)
            || !TryReadInt64(values, "updated", out long updated)
            || !TryReadInt64(values, "deleted", out long deleted)
            || !TryReadInt64(values, "noops", out long noops)
            || !TryReadInt64(values, "version_conflicts", out long versionConflicts)
            || !TryReadFailures(values, serializer, out var failures))
        {
            return null;
        }

        return new ElasticReindexTaskResponse
        {
            Total = total,
            Created = created,
            Updated = updated,
            Deleted = deleted,
            Noops = noops,
            VersionConflicts = versionConflicts,
            Failures = failures
        };
    }

    public static ElasticReindexTaskStatus? ReadStatus(object? status)
    {
        if (status is JsonElement element
            && TryReadStatus(element, out var elementStatus))
        {
            return elementStatus;
        }

        if (status is not IDictionary<string, object> values)
            return null;

        if (!TryReadInt64(values, "total", out long total)
            || !TryReadInt64(values, "created", out long created)
            || !TryReadInt64(values, "updated", out long updated)
            || !TryReadInt64(values, "deleted", out long deleted)
            || !TryReadInt64(values, "noops", out long noops)
            || !TryReadInt64(values, "version_conflicts", out long versionConflicts))
        {
            return null;
        }

        return new ElasticReindexTaskStatus(total, created, updated, deleted, noops, versionConflicts);
    }

    private static bool HasCompletedResponseShape(JsonElement element)
    {
        return element.ValueKind is JsonValueKind.Object
            && TryReadStatus(element, out _)
            && element.TryGetProperty("failures", out var failures)
            && failures.ValueKind is JsonValueKind.Array;
    }

    private static bool TryReadStatus(JsonElement element, out ElasticReindexTaskStatus status)
    {
        status = default;
        if (element.ValueKind is not JsonValueKind.Object
            || !TryReadInt64(element, "total", out long total)
            || !TryReadInt64(element, "created", out long created)
            || !TryReadInt64(element, "updated", out long updated)
            || !TryReadInt64(element, "deleted", out long deleted)
            || !TryReadInt64(element, "noops", out long noops)
            || !TryReadInt64(element, "version_conflicts", out long versionConflicts))
        {
            return false;
        }

        status = new ElasticReindexTaskStatus(total, created, updated, deleted, noops, versionConflicts);
        return true;
    }

    private static bool TryReadFailures(
        IDictionary<string, object> values,
        ITextSerializer? serializer,
        out IReadOnlyCollection<ElasticReindexTaskFailure>? parsedFailures)
    {
        parsedFailures = null;
        if (!values.TryGetValue("failures", out object? failures) || failures is null)
            return false;

        if (failures is JsonElement element)
        {
            if (element.ValueKind is not JsonValueKind.Array)
                return false;

            parsedFailures = serializer is null
                ? element.Deserialize<IReadOnlyCollection<ElasticReindexTaskFailure>>(SerializerOptions)
                : serializer.Deserialize<IReadOnlyCollection<ElasticReindexTaskFailure>>(element.GetRawText());
            return parsedFailures is not null;
        }

        if (failures is not IEnumerable<object> items)
            return false;

        var parsed = new List<ElasticReindexTaskFailure>();
        foreach (object item in items)
        {
            if (item is JsonElement failureElement && failureElement.ValueKind is JsonValueKind.Object)
            {
                var failure = serializer is null
                    ? failureElement.Deserialize<ElasticReindexTaskFailure>(SerializerOptions)
                    : serializer.Deserialize<ElasticReindexTaskFailure>(failureElement.GetRawText());
                if (failure is null)
                    return false;
                parsed.Add(failure);
            }
            else if (item is IDictionary<string, object> failureValues)
            {
                if (!TryReadInt64(failureValues, "status", out long status)
                    || status is < Int32.MinValue or > Int32.MaxValue)
                {
                    return false;
                }

                parsed.Add(new ElasticReindexTaskFailure
                {
                    Cause = ReadFailureCause(failureValues, serializer),
                    Id = ReadString(failureValues, "id"),
                    Index = ReadString(failureValues, "index"),
                    Status = (int)status,
                    Type = ReadString(failureValues, "type")
                });
            }
            else
            {
                return false;
            }
        }

        parsedFailures = parsed;
        return true;
    }

    private static ElasticReindexTaskFailureCause? ReadFailureCause(
        IDictionary<string, object> failureValues,
        ITextSerializer? serializer)
    {
        if (!failureValues.TryGetValue("cause", out object? cause) || cause is null)
            return null;

        if (cause is JsonElement element && element.ValueKind is JsonValueKind.Object)
        {
            return serializer is null
                ? element.Deserialize<ElasticReindexTaskFailureCause>(SerializerOptions)
                : serializer.Deserialize<ElasticReindexTaskFailureCause>(element.GetRawText());
        }

        if (cause is not IDictionary<string, object> values)
            return null;

        return new ElasticReindexTaskFailureCause
        {
            Type = ReadString(values, "type"),
            Reason = ReadString(values, "reason"),
            StackTrace = ReadString(values, "stack_trace")
        };
    }

    private static bool TryReadInt64(JsonElement element, string propertyName, out long number)
    {
        number = 0;
        return element.TryGetProperty(propertyName, out var value) && value.TryGetInt64(out number);
    }

    private static bool TryReadInt64(IDictionary<string, object> values, string key, out long number)
    {
        number = 0;
        if (!values.TryGetValue(key, out object? value) || value is null)
            return false;

        if (value is JsonElement element)
            return element.TryGetInt64(out number);

        try
        {
            number = Convert.ToInt64(value);
            return true;
        }
        catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
        {
            return false;
        }
    }

    private static string? ReadString(IDictionary<string, object> values, string key)
    {
        if (!values.TryGetValue(key, out object? value) || value is null)
            return null;

        return value is JsonElement element
            ? element.ValueKind is JsonValueKind.String ? element.GetString() : null
            : Convert.ToString(value);
    }
}

internal sealed record ElasticReindexTaskResponse
{
    public long Total { get; init; }
    public long Created { get; init; }
    public long Updated { get; init; }
    public long Deleted { get; init; }
    public long Noops { get; init; }

    [JsonPropertyName("version_conflicts")]
    public long VersionConflicts { get; init; }

    public IReadOnlyCollection<ElasticReindexTaskFailure>? Failures { get; init; }
}

internal readonly record struct ElasticReindexTaskStatus(
    long Total,
    long Created,
    long Updated,
    long Deleted,
    long Noops,
    long VersionConflicts);

internal sealed record ElasticReindexTaskFailure
{
    public ElasticReindexTaskFailureCause? Cause { get; init; }
    public string? Id { get; init; }
    public string? Index { get; init; }
    public int Status { get; init; }
    public string? Type { get; init; }
}

internal sealed record ElasticReindexTaskFailureCause
{
    public string? Type { get; init; }
    public string? Reason { get; init; }

    [JsonPropertyName("stack_trace")]
    public string? StackTrace { get; init; }
}
