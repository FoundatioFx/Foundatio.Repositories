using System;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

internal static class TimeSpanExtensions
{
    /// <summary>
    /// Converts a <see cref="TimeSpan"/> to an Elasticsearch duration string (e.g., "500ms", "30s", "5m", "2h", "1d").
    /// Uses the largest whole unit that fits: days, hours, minutes, seconds, then milliseconds.
    /// </summary>
    public static string ToElasticDuration(this TimeSpan timeSpan)
    {
        if (timeSpan.TotalDays >= 1 && timeSpan.TotalDays == Math.Truncate(timeSpan.TotalDays))
            return $"{(int)timeSpan.TotalDays}d";

        if (timeSpan.TotalHours >= 1 && timeSpan.TotalHours == Math.Truncate(timeSpan.TotalHours))
            return $"{(int)timeSpan.TotalHours}h";

        if (timeSpan.TotalMinutes >= 1 && timeSpan.TotalMinutes == Math.Truncate(timeSpan.TotalMinutes))
            return $"{(int)timeSpan.TotalMinutes}m";

        if (timeSpan.TotalSeconds >= 1 && timeSpan.TotalSeconds == Math.Truncate(timeSpan.TotalSeconds))
            return $"{(int)timeSpan.TotalSeconds}s";

        return $"{(int)timeSpan.TotalMilliseconds}ms";
    }
}
