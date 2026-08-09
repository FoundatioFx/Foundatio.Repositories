using System;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

internal static class CompatibilityIndexName
{
    private const string Prefix = "reindexed-v";

    public static string Create(string sourceIndex, int serverMajor)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceIndex);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(serverMajor);

        return $"{Prefix}{serverMajor}-{GetCanonicalName(sourceIndex)}";
    }

    public static string Create(string sourceIndex, int serverMajor, string configuredIndexName)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceIndex);
        ArgumentException.ThrowIfNullOrEmpty(configuredIndexName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(serverMajor);

        return $"{Prefix}{serverMajor}-{GetCanonicalName(sourceIndex, configuredIndexName)}";
    }

    public static string GetCanonicalName(string index, string configuredIndexName)
    {
        ArgumentException.ThrowIfNullOrEmpty(index);
        ArgumentException.ThrowIfNullOrEmpty(configuredIndexName);

        // A user may legitimately configure a name beginning with "reindexed-vN-". Treat its own
        // physical names as canonical; only strip a compatibility prefix wrapped around that name.
        if (index.StartsWith($"{configuredIndexName}-v", StringComparison.Ordinal))
            return index;

        return GetCanonicalName(index);
    }

    public static string GetCanonicalName(string index)
    {
        ArgumentException.ThrowIfNullOrEmpty(index);

        if (!index.StartsWith(Prefix, StringComparison.Ordinal))
            return index;

        int separatorIndex = index.IndexOf('-', Prefix.Length);
        if (separatorIndex < 0 || !Int32.TryParse(index.AsSpan(Prefix.Length, separatorIndex - Prefix.Length), out int major) || major <= 0)
            return index;

        return index[(separatorIndex + 1)..];
    }

    public static string CreatePattern(string canonicalPattern)
    {
        ArgumentException.ThrowIfNullOrEmpty(canonicalPattern);
        return $"{Prefix}*-{canonicalPattern}";
    }
}
