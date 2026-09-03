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
        ReadOnlySpan<char> canonicalName = GetCanonicalNameSpan(index, configuredIndexName);
        return canonicalName.Length == index.Length ? index : canonicalName.ToString();
    }

    public static string GetCanonicalName(string index)
    {
        ArgumentException.ThrowIfNullOrEmpty(index);

        if (!TryRemovePrefix(index, out ReadOnlySpan<char> canonicalName))
            return index;

        return canonicalName.ToString();
    }

    internal static ReadOnlySpan<char> GetCanonicalNameSpan(string index, string configuredIndexName)
    {
        ArgumentException.ThrowIfNullOrEmpty(index);
        ArgumentException.ThrowIfNullOrEmpty(configuredIndexName);

        ReadOnlySpan<char> indexSpan = index;
        ReadOnlySpan<char> configuredSpan = configuredIndexName;
        bool hasCompatibilityPrefix = TryRemovePrefix(index, out ReadOnlySpan<char> canonicalName);
        if (hasCompatibilityPrefix
            && (canonicalName.Equals(configuredSpan, StringComparison.Ordinal)
                || (canonicalName.Length > configuredSpan.Length
                && canonicalName.StartsWith(configuredSpan, StringComparison.Ordinal)
                && canonicalName[configuredSpan.Length] is '-')))
        {
            return canonicalName;
        }

        if (indexSpan.Equals(configuredSpan, StringComparison.Ordinal)
            || (indexSpan.Length > configuredSpan.Length
                && indexSpan.StartsWith(configuredSpan, StringComparison.Ordinal)
                && indexSpan[configuredSpan.Length] is '-'))
        {
            return indexSpan;
        }

        return hasCompatibilityPrefix ? canonicalName : indexSpan;
    }

    internal static bool TryRemovePrefix(string index, out ReadOnlySpan<char> canonicalName)
    {
        ArgumentException.ThrowIfNullOrEmpty(index);
        ReadOnlySpan<char> value = index;
        ReadOnlySpan<char> prefix = Prefix;
        canonicalName = value;
        if (!value.StartsWith(prefix, StringComparison.Ordinal))
            return false;

        int separatorOffset = value[prefix.Length..].IndexOf('-');
        if (separatorOffset <= 0)
            return false;

        ReadOnlySpan<char> major = value.Slice(prefix.Length, separatorOffset);
        if (major[0] is < '1' or > '9')
            return false;

        foreach (char character in major[1..])
        {
            if (character is < '0' or > '9')
                return false;
        }

        int canonicalOffset = prefix.Length + separatorOffset + 1;
        if (canonicalOffset >= value.Length)
            return false;

        canonicalName = value[canonicalOffset..];
        return true;
    }
}
