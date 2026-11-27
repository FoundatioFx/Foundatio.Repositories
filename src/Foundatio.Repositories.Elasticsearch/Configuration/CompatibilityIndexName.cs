using System;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

internal static class CompatibilityIndexName
{
    private const string Prefix = "reindexed-v";

    internal static bool IsVersionedName(ReadOnlySpan<char> sourceIndex, ReadOnlySpan<char> name, bool allowSuffix)
    {
        if (sourceIndex.Length <= name.Length + 2
            || !sourceIndex.StartsWith(name, StringComparison.Ordinal)
            || sourceIndex[name.Length] is not '-'
            || sourceIndex[name.Length + 1] is not 'v')
        {
            return false;
        }

        int versionStart = name.Length + 2;
        int offset = versionStart;
        while (offset < sourceIndex.Length && sourceIndex[offset] is >= '0' and <= '9')
            offset++;

        return offset > versionStart && (allowSuffix || offset == sourceIndex.Length);
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
