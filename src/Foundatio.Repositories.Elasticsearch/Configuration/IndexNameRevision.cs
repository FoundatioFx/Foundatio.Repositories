using System;
using System.Globalization;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

internal readonly record struct IndexNameRevision(string BaseName, int Revision, bool HasRevision)
{
    public static IndexNameRevision Parse(string name)
    {
        int index = name.LastIndexOf("-r", StringComparison.Ordinal);
        if (index <= 0)
            return new(name, 0, false);

        var digits = name.AsSpan(index + 2);
        return !digits.IsEmpty && Int32.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out int revision)
            ? new(name[..index], revision, true)
            : new(name, 0, false);
    }
}
