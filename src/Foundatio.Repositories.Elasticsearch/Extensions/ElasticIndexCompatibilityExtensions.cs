using System.Collections.Generic;
using Elastic.Clients.Elasticsearch.IndexManagement;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

internal static class ElasticIndexCompatibilityExtensions
{
    internal static GetIndexRequestDescriptor LimitToIndexCompatibility(this GetIndexRequestDescriptor descriptor)
    {
        return descriptor.Features(Feature.Aliases, Feature.Settings).IncludeDefaults(false);
    }

    internal static bool HasExactHiddenAlias(this IReadOnlyDictionary<string, Alias>? aliases, string aliasName)
    {
        return aliases is not null
            && aliases.TryGetValue(aliasName, out var alias)
            && alias.IsHidden is true
            && alias.IsWriteIndex is null
            && alias.Filter is null
            && alias.IndexRouting is null
            && alias.Routing is null
            && alias.SearchRouting is null;
    }

    internal static bool HasCanonicalCompatibilityAlias(this IReadOnlyDictionary<string, Alias>? aliases, string aliasName)
    {
        return aliases is not null
            && aliases.TryGetValue(aliasName, out var alias)
            && alias.IsWriteIndex is null
            && alias.Filter is null
            && alias.IndexRouting is null
            && alias.Routing is null
            && alias.SearchRouting is null;
    }
}
