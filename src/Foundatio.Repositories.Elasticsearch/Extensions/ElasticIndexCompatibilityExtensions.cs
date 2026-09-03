using System;
using System.Collections.Generic;
using System.Linq;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Foundatio.Repositories.Exceptions;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

internal static class ElasticIndexCompatibilityExtensions
{
    internal static GetIndexRequestDescriptor LimitToIndexSettings(this GetIndexRequestDescriptor descriptor)
    {
        return descriptor.Features(Feature.Settings).IncludeDefaults(false);
    }

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

    internal static IndexState RequireSingleResolvedIndexState(this GetIndexResponse response, string requestedName)
    {
        ArgumentNullException.ThrowIfNull(response);
        ArgumentException.ThrowIfNullOrEmpty(requestedName);

        if (response.Indices is not null && response.Indices.TryGetValue(requestedName, out var exactState) && exactState is not null)
            return exactState;

        if (response.Indices is { Count: 1 })
            return response.Indices.Values.Single();

        int resolvedCount = response.Indices?.Count ?? 0;
        throw new RepositoryException($"Index expression '{requestedName}' must resolve to exactly one concrete index; found {resolvedCount}.");
    }

    internal static IndexState RequireSingleResolvedIndexState(this GetIndicesSettingsResponse response, string requestedName)
    {
        ArgumentNullException.ThrowIfNull(response);
        ArgumentException.ThrowIfNullOrEmpty(requestedName);

        if (response.Settings.TryGetValue(requestedName, out var exactState) && exactState is not null)
            return exactState;

        if (response.Settings.Count is 1)
            return response.Settings.Values.Single();

        throw new RepositoryException($"Index expression '{requestedName}' must resolve to exactly one concrete index; found {response.Settings.Count}.");
    }
}
