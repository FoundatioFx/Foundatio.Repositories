using System;
using System.Collections.Generic;
using System.Linq;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport.Products.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Exceptions;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

internal static class ElasticIndexCompatibilityExtensions
{
    /// <summary>
    /// Logs and returns <paramref name="response"/> when <see cref="ElasticsearchResponse.IsValidResponse"/> is
    /// true; otherwise logs the failure and throws a <see cref="RepositoryException"/> carrying
    /// <paramref name="errorMessage"/> and the response's original exception. Use the
    /// <c>Func&lt;TResponse, bool&gt;</c> overload when success requires more than a valid response (e.g. also
    /// <c>Acknowledged</c>).
    /// </summary>
    internal static TResponse EnsureValid<TResponse>(
        this TResponse response,
        ILogger logger,
        string logMessageTemplate,
        string errorMessage,
        params object?[] logArgs)
        where TResponse : ElasticsearchResponse
    {
        return response.EnsureValid(r => r.IsValidResponse, logger, logMessageTemplate, errorMessage, logArgs);
    }

    /// <summary>
    /// Logs and returns <paramref name="response"/> when <paramref name="isValid"/> returns true; otherwise logs
    /// the failure and throws a <see cref="RepositoryException"/> carrying <paramref name="errorMessage"/> and the
    /// response's original exception.
    /// </summary>
    internal static TResponse EnsureValid<TResponse>(
        this TResponse response,
        Func<TResponse, bool> isValid,
        ILogger logger,
        string logMessageTemplate,
        string errorMessage,
        params object?[] logArgs)
        where TResponse : ElasticsearchResponse
    {
        if (isValid(response))
        {
            logger.LogRequest(response);
            return response;
        }

        logger.LogErrorRequest(response, logMessageTemplate, logArgs);
        throw new RepositoryException(response.GetErrorMessage(errorMessage), response.OriginalException());
    }

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
