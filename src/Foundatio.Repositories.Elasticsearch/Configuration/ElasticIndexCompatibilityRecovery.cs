using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport;
using Elastic.Transport.Products.Elasticsearch;
using Foundatio.Lock;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

internal sealed class ElasticIndexCompatibilityRecovery
{
    private readonly ElasticsearchClient _client;
    private readonly ILockProvider _lockProvider;
    private readonly ILogger _logger;

    public ElasticIndexCompatibilityRecovery(ElasticsearchClient client, ILockProvider lockProvider, ILogger logger)
    {
        _client = client;
        _lockProvider = lockProvider;
        _logger = logger;
    }

    public async Task<IndexCompatibilityUpgradeStatus> InspectAsync(Index index, string sourceIndex, CancellationToken cancellationToken)
    {
        ValidateConcreteSourceName(sourceIndex);

        var infoResponse = await _client.InfoAsync(cancellationToken).AnyContext();
        _logger.LogRequest(infoResponse);
        string? serverVersion = infoResponse.Version?.Number;
        int? serverMajor = Index.ParseCreatedMajor(null, serverVersion);
        if (!infoResponse.IsValidResponse || serverMajor is not int currentMajor)
            throw new RepositoryException(infoResponse.GetErrorMessage("Unable to determine the current Elasticsearch server version while inspecting compatibility recovery."), infoResponse.OriginalException());

        string targetIndex = CompatibilityIndexName.Create(sourceIndex, currentMajor, index.Name);
        ValidateDistinctSourceAndTarget(sourceIndex, targetIndex);
        string names = String.Join(',', sourceIndex, targetIndex);
        var response = await _client.Indices.GetAsync(Indices.Parse(names), d => d
            .Features(Feature.Aliases, Feature.Settings)
            .IncludeDefaults(false)
            .ExpandWildcards(ExpandWildcard.All)
            .IgnoreUnavailable(), cancellationToken).AnyContext();
        _logger.LogRequest(response);
        if (!response.IsValidResponse && response.ElasticsearchServerError?.Status is not 404)
            throw new RepositoryException(response.GetErrorMessage($"Unable to inspect compatibility recovery topology for '{sourceIndex}'."), response.OriginalException());

        IndexState? sourceState = null;
        IndexState? targetState = null;
        bool sourceExists = response.Indices is not null && response.Indices.TryGetValue(sourceIndex, out sourceState);
        bool targetExists = response.Indices is not null && response.Indices.TryGetValue(targetIndex, out targetState);
        bool hasUnexpectedResolvedIndexes = response.Indices?.Keys.Any(name =>
            !String.Equals(name, sourceIndex, StringComparison.Ordinal)
            && !String.Equals(name, targetIndex, StringComparison.Ordinal)) is true;
        string[] sourceAliases = sourceExists ? sourceState?.Aliases?.Keys.Select(k => k.ToString()).Order(StringComparer.Ordinal).ToArray() ?? [] : [];
        string[] targetAliases = targetExists ? targetState?.Aliases?.Keys.Select(k => k.ToString()).Order(StringComparer.Ordinal).ToArray() ?? [] : [];
        string canonicalSourceAlias = CompatibilityIndexName.GetCanonicalName(sourceIndex, index.Name);
        bool targetHasCanonicalSourceAlias = targetAliases.Contains(canonicalSourceAlias, StringComparer.Ordinal);
        bool targetOwnershipConfirmed = targetExists && (targetState?.Aliases).HasExactHiddenAlias(ElasticIndexCompatibilityUpgrader.OwnershipAlias);
        bool sourceWriteBlocked = sourceExists && ReadWriteBlock(sourceState?.Settings);
        bool targetWriteBlocked = targetExists && ReadWriteBlock(targetState?.Settings);

        if (index.IsGeneratedErrorIndex(sourceIndex))
        {
            bool ownershipConfirmed = response.Indices is null
                || response.Indices.Count is 0
                || response.Indices.Values.All(state => state?.Aliases.HasExactHiddenAlias(ElasticReindexer.ErrorIndexOwnershipAlias) is true);
            if (!ownershipConfirmed)
                throw new ArgumentException($"Compatibility recovery source '{sourceIndex}' does not have the Foundatio error-index ownership marker.", nameof(sourceIndex));
        }

        int? activeReindexTasks = await GetActiveReindexTaskCountAsync(sourceIndex, targetIndex, cancellationToken).AnyContext();

        return new IndexCompatibilityUpgradeStatus
        {
            IndexName = index.Name,
            SourceIndex = sourceIndex,
            TargetIndex = targetIndex,
            State = hasUnexpectedResolvedIndexes
                ? IndexCompatibilityUpgradeRecoveryState.Ambiguous
                : Classify(sourceExists, targetExists, sourceWriteBlocked, targetWriteBlocked, targetAliases.Length, targetHasCanonicalSourceAlias, targetOwnershipConfirmed, activeReindexTasks),
            SourceExists = sourceExists,
            TargetExists = targetExists,
            SourceWriteBlocked = sourceWriteBlocked,
            TargetWriteBlocked = targetWriteBlocked,
            SourceAliases = sourceAliases,
            TargetAliases = targetAliases,
            TargetOwnershipConfirmed = targetOwnershipConfirmed,
            ActiveReindexTaskCount = activeReindexTasks
        };
    }

    public async Task<IndexCompatibilityUpgradeStatus> RecoverAsync(
        Index index,
        string sourceIndex,
        bool removeWriteBlock,
        CancellationToken cancellationToken)
    {
        ValidateConcreteSourceName(sourceIndex);

        string lockKey = ElasticReindexer.GetLockName(index.Name);
        await using var reindexLock = await _lockProvider.AcquireAsync(lockKey, TimeSpan.FromMinutes(5), cancellationToken).AnyContext();
        if (reindexLock is null)
            throw new RepositoryException($"Unable to acquire the reindex lock while recovering compatibility upgrade for index '{index.Name}'.");

        var status = await InspectAsync(index, sourceIndex, cancellationToken).AnyContext();
        if (!status.CanRecover)
            throw new RepositoryException($"Compatibility upgrade for '{sourceIndex}' is in state '{status.State}' and cannot be recovered automatically. No cluster state was changed.");

        if (status.State is (IndexCompatibilityUpgradeRecoveryState.SourceWriteBlocked or IndexCompatibilityUpgradeRecoveryState.CompletedWriteBlocked) && !removeWriteBlock)
            throw new RepositoryException("The surviving compatibility index is write blocked. Pass removeWriteBlock: true only after confirming the block was added by the interrupted upgrade.");

        if (status.State is IndexCompatibilityUpgradeRecoveryState.Interrupted)
        {
            var deleteResponse = await _client.Indices.DeleteAsync(status.TargetIndex, d => d.IgnoreUnavailable(), cancellationToken).AnyContext();
            _logger.LogRequest(deleteResponse);
            if (!deleteResponse.IsValidResponse || !deleteResponse.Acknowledged)
                throw new RepositoryException(deleteResponse.GetErrorMessage($"Unable to remove interrupted compatibility destination '{status.TargetIndex}'. Inspect both indexes before retrying."), deleteResponse.OriginalException());

            status = await InspectAsync(index, sourceIndex, cancellationToken).AnyContext();
            if (status.TargetExists || status.ActiveReindexTaskCount is not 0)
                throw new RepositoryException($"Unable to confirm cleanup of interrupted compatibility destination '{status.TargetIndex}'. The source remains write blocked.");
        }

        if (removeWriteBlock)
        {
            string blockedIndex = status.SourceExists ? sourceIndex : status.TargetIndex;
            var unblockResponse = await _client.Indices.PutSettingsAsync(blockedIndex,
                d => d.Settings(s => s.Blocks(b => b.Write(false))), cancellationToken).AnyContext();
            _logger.LogRequest(unblockResponse);
            if (!unblockResponse.IsValidResponse || !unblockResponse.Acknowledged)
                throw new RepositoryException(unblockResponse.GetErrorMessage($"Unable to remove the write block from recovered compatibility index '{blockedIndex}'."), unblockResponse.OriginalException());
        }

        return await InspectAsync(index, sourceIndex, cancellationToken).AnyContext();
    }

    private async Task<int?> GetActiveReindexTaskCountAsync(string sourceIndex, string targetIndex, CancellationToken cancellationToken)
    {
        var path = new EndpointPath(Elastic.Transport.HttpMethod.GET, "/_tasks?actions=*reindex&detailed=true");
        var response = await _client.Transport.RequestAsync<ElasticsearchStringResponse>(path, null, null, null, cancellationToken).AnyContext();
        if (!response.IsValidResponse || String.IsNullOrEmpty(response.Body))
        {
            _logger.LogError("Unable to establish active reindex task count while inspecting compatibility recovery: {DebugInformation}", response.DebugInformation);
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(response.Body);
            return ParseActiveReindexTaskCount(document.RootElement, sourceIndex, targetIndex);
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Unable to parse the active reindex task list while inspecting compatibility recovery");
            return null;
        }
    }

    internal static int? ParseActiveReindexTaskCount(JsonElement response, string sourceIndex, string targetIndex)
    {
        if (ElasticTaskResponseParser.HasPartialTaskListFailures(response)
            || !response.TryGetProperty("nodes", out var nodes)
            || nodes.ValueKind is not JsonValueKind.Object)
            return null;

        int count = 0;
        foreach (var node in nodes.EnumerateObject())
        {
            if (node.Value.ValueKind is not JsonValueKind.Object)
                return null;

            if (!node.Value.TryGetProperty("tasks", out var tasks) || tasks.ValueKind is not JsonValueKind.Object)
                return null;

            foreach (var task in tasks.EnumerateObject())
            {
                if (task.Value.ValueKind is not JsonValueKind.Object)
                    return null;

                if (!task.Value.TryGetProperty("description", out var description) || description.ValueKind is not JsonValueKind.String)
                {
                    count++;
                    continue;
                }

                string? value = description.GetString();
                if (value?.Contains(sourceIndex, StringComparison.Ordinal) is true
                    || value?.Contains(targetIndex, StringComparison.Ordinal) is true)
                    count++;
            }
        }

        return count;
    }

    private static IndexCompatibilityUpgradeRecoveryState Classify(
        bool sourceExists,
        bool targetExists,
        bool sourceWriteBlocked,
        bool targetWriteBlocked,
        int targetAliasCount,
        bool targetHasCanonicalSourceAlias,
        bool targetOwnershipConfirmed,
        int? activeReindexTaskCount)
    {
        if (!sourceExists && !targetExists)
            return IndexCompatibilityUpgradeRecoveryState.Missing;

        if (sourceExists && !targetExists)
            return sourceWriteBlocked ? IndexCompatibilityUpgradeRecoveryState.SourceWriteBlocked : IndexCompatibilityUpgradeRecoveryState.Ready;

        if (!sourceExists && targetExists)
        {
            if (!targetHasCanonicalSourceAlias || targetOwnershipConfirmed)
                return IndexCompatibilityUpgradeRecoveryState.Ambiguous;

            return targetWriteBlocked
                ? IndexCompatibilityUpgradeRecoveryState.CompletedWriteBlocked
                : IndexCompatibilityUpgradeRecoveryState.Completed;
        }

        if (!targetOwnershipConfirmed || targetAliasCount is not 1 || !activeReindexTaskCount.HasValue)
            return IndexCompatibilityUpgradeRecoveryState.Ambiguous;

        return activeReindexTaskCount.Value > 0
            ? IndexCompatibilityUpgradeRecoveryState.InProgress
            : IndexCompatibilityUpgradeRecoveryState.Interrupted;
    }

    private static bool ReadWriteBlock(IndexSettings? settings)
    {
        var indexSettings = settings?.Index ?? settings;
        return indexSettings?.Blocks?.Write is true;
    }

    internal static void ValidateConcreteSourceName(string sourceIndex)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceIndex);
        if (sourceIndex.AsSpan().IndexOfAny('*', '?', ',') >= 0)
            throw new ArgumentException("Compatibility recovery requires one exact concrete source index name.", nameof(sourceIndex));
    }

    internal static void ValidateDistinctSourceAndTarget(string sourceIndex, string targetIndex)
    {
        if (String.Equals(sourceIndex, targetIndex, StringComparison.Ordinal))
            throw new ArgumentException($"Compatibility recovery source '{sourceIndex}' is already the deterministic destination. Supply the original pre-upgrade concrete source name.", nameof(sourceIndex));
    }
}
