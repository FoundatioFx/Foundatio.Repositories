using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Tasks;
using Foundatio.Lock;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

internal sealed class ElasticIndexCompatibilityRecovery
{
    private static readonly TimeSpan RecoveryLockDuration = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan RecoveryLockTimeout = TimeSpan.FromMinutes(1);
    private readonly ElasticsearchClient _client;
    private readonly ILockProvider? _lockProvider;
    private readonly ILogger _logger;

    public ElasticIndexCompatibilityRecovery(ElasticsearchClient client, ILockProvider? lockProvider, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(logger);

        _client = client;
        _lockProvider = lockProvider;
        _logger = logger;
    }

    public async Task<IndexCompatibilityUpgradeStatus> InspectAsync(Index index, string sourceIndex, CancellationToken cancellationToken)
    {
        ValidateExactIndexName(sourceIndex);

        var infoResponse = await _client.InfoAsync(cancellationToken).AnyContext();
        infoResponse.EnsureValid(
            _logger,
            "Unable to determine the Elasticsearch version while inspecting compatibility recovery for {SourceIndex}",
            "Unable to determine the current Elasticsearch server version while inspecting compatibility recovery.",
            sourceIndex);

        string? serverVersion = infoResponse.Version?.Number;
        int? serverMajor = Index.ParseCreatedMajor(null, serverVersion);
        if (serverMajor is not int currentMajor)
            throw new RepositoryException("Unable to determine the current Elasticsearch server version while inspecting compatibility recovery.");

        string targetIndex = CompatibilityIndexName.Create(sourceIndex, currentMajor, index.Name);
        ValidateDistinctSourceAndTarget(sourceIndex, targetIndex);
        return await InspectTopologyAsync(index, sourceIndex, targetIndex, cancellationToken).AnyContext();
    }

    public async Task<IndexCompatibilityUpgradeStatus> RecoverAsync(Index index, string sourceIndex, CancellationToken cancellationToken)
    {
        ValidateExactIndexName(sourceIndex);
        if (_lockProvider is null)
            throw new InvalidOperationException("A lock provider is required for public compatibility recovery.");

        string lockKey = ElasticReindexer.GetLockName(index.Name);
        using var acquisitionCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        acquisitionCancellation.CancelAfter(RecoveryLockTimeout);
        await using var reindexLock = await _lockProvider.AcquireAsync(
            lockKey,
            RecoveryLockDuration,
            acquisitionCancellation.Token).AnyContext();

        await using var lease = new ReindexLeaseGuard(reindexLock, index.Configuration.TimeProvider);
        using var guarded = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, lease.LostToken);
        return await RecoverUnderLockAsync(index, sourceIndex, () => lease.RenewAsync(), guarded.Token).AnyContext();
    }

    internal async Task<IndexCompatibilityUpgradeStatus> RecoverUnderLockAsync(
        Index index,
        string sourceIndex,
        Func<Task> renewLeaseAsync,
        CancellationToken cancellationToken,
        string? expectedReceipt = null)
    {
        var status = await InspectAsync(index, sourceIndex, cancellationToken).AnyContext();
        if (status.Action is not IndexCompatibilityRecoveryAction.Finish || status.CutoverReceipt is null
            || (expectedReceipt is not null && !String.Equals(expectedReceipt, status.CutoverReceipt, StringComparison.Ordinal)))
            throw new RepositoryException($"Compatibility upgrade for '{sourceIndex}' requires action '{status.Action}' and cannot be recovered automatically. No cluster state was changed.");

        await FinishAsync(status, renewLeaseAsync, cancellationToken).AnyContext();
        return await InspectAsync(index, sourceIndex, cancellationToken).AnyContext();
    }

    private async Task<IndexCompatibilityUpgradeStatus> InspectTopologyAsync(
        Index index,
        string sourceIndex,
        string targetIndex,
        CancellationToken cancellationToken)
    {
        string names = String.Join(',', sourceIndex, targetIndex, ElasticIndexCompatibilityUpgrader.OwnershipAlias);
        var response = await _client.Indices.GetAsync(Indices.Parse(names), d => d
            .Features(Feature.Aliases, Feature.Settings)
            .IncludeDefaults(false)
            .ExpandWildcards(ExpandWildcard.All)
            .IgnoreUnavailable(), cancellationToken).AnyContext();
        response.EnsureValid(
            r => r.IsValidResponse || r.ElasticsearchServerError?.Status is 404,
            _logger,
            "Unable to inspect compatibility recovery topology for {SourceIndex}",
            $"Unable to inspect compatibility recovery topology for '{sourceIndex}'.",
            sourceIndex);

        string canonicalSource = CompatibilityIndexName.GetCanonicalName(sourceIndex, index.Name);
        var unexpectedIndexes = new List<string>();
        if (response.Indices is not null)
        {
            foreach (var candidate in response.Indices)
            {
                if (String.Equals(candidate.Key, sourceIndex, StringComparison.Ordinal)
                    || String.Equals(candidate.Key, targetIndex, StringComparison.Ordinal))
                {
                    continue;
                }

                bool isMarked = candidate.Value?.Aliases.HasExactHiddenAlias(ElasticIndexCompatibilityUpgrader.OwnershipAlias) is true;
                bool isSameLineage = CompatibilityIndexName.TryRemovePrefix(candidate.Key, out ReadOnlySpan<char> candidateCanonical)
                    && candidateCanonical.Equals(canonicalSource.AsSpan(), StringComparison.Ordinal);
                if (!isMarked || isSameLineage)
                    unexpectedIndexes.Add(candidate.Key);
            }
        }
        IndexState? sourceState = null;
        IndexState? targetState = null;
        bool sourceExists = response.Indices?.TryGetValue(sourceIndex, out sourceState) is true;
        bool targetExists = response.Indices?.TryGetValue(targetIndex, out targetState) is true;
        IReadOnlyDictionary<string, Alias> sourceAliases = sourceState?.Aliases ?? new Dictionary<string, Alias>();
        IReadOnlyDictionary<string, Alias> targetAliases = targetState?.Aliases ?? new Dictionary<string, Alias>();
        bool sourceMarker = sourceAliases.HasExactHiddenAlias(ElasticIndexCompatibilityUpgrader.OwnershipAlias);
        bool targetMarker = targetAliases.HasExactHiddenAlias(ElasticIndexCompatibilityUpgrader.OwnershipAlias);
        bool sourceBlocked = ReadWriteBlock(sourceState?.Settings);
        bool targetBlocked = ReadWriteBlock(targetState?.Settings);
        bool targetHasCanonicalSourceAlias = targetAliases.HasCanonicalCompatibilityAlias(canonicalSource);
        int? activeTasks = await GetActiveReindexTaskCountAsync(sourceIndex, targetIndex, cancellationToken).AnyContext();

        bool errorLineageAuthenticated = !index.IsPotentialCompatibilityErrorName(sourceIndex)
            || ((!sourceExists || sourceAliases.HasExactHiddenAlias(ElasticReindexer.ErrorIndexOwnershipAlias))
                && (!targetExists || targetAliases.HasExactHiddenAlias(ElasticReindexer.ErrorIndexOwnershipAlias)));

        string? targetUuid = (targetState?.Settings?.Index ?? targetState?.Settings)?.Uuid;
        string? receipt = CompatibilityCutoverReceipt.Find(_client, index.Name, sourceIndex, targetIndex, targetUuid, targetAliases);
        var observed = new ObservedTopology(
            sourceExists,
            targetExists,
            sourceBlocked,
            targetBlocked,
            sourceMarker,
            targetMarker,
            targetHasCanonicalSourceAlias,
            receipt is not null,
            targetAliases.Keys.Any(CompatibilityCutoverReceipt.IsReserved),
            unexpectedIndexes.Count > 0,
            errorLineageAuthenticated,
            activeTasks);

        return new IndexCompatibilityUpgradeStatus
        {
            IndexName = index.Name,
            ErrorLineageAuthenticated = errorLineageAuthenticated,
            SourceUuid = (sourceState?.Settings?.Index ?? sourceState?.Settings)?.Uuid,
            TargetUuid = targetUuid,
            CutoverReceipt = receipt,
            TargetAliasDefinitions = targetAliases,
            SourceIndex = sourceIndex,
            TargetIndex = targetIndex,
            Action = GetRecoveryAction(observed),
            SourceExists = sourceExists,
            TargetExists = targetExists,
            SourceWriteBlocked = sourceBlocked,
            TargetWriteBlocked = targetBlocked,
            SourceWorkflowMarkerPresent = sourceMarker,
            TargetWorkflowMarkerPresent = targetMarker,
            TargetHasCanonicalSourceAlias = targetHasCanonicalSourceAlias,
            SourceAliases = sourceAliases.Keys.Order(StringComparer.Ordinal).ToArray(),
            TargetAliases = targetAliases.Keys.Order(StringComparer.Ordinal).ToArray(),
            UnexpectedResolvedIndexes = unexpectedIndexes.Order(StringComparer.Ordinal).ToArray(),
            ActiveReindexTaskCount = activeTasks
        };
    }

    private async Task<int?> GetActiveReindexTaskCountAsync(string sourceIndex, string targetIndex, CancellationToken cancellationToken)
    {
        var response = await _client.Tasks.ListAsync(d => d
            .Actions("*reindex")
            .Detailed()
            .GroupBy(GroupBy.Nodes), cancellationToken).AnyContext();
        if (!response.IsValidResponse || response.NodeFailures is { Count: > 0 } || response.TaskFailures is { Count: > 0 } || response.Nodes is null)
        {
            _logger.LogErrorRequest(response, "Unable to establish active reindex tasks for {SourceIndex} -> {TargetIndex}", sourceIndex, targetIndex);
            return null;
        }

        _logger.LogRequest(response);
        string opaqueId = ElasticReindexTaskRunner.GetOpaqueId(sourceIndex, targetIndex);
        int count = 0;
        foreach (var node in response.Nodes.Values)
        {
            if (node.Tasks is null)
            {
                _logger.LogErrorRequest(response, "Elasticsearch omitted task details while inspecting compatibility recovery for {SourceIndex} -> {TargetIndex}", sourceIndex, targetIndex);
                return null;
            }

            foreach (var task in node.Tasks.Values)
            {
                if (task.Headers?.TryGetValue(ElasticReindexTaskRunner.OpaqueIdHeader, out string? taskOpaqueId) is not true
                    || !String.Equals(taskOpaqueId, opaqueId, StringComparison.Ordinal))
                {
                    // Task descriptions cannot prove which indexes an unidentified writer may mutate.
                    _logger.LogErrorRequest(response, "An unidentified reindex task prevents safe compatibility recovery for {SourceIndex} -> {TargetIndex}", sourceIndex, targetIndex);
                    return null;
                }

                count++;
            }
        }

        return count;
    }

    internal static bool CanResetSourceOnlyAttempt(IndexCompatibilityUpgradeStatus status, string? sourceUuid)
    {
        return status.Action is IndexCompatibilityRecoveryAction.ManualIntervention
            && status.ErrorLineageAuthenticated
            && sourceUuid is { Length: > 0 } && String.Equals(sourceUuid, status.SourceUuid, StringComparison.Ordinal)
            && status.SourceExists && !status.TargetExists
            && status.SourceWriteBlocked && status.SourceWorkflowMarkerPresent
            && status.ActiveReindexTaskCount is 0
            && status.UnexpectedResolvedIndexes.Count is 0;
    }

    // Only the running attempt can prove that target creation has not been dispatched.
    internal async Task ResetSourceOnlyAttemptAsync(Index index, string sourceIndex, string? sourceUuid, Func<Task> renewLeaseAsync, CancellationToken cancellationToken)
    {
        var status = await InspectAsync(index, sourceIndex, cancellationToken).AnyContext();
        if (!CanResetSourceOnlyAttempt(status, sourceUuid))
            throw new RepositoryException($"Compatibility source '{sourceIndex}' no longer has the source-only state required for cleanup. Keep writes stopped and inspect both indexes.");

        await renewLeaseAsync().AnyContext();
        await SetWriteBlockAsync(status.SourceIndex, false, cancellationToken).AnyContext();
        await renewLeaseAsync().AnyContext();
        await RemoveWorkflowMarkerAsync(status.SourceIndex, null, cancellationToken).AnyContext();
    }

    internal static bool CanResetCurrentAttempt(IndexCompatibilityUpgradeStatus status, string? sourceUuid, string? targetUuid)
    {
        return status.Action is IndexCompatibilityRecoveryAction.ManualIntervention
            && status.ErrorLineageAuthenticated
            && sourceUuid is { Length: > 0 } && String.Equals(sourceUuid, status.SourceUuid, StringComparison.Ordinal)
            && targetUuid is { Length: > 0 } && String.Equals(targetUuid, status.TargetUuid, StringComparison.Ordinal)
            && status.SourceExists && status.TargetExists
            && status.SourceWriteBlocked && status.SourceWorkflowMarkerPresent && status.TargetWorkflowMarkerPresent
            && !status.TargetHasCanonicalSourceAlias
            && status.ActiveReindexTaskCount is 0
            && status.UnexpectedResolvedIndexes.Count is 0
            && status.TargetAliases.All(alias => alias is ElasticIndexCompatibilityUpgrader.OwnershipAlias or ElasticReindexer.ErrorIndexOwnershipAlias);
    }

    // The caller must also hold positive evidence that its own requests cannot still mutate either index.
    internal async Task ResetCurrentAttemptAsync(Index index, string sourceIndex, string? sourceUuid, string? targetUuid, Func<Task> renewLeaseAsync, CancellationToken cancellationToken)
    {
        var status = await InspectAsync(index, sourceIndex, cancellationToken).AnyContext();
        if (!CanResetCurrentAttempt(status, sourceUuid, targetUuid))
            throw new RepositoryException($"Compatibility source '{sourceIndex}' no longer has the marked state required for cleanup. Keep writes stopped and inspect both indexes.");

        await renewLeaseAsync().AnyContext();
        var deleteResponse = await _client.Indices.DeleteAsync(status.TargetIndex, d => d.RequestConfiguration(r => SingleAttemptRequest.Configure(_client, r)), cancellationToken).AnyContext();
        deleteResponse.EnsureValid(
            r => r.IsValidResponse && r.Acknowledged,
            _logger,
            "Unable to remove interrupted compatibility destination {TargetIndex}",
            $"Unable to remove interrupted compatibility destination '{status.TargetIndex}'. The source remains write blocked.",
            status.TargetIndex);

        // Inspect both names again after deletion. A source recreated during the delete must stay fenced.
        await ResetSourceOnlyAttemptAsync(index, sourceIndex, sourceUuid, renewLeaseAsync, cancellationToken).AnyContext();
    }

    private async Task FinishAsync(IndexCompatibilityUpgradeStatus status, Func<Task> renewLeaseAsync, CancellationToken cancellationToken)
    {
        if (status.SourceExists || !status.TargetExists || !status.TargetWorkflowMarkerPresent || !status.TargetHasCanonicalSourceAlias)
            throw new RepositoryException($"Compatibility destination '{status.TargetIndex}' no longer has the marked, committed state required to finish recovery.");

        if (status.TargetWriteBlocked)
        {
            await renewLeaseAsync().AnyContext();
            await SetWriteBlockAsync(status.CutoverReceipt!, false, cancellationToken).AnyContext();
        }

        await renewLeaseAsync().AnyContext();
        await RemoveWorkflowMarkerAsync(status.TargetIndex, status.CutoverReceipt, cancellationToken).AnyContext();
    }

    private async Task SetWriteBlockAsync(string index, bool blocked, CancellationToken cancellationToken)
    {
        var response = await _client.Indices.PutSettingsAsync(index,
            d => d.Settings(s => s.Blocks(b => b.Write(blocked))).RequestConfiguration(r => SingleAttemptRequest.Configure(_client, r)), cancellationToken).AnyContext();
        response.EnsureValid(
            r => r.IsValidResponse && r.Acknowledged,
            _logger,
            "Unable to set write block to {WriteBlocked} on recovered compatibility index {IndexName}",
            $"Unable to set the write block to '{blocked}' on recovered compatibility index '{index}'.",
            blocked, index);
    }

    internal async Task RemoveWorkflowMarkerAsync(string index, string? receipt, CancellationToken cancellationToken)
    {
        var actions = new List<IndexUpdateAliasesAction>
        {
            new() { Remove = new RemoveAction { Index = index, Alias = ElasticIndexCompatibilityUpgrader.OwnershipAlias, MustExist = true } }
        };
        if (receipt is not null)
            actions.Add(new() { Remove = new RemoveAction { Index = index, Alias = receipt, MustExist = true } });
        var response = await _client.Indices.UpdateAliasesAsync(d => d.Actions(actions)
            .RequestConfiguration(r => SingleAttemptRequest.Configure(_client, r)), cancellationToken).AnyContext();
        response.EnsureValid(
            r => r.IsValidResponse && r.Acknowledged,
            _logger,
            "Unable to remove compatibility workflow marker from {IndexName}",
            $"Unable to remove the compatibility workflow marker from '{index}'.",
            index);
    }

    private static IndexCompatibilityRecoveryAction GetRecoveryAction(ObservedTopology topology)
    {
        if (topology.HasUnexpectedIndex || !topology.ErrorLineageAuthenticated || !topology.ActiveTaskCount.HasValue
            || (topology.ActiveTaskCount > 0 && (!topology.SourceExists || !topology.TargetExists)))
            return IndexCompatibilityRecoveryAction.ManualIntervention;

        if (!topology.SourceExists && !topology.TargetExists)
            return IndexCompatibilityRecoveryAction.None;

        if (topology.SourceExists && !topology.TargetExists)
        {
            if (!topology.SourceWorkflowMarker && !topology.SourceWriteBlocked)
                return IndexCompatibilityRecoveryAction.None;

            // A timed-out block or create request may still complete after this metadata snapshot.
            return IndexCompatibilityRecoveryAction.ManualIntervention;
        }

        if (topology.SourceExists && topology.TargetExists)
        {
            if (!topology.SourceWorkflowMarker || !topology.SourceWriteBlocked || !topology.TargetWorkflowMarker || topology.TargetHasCanonicalSourceAlias)
                return IndexCompatibilityRecoveryAction.ManualIntervention;

            return topology.ActiveTaskCount is 1
                ? IndexCompatibilityRecoveryAction.Wait
                : IndexCompatibilityRecoveryAction.ManualIntervention;
        }

        if (topology.TargetWorkflowMarker && topology.TargetHasCanonicalSourceAlias)
        {
            return topology.HasValidReceipt && topology.ActiveTaskCount is 0
                ? IndexCompatibilityRecoveryAction.Finish
                : IndexCompatibilityRecoveryAction.ManualIntervention;
        }

        return !topology.HasReceipt && !topology.TargetWorkflowMarker && !topology.TargetWriteBlocked && topology.TargetHasCanonicalSourceAlias && topology.ActiveTaskCount is 0
            ? IndexCompatibilityRecoveryAction.None
            : IndexCompatibilityRecoveryAction.ManualIntervention;
    }

    private static bool ReadWriteBlock(IndexSettings? settings)
    {
        var indexSettings = settings?.Index ?? settings;
        return indexSettings?.Blocks?.Write is true;
    }

    internal static void ValidateExactIndexName(string sourceIndex)
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

    private sealed record ObservedTopology(
        bool SourceExists,
        bool TargetExists,
        bool SourceWriteBlocked,
        bool TargetWriteBlocked,
        bool SourceWorkflowMarker,
        bool TargetWorkflowMarker,
        bool TargetHasCanonicalSourceAlias,
        bool HasValidReceipt,
        bool HasReceipt,
        bool HasUnexpectedIndex,
        bool ErrorLineageAuthenticated,
        int? ActiveTaskCount);
}
