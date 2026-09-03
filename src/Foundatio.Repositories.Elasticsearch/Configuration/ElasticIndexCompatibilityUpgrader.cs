using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport;
using Elastic.Transport.Extensions;
using Elastic.Transport.Products.Elasticsearch;
using Foundatio.Lock;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TransportHttpMethod = Elastic.Transport.HttpMethod;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

internal sealed class ElasticIndexCompatibilityUpgrader
{
    internal const string OwnershipAlias = ".foundatio-compatibility-upgrade";
    private static readonly Version MinimumCreateFromVersion = new(8, 18);
    private static readonly JsonElement NullSettingValue = JsonSerializer.SerializeToElement<object?>(null);
    private static readonly HashSet<string> TemporaryTargetSettings = new(StringComparer.Ordinal)
    {
        "index.number_of_replicas",
        "index.refresh_interval",
        "index.default_pipeline",
        "index.final_pipeline"
    };
    private static readonly HashSet<string> GeneratedIndexSettings = new(StringComparer.Ordinal)
    {
        "index.creation_date",
        "index.creation_date_string",
        "index.history.uuid",
        "index.provided_name",
        "index.resize.source.name",
        "index.resize.source.uuid",
        "index.routing.allocation.initial_recovery._id",
        "index.uuid",
        "index.verified_read_only",
        "index.version.created",
        "index.version.created_string",
        "index.verified_before_close"
    };
    private readonly ElasticsearchClient _client;
    private readonly ElasticReindexTaskRunner _reindexTaskRunner;
    private readonly ElasticIndexCompatibilityRecovery _recovery;
    private readonly ILogger _logger;

    public ElasticIndexCompatibilityUpgrader(
        ElasticsearchClient client,
        TimeProvider timeProvider,
        ILogger? logger = null,
        ILockProvider? lockProvider = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(timeProvider);

        _client = client;
        _logger = logger ?? NullLogger.Instance;
        _reindexTaskRunner = new ElasticReindexTaskRunner(client, timeProvider, _logger);
        _recovery = new ElasticIndexCompatibilityRecovery(client, lockProvider, _logger);
    }

    public async Task ValidateAsync(Index index, IndexCompatibilityInfo compatibility, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(index);
        ArgumentNullException.ThrowIfNull(compatibility);

        EnsureCreateFromSupported(compatibility.ServerVersion);
        ElasticReindexTaskRunner.ValidateOptions(index.ReindexBatchSize, index.ReindexRequestsPerSecond);
        string targetIndex = CompatibilityIndexName.Create(compatibility.Name, compatibility.ServerMajor, index.Name);
        await EnsureTargetDoesNotExistAsync(targetIndex, cancellationToken).AnyContext();
        var sourceState = await GetIndexStateAsync(compatibility.Name, cancellationToken).AnyContext();
        index.ValidateCompatibilityUpgradeSource(compatibility.Name, sourceState.Aliases);
        ValidateSource(sourceState);
    }

    public async Task UpgradeAsync(
        Index index,
        IndexCompatibilityInfo compatibility,
        ILock reindexLock,
        Func<int, string?, Task> progressCallbackAsync,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(index);
        ArgumentNullException.ThrowIfNull(compatibility);
        ArgumentNullException.ThrowIfNull(reindexLock);
        ArgumentNullException.ThrowIfNull(progressCallbackAsync);

        string sourceIndex = compatibility.Name;
        string targetIndex = CompatibilityIndexName.Create(sourceIndex, compatibility.ServerMajor, index.Name);
        async Task ReportProgressAsync(int progress, string? message)
        {
            await reindexLock.RenewAsync().AnyContext();
            _logger.LogInformation("Compatibility upgrade {SourceIndex} -> {TargetIndex} progress {Progress}%: {Message}", sourceIndex, targetIndex, progress, message);
            await progressCallbackAsync(progress, message).AnyContext();
        }

        bool workflowAttempted = false;
        IReadOnlyDictionary<string, Alias>? expectedCutoverAliases = null;
        try
        {
            await ReportProgressAsync(0, $"Inspecting {sourceIndex}").AnyContext();
            await EnsureTargetDoesNotExistAsync(targetIndex, cancellationToken).AnyContext();
            var sourceState = await GetIndexStateAsync(sourceIndex, cancellationToken).AnyContext();
            index.ValidateCompatibilityUpgradeSource(sourceIndex, sourceState.Aliases);
            ValidateSource(sourceState);

            workflowAttempted = true;
            await AddWorkflowMarkerAsync(sourceIndex, false, cancellationToken).AnyContext();
            await AddWriteBlockAsync(sourceIndex, cancellationToken).AnyContext();
            await RefreshAsync(sourceIndex, cancellationToken).AnyContext();
            await ReportProgressAsync(5, $"Blocked writes to {sourceIndex}").AnyContext();

            await CreateTargetAsync(sourceIndex, targetIndex, cancellationToken).AnyContext();
            bool isErrorIndex = sourceState.Aliases.HasExactHiddenAlias(ElasticReindexer.ErrorIndexOwnershipAlias);
            await AddWorkflowMarkerAsync(targetIndex, isErrorIndex, cancellationToken).AnyContext();
            var targetState = await GetIndexStateAsync(targetIndex, cancellationToken).AnyContext();
            if (!HasExpectedWorkflowMarkers(targetState.Aliases, isErrorIndex))
                throw new RepositoryException($"Compatibility destination index '{targetIndex}' has unexpected aliases before reindexing. The marked source remains intact and write blocked.");
            if (!JsonDefinitionsMatch(sourceState.Mapping, targetState.Mapping))
                throw new RepositoryException($"Compatibility destination index '{targetIndex}' did not receive the source mapping exactly. Adjust matching index templates before retrying.");
            EnsureExplicitSettingsMatch(sourceState.ExplicitSettings, targetState.ExplicitSettings, ignoreTemporarySettings: true, targetIndex);
            await ReportProgressAsync(10, $"Created {targetIndex} from {sourceIndex}").AnyContext();

            var reindexResult = await _reindexTaskRunner.RunCompatibilityReindexAsync(
                sourceIndex,
                targetIndex,
                index.ReindexBatchSize,
                index.ReindexRequestsPerSecond,
                ReportProgressAsync,
                cancellationToken).AnyContext();

            // _create_from must remove the copied source block so _reindex can write. Reapply the dedicated block
            // as soon as the task completes, before counting, so the verified destination stays stable through cutover.
            await AddWriteBlockAsync(targetIndex, cancellationToken).AnyContext();
            await RefreshAsync(targetIndex, cancellationToken).AnyContext();
            await VerifyDocumentCountsAsync(sourceIndex, targetIndex, reindexResult, cancellationToken).AnyContext();
            await RestoreTargetSettingsAsync(targetIndex, sourceState.Settings, cancellationToken).AnyContext();
            await WaitForTargetHealthAsync(targetIndex, cancellationToken).AnyContext();
            await ReportProgressAsync(92, $"Validated {reindexResult.Total:N0} documents and restored index settings").AnyContext();
            targetState = await GetIndexStateAsync(targetIndex, cancellationToken).AnyContext();
            if (!JsonDefinitionsMatch(sourceState.Mapping, targetState.Mapping)
                || !String.Equals(sourceState.RestorableSettingsSignature, targetState.RestorableSettingsSignature, StringComparison.Ordinal))
            {
                throw new RepositoryException(
                    $"Compatibility destination index '{targetIndex}' did not preserve the source mapping and restorable settings exactly. " +
                    $"Source settings: '{sourceState.RestorableSettingsSignature.Replace('\n', '|')}', target settings: '{targetState.RestorableSettingsSignature.Replace('\n', '|')}'. " +
                    "The source remains intact and write blocked.");
            }
            EnsureExplicitSettingsMatch(sourceState.ExplicitSettings, targetState.ExplicitSettings, ignoreTemporarySettings: false, targetIndex);
            if (!HasExpectedWorkflowMarkers(targetState.Aliases, isErrorIndex))
                throw new RepositoryException($"Compatibility destination index '{targetIndex}' received unexpected aliases before cutover. The source remains intact and write blocked.");

            var currentSourceState = await GetIndexStateAsync(sourceIndex, cancellationToken).AnyContext();
            ValidateSource(currentSourceState, allowWorkflowMarker: true);
            if (!AliasDefinitionsMatch(sourceState.Aliases, WithoutWorkflowMarker(currentSourceState.Aliases))
                || !JsonDefinitionsMatch(sourceState.Mapping, currentSourceState.Mapping)
                || !String.Equals(sourceState.RestorableSettingsSignature, currentSourceState.RestorableSettingsSignature, StringComparison.Ordinal))
            {
                throw new RepositoryException($"Aliases, mappings, or restorable settings on compatibility source index '{sourceIndex}' changed during the upgrade. No cutover was attempted; stop index-management jobs, inspect the source, and retry.");
            }
            var sourceSettingChanges = GetExplicitSettingDifferences(
                sourceState.ExplicitSettings,
                currentSourceState.ExplicitSettings,
                ignoreTemporarySettings: false);
            if (sourceSettingChanges.Length > 0)
            {
                throw new RepositoryException(
                    $"Explicit settings on compatibility source index '{sourceIndex}' changed during the upgrade: {String.Join(", ", sourceSettingChanges)}. No cutover was attempted; stop index-management jobs, inspect the source, and retry.");
            }

            expectedCutoverAliases = CreateAliasActions(index.Name, currentSourceState, targetIndex, out var aliasActions);
            aliasActions.Insert(0, new IndexUpdateAliasesAction
            {
                RemoveIndex = new RemoveIndexAction { Index = sourceIndex }
            });

            // Once this request is dispatched, Elasticsearch may have committed the atomic alias swap even if
            // the client observes a timeout or cancellation. Never delete the destination after that point until
            // the resulting topology has been positively established.
            var cutoverResponse = await _client.Indices.UpdateAliasesAsync(a => a.Actions(aliasActions), cancellationToken).AnyContext();
            if (cutoverResponse.IsValidResponse && cutoverResponse.Acknowledged)
                _logger.LogRequest(cutoverResponse);
            else
                _logger.LogErrorRequest(cutoverResponse, "Unable to atomically replace compatibility source {SourceIndex} with {TargetIndex}", sourceIndex, targetIndex);

            var topology = await GetTopologyIndependentlyAsync(sourceIndex, targetIndex, expectedCutoverAliases).AnyContext();
            if (topology is CutoverTopology.NotStarted && (!cutoverResponse.IsValidResponse || !cutoverResponse.Acknowledged))
            {
                throw new RepositoryException(cutoverResponse.GetErrorMessage($"Unable to atomically replace '{sourceIndex}' with '{targetIndex}'."), cutoverResponse.OriginalException());
            }
            if (topology is not CutoverTopology.Completed)
            {
                throw new RepositoryException($"Compatibility cutover for '{sourceIndex}' is in an unexpected state. Do not retry or delete either index until the aliases and both physical indexes have been inspected manually.");
            }

            await RemoveWriteBlockAsync(targetIndex, cancellationToken).AnyContext();
            await RemoveWorkflowMarkerAsync(targetIndex, cancellationToken).AnyContext();

            index.MappingResolver.RefreshMapping();
            try
            {
                await ReportProgressAsync(100, $"Replaced {sourceIndex} with {targetIndex}").AnyContext();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Compatibility upgrade {SourceIndex} -> {TargetIndex} completed, but final progress reporting failed", sourceIndex, targetIndex);
            }
        }
        catch (Exception upgradeException)
        {
            if (!workflowAttempted)
                throw;

            using var recoveryCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            IndexCompatibilityUpgradeStatus status;
            try
            {
                status = await _recovery.InspectAsync(index, sourceIndex, recoveryCancellation.Token).AnyContext();
            }
            catch (Exception inspectionException)
            {
                throw new RepositoryException(
                    $"Compatibility upgrade for '{sourceIndex}' failed and its recovery evidence could not be inspected. Keep writes stopped and inspect both physical indexes before retrying.",
                    new AggregateException(upgradeException, inspectionException));
            }

            if (status.Action is IndexCompatibilityRecoveryAction.Reset)
            {
                try
                {
                    await _recovery.RecoverUnderLockAsync(index, sourceIndex, recoveryCancellation.Token).AnyContext();
                }
                catch (Exception recoveryException)
                {
                    throw new RepositoryException(
                        $"Compatibility upgrade for '{sourceIndex}' failed and evidence-based reset did not complete. Inspect both physical indexes before retrying.",
                        new AggregateException(upgradeException, recoveryException));
                }

                throw;
            }

            if (status.Action is IndexCompatibilityRecoveryAction.Finish)
            {
                if (expectedCutoverAliases is null
                    || await GetTopologyIndependentlyAsync(sourceIndex, targetIndex, expectedCutoverAliases).AnyContext() is not CutoverTopology.Completed)
                {
                    throw new RepositoryException(
                        $"Compatibility cutover for '{sourceIndex}' committed with aliases that do not match the pre-cutover source. Keep the marked destination write blocked and inspect its complete alias definitions before recovery.",
                        upgradeException);
                }

                await _recovery.RecoverUnderLockAsync(index, sourceIndex, recoveryCancellation.Token).AnyContext();
                index.MappingResolver.RefreshMapping();
                _logger.LogWarning(upgradeException, "Compatibility cutover {SourceIndex} -> {TargetIndex} committed despite a lost or failed client response; recovery finished the marked destination", sourceIndex, targetIndex);
                return;
            }

            if (IsCompletedCutover(status))
            {
                index.MappingResolver.RefreshMapping();
                _logger.LogWarning(upgradeException, "Compatibility cutover {SourceIndex} -> {TargetIndex} completed despite a lost or failed final client response", sourceIndex, targetIndex);
                return;
            }

            if (status.Action is IndexCompatibilityRecoveryAction.None)
                throw;

            throw new RepositoryException(
                $"Compatibility upgrade for '{sourceIndex}' failed and now requires recovery action '{status.Action}'. No unmarked index was changed; inspect the reported topology before retrying.",
                upgradeException);
        }
    }

    internal static bool IsCompletedCutover(IndexCompatibilityUpgradeStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);

        return status.Action is IndexCompatibilityRecoveryAction.None
            && !status.SourceExists
            && status.TargetExists
            && !status.TargetWriteBlocked
            && !status.TargetWorkflowMarkerPresent
            && status.TargetHasCanonicalSourceAlias
            && status.ActiveReindexTaskCount is 0;
    }

    private async Task<CompatibilityIndexState> GetIndexStateAsync(string indexName, CancellationToken cancellationToken)
    {
        var response = await _client.Indices.GetAsync((Indices)indexName,
            d => d.Features(Feature.Aliases, Feature.Mappings, Feature.Settings).IncludeDefaults(false), cancellationToken).AnyContext();
        if (!response.IsValidResponse)
        {
            _logger.LogErrorRequest(response, "Unable to read compatibility index {IndexName}", indexName);
            throw new RepositoryException(response.GetErrorMessage($"Unable to read compatibility index '{indexName}'."), response.OriginalException());
        }

        _logger.LogRequest(response);

        if (response.Indices is null || response.Indices.Count is not 1 || !response.Indices.TryGetValue(indexName, out var state) || state is null)
            throw new RepositoryException($"Compatibility index '{indexName}' must identify exactly one concrete index.");

        var settings = state.Settings?.Index ?? state.Settings ?? new IndexSettings();
        var explicitSettings = await GetExplicitSettingsAsync(indexName, cancellationToken).AnyContext();
        return new CompatibilityIndexState(
            indexName,
            state.Aliases ?? new Dictionary<string, Alias>(),
            settings.Blocks?.Write is true,
            explicitSettings.ContainsKey("index.verified_before_close"),
            ReadBoolean(settings.Hidden),
            settings,
            state.DataStream,
            state.Mappings?.Source?.Enabled,
            _client.ElasticsearchClientSettings.RequestResponseSerializer.SerializeToString(state.Mappings),
            CreateRestorableSettingsSignature(settings),
            explicitSettings);
    }

    private static void ValidateSource(CompatibilityIndexState source, bool allowWorkflowMarker = false)
    {
        if (source.Name.StartsWith(".", StringComparison.Ordinal))
            throw new RepositoryException($"System or restricted index '{source.Name}' is not supported by the Foundatio compatibility upgrader.");

        if (!String.IsNullOrEmpty(source.DataStream))
            throw new RepositoryException($"Data stream backing index '{source.Name}' is not supported by the Foundatio compatibility upgrader.");

        if (source.SourceEnabled is false)
            throw new RepositoryException($"Index '{source.Name}' has _source disabled and cannot be reindexed.");

        if (source.IsClosed)
            throw new RepositoryException($"Index '{source.Name}' is closed and must be opened before using the Foundatio compatibility upgrader.");

        if (source.Aliases.ContainsKey(OwnershipAlias) && !allowWorkflowMarker)
            throw new RepositoryException($"Index '{source.Name}' uses the reserved compatibility ownership alias '{OwnershipAlias}'. Remove or rename that alias before using the compatibility upgrader.");

        if (allowWorkflowMarker && !source.Aliases.HasExactHiddenAlias(OwnershipAlias))
            throw new RepositoryException($"Compatibility source index '{source.Name}' lost its workflow marker while the upgrade was running.");

        if (allowWorkflowMarker && !source.WasWriteBlocked)
            throw new RepositoryException($"Compatibility source index '{source.Name}' lost its write block while the upgrade was running. No cutover was attempted; stop all writers and inspect both indexes manually.");

        if (source.WasWriteBlocked && !allowWorkflowMarker)
            throw new RepositoryException($"Index '{source.Name}' already has an index write block. Remove it before starting a compatibility upgrade so recovery never mistakes an administrator block for Foundatio workflow evidence.");

        var blocks = source.Settings.Blocks;
        if (blocks?.Read is true || blocks?.Metadata is true || blocks?.ReadOnly is true || blocks?.ReadOnlyAllowDelete is true)
            throw new RepositoryException($"Index '{source.Name}' has a read or metadata block. Remove that block before using the compatibility upgrader; only an index write block is supported.");

        if (!String.IsNullOrEmpty(source.Settings.Mode) && !String.Equals(source.Settings.Mode, "standard", StringComparison.OrdinalIgnoreCase))
            throw new RepositoryException($"Index '{source.Name}' uses index mode '{source.Settings.Mode}', which is not supported by the Foundatio compatibility upgrader.");

        if (source.Settings.Lifecycle?.Name is not null)
            throw new RepositoryException($"Index '{source.Name}' is managed by ILM and must be upgraded with its lifecycle tooling.");

        if (source.Settings.OtherSettings?.ContainsKey("xpack.ccr.following_index") is true)
            throw new RepositoryException($"CCR follower index '{source.Name}' is not supported by the Foundatio compatibility upgrader.");
    }

    private async Task EnsureTargetDoesNotExistAsync(string targetIndex, CancellationToken cancellationToken)
    {
        var response = await _client.Indices.ExistsAsync(targetIndex, cancellationToken).AnyContext();
        if (response.ApiCallDetails.HasSuccessfulStatusCode && response.Exists)
        {
            _logger.LogRequest(response);
            throw new RepositoryException($"Compatibility destination index '{targetIndex}' already exists. Inspect it and remove it only after confirming that it is an unaliased artifact from an interrupted attempt.");
        }

        if (!response.ApiCallDetails.HasSuccessfulStatusCode && response.ApiCallDetails.HttpStatusCode is not 404)
        {
            _logger.LogErrorRequest(response, "Unable to determine whether compatibility destination index {TargetIndex} exists", targetIndex);
            throw new RepositoryException(response.GetErrorMessage($"Unable to determine whether compatibility destination index '{targetIndex}' exists."), response.OriginalException());
        }

        _logger.LogRequest(response);
    }

    private async Task AddWriteBlockAsync(string indexName, CancellationToken cancellationToken)
    {
        string escapedIndex = Uri.EscapeDataString(indexName);
        var path = new EndpointPath(TransportHttpMethod.PUT, $"/{escapedIndex}/_block/write");
        var response = await _client.Transport.RequestAsync<ElasticsearchStringResponse>(path, null, null, null, cancellationToken).AnyContext();
        if (!response.IsValidResponse || String.IsNullOrEmpty(response.Body))
        {
            _logger.LogErrorRequest(response, "Unable to add the dedicated write block to compatibility index {IndexName}", indexName);
            response.TryGetOriginalException(out var originalException);
            throw new RepositoryException(
                String.IsNullOrWhiteSpace(response.Body)
                    ? $"Unable to add a write block to compatibility index '{indexName}'. {response.DebugInformation}"
                    : $"Unable to add a write block to compatibility index '{indexName}'. {response.Body}",
                originalException);
        }

        _logger.LogRequest(response);
        AddBlockResponse? result;
        try
        {
            result = JsonSerializer.Deserialize<AddBlockResponse>(response.Body);
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Unable to parse the add-index-block response for compatibility index {IndexName}: {ResponseBody}", indexName, response.Body);
            throw new RepositoryException($"Elasticsearch returned an unrecognized add-index-block response for '{indexName}'.", ex);
        }

        if (!IsWriteBlockConfirmed(result, indexName))
        {
            _logger.LogError("Elasticsearch did not fully acknowledge the dedicated write block for compatibility index {IndexName}: {ResponseBody}", indexName, response.Body);
            throw new RepositoryException($"Elasticsearch did not confirm that all shards of compatibility index '{indexName}' were write blocked.");
        }
    }

    private static bool IsWriteBlockConfirmed(AddBlockResponse? response, string sourceIndex)
    {
        return response is { Acknowledged: true, ShardsAcknowledged: true, Indices.Count: 1 }
            && String.Equals(response.Indices[0].Name, sourceIndex, StringComparison.Ordinal)
            && response.Indices[0].Blocked;
    }

    internal static bool IsWriteBlockConfirmed(string response, string sourceIndex)
    {
        ArgumentException.ThrowIfNullOrEmpty(response);
        ArgumentException.ThrowIfNullOrEmpty(sourceIndex);
        return IsWriteBlockConfirmed(JsonSerializer.Deserialize<AddBlockResponse>(response), sourceIndex);
    }

    private async Task CreateTargetAsync(string sourceIndex, string targetIndex, CancellationToken cancellationToken)
    {
        var request = new CreateFromRequest(sourceIndex, targetIndex)
        {
            CreateFrom = new CreateFrom
            {
                RemoveIndexBlocks = true,
                SettingsOverride = new IndexSettings
                {
                    NumberOfReplicas = 0,
                    RefreshInterval = Duration.MinusOne,
                    DefaultPipeline = "_none",
                    FinalPipeline = "_none"
                }
            }
        };

        try
        {
            var response = await _client.Indices.CreateFromAsync(request, cancellationToken).AnyContext();
            // Any unsuccessful outcome leaves uncertainty: a lost response may have committed on the server, and
            // even a definitive error response cannot prove a partial creation attempt left nothing behind.
            // Treat every failed create like an ambiguous reindex start: retain the destination and the source
            // write block until both indexes have been inspected instead of attempting automatic cleanup.
            if (!response.IsValidResponse || !response.Acknowledged || !String.Equals(response.Index, targetIndex, StringComparison.Ordinal))
            {
                _logger.LogErrorRequest(response, "Unable to create compatibility destination {TargetIndex} from {SourceIndex}", targetIndex, sourceIndex);
                throw new ElasticCompatibilityOperationUncertainException(
                    $"The compatibility destination creation outcome for '{sourceIndex}' -> '{targetIndex}' is unknown. Keep the source write blocked and retain the destination until both indexes have been inspected.",
                    response.OriginalException() ?? new RepositoryException(response.GetErrorMessage($"Unable to create compatibility destination index '{targetIndex}' from '{sourceIndex}'.")));
            }
            _logger.LogRequest(response);
        }
        catch (RepositoryException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new ElasticCompatibilityOperationUncertainException(
                $"The compatibility destination creation outcome for '{sourceIndex}' -> '{targetIndex}' is unknown because no response was received. Keep the source write blocked and retain the destination until both indexes have been inspected.",
                ex);
        }
    }

    private async Task AddWorkflowMarkerAsync(string index, bool includeErrorMarker, CancellationToken cancellationToken)
    {
        var actions = new List<IndexUpdateAliasesAction>(includeErrorMarker ? 2 : 1)
        {
            new()
            {
                Add = new AddAction { Index = index, Alias = OwnershipAlias, IsHidden = true }
            }
        };
        if (includeErrorMarker)
        {
            actions.Add(new IndexUpdateAliasesAction
            {
                Add = new AddAction { Index = index, Alias = ElasticReindexer.ErrorIndexOwnershipAlias, IsHidden = true }
            });
        }

        var response = await _client.Indices.UpdateAliasesAsync(a => a.Actions(actions), cancellationToken).AnyContext();
        if (!response.IsValidResponse || !response.Acknowledged)
        {
            _logger.LogErrorRequest(response, "Unable to add compatibility workflow marker to {IndexName}", index);
            throw new RepositoryException(response.GetErrorMessage($"Unable to mark compatibility index '{index}' for safe recovery."), response.OriginalException());
        }

        _logger.LogRequest(response);
    }

    private async Task RemoveWorkflowMarkerAsync(string index, CancellationToken cancellationToken)
    {
        var response = await _client.Indices.UpdateAliasesAsync(a => a.Actions(action => action.Remove(remove => remove
            .Index(index)
            .Alias(OwnershipAlias))), cancellationToken).AnyContext();
        if (!response.IsValidResponse || !response.Acknowledged)
        {
            _logger.LogErrorRequest(response, "Unable to remove compatibility workflow marker from {IndexName}", index);
            throw new RepositoryException(response.GetErrorMessage($"Compatibility cutover completed, but the workflow marker could not be removed from '{index}'."), response.OriginalException());
        }

        _logger.LogRequest(response);
    }

    private async Task<IReadOnlyDictionary<string, string?>> GetExplicitSettingsAsync(string index, CancellationToken cancellationToken)
    {
        var response = await _client.Indices.GetSettingsAsync((Indices)index,
            d => d.FlatSettings().IncludeDefaults(false), cancellationToken).AnyContext();
        if (!response.IsValidResponse)
        {
            _logger.LogErrorRequest(response, "Unable to read explicit settings for compatibility index {IndexName}", index);
            throw new RepositoryException(response.GetErrorMessage($"Unable to read explicit settings for compatibility index '{index}'."), response.OriginalException());
        }

        _logger.LogRequest(response);
        var state = response.RequireSingleResolvedIndexState(index);
        var settings = state.Settings?.Index ?? state.Settings;
        var result = new Dictionary<string, string?>(settings?.OtherSettings?.Count ?? 0, StringComparer.Ordinal);
        if (settings?.OtherSettings is null)
            return result;

        foreach (var setting in settings.OtherSettings)
        {
            result[setting.Key] = setting.Value switch
            {
                null => null,
                JsonElement element when element.ValueKind is JsonValueKind.String => element.GetString(),
                JsonElement element => element.GetRawText(),
                _ => setting.Value.ToString()
            };
        }

        return result;
    }

    private async Task VerifyDocumentCountsAsync(
        string sourceIndex,
        string targetIndex,
        ElasticReindexTaskResult reindexResult,
        CancellationToken cancellationToken)
    {
        var sourceCount = await _client.CountAsync<object>(d => d.Indices(sourceIndex), cancellationToken).AnyContext();
        if (!sourceCount.IsValidResponse || !ShardsSucceeded(sourceCount.Shards))
        {
            _logger.LogErrorRequest(sourceCount, "Unable to count compatibility source index {SourceIndex}", sourceIndex);
            throw new RepositoryException(sourceCount.GetErrorMessage($"Unable to count compatibility source index '{sourceIndex}'."), sourceCount.OriginalException());
        }

        _logger.LogRequest(sourceCount);

        var targetCount = await _client.CountAsync<object>(d => d.Indices(targetIndex), cancellationToken).AnyContext();
        if (!targetCount.IsValidResponse || !ShardsSucceeded(targetCount.Shards))
        {
            _logger.LogErrorRequest(targetCount, "Unable to count compatibility destination index {TargetIndex}", targetIndex);
            throw new RepositoryException(targetCount.GetErrorMessage($"Unable to count compatibility destination index '{targetIndex}'."), targetCount.OriginalException());
        }

        _logger.LogRequest(targetCount);

        if (sourceCount.Count != targetCount.Count || sourceCount.Count != reindexResult.Total || targetCount.Count != reindexResult.Created)
        {
            throw new RepositoryException(
                $"Compatibility reindex count mismatch for '{sourceIndex}' -> '{targetIndex}'. " +
                $"Source: {sourceCount.Count}, Target: {targetCount.Count}, Reindex total: {reindexResult.Total}, Created: {reindexResult.Created}.");
        }
    }

    private async Task RestoreTargetSettingsAsync(string targetIndex, IndexSettings sourceSettings, CancellationToken cancellationToken)
    {
        var settings = new Dictionary<string, object>
        {
            ["index.number_of_replicas"] = GetSettingValue(GetValue(sourceSettings.NumberOfReplicas)),
            ["index.refresh_interval"] = GetSettingValue(sourceSettings.RefreshInterval?.ToString()),
            ["index.default_pipeline"] = GetSettingValue(sourceSettings.DefaultPipeline),
            ["index.final_pipeline"] = GetSettingValue(sourceSettings.FinalPipeline),
            ["index.blocks.write"] = true
        };

        var response = await _client.Indices.PutSettingsAsync(targetIndex,
            d => d.Settings(new IndexSettings { OtherSettings = settings }), cancellationToken).AnyContext();
        if (!response.IsValidResponse || !response.Acknowledged)
        {
            _logger.LogErrorRequest(response, "Unable to restore settings on compatibility destination {TargetIndex}", targetIndex);
            throw new RepositoryException(response.GetErrorMessage($"Unable to restore settings on compatibility destination index '{targetIndex}'."), response.OriginalException());
        }

        _logger.LogRequest(response);
    }

    private async Task RemoveWriteBlockAsync(string targetIndex, CancellationToken cancellationToken)
    {
        var response = await _client.Indices.PutSettingsAsync(targetIndex,
            d => d.Settings(s => s.Blocks(b => b.Write(false))), cancellationToken).AnyContext();
        if (!response.IsValidResponse || !response.Acknowledged)
        {
            _logger.LogErrorRequest(response, "Unable to remove the write block from compatibility destination {TargetIndex}", targetIndex);
            throw new RepositoryException(response.GetErrorMessage($"Compatibility cutover completed, but the write block could not be removed from destination index '{targetIndex}'. The source was replaced successfully; inspect and unblock the destination before resuming writes."), response.OriginalException());
        }

        _logger.LogRequest(response);
    }

    private async Task WaitForTargetHealthAsync(string targetIndex, CancellationToken cancellationToken)
    {
        var response = await _client.Cluster.HealthAsync(d => d
            .Indices(targetIndex)
            .WaitForStatus(HealthStatus.Yellow)
            .WaitForNoInitializingShards()
            .WaitForNoRelocatingShards()
            .Timeout("30s"), cancellationToken).AnyContext();
        if (!response.IsValidResponse
            || response.TimedOut
            || response.Status is not HealthStatus.Yellow and not HealthStatus.Green)
        {
            _logger.LogErrorRequest(response, "Compatibility destination {TargetIndex} did not reach the required shard health", targetIndex);
            throw new RepositoryException(response.GetErrorMessage($"Compatibility destination index '{targetIndex}' did not make all primary shards available after restoring replicas. The source remains intact and write blocked."), response.OriginalException());
        }

        _logger.LogRequest(response);
    }

    private static IReadOnlyDictionary<string, Alias> CreateAliasActions(string logicalIndexName, CompatibilityIndexState source, string targetIndex, out List<IndexUpdateAliasesAction> actions)
    {
        actions = new List<IndexUpdateAliasesAction>(source.Aliases.Count + 2);
        var expectedAliases = new Dictionary<string, Alias>(source.Aliases.Count + 2, StringComparer.Ordinal);

        foreach (var alias in source.Aliases)
        {
            if (String.Equals(alias.Key, OwnershipAlias, StringComparison.Ordinal))
                continue;

            expectedAliases.Add(alias.Key, alias.Value);
            actions.Add(new IndexUpdateAliasesAction
            {
                Add = new AddAction
                {
                    Index = targetIndex,
                    Alias = alias.Key,
                    Filter = alias.Value.Filter,
                    IndexRouting = alias.Value.IndexRouting,
                    IsHidden = alias.Value.IsHidden,
                    IsWriteIndex = alias.Value.IsWriteIndex,
                    Routing = alias.Value.Routing,
                    SearchRouting = alias.Value.SearchRouting
                }
            });
        }

        expectedAliases.Add(OwnershipAlias, new Alias { IsHidden = true });

        string canonicalPhysicalName = CompatibilityIndexName.GetCanonicalName(source.Name, logicalIndexName);
        if (!expectedAliases.ContainsKey(canonicalPhysicalName))
        {
            var canonicalAlias = new Alias { IsHidden = source.IsHidden };
            expectedAliases.Add(canonicalPhysicalName, canonicalAlias);
            actions.Add(new IndexUpdateAliasesAction
            {
                Add = new AddAction { Index = targetIndex, Alias = canonicalPhysicalName, IsHidden = canonicalAlias.IsHidden }
            });
        }

        return expectedAliases;
    }

    private bool AliasDefinitionsMatch(IReadOnlyDictionary<string, Alias> expected, IReadOnlyDictionary<string, Alias> actual)
    {
        if (expected.Count != actual.Count)
            return false;

        foreach (var expectedAlias in expected)
        {
            if (!actual.TryGetValue(expectedAlias.Key, out var actualAlias)
                || expectedAlias.Value.IsHidden != actualAlias.IsHidden
                || expectedAlias.Value.IsWriteIndex != actualAlias.IsWriteIndex
                || !String.Equals(expectedAlias.Value.IndexRouting?.ToString(), actualAlias.IndexRouting?.ToString(), StringComparison.Ordinal)
                || !String.Equals(expectedAlias.Value.Routing?.ToString(), actualAlias.Routing?.ToString(), StringComparison.Ordinal)
                || !String.Equals(expectedAlias.Value.SearchRouting?.ToString(), actualAlias.SearchRouting?.ToString(), StringComparison.Ordinal)
                || !JsonDefinitionsMatch(
                    _client.ElasticsearchClientSettings.RequestResponseSerializer.SerializeToString(expectedAlias.Value.Filter),
                    _client.ElasticsearchClientSettings.RequestResponseSerializer.SerializeToString(actualAlias.Filter)))
            {
                return false;
            }
        }

        return true;
    }

    private async Task<CutoverTopology> GetTopologyAsync(
        string sourceIndex,
        string targetIndex,
        IReadOnlyDictionary<string, Alias> expectedAliases,
        CancellationToken cancellationToken)
    {
        string names = String.Join(',', sourceIndex, targetIndex);
        var response = await _client.Indices.GetAsync(Indices.Parse(names), d => d.LimitToNamesAndAliases().IgnoreUnavailable(), cancellationToken).AnyContext();
        if (!response.IsValidResponse && response.ElasticsearchServerError?.Status is not 404)
        {
            _logger.LogErrorRequest(response, "Unable to inspect compatibility cutover topology for {SourceIndex} -> {TargetIndex}", sourceIndex, targetIndex);
            return CutoverTopology.Uncertain;
        }

        _logger.LogRequest(response);

        bool sourceExists = response.Indices?.ContainsKey(sourceIndex) is true;
        bool targetExists = response.Indices?.ContainsKey(targetIndex) is true;
        IReadOnlyDictionary<string, Alias> targetAliases = response.Indices is not null && response.Indices.TryGetValue(targetIndex, out var targetState)
            ? targetState.Aliases ?? new Dictionary<string, Alias>()
            : new Dictionary<string, Alias>();

        if (!sourceExists && targetExists && AliasDefinitionsMatch(expectedAliases, targetAliases))
            return CutoverTopology.Completed;

        bool hasErrorMarker = targetAliases.HasExactHiddenAlias(ElasticReindexer.ErrorIndexOwnershipAlias);
        if (sourceExists && targetExists && HasExpectedWorkflowMarkers(targetAliases, hasErrorMarker))
            return CutoverTopology.NotStarted;

        return CutoverTopology.Uncertain;
    }

    private async Task<CutoverTopology> GetTopologyIndependentlyAsync(
        string sourceIndex,
        string targetIndex,
        IReadOnlyDictionary<string, Alias> expectedAliases)
    {
        using var reconciliationCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        return await GetTopologyAsync(sourceIndex, targetIndex, expectedAliases, reconciliationCancellation.Token).AnyContext();
    }

    private static IReadOnlyDictionary<string, Alias> WithoutWorkflowMarker(IReadOnlyDictionary<string, Alias> aliases)
    {
        var result = new Dictionary<string, Alias>(aliases.Count, StringComparer.Ordinal);
        foreach (var alias in aliases)
        {
            if (!String.Equals(alias.Key, OwnershipAlias, StringComparison.Ordinal))
                result.Add(alias.Key, alias.Value);
        }

        return result;
    }

    private static bool HasExpectedWorkflowMarkers(IReadOnlyDictionary<string, Alias> aliases, bool includeErrorMarker)
    {
        int expectedCount = includeErrorMarker ? 2 : 1;
        return aliases.Count is var count
            && count == expectedCount
            && aliases.HasExactHiddenAlias(OwnershipAlias)
            && (!includeErrorMarker || aliases.HasExactHiddenAlias(ElasticReindexer.ErrorIndexOwnershipAlias));
    }

    private static void EnsureExplicitSettingsMatch(
        IReadOnlyDictionary<string, string?> source,
        IReadOnlyDictionary<string, string?> target,
        bool ignoreTemporarySettings,
        string targetIndex)
    {
        var differences = GetExplicitSettingDifferences(source, target, ignoreTemporarySettings);
        if (differences.Length > 0)
        {
            throw new RepositoryException(
                $"Compatibility destination index '{targetIndex}' did not preserve explicit settings: {String.Join(", ", differences)}. The marked source remains intact and write blocked.");
        }
    }

    private static string[] GetExplicitSettingDifferences(
        IReadOnlyDictionary<string, string?> expected,
        IReadOnlyDictionary<string, string?> actual,
        bool ignoreTemporarySettings)
    {
        var expectedComparable = GetComparableSettings(expected, ignoreTemporarySettings);
        var actualComparable = GetComparableSettings(actual, ignoreTemporarySettings);
        return expectedComparable.Keys
            .Union(actualComparable.Keys, StringComparer.Ordinal)
            .Where(key => !expectedComparable.TryGetValue(key, out string? expectedValue)
                || !actualComparable.TryGetValue(key, out string? actualValue)
                || !String.Equals(expectedValue, actualValue, StringComparison.Ordinal))
            .Order(StringComparer.Ordinal)
            .ToArray();
    }

    private static Dictionary<string, string?> GetComparableSettings(IReadOnlyDictionary<string, string?> settings, bool ignoreTemporarySettings)
    {
        var result = new Dictionary<string, string?>(settings.Count, StringComparer.Ordinal);
        foreach (var setting in settings)
        {
            if (GeneratedIndexSettings.Contains(setting.Key)
                || setting.Key.StartsWith("index.blocks.", StringComparison.Ordinal)
                || (ignoreTemporarySettings && TemporaryTargetSettings.Contains(setting.Key)))
            {
                continue;
            }

            result.Add(setting.Key, setting.Value);
        }

        return result;
    }

    private async Task RefreshAsync(string index, CancellationToken cancellationToken)
    {
        var response = await _client.Indices.RefreshAsync((Indices)index, cancellationToken).AnyContext();
        if (!response.IsValidResponse || !ShardsSucceeded(response.Shards))
        {
            _logger.LogErrorRequest(response, "Unable to refresh compatibility index {IndexName}", index);
            throw new RepositoryException(response.GetErrorMessage($"Unable to refresh compatibility index '{index}'."), response.OriginalException());
        }

        _logger.LogRequest(response);
    }

    internal static bool ShardsSucceeded(ShardStatistics? shards)
    {
        // Refresh includes unassigned replicas in Total on yellow clusters. Failed is the authoritative
        // partial-operation signal; at least one successful shard also rules out empty/unknown responses.
        return shards is not null && shards.Failed is 0 && shards.Successful > 0;
    }

    private static void EnsureCreateFromSupported(string serverVersion)
    {
        string normalizedVersion = serverVersion.Split('-', 2)[0];
        if (!Version.TryParse(normalizedVersion, out var version) || version < MinimumCreateFromVersion)
            throw new NotSupportedException($"Elasticsearch {serverVersion} does not support the _create_from API. Compatibility upgrades require Elasticsearch 8.18 or later.");
    }

    private static bool ReadBoolean(Union<bool, string>? value)
    {
        return value?.Match(first => first, second => Boolean.TryParse(second, out bool result) && result) is true;
    }

    private static object? GetValue(Union<int, string>? value)
    {
        return value?.Match<object?>(first => first, second => second);
    }

    private static object GetSettingValue(object? value) => value ?? NullSettingValue;

    internal static bool JsonDefinitionsMatch(string expected, string actual)
    {
        return JsonNode.DeepEquals(JsonNode.Parse(expected), JsonNode.Parse(actual));
    }

    private static string CreateRestorableSettingsSignature(IndexSettings settings)
    {
        return String.Join('\n',
            GetValue(settings.NumberOfReplicas)?.ToString(),
            settings.RefreshInterval?.ToString(),
            settings.DefaultPipeline,
            settings.FinalPipeline);
    }

    private sealed record CompatibilityIndexState(
        string Name,
        IReadOnlyDictionary<string, Alias> Aliases,
        bool WasWriteBlocked,
        bool IsClosed,
        bool IsHidden,
        IndexSettings Settings,
        string? DataStream,
        bool? SourceEnabled,
        string Mapping,
        string RestorableSettingsSignature,
        IReadOnlyDictionary<string, string?> ExplicitSettings);

    private sealed record AddBlockResponse
    {
        [JsonPropertyName("acknowledged")]
        public bool Acknowledged { get; init; }

        [JsonPropertyName("shards_acknowledged")]
        public bool ShardsAcknowledged { get; init; }

        [JsonPropertyName("indices")]
        public IReadOnlyList<AddBlockIndexResult> Indices { get; init; } = [];
    }

    private sealed record AddBlockIndexResult
    {
        [JsonPropertyName("name")]
        public string? Name { get; init; }

        [JsonPropertyName("blocked")]
        public bool Blocked { get; init; }
    }

    private enum CutoverTopology
    {
        NotStarted,
        Completed,
        Uncertain
    }
}

internal sealed class ElasticCompatibilityOperationUncertainException : RepositoryException
{
    public ElasticCompatibilityOperationUncertainException(string message, Exception innerException) : base(message, innerException) { }
}
