using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
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
    private static readonly HashSet<string> TemporaryTargetSettings = new(StringComparer.Ordinal)
    {
        "index.number_of_replicas",
        "index.refresh_interval",
        "index.default_pipeline",
        "index.final_pipeline"
    };
    private readonly ElasticsearchClient _client;
    private readonly ElasticReindexTaskRunner _reindexTaskRunner;
    private readonly ILogger _logger;

    public ElasticIndexCompatibilityUpgrader(ElasticsearchClient client, TimeProvider timeProvider, ILogger? logger = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _logger = logger ?? NullLogger.Instance;
        _reindexTaskRunner = new ElasticReindexTaskRunner(client, timeProvider, _logger);
    }

    public async Task ValidateAsync(Index index, IndexCompatibilityInfo compatibility, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(index);
        ArgumentNullException.ThrowIfNull(compatibility);

        EnsureCreateFromSupported(compatibility.ServerVersion);
        ElasticReindexTaskRunner.ValidateOptions(index.ReindexBatchSize, index.ReindexRequestsPerSecond);
        string targetIndex = CompatibilityIndexName.Create(compatibility.Name, compatibility.ServerMajor, index.Name);
        await EnsureTargetDoesNotExistAsync(targetIndex, cancellationToken).AnyContext();
        var sourceState = await GetSourceStateAsync(compatibility.Name, cancellationToken).AnyContext();
        index.ValidateCompatibilityUpgradeSource(compatibility.Name, sourceState.Aliases.ContainsKey(index.Name));
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
        bool sourceBlockAttempted = false;
        bool sourceBlockConfirmed = false;
        bool sourceBlockAdded = false;
        bool targetCreated = false;
        bool cutoverAttempted = false;
        bool cutoverCompleted = false;
        bool topologyUncertain = false;

        async Task ReportProgressAsync(int progress, string? message)
        {
            await reindexLock.RenewAsync().AnyContext();
            _logger.LogInformation("Compatibility upgrade {SourceIndex} -> {TargetIndex} progress {Progress}%: {Message}", sourceIndex, targetIndex, progress, message);
            await progressCallbackAsync(progress, message).AnyContext();
        }

        SourceIndexState? sourceState = null;
        try
        {
            await ReportProgressAsync(0, $"Inspecting {sourceIndex}").AnyContext();
            await EnsureTargetDoesNotExistAsync(targetIndex, cancellationToken).AnyContext();
            sourceState = await GetSourceStateAsync(sourceIndex, cancellationToken).AnyContext();
            index.ValidateCompatibilityUpgradeSource(sourceIndex, sourceState.Aliases.ContainsKey(index.Name));
            ValidateSource(sourceState);

            sourceBlockAttempted = true;
            await AddWriteBlockAsync(sourceIndex, cancellationToken).AnyContext();
            sourceBlockConfirmed = true;
            sourceBlockAdded = !sourceState.WasWriteBlocked;

            await RefreshAsync(sourceIndex, cancellationToken).AnyContext();
            await ReportProgressAsync(5, $"Blocked writes to {sourceIndex}").AnyContext();

            await CreateTargetAsync(sourceIndex, targetIndex, cancellationToken).AnyContext();
            targetCreated = true;
            var targetState = await GetSourceStateAsync(targetIndex, cancellationToken).AnyContext();
            if (targetState.Aliases.Count > 0)
                throw new RepositoryException($"Compatibility destination index '{targetIndex}' received aliases during creation. Adjust matching index templates before retrying.");
            if (!String.Equals(sourceState.MappingSignature, targetState.MappingSignature, StringComparison.Ordinal))
                throw new RepositoryException($"Compatibility destination index '{targetIndex}' did not receive the source mapping exactly. Adjust matching index templates before retrying.");
            var unexpectedTargetSettings = targetState.ExplicitSettings.Keys
                .Where(setting => !sourceState.ExplicitSettings.ContainsKey(setting) && !TemporaryTargetSettings.Contains(setting))
                .Order(StringComparer.Ordinal)
                .ToArray();
            if (unexpectedTargetSettings.Length > 0)
            {
                throw new RepositoryException(
                    $"Compatibility destination index '{targetIndex}' received unexpected index settings: {String.Join(", ", unexpectedTargetSettings)}. Adjust matching index templates before retrying.");
            }

            await AddOwnershipAliasAsync(targetIndex, cancellationToken).AnyContext();
            await ReportProgressAsync(10, $"Created {targetIndex} from {sourceIndex}").AnyContext();

            var reindexResult = await _reindexTaskRunner.RunCompatibilityReindexAsync(
                sourceIndex,
                targetIndex,
                index.ReindexBatchSize,
                index.ReindexRequestsPerSecond,
                ReportProgressAsync,
                cancellationToken).AnyContext();

            await RefreshAsync(targetIndex, cancellationToken).AnyContext();
            await VerifyDocumentCountsAsync(sourceIndex, targetIndex, reindexResult, cancellationToken).AnyContext();
            await RestoreTargetSettingsAsync(targetIndex, sourceState.Settings, cancellationToken).AnyContext();
            await WaitForTargetHealthAsync(targetIndex, cancellationToken).AnyContext();
            targetState = await GetSourceStateAsync(targetIndex, cancellationToken).AnyContext();
            if (!String.Equals(sourceState.MappingSignature, targetState.MappingSignature, StringComparison.Ordinal)
                || !String.Equals(sourceState.RestorableSettingsSignature, targetState.RestorableSettingsSignature, StringComparison.Ordinal))
            {
                throw new RepositoryException($"Compatibility destination index '{targetIndex}' did not preserve the source mapping and restorable settings exactly. The source remains intact and write blocked.");
            }
            if (targetState.Aliases.Count is not 1 || !HasOwnershipAlias(targetState.Aliases))
                throw new RepositoryException($"Compatibility destination index '{targetIndex}' received unexpected aliases before cutover. The source remains intact and write blocked.");
            await ReportProgressAsync(92, $"Validated {reindexResult.Total:N0} documents and restored index settings").AnyContext();

            var currentSourceState = await GetSourceStateAsync(sourceIndex, cancellationToken).AnyContext();
            ValidateSource(currentSourceState);
            if (!AliasDefinitionsMatch(sourceState.Aliases, currentSourceState.Aliases)
                || !String.Equals(sourceState.MappingSignature, currentSourceState.MappingSignature, StringComparison.Ordinal)
                || !String.Equals(sourceState.RestorableSettingsSignature, currentSourceState.RestorableSettingsSignature, StringComparison.Ordinal))
            {
                throw new RepositoryException($"Aliases, mappings, or restorable settings on compatibility source index '{sourceIndex}' changed during the upgrade. No cutover was attempted; stop index-management jobs, inspect the source, and retry.");
            }

            var expectedAliases = CreateAliasActions(index.Name, currentSourceState, targetIndex, out var aliasActions);
            aliasActions.Insert(0, new IndexUpdateAliasesAction
            {
                Remove = new RemoveAction { Index = targetIndex, Alias = OwnershipAlias }
            });
            aliasActions.Insert(0, new IndexUpdateAliasesAction
            {
                RemoveIndex = new RemoveIndexAction { Index = sourceIndex }
            });

            // Once this request is dispatched, Elasticsearch may have committed the atomic alias swap even if
            // the client observes a timeout or cancellation. Never delete the destination after that point until
            // the resulting topology has been positively established.
            cutoverAttempted = true;
            var cutoverResponse = await _client.Indices.UpdateAliasesAsync(a => a.Actions(aliasActions), cancellationToken).AnyContext();
            _logger.LogRequest(cutoverResponse);

            var topology = await GetTopologyIndependentlyAsync(sourceIndex, targetIndex, expectedAliases).AnyContext();
            if (topology is CutoverTopology.Completed)
            {
                cutoverCompleted = true;
            }
            else if (topology is CutoverTopology.NotStarted && !cutoverResponse.IsValidResponse)
            {
                // The independent topology read proved that the failed request did not change either index.
                cutoverAttempted = false;
                throw new RepositoryException(cutoverResponse.GetErrorMessage($"Unable to atomically replace '{sourceIndex}' with '{targetIndex}'."), cutoverResponse.OriginalException());
            }
            else
            {
                topologyUncertain = true;
                throw new RepositoryException($"Compatibility cutover for '{sourceIndex}' is in an unexpected state. Do not retry or delete either index until the aliases and both physical indexes have been inspected manually.");
            }

            if (!sourceState.WasWriteBlocked)
                await RemoveWriteBlockAsync(targetIndex, cancellationToken).AnyContext();

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
            if (sourceBlockAttempted && !sourceBlockConfirmed)
            {
                const string message = "The request outcome is unknown; inspect the source write block before retrying.";
                if (upgradeException is OperationCanceledException)
                    throw new OperationCanceledException($"Compatibility upgrade for '{sourceIndex}' was canceled while adding its write block. {message}", upgradeException, cancellationToken);

                throw new RepositoryException($"Compatibility upgrade for '{sourceIndex}' failed while adding its write block. {message}", upgradeException);
            }

            string? uncertainCutoverMessage = null;
            bool cleanupUnsafe = upgradeException is ElasticReindexTaskUncertainException or ElasticCompatibilityOperationUncertainException;
            if (cutoverAttempted && !cutoverCompleted)
            {
                topologyUncertain = true;
                uncertainCutoverMessage = $"Compatibility cutover for '{sourceIndex}' may have committed before the client observed the failure. Do not retry or delete either index until the aliases and both physical indexes have been inspected manually.";
            }

            if (!cutoverCompleted && !topologyUncertain && !cleanupUnsafe)
            {
                try
                {
                    await CleanupAsync(sourceIndex, targetIndex, targetCreated, sourceBlockAdded).AnyContext();
                }
                catch (Exception cleanupException)
                {
                    throw new RepositoryException(
                        $"Compatibility upgrade for '{sourceIndex}' failed and automatic cleanup did not complete. Inspect the source write block and destination index '{targetIndex}' before retrying.",
                        new AggregateException(upgradeException, cleanupException));
                }
            }

            if (uncertainCutoverMessage is not null)
            {
                if (upgradeException is OperationCanceledException)
                    throw new OperationCanceledException(uncertainCutoverMessage, upgradeException, cancellationToken);

                throw new RepositoryException(uncertainCutoverMessage, upgradeException);
            }

            if (cleanupUnsafe && upgradeException.GetBaseException() is OperationCanceledException)
                throw new OperationCanceledException(upgradeException.Message, upgradeException, cancellationToken);

            throw;
        }
    }

    private async Task<SourceIndexState> GetSourceStateAsync(string sourceIndex, CancellationToken cancellationToken)
    {
        var response = await _client.Indices.GetAsync((Indices)sourceIndex,
            d => d.Features(Feature.Aliases, Feature.Mappings, Feature.Settings).IncludeDefaults(false), cancellationToken).AnyContext();
        _logger.LogRequest(response);

        if (!response.IsValidResponse)
            throw new RepositoryException(response.GetErrorMessage($"Unable to read compatibility source index '{sourceIndex}'."), response.OriginalException());

        if (response.Indices is null || response.Indices.Count is not 1 || !response.Indices.TryGetValue(sourceIndex, out var state) || state is null)
            throw new RepositoryException($"Compatibility source '{sourceIndex}' must identify exactly one concrete index.");

        var settings = state.Settings?.Index ?? state.Settings ?? new IndexSettings();
        var explicitSettings = await GetExplicitSettingsAsync(sourceIndex, cancellationToken).AnyContext();
        return new SourceIndexState(
            sourceIndex,
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

    private static void ValidateSource(SourceIndexState source)
    {
        if (source.Name.StartsWith(".", StringComparison.Ordinal))
            throw new RepositoryException($"System or restricted index '{source.Name}' is not supported by the Foundatio compatibility upgrader.");

        if (!String.IsNullOrEmpty(source.DataStream))
            throw new RepositoryException($"Data stream backing index '{source.Name}' is not supported by the Foundatio compatibility upgrader.");

        if (source.SourceEnabled is false)
            throw new RepositoryException($"Index '{source.Name}' has _source disabled and cannot be reindexed.");

        if (source.IsClosed)
            throw new RepositoryException($"Index '{source.Name}' is closed and must be opened before using the Foundatio compatibility upgrader.");

        if (source.Aliases.ContainsKey(OwnershipAlias))
            throw new RepositoryException($"Index '{source.Name}' uses the reserved compatibility ownership alias '{OwnershipAlias}'. Remove or rename that alias before using the compatibility upgrader.");

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
        _logger.LogRequest(response);

        if (response.ApiCallDetails.HasSuccessfulStatusCode && response.Exists)
            throw new RepositoryException($"Compatibility destination index '{targetIndex}' already exists. Inspect it and remove it only after confirming that it is an unaliased artifact from an interrupted attempt.");

        if (!response.ApiCallDetails.HasSuccessfulStatusCode && response.ApiCallDetails.HttpStatusCode is not 404)
            throw new RepositoryException(response.GetErrorMessage($"Unable to determine whether compatibility destination index '{targetIndex}' exists."), response.OriginalException());
    }

    private async Task AddWriteBlockAsync(string sourceIndex, CancellationToken cancellationToken)
    {
        string escapedIndex = Uri.EscapeDataString(sourceIndex);
        var path = new EndpointPath(TransportHttpMethod.PUT, $"/{escapedIndex}/_block/write");
        var response = await _client.Transport.RequestAsync<ElasticsearchStringResponse>(path, null, null, null, cancellationToken).AnyContext();
        if (!response.IsValidResponse || String.IsNullOrEmpty(response.Body))
            throw new RepositoryException($"Unable to add a write block to compatibility source index '{sourceIndex}'. {response.DebugInformation}");

        using var document = JsonDocument.Parse(response.Body);
        if (!IsWriteBlockConfirmed(document.RootElement, sourceIndex))
            throw new RepositoryException($"Elasticsearch did not confirm that all shards of compatibility source index '{sourceIndex}' were write blocked.");
    }

    internal static bool IsWriteBlockConfirmed(JsonElement response, string sourceIndex)
    {
        if (!response.TryGetProperty("acknowledged", out var acknowledged) || acknowledged.ValueKind is not JsonValueKind.True)
            return false;

        if (!response.TryGetProperty("shards_acknowledged", out var shardsAcknowledged) || shardsAcknowledged.ValueKind is not JsonValueKind.True)
            return false;

        if (!response.TryGetProperty("indices", out var indices) || indices.ValueKind is not JsonValueKind.Array || indices.GetArrayLength() is not 1)
            return false;

        var index = indices[0];
        return index.TryGetProperty("name", out var name)
            && String.Equals(name.GetString(), sourceIndex, StringComparison.Ordinal)
            && index.TryGetProperty("blocked", out var blocked)
            && blocked.ValueKind is JsonValueKind.True;
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
            _logger.LogRequest(response);

            // Any unsuccessful outcome leaves uncertainty: a lost response may have committed on the server, and
            // even a definitive error response cannot prove a partial creation attempt left nothing behind.
            // Treat every failed create like an ambiguous reindex start: retain the destination and the source
            // write block until both indexes have been inspected instead of attempting automatic cleanup.
            if (!response.IsValidResponse || !response.Acknowledged || !String.Equals(response.Index, targetIndex, StringComparison.Ordinal))
            {
                throw new ElasticCompatibilityOperationUncertainException(
                    $"The compatibility destination creation outcome for '{sourceIndex}' -> '{targetIndex}' is unknown. Keep the source write blocked and retain the destination until both indexes have been inspected.",
                    response.OriginalException() ?? new RepositoryException(response.GetErrorMessage($"Unable to create compatibility destination index '{targetIndex}' from '{sourceIndex}'.")));
            }
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

    private async Task AddOwnershipAliasAsync(string targetIndex, CancellationToken cancellationToken)
    {
        var response = await _client.Indices.UpdateAliasesAsync(a => a.Actions(actions => actions.Add(add => add
            .Index(targetIndex)
            .Alias(OwnershipAlias)
            .IsHidden(true))), cancellationToken).AnyContext();
        _logger.LogRequest(response);
        if (!response.IsValidResponse || !response.Acknowledged)
            throw new RepositoryException(response.GetErrorMessage($"Unable to mark compatibility destination index '{targetIndex}' for safe recovery."), response.OriginalException());
    }

    private async Task<IReadOnlyDictionary<string, string?>> GetExplicitSettingsAsync(string index, CancellationToken cancellationToken)
    {
        string escapedIndex = Uri.EscapeDataString(index);
        var path = new EndpointPath(TransportHttpMethod.GET, $"/{escapedIndex}/_settings?flat_settings=true&include_defaults=false");
        var response = await _client.Transport.RequestAsync<ElasticsearchStringResponse>(path, null, null, null, cancellationToken).AnyContext();
        if (!response.IsValidResponse || String.IsNullOrEmpty(response.Body))
            throw new RepositoryException($"Unable to read explicit settings for compatibility index '{index}'. {response.DebugInformation}");

        using var document = JsonDocument.Parse(response.Body);
        if (!document.RootElement.TryGetProperty(index, out var indexState)
            || !indexState.TryGetProperty("settings", out var settings)
            || settings.ValueKind is not JsonValueKind.Object)
        {
            throw new RepositoryException($"Elasticsearch did not return explicit settings for compatibility index '{index}'.");
        }

        var result = new Dictionary<string, string?>(StringComparer.Ordinal);
        foreach (var setting in settings.EnumerateObject())
        {
            result[setting.Name] = setting.Value.ValueKind is JsonValueKind.String
                ? setting.Value.GetString()
                : setting.Value.GetRawText();
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
        _logger.LogRequest(sourceCount);
        if (!sourceCount.IsValidResponse || !ShardsSucceeded(sourceCount.Shards))
            throw new RepositoryException(sourceCount.GetErrorMessage($"Unable to count compatibility source index '{sourceIndex}'."), sourceCount.OriginalException());

        var targetCount = await _client.CountAsync<object>(d => d.Indices(targetIndex), cancellationToken).AnyContext();
        _logger.LogRequest(targetCount);
        if (!targetCount.IsValidResponse || !ShardsSucceeded(targetCount.Shards))
            throw new RepositoryException(targetCount.GetErrorMessage($"Unable to count compatibility destination index '{targetIndex}'."), targetCount.OriginalException());

        if (sourceCount.Count != targetCount.Count || sourceCount.Count != reindexResult.Total || targetCount.Count != reindexResult.Created)
        {
            throw new RepositoryException(
                $"Compatibility reindex count mismatch for '{sourceIndex}' -> '{targetIndex}'. " +
                $"Source: {sourceCount.Count}, Target: {targetCount.Count}, Reindex total: {reindexResult.Total}, Created: {reindexResult.Created}.");
        }
    }

    private async Task RestoreTargetSettingsAsync(string targetIndex, IndexSettings sourceSettings, CancellationToken cancellationToken)
    {
        var settings = new Dictionary<string, object?>
        {
            ["index.number_of_replicas"] = GetValue(sourceSettings.NumberOfReplicas),
            ["index.refresh_interval"] = sourceSettings.RefreshInterval?.ToString(),
            ["index.default_pipeline"] = sourceSettings.DefaultPipeline,
            ["index.final_pipeline"] = sourceSettings.FinalPipeline
        };

        settings["index.blocks.write"] = true;

        string body = JsonSerializer.Serialize(settings);
        string escapedIndex = Uri.EscapeDataString(targetIndex);
        var path = new EndpointPath(TransportHttpMethod.PUT, $"/{escapedIndex}/_settings");
        var response = await _client.Transport.RequestAsync<ElasticsearchStringResponse>(path, PostData.String(body), null, null, cancellationToken).AnyContext();
        EnsureAcknowledged(response, $"Unable to restore settings on compatibility destination index '{targetIndex}'.");
    }

    private async Task RemoveWriteBlockAsync(string targetIndex, CancellationToken cancellationToken)
    {
        var response = await _client.Indices.PutSettingsAsync(targetIndex,
            d => d.Settings(s => s.Blocks(b => b.Write(false))), cancellationToken).AnyContext();
        _logger.LogRequest(response);
        if (!response.IsValidResponse || !response.Acknowledged)
            throw new RepositoryException(response.GetErrorMessage($"Compatibility cutover completed, but the write block could not be removed from destination index '{targetIndex}'. The source was replaced successfully; inspect and unblock the destination before resuming writes."), response.OriginalException());
    }

    private async Task WaitForTargetHealthAsync(string targetIndex, CancellationToken cancellationToken)
    {
        var response = await _client.Cluster.HealthAsync(d => d
            .Indices(targetIndex)
            .WaitForStatus(HealthStatus.Yellow)
            .WaitForNoInitializingShards()
            .WaitForNoRelocatingShards()
            .Timeout("30s"), cancellationToken).AnyContext();
        _logger.LogRequest(response);
        if (!response.IsValidResponse || response.TimedOut || response.Status is HealthStatus.Red)
            throw new RepositoryException(response.GetErrorMessage($"Compatibility destination index '{targetIndex}' did not make all primary shards available after restoring replicas. The source remains intact and write blocked."), response.OriginalException());
    }

    private static IReadOnlyDictionary<string, Alias> CreateAliasActions(string logicalIndexName, SourceIndexState source, string targetIndex, out List<IndexUpdateAliasesAction> actions)
    {
        actions = new List<IndexUpdateAliasesAction>(source.Aliases.Count + 2);
        var expectedAliases = new Dictionary<string, Alias>(source.Aliases, StringComparer.Ordinal);

        foreach (var alias in source.Aliases)
        {
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
                || !String.Equals(
                    _client.ElasticsearchClientSettings.RequestResponseSerializer.SerializeToString(expectedAlias.Value.Filter),
                    _client.ElasticsearchClientSettings.RequestResponseSerializer.SerializeToString(actualAlias.Filter),
                    StringComparison.Ordinal))
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
        _logger.LogRequest(response);
        if (!response.IsValidResponse && response.ElasticsearchServerError?.Status is not 404)
            return CutoverTopology.Uncertain;

        bool sourceExists = response.Indices?.ContainsKey(sourceIndex) is true;
        bool targetExists = response.Indices?.ContainsKey(targetIndex) is true;
        IReadOnlyDictionary<string, Alias> targetAliases = response.Indices is not null && response.Indices.TryGetValue(targetIndex, out var targetState)
            ? targetState.Aliases ?? new Dictionary<string, Alias>()
            : new Dictionary<string, Alias>();

        if (!sourceExists && targetExists && AliasDefinitionsMatch(expectedAliases, targetAliases))
            return CutoverTopology.Completed;

        if (sourceExists && targetExists && HasOwnershipAlias(targetAliases) && targetAliases.Count is 1)
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

    private async Task CleanupAsync(string sourceIndex, string targetIndex, bool targetCreated, bool sourceBlockAdded)
    {
        using var cleanupCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var failures = new List<Exception>();

        if (targetCreated)
        {
            try
            {
                var deleteResponse = await _client.Indices.DeleteAsync(targetIndex, d => d.IgnoreUnavailable(), cleanupCancellation.Token).AnyContext();
                if (deleteResponse.IsValidResponse || deleteResponse.ElasticsearchServerError?.Status is 404)
                    _logger.LogRequest(deleteResponse);
                else
                {
                    _logger.LogErrorRequest(deleteResponse, "Failed to remove compatibility destination index {TargetIndex} during cleanup", targetIndex);
                    failures.Add(new RepositoryException(deleteResponse.GetErrorMessage($"Failed to remove compatibility destination index '{targetIndex}' during cleanup."), deleteResponse.OriginalException()));
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Exception removing compatibility destination index {TargetIndex} during cleanup", targetIndex);
                failures.Add(ex);
            }
        }

        if (sourceBlockAdded)
        {
            try
            {
                var unblockResponse = await _client.Indices.PutSettingsAsync(sourceIndex,
                    d => d.Settings(s => s.Blocks(b => b.Write(false))), cleanupCancellation.Token).AnyContext();
                if (unblockResponse.IsValidResponse && unblockResponse.Acknowledged)
                    _logger.LogRequest(unblockResponse);
                else if (unblockResponse.ElasticsearchServerError?.Status is not 404)
                {
                    _logger.LogErrorRequest(unblockResponse, "Failed to remove write block from compatibility source index {SourceIndex} during cleanup", sourceIndex);
                    failures.Add(new RepositoryException(unblockResponse.GetErrorMessage($"Failed to remove the write block from compatibility source index '{sourceIndex}' during cleanup."), unblockResponse.OriginalException()));
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Exception removing write block from compatibility source index {SourceIndex} during cleanup", sourceIndex);
                failures.Add(ex);
            }
        }

        if (failures.Count > 0)
            throw new AggregateException(failures);
    }

    private async Task RefreshAsync(string index, CancellationToken cancellationToken)
    {
        var response = await _client.Indices.RefreshAsync((Indices)index, cancellationToken).AnyContext();
        _logger.LogRequest(response);
        if (!response.IsValidResponse || !ShardsSucceeded(response.Shards))
            throw new RepositoryException(response.GetErrorMessage($"Unable to refresh compatibility index '{index}'."), response.OriginalException());
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

    private static void EnsureAcknowledged(ElasticsearchStringResponse response, string message)
    {
        if (!response.IsValidResponse || String.IsNullOrEmpty(response.Body))
            throw new RepositoryException($"{message} {response.DebugInformation}");

        using var document = JsonDocument.Parse(response.Body);
        if (!document.RootElement.TryGetProperty("acknowledged", out var acknowledged) || !acknowledged.GetBoolean())
            throw new RepositoryException(message);
    }

    private static bool ReadBoolean(Union<bool, string>? value)
    {
        return value?.Match(first => first, second => Boolean.TryParse(second, out bool result) && result) is true;
    }

    private static object? GetValue(Union<int, string>? value)
    {
        return value?.Match<object?>(first => first, second => second);
    }

    internal static bool HasOwnershipAlias(IReadOnlyDictionary<string, Alias>? aliases)
    {
        if (aliases is null || !aliases.TryGetValue(OwnershipAlias, out var alias))
            return false;

        return alias.IsHidden is true
            && alias.IsWriteIndex is null
            && alias.Filter is null
            && alias.IndexRouting is null
            && alias.Routing is null
            && alias.SearchRouting is null;
    }

    private static string CreateRestorableSettingsSignature(IndexSettings settings)
    {
        return String.Join('\n',
            GetValue(settings.NumberOfReplicas)?.ToString(),
            settings.RefreshInterval?.ToString(),
            settings.DefaultPipeline,
            settings.FinalPipeline);
    }

    private sealed record SourceIndexState(
        string Name,
        IReadOnlyDictionary<string, Alias> Aliases,
        bool WasWriteBlocked,
        bool IsClosed,
        bool IsHidden,
        IndexSettings Settings,
        string? DataStream,
        bool? SourceEnabled,
        string MappingSignature,
        string RestorableSettingsSignature,
        IReadOnlyDictionary<string, string?> ExplicitSettings);

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
