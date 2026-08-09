using System;
using System.Collections.Generic;
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
using Foundatio.Serializer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TransportHttpMethod = Elastic.Transport.HttpMethod;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

internal sealed class ElasticIndexCompatibilityUpgrader
{
    private static readonly Version MinimumCreateFromVersion = new(8, 18);
    private readonly ElasticsearchClient _client;
    private readonly ElasticReindexTaskRunner _reindexTaskRunner;
    private readonly ILogger _logger;

    public ElasticIndexCompatibilityUpgrader(ElasticsearchClient client, ITextSerializer serializer, TimeProvider timeProvider, ILogger? logger = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _logger = logger ?? NullLogger.Instance;
        _reindexTaskRunner = new ElasticReindexTaskRunner(client, serializer, timeProvider, _logger);
    }

    public async Task ValidateAsync(Index index, IndexCompatibilityInfo compatibility, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(index);
        ArgumentNullException.ThrowIfNull(compatibility);

        EnsureCreateFromSupported(compatibility.ServerVersion);
        string targetIndex = CompatibilityIndexName.Create(compatibility.Name, compatibility.ServerMajor, index.Name);
        await EnsureTargetDoesNotExistAsync(targetIndex, cancellationToken).AnyContext();
        var sourceState = await GetSourceStateAsync(compatibility.Name, cancellationToken).AnyContext();
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
            ValidateSource(sourceState);

            if (!sourceState.WasWriteBlocked)
            {
                sourceBlockAttempted = true;
                await AddWriteBlockAsync(sourceIndex, cancellationToken).AnyContext();
                sourceBlockAdded = true;
            }

            await RefreshAsync(sourceIndex, cancellationToken).AnyContext();
            await ReportProgressAsync(5, $"Blocked writes to {sourceIndex}").AnyContext();

            await CreateTargetAsync(sourceIndex, targetIndex, cancellationToken).AnyContext();
            targetCreated = true;
            await EnsureTargetHasNoAliasesAsync(targetIndex, cancellationToken).AnyContext();
            await ReportProgressAsync(10, $"Created {targetIndex} from {sourceIndex}").AnyContext();

            var reindexResult = await _reindexTaskRunner.RunCompatibilityReindexAsync(
                sourceIndex,
                targetIndex,
                index.ReindexBatchSize,
                index.ReindexRequestsPerSecond,
                ReportProgressAsync,
                cancellationToken).AnyContext();

            await VerifyDocumentCountsAsync(sourceIndex, targetIndex, reindexResult, cancellationToken).AnyContext();
            await RestoreTargetSettingsAsync(targetIndex, sourceState.Settings, sourceState.WasWriteBlocked, cancellationToken).AnyContext();
            await ReportProgressAsync(92, $"Validated {reindexResult.Total:N0} documents and restored index settings").AnyContext();

            var expectedAliases = CreateAliasActions(index.Name, sourceState, targetIndex, out var aliasActions);
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

            var topology = await GetTopologyAsync(sourceIndex, targetIndex, expectedAliases, cancellationToken).AnyContext();
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

            index.MappingResolver.RefreshMapping();
            await ReportProgressAsync(100, $"Replaced {sourceIndex} with {targetIndex}").AnyContext();
        }
        catch (Exception upgradeException)
        {
            if (sourceBlockAttempted && !sourceBlockAdded)
            {
                const string message = "The request outcome is unknown; inspect the source write block before retrying.";
                if (upgradeException is OperationCanceledException)
                    throw new OperationCanceledException($"Compatibility upgrade for '{sourceIndex}' was canceled while adding its write block. {message}", upgradeException, cancellationToken);

                throw new RepositoryException($"Compatibility upgrade for '{sourceIndex}' failed while adding its write block. {message}", upgradeException);
            }

            string? uncertainCutoverMessage = null;
            if (cutoverAttempted && !cutoverCompleted)
            {
                topologyUncertain = true;
                uncertainCutoverMessage = $"Compatibility cutover for '{sourceIndex}' may have committed before the client observed the failure. Do not retry or delete either index until the aliases and both physical indexes have been inspected manually.";
            }

            if (!cutoverCompleted && !topologyUncertain)
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
        return new SourceIndexState(
            sourceIndex,
            state.Aliases ?? new Dictionary<string, Alias>(),
            settings.Blocks?.Write is true,
            ReadBoolean(settings.Hidden),
            settings,
            state.DataStream,
            state.Mappings?.Source?.Enabled);
    }

    private static void ValidateSource(SourceIndexState source)
    {
        if (source.Name.StartsWith(".", StringComparison.Ordinal))
            throw new RepositoryException($"System or restricted index '{source.Name}' is not supported by the Foundatio compatibility upgrader.");

        if (!String.IsNullOrEmpty(source.DataStream))
            throw new RepositoryException($"Data stream backing index '{source.Name}' is not supported by the Foundatio compatibility upgrader.");

        if (source.SourceEnabled is false)
            throw new RepositoryException($"Index '{source.Name}' has _source disabled and cannot be reindexed.");

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
            throw new RepositoryException($"Compatibility destination index '{targetIndex}' already exists. Inspect it and remove or rename it before retrying.");

        if (!response.ApiCallDetails.HasSuccessfulStatusCode && response.ApiCallDetails.HttpStatusCode is not 404)
            throw new RepositoryException(response.GetErrorMessage($"Unable to determine whether compatibility destination index '{targetIndex}' exists."), response.OriginalException());
    }

    private async Task AddWriteBlockAsync(string sourceIndex, CancellationToken cancellationToken)
    {
        string escapedIndex = Uri.EscapeDataString(sourceIndex);
        var path = new EndpointPath(TransportHttpMethod.PUT, $"/{escapedIndex}/_block/write");
        var response = await _client.Transport.RequestAsync<ElasticsearchStringResponse>(path, null, null, null, cancellationToken).AnyContext();
        EnsureAcknowledged(response, $"Unable to add a write block to compatibility source index '{sourceIndex}'.");
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

        var response = await _client.Indices.CreateFromAsync(request, cancellationToken).AnyContext();
        _logger.LogRequest(response);
        if (!response.IsValidResponse || !response.Acknowledged || !String.Equals(response.Index, targetIndex, StringComparison.Ordinal))
            throw new RepositoryException(response.GetErrorMessage($"Unable to create compatibility destination index '{targetIndex}' from '{sourceIndex}'."), response.OriginalException());
    }

    private async Task EnsureTargetHasNoAliasesAsync(string targetIndex, CancellationToken cancellationToken)
    {
        var response = await _client.Indices.GetAsync((Indices)targetIndex, d => d.LimitToNamesAndAliases(), cancellationToken).AnyContext();
        _logger.LogRequest(response);
        if (!response.IsValidResponse || response.Indices is null || !response.Indices.TryGetValue(targetIndex, out var targetState) || targetState is null)
            throw new RepositoryException(response.GetErrorMessage($"Unable to verify compatibility destination index '{targetIndex}'."), response.OriginalException());

        if (targetState.Aliases is { Count: > 0 })
            throw new RepositoryException($"Compatibility destination index '{targetIndex}' received aliases during creation. Adjust matching index templates before retrying.");
    }

    private async Task VerifyDocumentCountsAsync(
        string sourceIndex,
        string targetIndex,
        ElasticReindexTaskResult reindexResult,
        CancellationToken cancellationToken)
    {
        var sourceCount = await _client.CountAsync<object>(d => d.Indices(sourceIndex), cancellationToken).AnyContext();
        _logger.LogRequest(sourceCount);
        if (!sourceCount.IsValidResponse)
            throw new RepositoryException(sourceCount.GetErrorMessage($"Unable to count compatibility source index '{sourceIndex}'."), sourceCount.OriginalException());

        var targetCount = await _client.CountAsync<object>(d => d.Indices(targetIndex), cancellationToken).AnyContext();
        _logger.LogRequest(targetCount);
        if (!targetCount.IsValidResponse)
            throw new RepositoryException(targetCount.GetErrorMessage($"Unable to count compatibility destination index '{targetIndex}'."), targetCount.OriginalException());

        if (sourceCount.Count != targetCount.Count || sourceCount.Count != reindexResult.Total || targetCount.Count != reindexResult.Created)
        {
            throw new RepositoryException(
                $"Compatibility reindex count mismatch for '{sourceIndex}' -> '{targetIndex}'. " +
                $"Source: {sourceCount.Count}, Target: {targetCount.Count}, Reindex total: {reindexResult.Total}, Created: {reindexResult.Created}.");
        }
    }

    private async Task RestoreTargetSettingsAsync(string targetIndex, IndexSettings sourceSettings, bool preserveWriteBlock, CancellationToken cancellationToken)
    {
        var settings = new Dictionary<string, object?>
        {
            ["index.number_of_replicas"] = GetValue(sourceSettings.NumberOfReplicas),
            ["index.refresh_interval"] = sourceSettings.RefreshInterval?.ToString(),
            ["index.default_pipeline"] = sourceSettings.DefaultPipeline,
            ["index.final_pipeline"] = sourceSettings.FinalPipeline
        };

        if (preserveWriteBlock)
            settings["index.blocks.write"] = true;

        string body = JsonSerializer.Serialize(settings);
        string escapedIndex = Uri.EscapeDataString(targetIndex);
        var path = new EndpointPath(TransportHttpMethod.PUT, $"/{escapedIndex}/_settings");
        var response = await _client.Transport.RequestAsync<ElasticsearchStringResponse>(path, PostData.String(body), null, null, cancellationToken).AnyContext();
        EnsureAcknowledged(response, $"Unable to restore settings on compatibility destination index '{targetIndex}'.");
    }

    private static HashSet<string> CreateAliasActions(string logicalIndexName, SourceIndexState source, string targetIndex, out List<IndexUpdateAliasesAction> actions)
    {
        actions = new List<IndexUpdateAliasesAction>(source.Aliases.Count + 2);
        var expectedAliases = new HashSet<string>(source.Aliases.Keys, StringComparer.Ordinal);

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

        if (String.Equals(source.Name, logicalIndexName, StringComparison.Ordinal) && expectedAliases.Add(logicalIndexName))
        {
            actions.Add(new IndexUpdateAliasesAction
            {
                Add = new AddAction { Index = targetIndex, Alias = logicalIndexName, IsHidden = source.IsHidden }
            });
        }

        return expectedAliases;
    }

    private async Task<CutoverTopology> GetTopologyAsync(
        string sourceIndex,
        string targetIndex,
        HashSet<string> expectedAliases,
        CancellationToken cancellationToken)
    {
        string names = String.Join(',', sourceIndex, targetIndex);
        var response = await _client.Indices.GetAsync(Indices.Parse(names), d => d.LimitToNamesAndAliases().IgnoreUnavailable(), cancellationToken).AnyContext();
        _logger.LogRequest(response);
        if (!response.IsValidResponse && response.ElasticsearchServerError?.Status is not 404)
            return CutoverTopology.Uncertain;

        bool sourceExists = response.Indices?.ContainsKey(sourceIndex) is true;
        bool targetExists = response.Indices?.ContainsKey(targetIndex) is true;
        var targetAliases = response.Indices is not null && response.Indices.TryGetValue(targetIndex, out var targetState)
            ? targetState.Aliases?.Keys.ToHashSet(StringComparer.Ordinal) ?? []
            : [];

        if (!sourceExists && targetExists && targetAliases.SetEquals(expectedAliases))
            return CutoverTopology.Completed;

        if (sourceExists && targetExists && targetAliases.Count is 0)
            return CutoverTopology.NotStarted;

        return CutoverTopology.Uncertain;
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
        if (!response.IsValidResponse)
            throw new RepositoryException(response.GetErrorMessage($"Unable to refresh compatibility source index '{index}'."), response.OriginalException());
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

    private sealed record SourceIndexState(
        string Name,
        IReadOnlyDictionary<string, Alias> Aliases,
        bool WasWriteBlocked,
        bool IsHidden,
        IndexSettings Settings,
        string? DataStream,
        bool? SourceEnabled);

    private enum CutoverTopology
    {
        NotStarted,
        Completed,
        Uncertain
    }
}
