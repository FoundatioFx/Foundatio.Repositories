using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// Holds an Elasticsearch write block on a single index while a copy is reconciled.
/// </summary>
/// <remarks>
/// <para>
/// Reads are unaffected while application writes are rejected with <c>403 cluster_block_exception</c>.
/// The dedicated <c>PUT /{index}/_block/write</c> API waits for in-flight writes on every shard;
/// setting <c>index.blocks.write</c> directly does not provide that guarantee.
/// </para>
/// <para>
/// The prior block state must be readable before acquisition. Operator-owned blocks are never removed.
/// Reindex work items record their ownership durably before applying a new block and confirm it after
/// shard acknowledgement. A retry may recover a confirmed block for the same migration and source UUID.
/// Ambiguous acquisition or mismatched ownership requires operator review; a new request is not proof
/// of ownership. Operators must coordinate block changes with the migration lock.
/// </para>
/// <para>
/// <see cref="ReleaseAsync"/> surfaces release failures on the success path. <see cref="DisposeAsync"/>
/// logs them without replacing an exception already unwinding through a <c>finally</c>. Both use an
/// independent bounded timeout, so caller cancellation cannot abandon cleanup or make it unbounded.
/// </para>
/// </remarks>
internal sealed class IndexWriteBlock : IAsyncDisposable
{
    private readonly ElasticsearchClient _client;
    private readonly ILogger _logger;
    private readonly bool _wasAlreadyBlocked;
    private readonly TimeSpan _releaseTimeout;
    private bool _releaseAttempted;
    private string? _ownershipId;
    private string? _sourceUuid;
    private string? _ownershipToken;

    private static readonly TimeSpan DefaultReleaseTimeout = TimeSpan.FromSeconds(30);

    private IndexWriteBlock(ElasticsearchClient client, string index, bool wasAlreadyBlocked, TimeSpan releaseTimeout, ILogger logger)
    {
        _client = client;
        Index = index;
        _wasAlreadyBlocked = wasAlreadyBlocked;
        _releaseTimeout = releaseTimeout;
        _logger = logger;
    }

    /// <summary>The exact physical index whose writes are blocked.</summary>
    public string Index { get; }

    /// <summary>Blocks writes to the index and requires acknowledgement from every shard.</summary>
    public static async Task<IndexWriteBlock> ApplyAsync(ElasticsearchClient client, string index, ILogger logger, CancellationToken cancellationToken = default)
        => await ApplyAsync(client, index, logger, DefaultReleaseTimeout, cancellationToken).AnyContext();

    /// <summary>Blocks writes to the index and requires acknowledgement from every shard.</summary>
    /// <param name="client">Client used for block and release requests.</param>
    /// <param name="index">The exact physical index to block.</param>
    /// <param name="logger">Logger for acquisition and release outcomes.</param>
    /// <param name="releaseTimeout">Independent timeout for mandatory cleanup.</param>
    /// <param name="cancellationToken">Cancels acquisition, but not cleanup.</param>
    /// <exception cref="RepositoryException">
    /// The prior state could not be established, or the server did not confirm the block.
    /// </exception>
    public static Task<IndexWriteBlock> ApplyAsync(ElasticsearchClient client, string index, ILogger logger, TimeSpan releaseTimeout, CancellationToken cancellationToken = default)
        => ApplyCoreAsync(client, index, logger, releaseTimeout, null, cancellationToken);

    /// <summary>Acquires a source block with durable, generation-bound migration ownership.</summary>
    public static Task<IndexWriteBlock> ApplyAsync(ElasticsearchClient client, ReindexWorkItem workItem, ILogger logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(workItem);
        return ApplyCoreAsync(client, workItem.OldIndex, logger, DefaultReleaseTimeout, workItem, cancellationToken);
    }

    private static async Task<IndexWriteBlock> ApplyCoreAsync(ElasticsearchClient client, string index, ILogger logger, TimeSpan releaseTimeout, ReindexWorkItem? workItem, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrEmpty(index);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(releaseTimeout, TimeSpan.Zero);

        bool wasAlreadyBlocked = await IsWriteBlockedAsync(client, index, cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        string? ownershipId = null;
        ReindexSafetyState.Entry? ownership = null;
        if (workItem is not null)
        {
            string existingId = ReindexSafetyState.GetId("block", index);
            if (await ReindexSafetyState.ReadAsync(client, existingId, cancellationToken).AnyContext() is not null)
                throw new ReindexCompletionUnknownException(workItem.Alias, index, workItem.NewIndex,
                    "a prior migration owns the source block; recover that ownership before acquiring another block");
        }
        if (workItem is not null && !wasAlreadyBlocked)
        {
            string uuid = await ReindexSafetyState.ReadIndexUuidAsync(client, index, cancellationToken).AnyContext();
            ownershipId = ReindexSafetyState.GetId("block", index);
            ownership = new ReindexSafetyState.Entry("block", index, workItem.NewIndex, workItem.Alias, uuid, Phase: "applying");
            await ReindexSafetyState.WriteAsync(client, ownershipId, ownership, true, cancellationToken).AnyContext();
        }

        // The request may apply the setting even when its response is lost or cannot be validated.
        // Establish cleanup responsibility before dispatch, not after acknowledgement.
        var block = new IndexWriteBlock(client, index, wasAlreadyBlocked, releaseTimeout, logger)
        {
            _ownershipId = ownershipId,
            _sourceUuid = ownership?.SourceUuid,
            _ownershipToken = ownership?.OwnerToken
        };
        try
        {
            var response = await client.Transport
                .RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.PUT, $"/{Uri.EscapeDataString(index)}/_block/write", PostData.Empty, cancellationToken)
                .AnyContext();

            int? statusCode = response.ApiCallDetails?.HttpStatusCode;
            if (response.ApiCallDetails?.HasSuccessfulStatusCode != true)
                throw new RepositoryException($"Error blocking writes to index {index}: the request failed with status {statusCode?.ToString() ?? "unknown"}.");

            cancellationToken.ThrowIfCancellationRequested();
            EnsureBlockConfirmed(index, response.Body);
            if (ownership is not null)
                await ReindexSafetyState.WriteAsync(client, ownershipId!, ownership with { Phase = "applied" }, false, cancellationToken).AnyContext();
            logger.LogInformation("Blocked writes to index {Index} for final reconciliation and cutover.", index);
            return block;
        }
        catch
        {
            await block.DisposeAsync().AnyContext();
            throw;
        }
    }

    /// <summary>Recovers a confirmed block owned by this migration before retrying its first pass.</summary>
    /// <remarks>An unconfirmed acquisition, another migration, or a recreated index requires operator review.</remarks>
    internal static async Task RecoverAsync(ElasticsearchClient client, ReindexWorkItem workItem, ILogger logger, CancellationToken cancellationToken)
    {
        string id = ReindexSafetyState.GetId("block", workItem.OldIndex);
        var entry = await ReindexSafetyState.ReadAsync(client, id, cancellationToken).AnyContext();
        if (entry is null)
            return;
        string uuid = await ReindexSafetyState.ReadIndexUuidAsync(client, workItem.OldIndex, cancellationToken).AnyContext();
        if (entry.Kind is not "block" || entry.Source != workItem.OldIndex || entry.Destination != workItem.NewIndex
            || entry.Alias != workItem.Alias || entry.SourceUuid != uuid)
            throw new ReindexCompletionUnknownException(workItem.Alias, workItem.OldIndex, workItem.NewIndex,
                "a source block ownership record belongs to another migration or index generation; no block was changed");
        bool blocked = await IsWriteBlockedAsync(client, workItem.OldIndex, cancellationToken).AnyContext();
        if (!blocked)
        {
            await ReindexSafetyState.DeleteAsync(client, id, entry.OwnerToken, cancellationToken).AnyContext();
            return;
        }
        if (entry.Phase is not "applied")
            throw new ReindexCompletionUnknownException(workItem.Alias, workItem.OldIndex, workItem.NewIndex,
                "source block acquisition was not confirmed durably; inspect its ownership before clearing the block");
        await using var block = new IndexWriteBlock(client, workItem.OldIndex, false, DefaultReleaseTimeout, logger)
        {
            _ownershipId = id,
            _sourceUuid = uuid,
            _ownershipToken = entry.OwnerToken
        };
        await block.ReleaseAsync().AnyContext();
    }

    private static void EnsureBlockConfirmed(string index, string? body)
    {
        if (String.IsNullOrEmpty(body))
            throw new RepositoryException($"Error blocking writes to index {index}: the server returned an empty response, so the block could not be confirmed.");

        bool acknowledged;
        bool shardsAcknowledged;
        bool blocked;
        try
        {
            using var document = JsonDocument.Parse(body);
            var root = document.RootElement;
            acknowledged = HasTrueProperty(root, "acknowledged");
            shardsAcknowledged = HasTrueProperty(root, "shards_acknowledged");
            blocked = TryReadIndexBlocked(root, index);
        }
        catch (JsonException ex)
        {
            throw new RepositoryException($"Error blocking writes to index {index}: the block response could not be parsed, so the block could not be confirmed.", ex);
        }

        if (!acknowledged || !shardsAcknowledged || !blocked)
            throw new RepositoryException(
                $"Error blocking writes to index {index}: the server did not confirm the block (acknowledged: {acknowledged}, shards_acknowledged: {shardsAcknowledged}, blocked: {blocked}). Refusing to continue, because writes may still be accepted.");
    }

    private static bool HasTrueProperty(JsonElement element, string property)
        => element.ValueKind is JsonValueKind.Object
            && element.TryGetProperty(property, out var value)
            && value.ValueKind is JsonValueKind.True;

    private static bool TryReadIndexBlocked(JsonElement root, string index)
    {
        if (root.ValueKind is not JsonValueKind.Object
            || !root.TryGetProperty("indices", out var indices)
            || indices.ValueKind is not JsonValueKind.Array
            || indices.GetArrayLength() is not 1)
            return false;

        var entry = indices[0];
        return entry.ValueKind is JsonValueKind.Object
            && entry.TryGetProperty("name", out var name)
            && name.ValueKind is JsonValueKind.String
            && String.Equals(name.GetString(), index, StringComparison.Ordinal)
            && HasTrueProperty(entry, "blocked");
    }

    private static async Task<bool> IsWriteBlockedAsync(ElasticsearchClient client, string index, CancellationToken cancellationToken)
    {
        var response = await client.Transport
            .RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.GET, $"/{Uri.EscapeDataString(index)}/_settings", cancellationToken)
            .AnyContext();

        cancellationToken.ThrowIfCancellationRequested();
        if (response.ApiCallDetails?.HasSuccessfulStatusCode != true || String.IsNullOrEmpty(response.Body))
            throw new RepositoryException($"Could not read the prior write-block state of index {index}; refusing to change a block whose ownership is unknown.");

        try
        {
            using var document = JsonDocument.Parse(response.Body);
            var root = document.RootElement;
            if (root.ValueKind is not JsonValueKind.Object
                || !root.TryGetProperty(index, out var indexEntry)
                || indexEntry.ValueKind is not JsonValueKind.Object
                || !indexEntry.TryGetProperty("settings", out var settings)
                || settings.ValueKind is not JsonValueKind.Object
                || !settings.TryGetProperty("index", out var indexSettings)
                || indexSettings.ValueKind is not JsonValueKind.Object)
                throw new RepositoryException($"The settings response did not contain the exact index {index}; its prior write-block state is unknown.");

            if (!indexSettings.TryGetProperty("blocks", out var blocks))
                return false;

            if (blocks.ValueKind is not JsonValueKind.Object)
                throw new RepositoryException($"The block settings for index {index} could not be interpreted; refusing to assume the index is unblocked.");

            if (!blocks.TryGetProperty("write", out var write))
                return false;

            return write.ValueKind switch
            {
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.String when Boolean.TryParse(write.GetString(), out bool value) => value,
                _ => throw new RepositoryException($"The write-block setting for index {index} could not be interpreted; refusing to assume the index is unblocked.")
            };
        }
        catch (JsonException ex)
        {
            throw new RepositoryException($"Could not parse the prior write-block state of index {index}; refusing to change a block whose ownership is unknown.", ex);
        }
    }

    /// <summary>Removes a block acquired by this instance, with an independent bounded timeout.</summary>
    /// <remarks>
    /// A pre-existing block is preserved. At most one release attempt is made, including when disposal
    /// follows an explicit release. An unconfirmed release requires operator inspection.
    /// </remarks>
    /// <exception cref="RepositoryException">Removal of the block could not be confirmed.</exception>
    public async Task ReleaseAsync()
    {
        if (!TryBeginRelease())
            return;

        var (succeeded, statusCode, exception) = await TryRemoveBlockAsync().AnyContext();
        if (succeeded)
        {
            _logger.LogInformation("Removed the write block from index {Index}.", Index);
            return;
        }

        LogReleaseFailure(statusCode, exception);
        throw new RepositoryException(
            $"Could not confirm removal of the write block from index {Index} (status {statusCode?.ToString() ?? "unknown"}). Inspect index.blocks.write; it must be unblocked manually if the block remains.",
            exception);
    }

    /// <summary>Attempts mandatory cleanup without replacing an exception from the caller.</summary>
    public async ValueTask DisposeAsync()
    {
        if (!TryBeginRelease())
            return;

        try
        {
            var (succeeded, statusCode, exception) = await TryRemoveBlockAsync().AnyContext();
            if (succeeded)
            {
                _logger.LogInformation("Removed the write block from index {Index}.", Index);
                return;
            }

            LogReleaseFailure(statusCode, exception);
        }
        catch (Exception ex)
        {
            LogReleaseFailure(null, ex);
        }
    }

    private bool TryBeginRelease()
    {
        if (_releaseAttempted)
            return false;

        _releaseAttempted = true;
        if (_wasAlreadyBlocked)
        {
            _logger.LogInformation("Leaving the write block on index {Index} in place because it was already blocked before the reindex started.", Index);
            return false;
        }

        return true;
    }

    private async Task<(bool Succeeded, int? StatusCode, Exception? Exception)> TryRemoveBlockAsync()
    {
        try
        {
            using var timeout = new CancellationTokenSource(_releaseTimeout);
            if (_ownershipId is not null)
            {
                var ownership = await ReindexSafetyState.ReadAsync(_client, _ownershipId, timeout.Token).AnyContext();
                if (ownership?.OwnerToken != _ownershipToken)
                    throw new RepositoryException("Source block ownership changed; refusing to clear another attempt's block.");
            }
            if (_sourceUuid is not null)
            {
                string uuid = await ReindexSafetyState.ReadIndexUuidAsync(_client, Index, timeout.Token).AnyContext();
                if (!String.Equals(uuid, _sourceUuid, StringComparison.Ordinal))
                    throw new RepositoryException("Source index generation changed; refusing to clear an unrelated write block.");
            }
            var response = await _client.Transport
                .RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.PUT, $"/{Uri.EscapeDataString(Index)}/_settings",
                    PostData.String("""{"index.blocks.write":null}"""), timeout.Token)
                .AnyContext();

            bool acknowledged = false;
            if (response.ApiCallDetails?.HasSuccessfulStatusCode == true && !String.IsNullOrEmpty(response.Body))
            {
                using var document = JsonDocument.Parse(response.Body);
                acknowledged = HasTrueProperty(document.RootElement, "acknowledged");
            }

            if (acknowledged && _ownershipId is not null)
            {
                try
                {
                    await ReindexSafetyState.DeleteAsync(_client, _ownershipId, _ownershipToken!, timeout.Token).AnyContext();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Writes were restored but block ownership record {OwnershipId} could not be removed.", _ownershipId);
                }
            }
            return (acknowledged, response.ApiCallDetails?.HttpStatusCode, response.ApiCallDetails?.OriginalException);
        }
        catch (Exception ex)
        {
            return (false, null, ex);
        }
    }

    private void LogReleaseFailure(int? statusCode, Exception? exception)
    {
        _logger.LogError(exception,
            "Could not confirm removal of the write block from index {Index} (status {StatusCode}). Inspect index.blocks.write and clear it manually if it remains; the index may still reject writes.",
            Index, statusCode);
    }
}
