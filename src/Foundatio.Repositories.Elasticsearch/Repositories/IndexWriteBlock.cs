using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// Holds an Elasticsearch write block on a single index for as long as the instance is alive, so a copy can be
/// reconciled against a source that cannot change underneath it.
/// </summary>
/// <remarks>
/// <para>
/// Reads are unaffected by the block, which is what makes it usable here: the reindex still scrolls the source
/// while application writes are rejected with <c>403 cluster_block_exception</c>.
/// </para>
/// <para>
/// The block is applied through the dedicated <c>PUT /{index}/_block/write</c> API rather than by setting
/// <c>index.blocks.write</c>. Only the dedicated API waits for every shard to acknowledge, and that cross-shard
/// accounting is the whole guarantee — without it, writes already in flight on a shard could still land after the
/// call returned. The typed client exposes no equivalent, so the request goes through the low-level transport and
/// its response is validated by hand.
/// </para>
/// <para>
/// Release necessarily goes the other way, through settings, because Elasticsearch has no remove-block API. That
/// is safe: clearing <c>index.blocks.write</c> also clears a block that was applied through the dedicated API.
/// </para>
/// <para>
/// A block <b>persists</b> on the index. Any path that leaves the source in place must remove it again, including
/// on failure, which is why this is <see cref="IAsyncDisposable"/> rather than a pair of method calls.
/// </para>
/// <para>
/// Removal comes in two flavours, and the difference matters. <see cref="ReleaseAsync"/> is the deliberate call
/// for the success path: it is an ordered step of the migration, so a failure to unblock is surfaced as an
/// exception. <see cref="DisposeAsync"/> is the backstop for every other path: it makes the same attempt but only
/// logs a failure, because dispose runs from a <c>finally</c> and a throwing <c>finally</c> replaces the exception
/// that caused the unwind — losing the reason the migration failed in order to report a second problem.
/// </para>
/// <para>
/// Neither honours the caller's <see cref="CancellationToken"/>. Removing the block is not optional work that a
/// cancellation should be able to abandon: leaving an index read-only is a worse outcome than any operation this
/// class is used inside. It is still bounded, by an independent timeout, so dispose cannot hang forever against an
/// unresponsive cluster.
/// </para>
/// </remarks>
internal sealed class IndexWriteBlock : IAsyncDisposable
{
    private readonly ElasticsearchClient _client;
    private readonly ILogger _logger;
    private readonly bool _wasAlreadyBlocked;
    private readonly TimeSpan _releaseTimeout;
    private bool _releaseAttempted;

    /// <summary>
    /// Bound on the release request when the caller does not supply one. Matches the independent timeout the
    /// reindexer uses to cancel a server-side task, for the same reason: mandatory cleanup that must not inherit
    /// a cancelled token still has to give up eventually rather than hang.
    /// </summary>
    private static readonly TimeSpan DefaultReleaseTimeout = TimeSpan.FromSeconds(30);

    private IndexWriteBlock(ElasticsearchClient client, string index, bool wasAlreadyBlocked, TimeSpan releaseTimeout, ILogger logger)
    {
        _client = client;
        Index = index;
        _wasAlreadyBlocked = wasAlreadyBlocked;
        _releaseTimeout = releaseTimeout;
        _logger = logger;
    }

    /// <summary>
    /// The index whose writes are blocked.
    /// </summary>
    public string Index { get; }

    /// <summary>
    /// Blocks writes to <paramref name="index"/> and waits for every shard to acknowledge.
    /// </summary>
    /// <exception cref="RepositoryException">
    /// The block could not be applied, or the server did not confirm it across all shards. Callers must treat this
    /// as fatal: an unconfirmed block cannot be relied on to hold writes still.
    /// </exception>
    public static async Task<IndexWriteBlock> ApplyAsync(ElasticsearchClient client, string index, ILogger logger, CancellationToken cancellationToken = default)
        => await ApplyAsync(client, index, logger, DefaultReleaseTimeout, cancellationToken).AnyContext();

    /// <summary>
    /// Blocks writes to <paramref name="index"/> and waits for every shard to acknowledge.
    /// </summary>
    /// <param name="client">Client used to issue the block and release requests.</param>
    /// <param name="index">Index whose writes are blocked.</param>
    /// <param name="logger">Logger for block and release outcomes.</param>
    /// <param name="releaseTimeout">
    /// Bound on the release request. Release never honours the caller's cancellation token, so this is what stops
    /// it hanging against an unresponsive cluster.
    /// </param>
    /// <param name="cancellationToken">Cancels applying the block. Deliberately does not affect releasing it.</param>
    /// <exception cref="RepositoryException">
    /// The block could not be applied, or the server did not confirm it across all shards. Callers must treat this
    /// as fatal: an unconfirmed block cannot be relied on to hold writes still.
    /// </exception>
    public static async Task<IndexWriteBlock> ApplyAsync(ElasticsearchClient client, string index, ILogger logger, TimeSpan releaseTimeout, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrEmpty(index);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(releaseTimeout, TimeSpan.Zero);

        // Whether the caller had already blocked this index for their own reasons. If so, releasing it would be
        // undoing a decision that is not ours to undo, so the block is left exactly as it was found.
        bool wasAlreadyBlocked = await IsWriteBlockedAsync(client, index, logger, cancellationToken).AnyContext();

        var response = await client.Transport
            .RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.PUT, $"/{index}/_block/write", PostData.Empty, cancellationToken)
            .AnyContext();

        int? statusCode = response.ApiCallDetails?.HttpStatusCode;
        if (response.ApiCallDetails?.HasSuccessfulStatusCode != true)
            throw new RepositoryException($"Error blocking writes to index {index}: the request failed with status {statusCode?.ToString() ?? "unknown"}.");

        EnsureBlockConfirmed(index, response.Body);

        logger.LogInformation("Blocked writes to index {Index} for the duration of the reindex.", index);

        return new IndexWriteBlock(client, index, wasAlreadyBlocked, releaseTimeout, logger);
    }

    /// <summary>
    /// Verifies the server confirmed the block on every shard.
    /// </summary>
    /// <remarks>
    /// A 200 is not sufficient. <c>acknowledged</c> alone means the cluster state was updated; only
    /// <c>shards_acknowledged</c> together with the per-index <c>blocked</c> flag means no shard can still accept a
    /// write. Anything less is treated as a failure rather than optimistically accepted.
    /// </remarks>
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

            acknowledged = root.TryGetProperty("acknowledged", out var acknowledgedElement) && acknowledgedElement.ValueKind is JsonValueKind.True;
            shardsAcknowledged = root.TryGetProperty("shards_acknowledged", out var shardsElement) && shardsElement.ValueKind is JsonValueKind.True;
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

    private static bool TryReadIndexBlocked(JsonElement root, string index)
    {
        if (!root.TryGetProperty("indices", out var indices) || indices.ValueKind is not JsonValueKind.Array)
            return false;

        foreach (var entry in indices.EnumerateArray())
        {
            if (!entry.TryGetProperty("name", out var name) || name.GetString() != index)
                continue;

            return entry.TryGetProperty("blocked", out var blockedElement) && blockedElement.ValueKind is JsonValueKind.True;
        }

        return false;
    }

    /// <summary>
    /// Reports whether writes to the index are already blocked.
    /// </summary>
    /// <remarks>
    /// Read through the low-level transport and parsed explicitly because Elasticsearch returns index settings as
    /// <em>strings</em> - <c>"blocks": {"write": "true"}</c>, not a JSON boolean - so binding it to a <c>bool</c>
    /// property depends on coercion this codebase has already been bitten by once (the snake_case task counters).
    /// Getting this wrong would silently lift a block the operator set deliberately, so it is parsed by hand.
    /// </remarks>
    private static async Task<bool> IsWriteBlockedAsync(ElasticsearchClient client, string index, ILogger logger, CancellationToken cancellationToken)
    {
        var response = await client.Transport
            .RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.GET, $"/{index}/_settings", cancellationToken)
            .AnyContext();

        if (response.ApiCallDetails?.HasSuccessfulStatusCode != true || String.IsNullOrEmpty(response.Body))
            return false;

        try
        {
            using var document = JsonDocument.Parse(response.Body);

            foreach (var indexEntry in document.RootElement.EnumerateObject())
            {
                if (indexEntry.Value.TryGetProperty("settings", out var settings)
                    && settings.TryGetProperty("index", out var indexSettings)
                    && indexSettings.TryGetProperty("blocks", out var blocks)
                    && blocks.TryGetProperty("write", out var write)
                    && IsTrue(write))
                {
                    return true;
                }
            }
        }
        catch (JsonException ex)
        {
            // Treated as "not already blocked", which is the conservative answer: it means this instance takes
            // responsibility for releasing the block it applied rather than leaving one behind.
            logger.LogWarning(ex, "Could not determine whether index {Index} was already write blocked.", index);
        }

        return false;
    }

    private static bool IsTrue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.String => String.Equals(element.GetString(), "true", StringComparison.OrdinalIgnoreCase),
            _ => false
        };
    }

    /// <summary>
    /// Removes the write block, restoring normal writes to the index.
    /// </summary>
    /// <remarks>
    /// Use this on the success path, where unblocking is an ordered step of the operation and a failure to unblock
    /// should stop it. Does not honour any caller cancellation - see the class remarks - and is bounded by the
    /// release timeout given to <see cref="ApplyAsync(ElasticsearchClient, string, ILogger, TimeSpan, CancellationToken)"/>.
    /// Safe to call more than once, and safe to call before <see cref="DisposeAsync"/>; the second call is a no-op.
    /// </remarks>
    /// <exception cref="RepositoryException">
    /// The block could not be removed. The index is still rejecting writes and needs manual intervention.
    /// </exception>
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
            $"Error removing the write block from index {Index}: the request failed with status {statusCode?.ToString() ?? "unknown"}. It is still rejecting writes and must be unblocked manually by setting index.blocks.write to null.",
            exception);
    }

    /// <summary>
    /// Backstop removal of the write block for paths that did not call <see cref="ReleaseAsync"/>.
    /// </summary>
    /// <remarks>
    /// Makes the same attempt as <see cref="ReleaseAsync"/> but <b>never throws</b>. Dispose runs from a
    /// <c>finally</c>, and an exception thrown there replaces the one that caused the unwind - so throwing here
    /// would discard the reason the operation failed in order to report that cleanup also failed. The operator needs
    /// both, so this failure is logged at error level and the original exception is left to propagate.
    /// </remarks>
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

    /// <summary>
    /// Claims the single release attempt, so an explicit <see cref="ReleaseAsync"/> followed by the
    /// <c>await using</c> dispose does not issue the request twice.
    /// </summary>
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
        // Release goes through settings rather than a dedicated API because Elasticsearch has no remove-block
        // endpoint. The typed settings descriptor cannot express an explicit JSON null - `Blocks(b => b.Write(null))`
        // serializes to an empty object, which Elasticsearch rejects with "no settings to update" - and null is the
        // only value that *removes* the block rather than setting it to false. Verified against a live cluster.
        //
        // The token is this class's own, never the caller's: unblocking is mandatory cleanup that a cancelled
        // operation must not be able to abandon, but it still has to be bounded so this cannot hang.
        using var timeout = new CancellationTokenSource(_releaseTimeout);

        var response = await _client.Transport
            .RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.PUT, $"/{Index}/_settings",
                PostData.String("""{"index.blocks.write":null}"""), timeout.Token)
            .AnyContext();

        bool succeeded = response.ApiCallDetails?.HasSuccessfulStatusCode == true;
        return (succeeded, response.ApiCallDetails?.HttpStatusCode, response.ApiCallDetails?.OriginalException);
    }

    private void LogReleaseFailure(int? statusCode, Exception? exception)
    {
        _logger.LogError(exception,
            "Failed to remove the write block from index {Index} (status {StatusCode}). It will keep rejecting writes until the block is cleared manually by setting index.blocks.write to null on that index.",
            Index, statusCode);
    }
}
