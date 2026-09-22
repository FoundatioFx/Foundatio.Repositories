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
/// Holds an Elasticsearch write block on a single index while a copy is reconciled.
/// </summary>
/// <remarks>
/// <para>
/// Reads are unaffected while application writes are rejected with <c>403 cluster_block_exception</c>.
/// The dedicated <c>PUT /{index}/_block/write</c> API waits for in-flight writes on every shard;
/// setting <c>index.blocks.write</c> directly does not provide that guarantee.
/// </para>
/// <para>
/// The prior block state must be readable before acquisition. A pre-existing block is never removed.
/// If applying or confirming a new block fails, acquisition attempts to release it before propagating
/// the original exception. Cleanup cannot guarantee recovery after process termination or an unavailable
/// cluster: the index setting persists independently of this instance.
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
    public static async Task<IndexWriteBlock> ApplyAsync(ElasticsearchClient client, string index, ILogger logger, TimeSpan releaseTimeout, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrEmpty(index);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(releaseTimeout, TimeSpan.Zero);

        bool wasAlreadyBlocked = await IsWriteBlockedAsync(client, index, cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();

        // The request may apply the setting even when its response is lost or cannot be validated.
        // Establish cleanup responsibility before dispatch, not after acknowledgement.
        var block = new IndexWriteBlock(client, index, wasAlreadyBlocked, releaseTimeout, logger);
        try
        {
            var response = await client.Transport
                .RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.PUT, $"/{Uri.EscapeDataString(index)}/_block/write", PostData.Empty, cancellationToken)
                .AnyContext();

            int? statusCode = response.ApiCallDetails?.HttpStatusCode;
            if (response.ApiCallDetails?.HasSuccessfulStatusCode != true)
                throw new RepositoryException($"Error blocking writes to index {index}: the request failed with status {statusCode?.ToString() ?? "unknown"}.");

            EnsureBlockConfirmed(index, response.Body);
            logger.LogInformation("Blocked writes to index {Index} for the duration of the reindex.", index);
            return block;
        }
        catch
        {
            await block.DisposeAsync().AnyContext();
            throw;
        }
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
