using System;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>Prevents a retry from copying while an earlier asynchronous server task can still write.</summary>
internal sealed class ReindexTaskLease : IAsyncDisposable
{
    private readonly ElasticsearchClient _client;
    private readonly ILogger _logger;
    private readonly string _id;
    private ReindexSafetyState.Entry _entry;
    private bool _terminal;

    private ReindexTaskLease(ElasticsearchClient client, ILogger logger, string id, ReindexSafetyState.Entry entry)
    {
        _client = client;
        _logger = logger;
        _id = id;
        _entry = entry;
    }

    public static async Task<ReindexTaskLease> AcquireAsync(ElasticsearchClient client, ReindexWorkItem workItem, ILogger logger, CancellationToken cancellationToken)
    {
        string id = ReindexSafetyState.GetId("task", workItem.NewIndex);
        var previous = await ReindexSafetyState.ReadAsync(client, id, cancellationToken).AnyContext();
        if (previous is not null)
        {
            if (previous.Kind is not "task" || previous.Source != workItem.OldIndex || previous.Destination != workItem.NewIndex || previous.Alias != workItem.Alias)
                throw new RepositoryException("Reindex task ownership does not match this migration.");
            if (String.IsNullOrEmpty(previous.TaskId))
                throw new ReindexCompletionUnknownException(workItem.Alias, workItem.OldIndex, workItem.NewIndex,
                    "an earlier task dispatch has an unknown outcome; verify and stop server tasks before clearing its safety record");
            if (!await StopAsync(client, previous.TaskId, cancellationToken).AnyContext())
                throw new ReindexCompletionUnknownException(workItem.Alias, workItem.OldIndex, workItem.NewIndex,
                    "an earlier server task could not be confirmed stopped; refusing an overlapping copy");
            await ReindexSafetyState.DeleteAsync(client, id, previous.OwnerToken, cancellationToken).AnyContext();
        }
        var entry = new ReindexSafetyState.Entry("task", workItem.OldIndex, workItem.NewIndex, workItem.Alias, String.Empty);
        // Persist dispatch intent BEFORE sending a non-idempotent asynchronous request. Missing task ID on
        // recovery is ambiguous, not evidence that no task started.
        await ReindexSafetyState.WriteAsync(client, id, entry, true, cancellationToken).AnyContext();
        return new ReindexTaskLease(client, logger, id, entry);
    }

    public async Task RecordTaskAsync(string taskId)
    {
        _entry = _entry with { TaskId = taskId };
        using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await ReindexSafetyState.WriteAsync(_client, _id, _entry, false, cleanup.Token).AnyContext();
    }

    public void MarkTerminal() => _terminal = true;

    public async ValueTask DisposeAsync()
    {
        using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try
        {
            if (_terminal || _entry.TaskId is not null && await StopAsync(_client, _entry.TaskId, cleanup.Token).AnyContext())
            {
                await ReindexSafetyState.DeleteAsync(_client, _id, _entry.OwnerToken, cleanup.Token).AnyContext();
                return;
            }
            _logger.LogError("Reindex task termination is unconfirmed. Safety record {SafetyIndex}/{SafetyId} prevents another copy; operator inspection is required.", ReindexSafetyState.Index, _id);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Could not confirm task cleanup; keeping safety record {SafetyIndex}/{SafetyId} to prevent an overlapping retry.", ReindexSafetyState.Index, _id);
        }
    }

    private static async Task<bool> StopAsync(ElasticsearchClient client, string taskId, CancellationToken cancellationToken)
    {
        var status = await client.Tasks.GetAsync(taskId, cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        // A task 404 is not positive termination evidence. Elasticsearch can fall back to stored task
        // results when the owning node is unavailable; a missing result cannot prove the task stopped.
        if (status.IsValidResponse && status.Completed)
            return true;
        if (!status.IsValidResponse)
            return false;
        // A cancellation acknowledgement alone does not mean the task has stopped.
        await client.Tasks.CancelAsync(c => c.TaskId(taskId).WaitForCompletion(true), cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        status = await client.Tasks.GetAsync(taskId, cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        return status.IsValidResponse && status.Completed;
    }
}
