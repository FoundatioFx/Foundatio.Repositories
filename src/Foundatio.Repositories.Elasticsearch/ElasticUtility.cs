using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Elasticsearch;

public class ElasticUtility
{
    private readonly ElasticsearchClient _client;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    public ElasticUtility(ElasticsearchClient client, ILogger logger) : this(client, TimeProvider.System, logger)
    {
    }

    public ElasticUtility(ElasticsearchClient client, TimeProvider timeProvider, ILogger logger)
    {
        _client = client;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger.Instance;
    }

    public async Task<bool> SnapshotRepositoryExistsAsync(string repository)
    {
        var repositoriesResponse = await _client.Snapshot.GetRepositoryAsync(r => r.Name(repository)).AnyContext();
        _logger.LogRequest(repositoriesResponse);
        return repositoriesResponse.IsValidResponse && repositoriesResponse.Repositories.Count() > 0;
    }

    public async Task<bool> SnapshotInProgressAsync(string repository = null)
    {
        if (!String.IsNullOrEmpty(repository))
        {
            var snapshotsResponse = await _client.Snapshot.GetAsync(new Elastic.Clients.Elasticsearch.Snapshot.GetSnapshotRequest(repository, "*")).AnyContext();
            _logger.LogRequest(snapshotsResponse);
            if (snapshotsResponse.IsValidResponse)
            {
                foreach (var snapshot in snapshotsResponse.Snapshots)
                {
                    if (snapshot.State == "IN_PROGRESS")
                        return true;
                }
            }
        }
        else
        {
            var repositoriesResponse = await _client.Snapshot.GetRepositoryAsync().AnyContext();
            _logger.LogRequest(repositoriesResponse);
            if (!repositoriesResponse.IsValidResponse || repositoriesResponse.Repositories.Count() == 0)
                return false;

            foreach (var repo in repositoriesResponse.Repositories)
            {
                var snapshotsResponse = await _client.Snapshot.GetAsync(new Elastic.Clients.Elasticsearch.Snapshot.GetSnapshotRequest(repo.Key, "*")).AnyContext();
                _logger.LogRequest(snapshotsResponse);
                if (snapshotsResponse.IsValidResponse)
                {
                    foreach (var snapshot in snapshotsResponse.Snapshots)
                    {
                        if (snapshot.State == "IN_PROGRESS")
                            return true;
                    }
                }
            }
        }

        var tasksResponse = await _client.Tasks.ListAsync().AnyContext();
        _logger.LogRequest(tasksResponse);
        if (!tasksResponse.IsValidResponse)
        {
            _logger.LogWarning("Failed to list tasks: {Error}", tasksResponse.ElasticsearchServerError);
            return false;
        }

        foreach (var node in tasksResponse.Nodes.Values)
        {
            foreach (var task in node.Tasks.Values)
            {
                if (task.Action.Contains("snapshot"))
                    return true;
            }
        }

        return false;
    }

    public async Task<ICollection<string>> GetSnapshotListAsync(string repository)
    {
        var snapshotsResponse = await _client.Snapshot.GetAsync(new Elastic.Clients.Elasticsearch.Snapshot.GetSnapshotRequest(repository, "*")).AnyContext();
        _logger.LogRequest(snapshotsResponse);
        if (!snapshotsResponse.IsValidResponse)
        {
            _logger.LogWarning("Failed to get snapshot list for {Repository}: {Error}", repository, snapshotsResponse.ElasticsearchServerError);
            return Array.Empty<string>();
        }

        return snapshotsResponse.Snapshots.Select(s => s.Snapshot).ToList();
    }

    public async Task<ICollection<string>> GetIndexListAsync()
    {
        var resolveResponse = await _client.Indices.ResolveIndexAsync("*").AnyContext();
        _logger.LogRequest(resolveResponse);
        if (!resolveResponse.IsValidResponse)
        {
            _logger.LogWarning("Failed to get index list: {Error}", resolveResponse.ElasticsearchServerError);
            return Array.Empty<string>();
        }

        return resolveResponse.Indices.Select(i => i.Name).ToList();
    }

    /// <summary>
    /// Waits for an Elasticsearch task to complete, polling at intervals until completion or timeout.
    /// </summary>
    /// <param name="taskId">The task ID to wait for (format: "nodeId:taskId").</param>
    /// <param name="maxWaitTime">Maximum time to wait before returning false. Defaults to 5 minutes.</param>
    /// <param name="waitInterval">Interval between polls. Defaults to 2 seconds.</param>
    /// <returns>True if the task completed successfully; false if it timed out or failed.</returns>
    public async Task<bool> WaitForTaskAsync(string taskId, TimeSpan? maxWaitTime = null, TimeSpan? waitInterval = null)
    {
        if (String.IsNullOrEmpty(taskId))
            return false;

        var maxWait = maxWaitTime ?? TimeSpan.FromMinutes(5);
        var interval = waitInterval ?? TimeSpan.FromSeconds(2);
        var started = _timeProvider.GetUtcNow();

        while (_timeProvider.GetUtcNow() - started < maxWait)
        {
            var getTaskResponse = await _client.Tasks.GetAsync(taskId).AnyContext();
            _logger.LogRequest(getTaskResponse);

            if (!getTaskResponse.IsValidResponse)
                return false;

            if (getTaskResponse.Completed)
                return true;

            await Task.Delay(interval).AnyContext();
        }

        _logger.LogWarning("Timed out waiting for task {TaskId} after {MaxWaitTime}", taskId, maxWait);
        return false;
    }

    /// <summary>
    /// Waits until no snapshots are in progress, polling at intervals.
    /// </summary>
    /// <param name="repository">The snapshot repository to check.</param>
    /// <param name="maxWaitTime">Maximum time to wait. Defaults to 30 minutes.</param>
    /// <param name="waitInterval">Interval between polls. Defaults to 5 seconds.</param>
    /// <returns>True if safe to snapshot; false if timed out.</returns>
    public async Task<bool> WaitForSafeToSnapshotAsync(string repository, TimeSpan? maxWaitTime = null, TimeSpan? waitInterval = null)
    {
        var maxWait = maxWaitTime ?? TimeSpan.FromMinutes(30);
        var interval = waitInterval ?? TimeSpan.FromSeconds(5);
        var started = _timeProvider.GetUtcNow();

        while (_timeProvider.GetUtcNow() - started < maxWait)
        {
            bool inProgress = await SnapshotInProgressAsync(repository).AnyContext();
            if (!inProgress)
                return true;

            _logger.LogDebug("Snapshot in progress for repository {Repository}; waiting {Interval}...", repository, interval);
            await Task.Delay(interval).AnyContext();
        }

        _logger.LogWarning("Timed out waiting for safe snapshot window after {MaxWaitTime}", maxWait);
        return false;
    }

    public async Task<bool> CreateSnapshotAsync(CreateSnapshotOptions options)
    {
        if (String.IsNullOrEmpty(options.Name))
            options.Name = _timeProvider.GetUtcNow().UtcDateTime.ToString("'" + options.Repository + "-'yyyy-MM-dd-HH-mm");

        bool repoExists = await SnapshotRepositoryExistsAsync(options.Repository).AnyContext();
        if (!repoExists)
            throw new RepositoryException($"Snapshot repository '{options.Repository}' does not exist.");

        bool safe = await WaitForSafeToSnapshotAsync(options.Repository).AnyContext();
        if (!safe)
            throw new RepositoryException($"Timed out waiting for a safe window to create snapshot in repository '{options.Repository}'.");

        var snapshotResponse = await _client.Snapshot.CreateAsync(options.Repository, options.Name, s => s
            .Indices(options.Indices != null ? Indices.Parse(String.Join(",", options.Indices)) : Indices.All)
            .WaitForCompletion(false)
            .IgnoreUnavailable(options.IgnoreUnavailable)
            .IncludeGlobalState(options.IncludeGlobalState)
        ).AnyContext();
        _logger.LogRequest(snapshotResponse);

        if (!snapshotResponse.IsValidResponse)
        {
            _logger.LogError("Failed to create snapshot '{SnapshotName}' in repository '{Repository}'", options.Name, options.Repository);
            return false;
        }

        // Wait for the snapshot to complete by polling until it's no longer IN_PROGRESS
        bool completed = await WaitForSafeToSnapshotAsync(options.Repository, maxWaitTime: TimeSpan.FromHours(2)).AnyContext();
        return completed;
    }

    /// <summary>
    /// Deletes the specified snapshots with configurable retries.
    /// </summary>
    /// <param name="repository">The snapshot repository.</param>
    /// <param name="snapshots">The snapshot names to delete.</param>
    /// <param name="maxRetries">Number of retry attempts per snapshot. Defaults to 3.</param>
    /// <param name="retryInterval">Interval between retries. Defaults to 2 seconds.</param>
    /// <returns>True if all snapshots were deleted; false if any deletion failed after retries.</returns>
    public async Task<bool> DeleteSnapshotsAsync(string repository, ICollection<string> snapshots, int? maxRetries = null, TimeSpan? retryInterval = null)
    {
        if (snapshots == null || snapshots.Count == 0)
            return true;

        int retries = maxRetries ?? 3;
        var interval = retryInterval ?? TimeSpan.FromSeconds(2);
        bool allSucceeded = true;

        foreach (var snapshot in snapshots)
        {
            bool deleted = false;
            for (int attempt = 0; attempt <= retries; attempt++)
            {
                var response = await _client.Snapshot.DeleteAsync(repository, snapshot).AnyContext();
                _logger.LogRequest(response);

                if (response.IsValidResponse)
                {
                    deleted = true;
                    break;
                }

                if (attempt < retries)
                {
                    _logger.LogWarning("Failed to delete snapshot '{Snapshot}' (attempt {Attempt}/{Retries}); retrying...", snapshot, attempt + 1, retries);
                    await Task.Delay(interval).AnyContext();
                }
            }

            if (!deleted)
            {
                _logger.LogError("Failed to delete snapshot '{Snapshot}' after {Retries} attempt(s)", snapshot, retries);
                allSucceeded = false;
            }
        }

        return allSucceeded;
    }

    /// <summary>
    /// Deletes the specified indices with configurable retries.
    /// </summary>
    /// <param name="indices">The index names to delete.</param>
    /// <param name="maxRetries">Number of retry attempts. Defaults to 3.</param>
    /// <param name="retryInterval">Interval between retries. Defaults to 2 seconds.</param>
    /// <returns>True if all indices were deleted; false if any deletion failed after retries.</returns>
    public async Task<bool> DeleteIndicesAsync(ICollection<string> indices, int? maxRetries = null, TimeSpan? retryInterval = null)
    {
        if (indices == null || indices.Count == 0)
            return true;

        int retries = maxRetries ?? 3;
        var interval = retryInterval ?? TimeSpan.FromSeconds(2);

        for (int attempt = 0; attempt <= retries; attempt++)
        {
            var response = await _client.Indices.DeleteAsync(Indices.Parse(String.Join(",", indices))).AnyContext();
            _logger.LogRequest(response);

            if (response.IsValidResponse)
                return true;

            if (attempt < retries)
            {
                _logger.LogWarning("Failed to delete indices (attempt {Attempt}/{Retries}); retrying...", attempt + 1, retries);
                await Task.Delay(interval).AnyContext();
            }
        }

        _logger.LogError("Failed to delete indices [{Indices}] after {Retries} attempt(s)", String.Join(", ", indices), retries);
        return false;
    }
}

public delegate DateTime? ExtractDateFunc(string name);

public class CreateSnapshotOptions
{
    public string Repository { get; set; }
    public string Name { get; set; }
    public ICollection<string> Indices { get; set; }
    public bool IgnoreUnavailable { get; set; } = false;
    public bool IncludeGlobalState { get; set; } = true;
}
