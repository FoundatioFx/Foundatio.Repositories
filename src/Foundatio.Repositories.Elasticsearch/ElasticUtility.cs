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

    public async Task<bool> SnapshotInProgressAsync()
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

        var tasksResponse = await _client.Tasks.ListAsync().AnyContext();
        _logger.LogRequest(tasksResponse);
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
        return snapshotsResponse.Snapshots.Select(s => s.Snapshot).ToList();
    }

    public async Task<ICollection<string>> GetIndexListAsync()
    {
        var indicesResponse = await _client.Indices.GetAsync(Indices.All).AnyContext();
        _logger.LogRequest(indicesResponse);
        return indicesResponse.Indices.Keys.Select(k => k.ToString()).ToList();
    }

    public Task<bool> WaitForTaskAsync(string taskId, TimeSpan? maxWaitTime = null, TimeSpan? waitInterval = null)
    {
        // check task is completed in loop
        return Task.FromResult(false);
    }

    public Task<bool> WaitForSafeToSnapshotAsync(string repository, TimeSpan? maxWaitTime = null, TimeSpan? waitInterval = null)
    {
        // check SnapshotInProgressAsync in loop
        return Task.FromResult(false);
    }

    public async Task<bool> CreateSnapshotAsync(CreateSnapshotOptions options)
    {
        if (String.IsNullOrEmpty(options.Name))
            options.Name = _timeProvider.GetUtcNow().UtcDateTime.ToString("'" + options.Repository + "-'yyyy-MM-dd-HH-mm");

        bool repoExists = await SnapshotRepositoryExistsAsync(options.Repository).AnyContext();
        if (!repoExists)
            throw new RepositoryException();

        bool success = await WaitForSafeToSnapshotAsync(options.Repository).AnyContext();
        if (!success)
            throw new RepositoryException();

        var snapshotResponse = await _client.Snapshot.CreateAsync(options.Repository, options.Name, s => s
            .Indices(options.Indices != null ? Indices.Parse(String.Join(",", options.Indices)) : Indices.All)
            .WaitForCompletion(false)
            .IgnoreUnavailable(options.IgnoreUnavailable)
            .IncludeGlobalState(options.IncludeGlobalState)
        ).AnyContext();
        _logger.LogRequest(snapshotResponse);

        // TODO: wait for snapshot to be success in loop

        return false;
        // TODO: should we use lock provider as well as checking for WaitForSafeToSnapshotAsync?
        // TODO: create a new snapshot in the repository with retries
    }

    public Task<bool> DeleteSnapshotsAsync(string repository, ICollection<string> snapshots, int? maxRetries = null, TimeSpan? retryInterval = null)
    {
        // TODO: attempt to delete all indices with retries and wait interval
        return Task.FromResult(true);
    }

    public Task<bool> DeleteIndicesAsync(ICollection<string> indices, int? maxRetries = null, TimeSpan? retryInterval = null)
    {
        // TODO: attempt to delete all indices with retries
        return Task.FromResult(true);
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
