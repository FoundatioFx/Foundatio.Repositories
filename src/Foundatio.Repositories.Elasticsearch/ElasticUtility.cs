using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch {
    public class ElasticUtility {
        private readonly IElasticClient _client;
        private readonly ILogger _logger;

        public ElasticUtility(IElasticClient client, ILogger logger) {
            _client = client;
            _logger = logger ?? NullLogger.Instance;
        }

        public async Task<bool> SnapshotRepositoryExistsAsync(string repository) {
            var repositoriesResponse = await _client.Snapshot.GetRepositoryAsync(new GetRepositoryRequest(repository)).AnyContext();
            return repositoriesResponse.Repositories.Count > 0;
        }

        public async Task<bool> SnapshotInProgressAsync() {
            var repositoriesResponse = await _client.Snapshot.GetRepositoryAsync().AnyContext();
            if (repositoriesResponse.Repositories.Count == 0)
                return false;

            foreach (string repo in repositoriesResponse.Repositories.Keys) {
                var snapshotsResponse = await _client.Cat.SnapshotsAsync(new CatSnapshotsRequest(repo)).AnyContext();
                foreach (var snapshot in snapshotsResponse.Records) {
                    if (snapshot.Status == "IN_PROGRESS")
                        return true;
                }
            }

            var tasksResponse = await _client.Tasks.ListAsync().AnyContext();
            foreach (var node in tasksResponse.Nodes.Values) {
                foreach (var task in node.Tasks.Values) {
                    if (task.Action.Contains("snapshot"))
                        return true;
                }
            }

            return false;
        }

        public async Task<ICollection<string>> GetSnapshotListAsync(string repository) {
            var snapshotsResponse = await _client.Cat.SnapshotsAsync(new CatSnapshotsRequest(repository)).AnyContext();
            return snapshotsResponse.Records.Select(r => r.Id).ToList();
        }

        public async Task<ICollection<string>> GetIndexListAsync() {
            var indicesResponse = await _client.Cat.IndicesAsync(new CatIndicesRequest()).AnyContext();
            return indicesResponse.Records.Select(r => r.Index).ToList();
        }

        public Task<bool> WaitForTaskAsync(string taskId, TimeSpan? maxWaitTime = null, TimeSpan? waitInterval = null) {
            // check task is completed in loop
            return Task.FromResult(false);
        }

        public Task<bool> WaitForSafeToSnapshotAsync(string repository, TimeSpan? maxWaitTime = null, TimeSpan? waitInterval = null) {
            // check SnapshotInProgressAsync in loop
            return Task.FromResult(false);
        }

        public async Task<bool> CreateSnapshotAsync(CreateSnapshotOptions options) {
            if (String.IsNullOrEmpty(options.Name))
                options.Name = SystemClock.UtcNow.ToString("'" + options.Repository + "-'yyyy-MM-dd-HH-mm");

            var repoExists = await SnapshotRepositoryExistsAsync(options.Repository).AnyContext();
            if (!repoExists)
                throw new RepositoryException();

            var success = await WaitForSafeToSnapshotAsync(options.Repository).AnyContext();
            if (!success)
                throw new RepositoryException();

            var res = await _client.Snapshot.SnapshotAsync(new SnapshotRequest(options.Repository, options.Name) {
                Indices = options.Indices != null ? Indices.Index(options.Indices) : Indices.All,
                WaitForCompletion = false,
                IgnoreUnavailable = options.IgnoreUnavailable,
                IncludeGlobalState = options.IncludeGlobalState
            }).AnyContext();

            // wait for snapshot to be success in loop

            return false;
            // should we use lock provider as well as checking for WaitForSafeToSnapshotAsync?
            // create a new snapshot in the repository with retries
        }

        public Task<bool> DeleteSnapshotsAsync(string repository, ICollection<string> snapshots, int? maxRetries = null, TimeSpan? retryInterval = null) {
            // attempt to delete all indices with retries and wait interval
            return Task.FromResult(true);
        }

        public Task<bool> DeleteIndicesAsync(ICollection<string> indices, int? maxRetries = null, TimeSpan? retryInterval = null) {
            // attempt to delete all indices with retries
            return Task.FromResult(true);
        }
    }

    public delegate DateTime? ExtractDateFunc(string name);

    public class CreateSnapshotOptions {
        public string Repository { get; set; }
        public string Name { get; set; }
        public ICollection<string> Indices { get; set; }
        public bool IgnoreUnavailable { get; set; } = false;
        public bool IncludeGlobalState { get; set; } = true;
    }
}
