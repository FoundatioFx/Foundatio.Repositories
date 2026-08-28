using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    private sealed class TestDailyIndex : Foundatio.Repositories.Elasticsearch.Configuration.DailyIndex
    {
        public TestDailyIndex(IElasticConfiguration configuration, string name, int version = 1) : base(configuration, name, version) { }

        public DateTime GetIndexDatePublic(string index) => GetIndexDate(index);

        public string GetCompatibilityIndexPatternPublic() => GetCompatibilityIndexPattern();
    }

    private sealed class TestVersionedIndex : VersionedIndex
    {
        public TestVersionedIndex(IElasticConfiguration configuration, string name, int version) : base(configuration, name, version) { }

        public int GetIndexVersionPublic(string name) => GetIndexVersion(name);

        public string GetCompatibilityIndexPatternPublic() => GetCompatibilityIndexPattern();
    }

    private sealed class TestPlainIndex : Index<object>
    {
        public TestPlainIndex(ElasticConfiguration configuration, string name) : base(configuration, name) { }

        public string GetCompatibilityIndexPatternPublic() => GetCompatibilityIndexPattern();
    }

    private sealed class BecomesCompatibleIndex : Index<object>
    {
        public BecomesCompatibleIndex(IElasticConfiguration configuration) : base(configuration, "becomes-compatible") { }

        public int CompatibilityChecks { get; private set; }

        public override Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            CompatibilityChecks++;
            IReadOnlyCollection<IndexCompatibilityInfo> result = CompatibilityChecks is 1
                ?
                [
                    new IndexCompatibilityInfo
                    {
                        Name = Name,
                        CreatedMajor = 8,
                        CreatedVersion = "8.0.0",
                        ServerMajor = 9,
                        ServerVersion = "9.0.0"
                    }
                ]
                : [];

            return Task.FromResult(result);
        }
    }

    private sealed class CanceledCompatibilityIndex : Index<object>
    {
        public CanceledCompatibilityIndex(IElasticConfiguration configuration) : base(configuration, "canceled-compatibility") { }

        public override Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromCanceled<IReadOnlyCollection<IndexCompatibilityInfo>>(new CancellationToken(true));
        }
    }

    private sealed class ConflictingDestinationIndex : VersionedIndex<object>
    {
        public ConflictingDestinationIndex(IElasticConfiguration configuration) : base(configuration, "conflicting-destination", 1) { }

        public int CompatibilityChecks { get; private set; }

        public override Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            CompatibilityChecks++;
            return Task.FromResult<IReadOnlyCollection<IndexCompatibilityInfo>>(
            [
                new IndexCompatibilityInfo
                {
                    Name = VersionedName,
                    CreatedMajor = 8,
                    CreatedVersion = "8.0.0",
                    ServerMajor = 9,
                    ServerVersion = "9.0.0"
                },
                new IndexCompatibilityInfo
                {
                    Name = $"reindexed-v8-{VersionedName}",
                    CreatedMajor = 8,
                    CreatedVersion = "8.0.0",
                    ServerMajor = 9,
                    ServerVersion = "9.0.0"
                }
            ]);
        }
    }

    private sealed class CountingCompatibilityIndex : Index<object>
    {
        public CountingCompatibilityIndex(IElasticConfiguration configuration) : base(configuration, "counting-compatibility") { }

        public int CompatibilityChecks { get; private set; }

        public override Task ConfigureAsync() => Task.CompletedTask;

        public override Task MaintainAsync(bool includeOptionalTasks = true) => Task.CompletedTask;

        public override Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            CompatibilityChecks++;
            return Task.FromResult<IReadOnlyCollection<IndexCompatibilityInfo>>([]);
        }
    }

    private sealed class UnsupportedCompatibilityIndex : Index<object>
    {
        public UnsupportedCompatibilityIndex(IElasticConfiguration configuration) : base(configuration, "unsupported-compatibility") { }

        public int CompatibilityChecks { get; private set; }

        public override Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            CompatibilityChecks++;
            return Task.FromResult<IReadOnlyCollection<IndexCompatibilityInfo>>(
            [
                new IndexCompatibilityInfo
                {
                    Name = Name,
                    CreatedMajor = 7,
                    CreatedVersion = "7.17.29",
                    ServerMajor = 9,
                    ServerVersion = "9.5.0"
                }
            ]);
        }
    }

    private sealed class UnparseableVersionElasticConfiguration : ElasticConfiguration
    {
        public int RequestCount { get; private set; }

        protected override ElasticsearchClient CreateElasticClient()
        {
            byte[] response = Encoding.UTF8.GetBytes("""
                {
                  "name": "test-node",
                  "cluster_name": "test-cluster",
                  "cluster_uuid": "test-cluster-id",
                  "version": {
                    "number": "not-a-version",
                    "build_flavor": "default",
                    "build_type": "unknown",
                    "build_hash": "unknown",
                    "build_date": "2026-01-01T00:00:00.000Z",
                    "build_snapshot": false,
                    "lucene_version": "10.0.0",
                    "minimum_wire_compatibility_version": "8.0.0",
                    "minimum_index_compatibility_version": "8.0.0"
                  },
                  "tagline": "You Know, for Search"
                }
                """);
            var headers = new Dictionary<string, IEnumerable<string>>
            {
                ["x-elastic-product"] = ["Elasticsearch"]
            };
            var requestInvoker = new InMemoryRequestInvoker(response, 200, null, "application/json", headers);
            var settings = new ElasticsearchClientSettings(requestInvoker)
                .OnRequestCompleted(_ => RequestCount++);

            return new ElasticsearchClient(settings);
        }
    }

    private sealed record StubResponse(int StatusCode, string Content, Exception? Exception = null);

    private sealed class SequenceRequestInvoker : IRequestInvoker
    {
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new()
        {
            ["x-elastic-product"] = ["Elasticsearch"]
        };

        private readonly Queue<StubResponse> _responses;
        private readonly InMemoryRequestInvoker _responseFactory = new();

        public SequenceRequestInvoker(params StubResponse[] responses)
        {
            _responses = new Queue<StubResponse>(responses);
        }

        public ResponseFactory ResponseFactory => _responseFactory.ResponseFactory;

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            return GetResponse().Request<TResponse>(endpoint, boundConfiguration, postData);
        }

        public Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
            where TResponse : TransportResponse, new()
        {
            return GetResponse().RequestAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken);
        }

        private InMemoryRequestInvoker GetResponse()
        {
            if (_responses.Count is 0)
                throw new InvalidOperationException("No response configured for request.");

            var response = _responses.Dequeue();
            return new InMemoryRequestInvoker(
                response.Exception is null ? Encoding.UTF8.GetBytes(response.Content) : [],
                response.StatusCode,
                response.Exception,
                "application/json",
                _headers);
        }

        public void Dispose()
        {
            ((IDisposable)_responseFactory).Dispose();
        }
    }
}
