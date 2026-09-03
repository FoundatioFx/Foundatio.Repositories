using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Transport;
using Foundatio.Caching;
using Foundatio.Lock;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Extensions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed partial class IndexCompatibilityUpgradeTests
{
    private async Task AssertIndexExistsAsync(string index, bool expected)
    {
        var response = await _client.Indices.ExistsAsync(index, cancellationToken: TestCancellationToken);
        Assert.True(expected ? response.Exists : !response.Exists, response.DebugInformation);
    }

    private sealed class EndpointAwareElasticConfiguration : ElasticConfiguration
    {
        protected override NodePool CreateConnectionPool()
        {
            string endpoint = Environment.GetEnvironmentVariable("ELASTICSEARCH_URL")?.Split(',')[0]
                ?? "http://localhost:9200";
            return new SingleNodePool(new Uri(endpoint));
        }
    }

    private class ForcedIncompatibleEmployeeIndex : Index<Employee>
    {
        public ForcedIncompatibleEmployeeIndex(IElasticConfiguration configuration, string name) : base(configuration, name) { }

        public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
        {
            map.Properties(p => p.SetupDefaults());
        }

        public override async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            var infos = await base.GetIndexCompatibilityAsync(cancellationToken).AnyContext();
            return ForceOriginalIndexesIncompatible(infos, Name);
        }
    }

    private sealed class ForcedIncompatibleVersionedEmployeeIndex : VersionedIndex<Employee>
    {
        public ForcedIncompatibleVersionedEmployeeIndex(IElasticConfiguration configuration, string name, int version) : base(configuration, name, version) { }

        public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
        {
            map.Properties(p => p.SetupDefaults());
        }

        public override async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            var infos = await base.GetIndexCompatibilityAsync(cancellationToken).AnyContext();
            return ForceOriginalIndexesIncompatible(infos, Name);
        }
    }

    private sealed class ForcedIncompatibleDailyEmployeeIndex : DailyIndex<Employee>
    {
        public ForcedIncompatibleDailyEmployeeIndex(IElasticConfiguration configuration, string name, int version) : base(configuration, name, version) { }

        public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
        {
            base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
        }

        public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
        {
            map.Properties(p => p.SetupDefaults());
        }

        public override async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            var infos = await base.GetIndexCompatibilityAsync(cancellationToken).AnyContext();
            return ForceOriginalIndexesIncompatible(infos, Name);
        }
    }

    private sealed class ForcedIncompatibleDynamicEmployeeIndex : DynamicIndex<Employee>
    {
        public ForcedIncompatibleDynamicEmployeeIndex(IElasticConfiguration configuration, string name) : base(configuration, name) { }

        public override async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            var infos = await base.GetIndexCompatibilityAsync(cancellationToken).AnyContext();
            return ForceOriginalIndexesIncompatible(infos, Name);
        }
    }

    private sealed class ForcedIncompatibleMonthlyEmployeeIndex : MonthlyIndex<Employee>
    {
        public ForcedIncompatibleMonthlyEmployeeIndex(IElasticConfiguration configuration, string name, int version) : base(configuration, name, version) { }

        public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
        {
            base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
        }

        public override async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            var infos = await base.GetIndexCompatibilityAsync(cancellationToken).AnyContext();
            return ForceOriginalIndexesIncompatible(infos, Name);
        }
    }

    private sealed class AlwaysIncompatibleEmployeeIndex : Index<Employee>
    {
        public AlwaysIncompatibleEmployeeIndex(IElasticConfiguration configuration, string name) : base(configuration, name) { }

        public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
        {
            map.Properties(p => p.SetupDefaults());
        }

        public override async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            var infos = await base.GetIndexCompatibilityAsync(cancellationToken).AnyContext();
            return infos.Select(i => i with { CreatedMajor = i.ServerMajor - 1 }).ToArray();
        }
    }

    private sealed class UnsupportedCreateFromVersionEmployeeIndex : ForcedIncompatibleEmployeeIndex
    {
        public UnsupportedCreateFromVersionEmployeeIndex(IElasticConfiguration configuration, string name) : base(configuration, name) { }

        public override async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            var infos = await base.GetIndexCompatibilityAsync(cancellationToken).AnyContext();
            return infos.Select(i => i with { CreatedMajor = 7, ServerMajor = 8, ServerVersion = "8.17.9" }).ToArray();
        }
    }

    private static IReadOnlyCollection<IndexCompatibilityInfo> ForceOriginalIndexesIncompatible(
        IReadOnlyCollection<IndexCompatibilityInfo> infos,
        string configuredIndexName)
    {
        return infos.Select(i => String.Equals(i.Name, CompatibilityIndexName.Create(i.Name, i.ServerMajor, configuredIndexName), StringComparison.Ordinal)
            ? i
            : i with { CreatedMajor = i.ServerMajor - 1 }).ToArray();
    }

    private sealed class RequestCountingElasticConfiguration : ElasticConfiguration
    {
        private int _infoRequestCount;
        private int _compatibilityMetadataRequestCount;
        private readonly List<string> _requestPaths = [];

        public int InfoRequestCount => _infoRequestCount;
        public int CompatibilityMetadataRequestCount => _compatibilityMetadataRequestCount;
        public IReadOnlyCollection<string> RequestPaths => _requestPaths.ToArray();

        protected override void ConfigureSettings(ElasticsearchClientSettings settings)
        {
            base.ConfigureSettings(settings);
            settings.OnRequestCompleted(call =>
            {
                var uri = call.Uri;
                if (uri is null)
                    return;

                _requestPaths.Add($"{call.HttpMethod} {uri.PathAndQuery}");

                if (uri.AbsolutePath is "/")
                    Interlocked.Increment(ref _infoRequestCount);

                if (uri.Query.Contains("features=", StringComparison.Ordinal)
                    && uri.Query.Contains("aliases", StringComparison.Ordinal)
                    && uri.Query.Contains("settings", StringComparison.Ordinal))
                    Interlocked.Increment(ref _compatibilityMetadataRequestCount);
            });
        }

        public void ResetRequestCounts()
        {
            Interlocked.Exchange(ref _infoRequestCount, 0);
            Interlocked.Exchange(ref _compatibilityMetadataRequestCount, 0);
            _requestPaths.Clear();
        }
    }

    private sealed class RequestCountingIndex : Index<object>
    {
        public RequestCountingIndex(IElasticConfiguration configuration, string name) : base(configuration, name) { }

        public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
        {
            base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
        }
    }

    private sealed class CompatibilityCleanupJob : CleanupIndexesJob
    {
        public CompatibilityCleanupJob(ElasticsearchClient client, string prefix = "logs")
            : base(client, new ThrottlingLockProvider(new InMemoryCacheClient()), TimeProvider.System, NullLoggerFactory.Instance)
        {
            AddIndex(prefix, TimeSpan.FromDays(1));
        }

        public List<string> DeletedIndexes { get; } = [];

        public override Task OnIndexDeleted(string indexName, TimeSpan duration)
        {
            DeletedIndexes.Add(indexName);
            return Task.CompletedTask;
        }
    }
}
