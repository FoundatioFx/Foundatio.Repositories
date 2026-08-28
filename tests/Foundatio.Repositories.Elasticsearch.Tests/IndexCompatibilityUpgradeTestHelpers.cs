using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed partial class IndexCompatibilityUpgradeTests
{
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

        public int InfoRequestCount => _infoRequestCount;
        public int CompatibilityMetadataRequestCount => _compatibilityMetadataRequestCount;

        protected override void ConfigureSettings(ElasticsearchClientSettings settings)
        {
            base.ConfigureSettings(settings);
            settings.OnRequestCompleted(call =>
            {
                var uri = call.Uri;
                if (uri is null)
                    return;

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
}
