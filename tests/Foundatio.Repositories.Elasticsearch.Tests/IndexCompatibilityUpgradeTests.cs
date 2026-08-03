using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class IndexCompatibilityUpgradeTests : ElasticRepositoryTestBase
{
    public IndexCompatibilityUpgradeTests(ITestOutputHelper output) : base(output) { }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync(false);
    }

    [Fact]
    public async Task CanUpgradeIndexCompatibilityForPlainIndexAsync()
    {
        var index = new ForcedIncompatibleEmployeeIndex(_configuration, "compat-upgrade-employees");
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();

        var repository = new EmployeeRepository(index);
        var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        var workItems = ElasticConfiguration.CreateCompatibilityReindexWorkItems(index, await index.GetIndexCompatibilityAsync());
        Assert.Single(workItems);
        var workItem = workItems.Single();
        Assert.Equal(index.Name, workItem.OldIndex);
        Assert.Equal($"{index.Name}-r1", workItem.NewIndex);
        Assert.Equal(index.Name, workItem.Alias);
        Assert.True(workItem.PreserveSourceIndexName);
        Assert.True(workItem.DeleteOld);

        var sourceMappingResponse = await _client.Indices.GetMappingAsync<Employee>(m => m.Indices(index.Name), cancellationToken: TestCancellationToken);
        _logger.LogRequest(sourceMappingResponse);
        Assert.True(sourceMappingResponse.IsValidResponse);
        var sourceProperties = sourceMappingResponse.Mappings.Values.Single().Mappings.Properties;
        Assert.NotNull(sourceProperties);
        var expectedPropertyNames = sourceProperties.Select(p => p.Key.ToString()).Order().ToArray();

        await _configuration.UpgradeIndexCompatibilityAsync(new[] { index });

        // the original nominal name should now resolve (via alias) to the new concrete revision index
        var getResponse = await _client.Indices.GetAsync((Indices)index.Name, cancellationToken: TestCancellationToken);
        Assert.True(getResponse.IsValidResponse);
        Assert.NotNull(getResponse.Indices);
        Assert.Single(getResponse.Indices);
        Assert.Equal($"{index.Name}-r1", getResponse.Indices.Keys.Single().ToString());

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(index.Name), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);

        var result = await repository.GetByIdAsync(employee.Id);
        Assert.NotNull(result);
        Assert.Equal(employee.Id, result.Id);

        var reindexer = new ElasticReindexer(_client, _serializer, _logger);
        await reindexer.ReindexAsync(workItem);
        Assert.NotNull(await repository.GetByIdAsync(employee.Id));

        var mappingResponse = await _client.Indices.GetMappingAsync<Employee>(m => m.Indices(workItem.NewIndex), cancellationToken: TestCancellationToken);
        _logger.LogRequest(mappingResponse);
        Assert.True(mappingResponse.IsValidResponse);
        var targetProperties = mappingResponse.Mappings.Values.Single().Mappings.Properties;
        Assert.NotNull(targetProperties);
        Assert.Equal(expectedPropertyNames, targetProperties.Select(p => p.Key.ToString()).Order());

        // A later server-major upgrade detects the current r1 physical index and advances to r2. Generated
        // revision names are not preserved as aliases; the stable logical alias moves across automatically.
        var followUpCompatibility = new IndexCompatibilityInfo
        {
            Name = workItem.NewIndex,
            CreatedMajor = 8,
            CreatedVersion = "8.0.0",
            RequiresReindexBeforeNextMajorUpgrade = true
        };
        var followUpWorkItems = ElasticConfiguration.CreateCompatibilityReindexWorkItems(index, [followUpCompatibility]);
        var followUpWorkItem = Assert.Single(followUpWorkItems);
        Assert.Equal($"{index.Name}-r1", followUpWorkItem.OldIndex);
        Assert.Equal($"{index.Name}-r2", followUpWorkItem.NewIndex);
        Assert.Equal(index.Name, followUpWorkItem.Alias);
        Assert.False(followUpWorkItem.PreserveSourceIndexName);
    }

    [Fact]
    public async Task CanUpgradeIndexCompatibilityForVersionedIndexAsync()
    {
        var index = new ForcedIncompatibleVersionedEmployeeIndex(_configuration, "compat-upgrade-versioned-employees", 1);
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();

        var repository = new EmployeeRepository(index);
        var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        var workItems = ElasticConfiguration.CreateCompatibilityReindexWorkItems(index, await index.GetIndexCompatibilityAsync());
        Assert.Single(workItems);
        var workItem = workItems.Single();
        Assert.Equal(index.VersionedName, workItem.OldIndex);
        Assert.Equal($"{index.VersionedName}-r1", workItem.NewIndex);
        Assert.True(workItem.PreserveSourceIndexName);

        await _configuration.UpgradeIndexCompatibilityAsync(new[] { index });

        var logicalAliasResponse = await _client.Indices.GetAsync((Indices)index.Name, cancellationToken: TestCancellationToken);
        Assert.True(logicalAliasResponse.IsValidResponse);
        Assert.Equal(workItem.NewIndex, logicalAliasResponse.Indices.Keys.Single().ToString());

        var versionedAliasResponse = await _client.Indices.GetAsync((Indices)index.VersionedName, cancellationToken: TestCancellationToken);
        Assert.True(versionedAliasResponse.IsValidResponse);
        Assert.Equal(workItem.NewIndex, versionedAliasResponse.Indices.Keys.Single().ToString());

        await index.ConfigureAsync();
        versionedAliasResponse = await _client.Indices.GetAsync((Indices)index.VersionedName, cancellationToken: TestCancellationToken);
        Assert.True(versionedAliasResponse.IsValidResponse);
        Assert.Single(versionedAliasResponse.Indices);
        Assert.Equal(workItem.NewIndex, versionedAliasResponse.Indices.Keys.Single().ToString());

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(index.Name), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);

        Assert.Equal(1, await index.GetCurrentVersionAsync());
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_DoesNotRequireUpgrade_ForCurrentlyCreatedIndex()
    {
        var realIndex = new EmployeeIndex(_configuration);
        await realIndex.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => realIndex.DeleteAsync());
        await realIndex.ConfigureAsync();

        var compatInfos = await realIndex.GetIndexCompatibilityAsync();
        Assert.Single(compatInfos);
        Assert.False(compatInfos.Single().RequiresReindexBeforeNextMajorUpgrade);

        var workItems = ElasticConfiguration.CreateCompatibilityReindexWorkItems(realIndex, compatInfos);
        Assert.Empty(workItems);
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_UsesOneCurrentServerAndSettingsRequest()
    {
        using var configuration = new RequestCountingElasticConfiguration();
        using var index = new RequestCountingIndex(configuration, $"compat-request-count-{Guid.NewGuid():N}");
        configuration.AddIndex(index);
        await configuration.Client.WaitForReadyAsync(TestCancellationToken);
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        configuration.ResetRequestCounts();

        var stopwatch = Stopwatch.StartNew();
        await index.GetIndexCompatibilityAsync();
        stopwatch.Stop();

        Assert.Equal(1, configuration.InfoRequestCount);
        Assert.Equal(1, configuration.CompatibilitySettingsRequestCount);
        _logger.LogInformation("Cold explicit compatibility detection completed in {ElapsedMilliseconds} ms", stopwatch.Elapsed.TotalMilliseconds);

        stopwatch.Restart();
        await index.GetIndexCompatibilityAsync();
        stopwatch.Stop();

        Assert.Equal(2, configuration.InfoRequestCount);
        Assert.Equal(2, configuration.CompatibilitySettingsRequestCount);
        _logger.LogInformation("Warm explicit compatibility detection completed in {ElapsedMilliseconds} ms", stopwatch.Elapsed.TotalMilliseconds);
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_IgnoresMatchingOrphanRevision()
    {
        using var configuration = new RequestCountingElasticConfiguration();
        string name = $"compat-orphan-{Guid.NewGuid():N}";
        using var index = new RequestCountingIndex(configuration, name);
        configuration.AddIndex(index);
        await configuration.Client.WaitForReadyAsync(TestCancellationToken);
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();

        string orphanName = $"{name}-r99";
        var createResponse = await configuration.Client.Indices.CreateAsync(orphanName,
            d => d.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)), TestCancellationToken);
        Assert.True(createResponse.IsValidResponse);

        var compatibility = await index.GetIndexCompatibilityAsync();

        var info = Assert.Single(compatibility);
        Assert.Equal(name, info.Name);
    }

    [Fact]
    public async Task ConfigureIndexesAsync_DoesNotIssueCompatibilityRequests()
    {
        using var configuration = new RequestCountingElasticConfiguration();
        using var index = new RequestCountingIndex(configuration, $"compat-configure-count-{Guid.NewGuid():N}");
        configuration.AddIndex(index);
        await configuration.Client.WaitForReadyAsync(TestCancellationToken);
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        configuration.ResetRequestCounts();

        await configuration.ConfigureIndexesAsync([index]);

        Assert.Equal(0, configuration.InfoRequestCount);
        Assert.Equal(0, configuration.CompatibilitySettingsRequestCount);
    }

    [Fact]
    public async Task CanUpgradeIndexCompatibilityForDailyIndexWithoutLegacyPhysicalAliasesAsync()
    {
        string name = $"compat-upgrade-daily-{Guid.NewGuid():N}";
        var index = new ForcedIncompatibleDailyEmployeeIndex(_configuration, name, 1);
        string windowedAlias = $"{name}-last7days";
        index.AddAlias(windowedAlias, TimeSpan.FromDays(7));
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        var repository = new EmployeeRepository(index);
        var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        string datedAlias = index.GetIndex(employee);
        string oldPhysicalIndex = index.GetVersionedIndex(employee.CreatedUtc);
        string routedAlias = $"{name}-routed";
        var addRoutedAliasResponse = await _client.Indices.UpdateAliasesAsync(a => a.Actions(actions => actions.Add(add => add
            .Index(oldPhysicalIndex)
            .Alias(routedAlias)
            .IndexRouting("write-route")
            .SearchRouting("search-route"))), TestCancellationToken);
        Assert.True(addRoutedAliasResponse.IsValidResponse);

        var workItems = ElasticConfiguration.CreateCompatibilityReindexWorkItems(index, await index.GetIndexCompatibilityAsync());
        var workItem = Assert.Single(workItems);
        Assert.Equal(oldPhysicalIndex, workItem.OldIndex);
        Assert.Equal($"{oldPhysicalIndex}-r1", workItem.NewIndex);
        Assert.False(workItem.PreserveSourceIndexName);

        await _configuration.UpgradeIndexCompatibilityAsync([index]);

        var datedAliasResponse = await _client.Indices.GetAsync((Indices)datedAlias, cancellationToken: TestCancellationToken);
        Assert.True(datedAliasResponse.IsValidResponse);
        Assert.Equal(workItem.NewIndex, datedAliasResponse.Indices.Keys.Single().ToString());

        var umbrellaAliasResponse = await _client.Indices.GetAsync((Indices)index.Name, cancellationToken: TestCancellationToken);
        Assert.True(umbrellaAliasResponse.IsValidResponse);
        Assert.Equal(workItem.NewIndex, umbrellaAliasResponse.Indices.Keys.Single().ToString());

        var windowedAliasResponse = await _client.Indices.GetAsync((Indices)windowedAlias, cancellationToken: TestCancellationToken);
        Assert.True(windowedAliasResponse.IsValidResponse);
        Assert.Equal(workItem.NewIndex, windowedAliasResponse.Indices.Keys.Single().ToString());

        var aliases = datedAliasResponse.Indices.Values.Single().Aliases;
        Assert.NotNull(aliases);
        Assert.Contains(datedAlias, aliases.Keys.Select(k => k.ToString()));
        Assert.Contains(index.Name, aliases.Keys.Select(k => k.ToString()));
        Assert.Contains(windowedAlias, aliases.Keys.Select(k => k.ToString()));
        Assert.DoesNotContain(oldPhysicalIndex, aliases.Keys.Select(k => k.ToString()));

        var routedAliasResponse = await _client.Indices.GetAliasAsync((Indices)workItem.NewIndex,
            d => d.Name(routedAlias), TestCancellationToken);
        Assert.True(routedAliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        var routedAliases = routedAliasResponse.Aliases;
#else
        var routedAliases = routedAliasResponse.Values;
#endif
        Assert.NotNull(routedAliases);
        var routedAliasDefinitions = routedAliases[workItem.NewIndex].Aliases;
        Assert.NotNull(routedAliasDefinitions);
        var routedAliasDefinition = routedAliasDefinitions[routedAlias];
        Assert.Equal("write-route", routedAliasDefinition.IndexRouting?.ToString());
        Assert.Equal("search-route", routedAliasDefinition.SearchRouting?.ToString());

        var oldPhysicalResponse = await _client.Indices.GetAsync((Indices)oldPhysicalIndex,
            d => d.IgnoreUnavailable(), TestCancellationToken);
        Assert.Empty(oldPhysicalResponse.Indices);
        Assert.NotNull(await repository.GetByIdAsync(employee.Id));
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_WithPendingDailySchemaUpgrade_ReturnsNoCompatibilityWork()
    {
        string name = $"compat-schema-precedence-{Guid.NewGuid():N}";
        var currentIndex = new DailyIndex<Employee>(_configuration, name, 1);
        await using AsyncDisposableAction _ = new(() => currentIndex.DeleteAsync());
        var repository = new EmployeeRepository(currentIndex);
        await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());

        using var nextIndex = new ForcedIncompatibleDailyEmployeeIndex(_configuration, name, 2);

        Assert.Empty(await nextIndex.GetIndexCompatibilityAsync());
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenCompatibilityRemains_Throws()
    {
        var index = new AlwaysIncompatibleEmployeeIndex(_configuration, $"compat-remains-{Guid.NewGuid():N}");
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();

        var exception = await Assert.ThrowsAsync<RepositoryException>(() => _configuration.UpgradeIndexCompatibilityAsync([index]));

        Assert.Contains("did not complete", exception.Message);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenDestinationExists_ThrowsBeforeReindex()
    {
        string name = $"compat-collision-{Guid.NewGuid():N}";
        var index = new ForcedIncompatibleEmployeeIndex(_configuration, name);
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();

        string destination = $"{name}-r1";
        var createResponse = await _client.Indices.CreateAsync(destination,
            d => d.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)), TestCancellationToken);
        Assert.True(createResponse.IsValidResponse);

        var exception = await Assert.ThrowsAsync<RepositoryException>(() => _configuration.UpgradeIndexCompatibilityAsync([index]));

        Assert.Contains(destination, exception.Message);
        var sourceResponse = await _client.Indices.GetAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.Contains(name, sourceResponse.Indices.Keys.Select(k => k.ToString()));
        var destinationResponse = await _client.Indices.GetAsync((Indices)destination, cancellationToken: TestCancellationToken);
        Assert.Contains(destination, destinationResponse.Indices.Keys.Select(k => k.ToString()));
    }

    private sealed class ForcedIncompatibleEmployeeIndex : Index<Employee>
    {
        public ForcedIncompatibleEmployeeIndex(IElasticConfiguration configuration, string name) : base(configuration, name) { }

        public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
        {
            map.Properties(p => p.SetupDefaults());
        }

        public override async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync()
        {
            var infos = await base.GetIndexCompatibilityAsync().AnyContext();
            return ForceOriginalIndexesIncompatible(infos);
        }
    }

    private sealed class ForcedIncompatibleVersionedEmployeeIndex : VersionedIndex<Employee>
    {
        public ForcedIncompatibleVersionedEmployeeIndex(IElasticConfiguration configuration, string name, int version) : base(configuration, name, version) { }

        public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
        {
            map.Properties(p => p.SetupDefaults());
        }

        public override async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync()
        {
            var infos = await base.GetIndexCompatibilityAsync().AnyContext();
            return ForceOriginalIndexesIncompatible(infos);
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

        public override async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync()
        {
            var infos = await base.GetIndexCompatibilityAsync().AnyContext();
            return ForceOriginalIndexesIncompatible(infos);
        }
    }

    private sealed class AlwaysIncompatibleEmployeeIndex : Index<Employee>
    {
        public AlwaysIncompatibleEmployeeIndex(IElasticConfiguration configuration, string name) : base(configuration, name) { }

        public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
        {
            map.Properties(p => p.SetupDefaults());
        }

        public override async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync()
        {
            var infos = await base.GetIndexCompatibilityAsync().AnyContext();
            return infos.Select(i => i with { RequiresReindexBeforeNextMajorUpgrade = true }).ToArray();
        }
    }

    private static IReadOnlyCollection<IndexCompatibilityInfo> ForceOriginalIndexesIncompatible(IReadOnlyCollection<IndexCompatibilityInfo> infos)
    {
        return infos.Select(i => i with
        {
            RequiresReindexBeforeNextMajorUpgrade = !IndexNameRevision.Parse(i.Name).HasRevision
        }).ToArray();
    }

    private sealed class RequestCountingElasticConfiguration : ElasticConfiguration
    {
        private int _infoRequestCount;
        private int _compatibilitySettingsRequestCount;

        public int InfoRequestCount => _infoRequestCount;
        public int CompatibilitySettingsRequestCount => _compatibilitySettingsRequestCount;

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

                if (uri.Query.Contains("features=settings", StringComparison.Ordinal))
                    Interlocked.Increment(ref _compatibilitySettingsRequestCount);
            });
        }

        public void ResetRequestCounts()
        {
            Interlocked.Exchange(ref _infoRequestCount, 0);
            Interlocked.Exchange(ref _compatibilitySettingsRequestCount, 0);
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
