using System;
using System.Linq;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Jobs;
using Foundatio.Logging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Utility;
using Nest;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class IndexTests : ElasticRepositoryTestBase {
        private readonly DailyLogEventRepository _dailyRepository;
        private readonly IQueue<WorkItemData> _workItemQueue = new InMemoryQueue<WorkItemData>();

        public IndexTests(ITestOutputHelper output) : base(output) {
            _dailyRepository = new DailyLogEventRepository(MyAppConfiguration, _cache, Log.CreateLogger<DailyLogEventRepository>());

            RemoveDataAsync().GetAwaiter().GetResult();
        }

        protected override ElasticConfiguration GetElasticConfiguration() {
            return new MyAppElasticConfiguration(_workItemQueue, _cache, Log);
        }

        private MyAppElasticConfiguration MyAppConfiguration => _configuration as MyAppElasticConfiguration;

        [Fact]
        public async Task CanUseVersionedIndex() {
            var version1EmployeeIndex = new VersionedEmployeeIndex(_client, 1, Log);
            var version1EmployeeRepository = new EmployeeRepository(_client, version1EmployeeIndex.Employee, _cache, Log.CreateLogger<EmployeeRepository>());
            var version2EmployeeIndex = new VersionedEmployeeIndex(_client, 2, Log);
            var version2EmployeeRepository = new EmployeeRepository(_client, version2EmployeeIndex.Employee, _cache, Log.CreateLogger<EmployeeRepository>());

            _client.DeleteIndex(i => i.Index("employees"));
            version1EmployeeIndex.Delete();
            version2EmployeeIndex.Delete();

            version1EmployeeIndex.Configure();

            var indexes = _client.GetIndicesPointingToAlias(version1EmployeeIndex.Name);
            Assert.Equal(1, indexes.Count);

            var alias = _client.GetAlias(descriptor => descriptor.Alias(version1EmployeeIndex.Name));
            Assert.True(alias.IsValid);
            Assert.Equal(1, alias.Indices.Count);
            Assert.Equal(version1EmployeeIndex.VersionedName, alias.Indices.First().Key);

            var employee = await version1EmployeeRepository.AddAsync(EmployeeGenerator.Default);
            Assert.NotNull(employee?.Id);
            _client.Refresh();

            var employeeCountResult = _client.Count(d => d.Index(version1EmployeeIndex.Name));
            Assert.True(employeeCountResult.IsValid);
            Assert.Equal(1, employeeCountResult.Count);

            Assert.Equal(1, version1EmployeeIndex.GetCurrentVersion());
            version2EmployeeIndex.Configure();

            // Make sure we can write to the index still. Should go to the old index until after the reindex is complete.
            await version2EmployeeRepository.AddAsync(EmployeeGenerator.Generate());
            _client.Refresh();
            employeeCountResult = _client.Count(d => d.Index(version1EmployeeIndex.VersionedName));
            Assert.True(employeeCountResult.IsValid);
            Assert.Equal(2, employeeCountResult.Count);

            employeeCountResult = _client.Count(d => d.Index(version2EmployeeIndex.VersionedName));
            Assert.True(employeeCountResult.IsValid);
            Assert.Equal(0, employeeCountResult.Count);

            // alias should still point to the old version until reindex
            alias = _client.GetAlias(descriptor => descriptor.Alias(version2EmployeeIndex.Name));
            Assert.True(alias.IsValid);
            Assert.Equal(1, alias.Indices.Count);
            Assert.Equal(version1EmployeeIndex.VersionedName, alias.Indices.First().Key);

            await version2EmployeeIndex.ReindexAsync();
            _client.Refresh();

            Assert.Equal(2, version2EmployeeIndex.GetCurrentVersion());

            alias = _client.GetAlias(descriptor => descriptor.Alias(version2EmployeeIndex.Name));
            Assert.True(alias.IsValid);
            Assert.Equal(1, alias.Indices.Count);
            Assert.Equal(version2EmployeeIndex.VersionedName, alias.Indices.First().Key);

            employeeCountResult = _client.Count(d => d.Index(version2EmployeeIndex.VersionedName));
            Assert.True(employeeCountResult.IsValid);
            Assert.Equal(2, employeeCountResult.Count);

            Assert.False(_client.IndexExists(d => d.Index(version1EmployeeIndex.VersionedName)).Exists);
 
            employee = await version2EmployeeRepository.AddAsync(EmployeeGenerator.Default);
            Assert.NotNull(employee?.Id);
            _client.Refresh();

            employeeCountResult = _client.Count(d => d.Index(version2EmployeeIndex.Name));
            Assert.True(employeeCountResult.IsValid);
            Assert.Equal(3, employeeCountResult.Count);
        }

        [Fact]
        public async Task GetByDateBasedIndex() {
            var indexes = await _client.GetIndicesPointingToAliasAsync(MyAppConfiguration.DailyLogEvents.Name);
            Assert.Equal(0, indexes.Count);

            var alias = await _client.GetAliasAsync(descriptor => descriptor.Alias(MyAppConfiguration.DailyLogEvents.Name));
            Assert.False(alias.IsValid);
            Assert.Equal(0, alias.Indices.Count);

            var logEvent = await _dailyRepository.AddAsync(LogEventGenerator.Default);
            Assert.NotNull(logEvent?.Id);

            logEvent = await _dailyRepository.AddAsync(LogEventGenerator.Generate(createdUtc: SystemClock.UtcNow.SubtractDays(1)));
            Assert.NotNull(logEvent?.Id);

            await _client.RefreshAsync();
            alias = await _client.GetAliasAsync(descriptor => descriptor.Alias(MyAppConfiguration.DailyLogEvents.Name));
            Assert.True(alias.IsValid);
            Assert.Equal(2, alias.Indices.Count);

            indexes = await _client.GetIndicesPointingToAliasAsync(MyAppConfiguration.DailyLogEvents.Name);
            Assert.Equal(2, indexes.Count);

            await _dailyRepository.RemoveAllAsync();
            await _client.RefreshAsync();

            Assert.Equal(0, await _dailyRepository.CountAsync());
        }
    }
}