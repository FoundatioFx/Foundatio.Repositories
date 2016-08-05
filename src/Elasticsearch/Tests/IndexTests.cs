using System;
using System.Linq;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Jobs;
using Foundatio.Logging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Utility;
using Nest;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class IndexTests : ElasticRepositoryTestBase {
        private readonly IQueue<WorkItemData> _workItemQueue = new InMemoryQueue<WorkItemData>();

        public IndexTests(ITestOutputHelper output) : base(output) {
            RemoveDataAsync(configureIndexes: false).GetAwaiter().GetResult();
        }

        protected override ElasticConfiguration GetElasticConfiguration() {
            return new MyAppElasticConfiguration(_workItemQueue, _cache, Log);
        }

        private MyAppElasticConfiguration MyAppConfiguration => _configuration as MyAppElasticConfiguration;

        [Fact]
        public void CannotAddInvalidTypeToDailyIndex() {
            Assert.Throws<ArgumentException>(() => new DailyEmployeeIndexWithWrongEmployeeType(_client, 1, Log));
        }

        [Fact]
        public async Task CanCreateDailyIndex() {
            var index = new DailyEmployeeIndex(_client, 1, Log);
            index.Delete();
            await _client.RefreshAsync();

            var existsResponse = await _client.TemplateExistsAsync(index.Name);
            _logger.Trace(() => existsResponse.GetRequest());
            Assert.True(existsResponse.IsValid);
            Assert.False(existsResponse.Exists);

            using (new DisposableAction(() => index.Delete())) {
                index.Configure();
                await _client.RefreshAsync();

                existsResponse = await _client.TemplateExistsAsync(index.Name);
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);
                
                existsResponse = await _client.AliasExistsAsync(index.Name);
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.False(existsResponse.Exists);
                
                var utcNow = SystemClock.UtcNow;
                existsResponse = await _client.IndexExistsAsync(index.GetIndex(utcNow));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.False(existsResponse.Exists);

                var repository = new EmployeeRepository(_client, index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());
                var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow));
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();
                
                existsResponse = await _client.AliasExistsAsync(index.Name);
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                existsResponse = await _client.IndexExistsAsync(index.GetIndex(utcNow));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);
            }
        }
        
        [Fact]
        public async Task CanCreateMonthlyIndex() {
            var index = new MonthlyEmployeeIndex(_client, 1, Log);
            index.Delete();
            await _client.RefreshAsync();

            var existsResponse = await _client.TemplateExistsAsync(index.Name);
            _logger.Trace(() => existsResponse.GetRequest());
            Assert.True(existsResponse.IsValid);
            Assert.False(existsResponse.Exists);

            using (new DisposableAction(() => index.Delete())) {
                index.Configure();
                await _client.RefreshAsync();

                existsResponse = await _client.TemplateExistsAsync(index.Name);
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                existsResponse = await _client.AliasExistsAsync(index.Name);
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.False(existsResponse.Exists);

                var utcNow = SystemClock.UtcNow;
                existsResponse = await _client.IndexExistsAsync(index.GetIndex(utcNow));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.False(existsResponse.Exists);

                var repository = new EmployeeRepository(_client, index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());
                var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow));
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                existsResponse = await _client.AliasExistsAsync(index.Name);
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                existsResponse = await _client.IndexExistsAsync(index.GetIndex(utcNow));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);
            }
        }
        
        [Fact]
        public async Task CanReindexVersionedIndex() {
            var version1Index = new VersionedEmployeeIndex(_client, 1, Log);
            version1Index.Delete();

            var version2Index = new VersionedEmployeeIndex(_client, 2, Log);
            version2Index.Delete();

            using (new DisposableAction(() => version1Index.Delete())) {
                version1Index.Configure();

                var indexes = _client.GetIndicesPointingToAlias(version1Index.Name);
                Assert.Equal(1, indexes.Count);

                var aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version1Index.Name));
                _logger.Trace(() => aliasResponse.GetRequest());
                Assert.True(aliasResponse.IsValid);
                Assert.Equal(1, aliasResponse.Indices.Count);
                Assert.Equal(version1Index.VersionedName, aliasResponse.Indices.First().Key);

                var version1Repository = new EmployeeRepository(_client, version1Index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Default);
                await _client.RefreshAsync();
                Assert.NotNull(employee?.Id);
                
                var countResponse = await _client.CountAsync(d => d.Index(version1Index.Name));
                _logger.Trace(() => countResponse.GetRequest());
                Assert.True(countResponse.IsValid);
                Assert.Equal(1, countResponse.Count);

                Assert.Equal(1, version1Index.GetCurrentVersion());

                using (new DisposableAction(() => version2Index.Delete())) {
                    version2Index.Configure();

                    // Make sure we can write to the index still. Should go to the old index until after the reindex is complete.
                    var version2Repository = new EmployeeRepository(_client, version2Index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());
                    await version2Repository.AddAsync(EmployeeGenerator.Generate());
                    await _client.RefreshAsync();

                    countResponse = await _client.CountAsync(d => d.Index(version1Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(2, countResponse.Count);

                    countResponse = await _client.CountAsync(d => d.Index(version2Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(0, countResponse.Count);
                    
                    Assert.Equal(1, version2Index.GetCurrentVersion());

                    // alias should still point to the old version until reindex
                    aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version1Index.VersionedName, aliasResponse.Indices.First().Key);

                    await version2Index.ReindexAsync();
                    await _client.RefreshAsync();
                    
                    aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version2Index.VersionedName, aliasResponse.Indices.First().Key);
                    
                    Assert.Equal(2, version1Index.GetCurrentVersion());
                    Assert.Equal(2, version2Index.GetCurrentVersion());

                    countResponse = await _client.CountAsync(d => d.Index(version2Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(2, countResponse.Count);

                    Assert.False(_client.IndexExists(d => d.Index(version1Index.VersionedName)).Exists);

                    employee = await version2Repository.AddAsync(EmployeeGenerator.Default);
                    await _client.RefreshAsync();
                    Assert.NotNull(employee?.Id);

                    countResponse = await _client.CountAsync(d => d.Index(version2Index.Name));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(3, countResponse.Count);
                }
            }
        }

        [Fact]
        public async Task CanReindexTimeSeriesIndex() {
            var version1Index = new DailyEmployeeIndex(_client, 1, Log);
            version1Index.Delete();

            var version2Index = new DailyEmployeeIndex(_client, 2, Log);
            version2Index.Delete();

            using (new DisposableAction(() => version1Index.Delete())) {
                version1Index.Configure();
                var version1Repository = new EmployeeRepository(_client, version1Index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());

                var utcNow = SystemClock.UtcNow;
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow));
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                Assert.Equal(1, version1Index.GetCurrentVersion());

                var aliasCountResponse = await _client.CountAsync(d => d.Index(version1Index.Name));
                _logger.Trace(() => aliasCountResponse.GetRequest());
                Assert.True(aliasCountResponse.IsValid);
                Assert.Equal(1, aliasCountResponse.Count);

                var indexCountResponse = await _client.CountAsync(d => d.Index(version1Index.GetIndex(utcNow)));
                _logger.Trace(() => indexCountResponse.GetRequest());
                Assert.True(indexCountResponse.IsValid);
                Assert.Equal(1, indexCountResponse.Count);

                using (new DisposableAction(() => version2Index.Delete())) {
                    version2Index.Configure();
                    var version2Repository = new EmployeeRepository(_client, version2Index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());

                    // Make sure we write to the new index.
                    await version2Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow));
                    await _client.RefreshAsync();

                    aliasCountResponse = await _client.CountAsync(d => d.Index(version1Index.Name));
                    _logger.Trace(() => aliasCountResponse.GetRequest());
                    Assert.True(aliasCountResponse.IsValid);
                    Assert.Equal(2, aliasCountResponse.Count);

                    indexCountResponse = await _client.CountAsync(d => d.Index(version1Index.GetVersionedIndex(utcNow)));
                    _logger.Trace(() => indexCountResponse.GetRequest());
                    Assert.True(indexCountResponse.IsValid);
                    Assert.Equal(2, indexCountResponse.Count);

                    var existsResponse = await _client.IndexExistsAsync(d => d.Index(version2Index.GetVersionedIndex(utcNow)));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.False(existsResponse.Exists);

                    // alias should still point to the old version until reindex
                    var alias = await _client.GetAliasAsync(descriptor => descriptor.Alias(version1Index.Name));
                    _logger.Trace(() => alias.GetRequest());
                    Assert.True(alias.IsValid);
                    Assert.Equal(1, alias.Indices.Count);

                    Assert.Equal(1, version2Index.GetCurrentVersion());

                    await version2Index.ReindexAsync((progress, message) => {
                        _logger.Info($"Reindex Progress {progress}%: {message}");
                        return Task.CompletedTask;
                    });

                    await _client.RefreshAsync();

                    alias = await _client.GetAliasAsync(descriptor => descriptor.Alias(version1Index.Name));
                    _logger.Trace(() => alias.GetRequest());
                    Assert.True(alias.IsValid);
                    Assert.Equal(1, alias.Indices.Count);
                    
                    Assert.Equal(2, version1Index.GetCurrentVersion());
                    Assert.Equal(2, version2Index.GetCurrentVersion());

                    existsResponse = await _client.IndexExistsAsync(d => d.Index(version1Index.GetVersionedIndex(utcNow)));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.False(existsResponse.Exists);

                    existsResponse = await _client.IndexExistsAsync(d => d.Index(version2Index.GetVersionedIndex(utcNow)));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);
                }
            }
        }

        [Fact]
        public async Task GetByDateBasedIndex() {
            MyAppConfiguration.DailyLogEvents.Configure();

            var indexes = await _client.GetIndicesPointingToAliasAsync(MyAppConfiguration.DailyLogEvents.Name);
            Assert.Equal(0, indexes.Count);

            var alias = await _client.GetAliasAsync(descriptor => descriptor.Alias(MyAppConfiguration.DailyLogEvents.Name));
            _logger.Trace(() => alias.GetRequest());
            Assert.False(alias.IsValid);
            Assert.Equal(0, alias.Indices.Count);

            var utcNow = SystemClock.UtcNow;
            var repository = new DailyLogEventRepository(MyAppConfiguration, _cache, Log.CreateLogger<DailyLogEventRepository>());
            var logEvent = await repository.AddAsync(LogEventGenerator.Generate(createdUtc: utcNow));
            Assert.NotNull(logEvent?.Id);

            logEvent = await repository.AddAsync(LogEventGenerator.Generate(createdUtc: utcNow.SubtractDays(1)));
            Assert.NotNull(logEvent?.Id);

            await _client.RefreshAsync();
            alias = await _client.GetAliasAsync(descriptor => descriptor.Alias(MyAppConfiguration.DailyLogEvents.Name));
            _logger.Trace(() => alias.GetRequest());
            Assert.True(alias.IsValid);
            Assert.Equal(2, alias.Indices.Count);

            indexes = await _client.GetIndicesPointingToAliasAsync(MyAppConfiguration.DailyLogEvents.Name);
            Assert.Equal(2, indexes.Count);

            await repository.RemoveAllAsync();
            await _client.RefreshAsync();

            Assert.Equal(0, await repository.CountAsync());
        }
    }
}