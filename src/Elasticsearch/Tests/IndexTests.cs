using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
using Nito.AsyncEx;
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
        public async Task CanCreateDailyAliases() {
            var index = new DailyEmployeeIndex(_client, 1, Log);
            index.Delete();

            var utcNow = SystemClock.UtcNow;
            using (new DisposableAction(() => index.Delete())) {
                index.Configure();
                var repository = new EmployeeRepository(_client, index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());

                for (int i = 0; i < 35; i += 5) {
                    var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractDays(i)));
                    Assert.NotNull(employee?.Id);

                    Assert.Equal(1, index.GetCurrentVersion());
                    var existsResponse = await _client.IndexExistsAsync(index.GetIndex(employee.CreatedUtc));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);

                    var aliasesResponse = await _client.GetAliasesAsync(a => a.Index(index.GetIndex(employee.CreatedUtc)));
                    _logger.Trace(() => aliasesResponse.GetRequest());
                    Assert.True(aliasesResponse.IsValid);
                    Assert.Equal(1, aliasesResponse.Indices.Count);

                    var aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                    aliases.Sort();

                    Assert.Equal(GetExpectedEmployeeDailyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));
                }
            }
        }

        [Fact]
        public async Task CanCreateMonthlyAliases() {
            var index = new MonthlyEmployeeIndex(_client, 1, Log);
            index.Delete();

            var utcNow = SystemClock.UtcNow;
            using (new DisposableAction(() => index.Delete())) {
                index.Configure();
                var repository = new EmployeeRepository(_client, index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());

                for (int i = 0; i < 4; i++) {
                    var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractMonths(i)));
                    Assert.NotNull(employee?.Id);

                    Assert.Equal(1, index.GetCurrentVersion());
                    var existsResponse = await _client.IndexExistsAsync(index.GetIndex(employee.CreatedUtc));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);

                    var aliasesResponse = await _client.GetAliasesAsync(a => a.Index(index.GetIndex(employee.CreatedUtc)));
                    _logger.Trace(() => aliasesResponse.GetRequest());
                    Assert.True(aliasesResponse.IsValid);
                    Assert.Equal(1, aliasesResponse.Indices.Count);

                    var aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                    aliases.Sort();

                    Assert.Equal(GetExpectedEmployeeMonthlyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));
                }
            }
        }

        [Fact]
        public async Task CanReindexSameIndex() {
            //throw new NotImplementedException();
        }

        [Fact]
        public async Task CanReindexVersionedIndex() {
            var version1Index = new VersionedEmployeeIndex(_client, 1, Log);
            version1Index.Delete();

            var version2Index = new VersionedEmployeeIndex(_client, 2, Log);
            version2Index.Delete();

            using (new DisposableAction(() => version1Index.Delete())) {
                version1Index.Configure();
                Assert.True(_client.IndexExists(version1Index.VersionedName).Exists);
                
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
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);

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
        public async Task CanReindexVersionedIndexWithCorrectMappings() {
            var version1Index = new VersionedEmployeeIndex(_client, 1, Log);
            version1Index.Delete();

            var version2Index = new VersionedEmployeeIndex(_client, 2, Log);
            version2Index.DiscardIndexesOnReindex = false;
            version2Index.Delete();

            using (new DisposableAction(() => version1Index.Delete())) {
                version1Index.Configure();
                var version1Repository = new EmployeeRepository(_client, version1Index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());

                var utcNow = SystemClock.UtcNow;
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow));
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                using (new DisposableAction(() => version2Index.Delete())) {
                    version2Index.Configure();

                    await version2Index.ReindexAsync((progress, message) => {
                        _logger.Info($"Reindex Progress {progress}%: {message}");
                        return Task.CompletedTask;
                    });

                    await _client.RefreshAsync();
                    var existsResponse = await _client.IndexExistsAsync(d => d.Index(version1Index.VersionedName));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);

                    var mappingResponse = await _client.GetMappingAsync<Employee>(m => m.Index(version1Index.VersionedName));
                    _logger.Trace(() => mappingResponse.GetRequest());
                    Assert.True(mappingResponse.IsValid);
                    Assert.NotNull(mappingResponse.Mappings);

                    existsResponse = await _client.IndexExistsAsync(d => d.Index(version2Index.VersionedName));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);

                    string version1Mappings = ToJson(mappingResponse.Mappings);
                    mappingResponse = await _client.GetMappingAsync<Employee>(m => m.Index(version1Index.VersionedName));
                    _logger.Trace(() => mappingResponse.GetRequest());
                    Assert.True(mappingResponse.IsValid);
                    Assert.NotNull(mappingResponse.Mappings);
                    Assert.Equal(version1Mappings, ToJson(mappingResponse.Mappings).Replace("-v2", "-v1"));
                }
            }
        }

        [Fact]
        public async Task CanReindexVersionedIndexWithDataInBothIndexes() {
            var version1Index = new VersionedEmployeeIndex(_client, 1, Log);
            version1Index.Delete();

            var version2Index = new VersionedEmployeeIndex(_client, 2, Log);
            version2Index.Delete();

            using (new DisposableAction(() => version1Index.Delete())) {
                version1Index.Configure();
                Assert.True(_client.IndexExists(version1Index.VersionedName).Exists);
                
                var version1Repository = new EmployeeRepository(_client, version1Index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Default);
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                using (new DisposableAction(() => version2Index.Delete())) {
                    version2Index.Configure();
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);
                    
                    // swap the alias so we write to v1 and v2 and try to reindex.
                    await _client.AliasAsync(x => x
                        .Remove(a => a.Alias(version1Index.Name).Index(version1Index.VersionedName))
                        .Add(a => a.Alias(version2Index.Name).Index(version2Index.VersionedName)));

                    var version2Repository = new EmployeeRepository(_client, version2Index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());
                    await version2Repository.AddAsync(EmployeeGenerator.Generate());
                    await _client.RefreshAsync();
                    
                    var countResponse = await _client.CountAsync(d => d.Index(version1Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(1, countResponse.Count);

                    countResponse = await _client.CountAsync(d => d.Index(version2Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(1, countResponse.Count);
                    
                    // swap back the alias
                    await _client.AliasAsync(x => x
                        .Remove(a => a.Alias(version2Index.Name).Index(version2Index.VersionedName))
                        .Add(a => a.Alias(version1Index.Name).Index(version1Index.VersionedName)));

                    Assert.Equal(1, version2Index.GetCurrentVersion());

                    // alias should still point to the old version until reindex
                    var aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version2Index.Name));
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
                }
            }
        }

        [Fact]
        public async Task CanReindexVersionedIndexWithUpdatedDocs() {
            var version1Index = new VersionedEmployeeIndex(_client, 1, Log);
            version1Index.Delete();

            var version2Index = new VersionedEmployeeIndex(_client, 2, Log);
            version2Index.Delete();

            using (new DisposableAction(() => version1Index.Delete())) {
                version1Index.Configure();
                Assert.True(_client.IndexExists(version1Index.VersionedName).Exists);

                var repository = new EmployeeRepository(_client, version1Index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());
                var employee = await repository.AddAsync(EmployeeGenerator.Default);
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                using (new DisposableAction(() => version2Index.Delete())) {
                    version2Index.Configure();
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);
                    Assert.Equal(1, version2Index.GetCurrentVersion());

                    // alias should still point to the old version until reindex
                    var aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version1Index.VersionedName, aliasResponse.Indices.First().Key);

                    var countdown = new AsyncCountdownEvent(1);
                    var reindexTask = version2Index.ReindexAsync(async (progress, message) => {
                        _logger.Info($"Reindex Progress {progress}%: {message}");
                        if (progress == 95) {
                            countdown.Signal();
                            await SystemClock.SleepAsync(1000);
                        }
                    });

                    // Wait until the first reindex pass is done.
                    await countdown.WaitAsync();
                    Assert.Equal(1, version1Index.GetCurrentVersion());
                    await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: SystemClock.UtcNow));
                    employee.Name = "Updated";
                    await repository.SaveAsync(employee);
                    
                    // Resume after everythings been indexed.
                    await reindexTask;
                    aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version2Index.VersionedName, aliasResponse.Indices.First().Key);

                    Assert.Equal(2, version1Index.GetCurrentVersion());
                    Assert.Equal(2, version2Index.GetCurrentVersion());

                    var countResponse = await _client.CountAsync(d => d.Index(version2Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(2, countResponse.Count);

                    Assert.Equal(employee, await repository.GetByIdAsync(employee.Id));
                    Assert.False(_client.IndexExists(d => d.Index(version1Index.VersionedName)).Exists);
                }
            }
        }

        [Fact]
        public async Task CanReindexVersionedIndexWithDeletedDocs() {
            var version1Index = new VersionedEmployeeIndex(_client, 1, Log);
            version1Index.Delete();

            var version2Index = new VersionedEmployeeIndex(_client, 2, Log);
            version2Index.Delete();

            using (new DisposableAction(() => version1Index.Delete())) {
                version1Index.Configure();
                Assert.True(_client.IndexExists(version1Index.VersionedName).Exists);

                var repository = new EmployeeRepository(_client, version1Index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());
                var employee = await repository.AddAsync(EmployeeGenerator.Default);
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                using (new DisposableAction(() => version2Index.Delete())) {
                    version2Index.Configure();
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);
                    Assert.Equal(1, version2Index.GetCurrentVersion());

                    // alias should still point to the old version until reindex
                    var aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version1Index.VersionedName, aliasResponse.Indices.First().Key);

                    var countdown = new AsyncCountdownEvent(1);
                    var reindexTask = version2Index.ReindexAsync(async (progress, message) => {
                        _logger.Info($"Reindex Progress {progress}%: {message}");
                        if (progress == 95) {
                            countdown.Signal();
                            await SystemClock.SleepAsync(1000);
                        }
                    });

                    // Wait until the first reindex pass is done.
                    await countdown.WaitAsync();
                    Assert.Equal(1, version1Index.GetCurrentVersion());
                    await repository.RemoveAllAsync();
                    await _client.RefreshAsync();

                    // Resume after everythings been indexed.
                    await reindexTask;
                    aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version2Index.Name));
                    Assert.True(aliasResponse.IsValid, aliasResponse.GetErrorMessage());
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version2Index.VersionedName, aliasResponse.Indices.First().Key);

                    Assert.Equal(2, version1Index.GetCurrentVersion());
                    Assert.Equal(2, version2Index.GetCurrentVersion());

                    var countResponse = await _client.CountAsync(d => d.Index(version1Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.ConnectionStatus.HttpStatusCode == 404, countResponse.GetErrorMessage());
                    Assert.Equal(0, countResponse.Count);

                    countResponse = await _client.CountAsync(d => d.Index(version2Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid, countResponse.GetErrorMessage());
                    Assert.Equal(1, countResponse.Count);

                    Assert.Equal(employee, await repository.GetByIdAsync(employee.Id));
                    Assert.False(_client.IndexExists(d => d.Index(version1Index.VersionedName)).Exists);
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
                
                indexCountResponse = await _client.CountAsync(d => d.Index(version1Index.GetVersionedIndex(utcNow, 1)));
                _logger.Trace(() => indexCountResponse.GetRequest());
                Assert.True(indexCountResponse.IsValid);
                Assert.Equal(1, indexCountResponse.Count);

                using (new DisposableAction(() => version2Index.Delete())) {
                    version2Index.Configure();
                    Assert.Equal(1, version2Index.GetCurrentVersion());
                    var version2Repository = new EmployeeRepository(_client, version2Index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());

                    // Make sure we write to the old index.
                    await version2Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow));
                    await _client.RefreshAsync();

                    aliasCountResponse = await _client.CountAsync(d => d.Index(version1Index.Name));
                    _logger.Trace(() => aliasCountResponse.GetRequest());
                    Assert.True(aliasCountResponse.IsValid);
                    Assert.Equal(2, aliasCountResponse.Count);

                    indexCountResponse = await _client.CountAsync(d => d.Index(version1Index.GetVersionedIndex(utcNow, 1)));
                    _logger.Trace(() => indexCountResponse.GetRequest());
                    Assert.True(indexCountResponse.IsValid);
                    Assert.Equal(2, indexCountResponse.Count);

                    var existsResponse = await _client.IndexExistsAsync(d => d.Index(version2Index.GetVersionedIndex(utcNow, 2)));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.False(existsResponse.Exists);

                    // alias should still point to the old version until reindex
                    var aliasesResponse = await _client.GetAliasesAsync(a => a.Index(version1Index.GetIndex(employee.CreatedUtc)));
                    _logger.Trace(() => aliasesResponse.GetRequest());
                    Assert.True(aliasesResponse.IsValid);
                    Assert.Equal(version1Index.GetVersionedIndex(employee.CreatedUtc, 1), aliasesResponse.Indices.Single().Key);

                    var aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                    aliases.Sort();
                    Assert.Equal(GetExpectedEmployeeDailyAliases(version1Index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

                    await version2Index.ReindexAsync((progress, message) => {
                        _logger.Info($"Reindex Progress {progress}%: {message}");
                        return Task.CompletedTask;
                    });

                    await _client.RefreshAsync();
                    Assert.Equal(2, version1Index.GetCurrentVersion());
                    Assert.Equal(2, version2Index.GetCurrentVersion());

                    aliasesResponse = await _client.GetAliasesAsync(a => a.Index(version1Index.GetIndex(employee.CreatedUtc)));
                    _logger.Trace(() => aliasesResponse.GetRequest());
                    Assert.True(aliasesResponse.IsValid);
                    Assert.Equal(version1Index.GetVersionedIndex(employee.CreatedUtc, 2), aliasesResponse.Indices.Single().Key);

                    aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                    aliases.Sort();
                    Assert.Equal(GetExpectedEmployeeDailyAliases(version1Index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));
                    
                    existsResponse = await _client.IndexExistsAsync(d => d.Index(version1Index.GetVersionedIndex(utcNow, 1)));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.False(existsResponse.Exists);

                    existsResponse = await _client.IndexExistsAsync(d => d.Index(version2Index.GetVersionedIndex(utcNow, 2)));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);
                }
            }
        }
        
        [Fact]
        public async Task CanReindexTimeSeriesIndexWithCorrectMappings() {
            var version1Index = new DailyEmployeeIndex(_client, 1, Log);
            version1Index.Delete();

            var version2Index = new DailyEmployeeIndex(_client, 2, Log);
            version2Index.DiscardIndexesOnReindex = false;
            version2Index.Delete();

            using (new DisposableAction(() => version1Index.Delete())) {
                version1Index.Configure();
                var version1Repository = new EmployeeRepository(_client, version1Index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());

                var utcNow = SystemClock.UtcNow;
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow));
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();
                
                using (new DisposableAction(() => version2Index.Delete())) {
                    version2Index.Configure();
                    
                    await version2Index.ReindexAsync((progress, message) => {
                        _logger.Info($"Reindex Progress {progress}%: {message}");
                        return Task.CompletedTask;
                    });

                    await _client.RefreshAsync();
                    var existsResponse = await _client.IndexExistsAsync(d => d.Index(version1Index.GetVersionedIndex(utcNow, 1)));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);

                    var mappingResponse = await _client.GetMappingAsync<Employee>(m => m.Index(version1Index.GetVersionedIndex(utcNow, 1)));
                    _logger.Trace(() => mappingResponse.GetRequest());
                    Assert.True(mappingResponse.IsValid);
                    Assert.NotNull(mappingResponse.Mappings);
                    
                    existsResponse = await _client.IndexExistsAsync(d => d.Index(version2Index.GetVersionedIndex(utcNow, 2)));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);
                    
                    string version1Mappings = ToJson(mappingResponse.Mappings);
                    mappingResponse = await _client.GetMappingAsync<Employee>(m => m.Index(version1Index.GetVersionedIndex(utcNow, 2)));
                    _logger.Trace(() => mappingResponse.GetRequest());
                    Assert.True(mappingResponse.IsValid);
                    Assert.NotNull(mappingResponse.Mappings);
                    Assert.Equal(version1Mappings, ToJson(mappingResponse.Mappings).Replace("-v2", "-v1"));
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

        [Fact]
        public async Task MaintainDailyIndexes() {
            var index = new DailyEmployeeIndex(_client, 1, Log);
            index.Delete();

            using (new DisposableAction(() => index.Delete())) {
                index.Configure();
                var repository = new EmployeeRepository(_client, index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());

                SystemClock.UtcNowFunc = () => DateTime.UtcNow.SubtractDays(15);
                var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: SystemClock.UtcNow));
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                index.Maintain();
                Assert.Equal(1, index.GetCurrentVersion());
                var existsResponse = await _client.IndexExistsAsync(index.GetIndex(employee.CreatedUtc));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                var aliasesResponse = await _client.GetAliasesAsync(a => a.Index(index.GetIndex(employee.CreatedUtc)));
                _logger.Trace(() => aliasesResponse.GetRequest());
                Assert.True(aliasesResponse.IsValid);
                Assert.Equal(1, aliasesResponse.Indices.Count);
                var aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                aliases.Sort();
                Assert.Equal(GetExpectedEmployeeDailyAliases(index, SystemClock.UtcNow, employee.CreatedUtc), String.Join(", ", aliases));

                SystemClock.UtcNowFunc = () => DateTime.UtcNow.SubtractDays(9);
                index.MaxIndexAge = TimeSpan.FromDays(10);
                index.Maintain();
                existsResponse = await _client.IndexExistsAsync(index.GetIndex(employee.CreatedUtc));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                aliasesResponse = await _client.GetAliasesAsync(a => a.Index(index.GetIndex(employee.CreatedUtc)));
                _logger.Trace(() => aliasesResponse.GetRequest());
                Assert.True(aliasesResponse.IsValid);
                Assert.Equal(1, aliasesResponse.Indices.Count);
                aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                aliases.Sort();
                Assert.Equal(GetExpectedEmployeeDailyAliases(index, SystemClock.UtcNow, employee.CreatedUtc), String.Join(", ", aliases));

                SystemClock.Reset();
                index.Maintain();
                existsResponse = await _client.IndexExistsAsync(index.GetIndex(employee.CreatedUtc));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.False(existsResponse.Exists);

                aliasesResponse = await _client.GetAliasesAsync(a => a.Index(index.GetIndex(employee.CreatedUtc)));
                _logger.Trace(() => aliasesResponse.GetRequest());
                Assert.True(aliasesResponse.IsValid);
                Assert.Equal(0, aliasesResponse.Indices.Count);
            }
        }
        
        [Fact]
        public async Task MaintainMonthlyIndexes() {
            var index = new MonthlyEmployeeIndex(_client, 1, Log);
            index.Delete();

            var utcNow = SystemClock.UtcNow;
            using (new DisposableAction(() => index.Delete())) {
                index.Configure();
                var repository = new EmployeeRepository(_client, index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());

                for (int i = 0; i < 4; i++) {
                    var created = utcNow.SubtractMonths(i);
                    var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: created));
                    Assert.NotNull(employee?.Id);
                    
                    Assert.Equal(1, index.GetCurrentVersion());
                    var existsResponse = await _client.IndexExistsAsync(index.GetIndex(employee.CreatedUtc));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);

                    var aliasesResponse = await _client.GetAliasesAsync(a => a.Index(index.GetIndex(employee.CreatedUtc)));
                    _logger.Trace(() => aliasesResponse.GetRequest());
                    Assert.True(aliasesResponse.IsValid);
                    Assert.Equal(1, aliasesResponse.Indices.Count);

                    var aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                    aliases.Sort();
                    
                    Assert.Equal(GetExpectedEmployeeMonthlyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));
                }

                index.Maintain();

                for (int i = 0; i < 4; i++) {
                    var created = utcNow.SubtractMonths(i);
                    var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: created));
                    Assert.NotNull(employee?.Id);

                    Assert.Equal(1, index.GetCurrentVersion());
                    var existsResponse = await _client.IndexExistsAsync(index.GetIndex(employee.CreatedUtc));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);

                    var aliasesResponse = await _client.GetAliasesAsync(a => a.Index(index.GetIndex(employee.CreatedUtc)));
                    _logger.Trace(() => aliasesResponse.GetRequest());
                    Assert.True(aliasesResponse.IsValid);
                    Assert.Equal(1, aliasesResponse.Indices.Count);

                    var aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                    aliases.Sort();
                    
                    Assert.Equal(GetExpectedEmployeeMonthlyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));
                }
            }
        }
        
        [Fact]
        public async Task DailyAliasMaxAge() {
            SystemClock.UtcNowFunc = () => DateTime.UtcNow.EndOfMonth();
            var index = new DailyEmployeeIndex(_client, 1, Log);
            index.MaxIndexAge = TimeSpan.FromDays(45);
            index.Delete();

            using (new DisposableAction(() => index.Delete())) {
                index.Configure();
                var version1Repository = new EmployeeRepository(_client, index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());
                
                var utcNow = SystemClock.UtcNow;
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow));
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                var existsResponse = await _client.IndexExistsAsync(index.GetIndex(employee.CreatedUtc));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                var aliasesResponse = await _client.GetAliasesAsync(a => a.Index(index.GetIndex(employee.CreatedUtc)));
                _logger.Trace(() => aliasesResponse.GetRequest());
                Assert.True(aliasesResponse.IsValid);
                Assert.Equal(1, aliasesResponse.Indices.Count);
                var aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                aliases.Sort();
                Assert.Equal(GetExpectedEmployeeDailyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

                employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractDays(2)));
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                existsResponse = await _client.IndexExistsAsync(index.GetIndex(employee.CreatedUtc));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                aliasesResponse = await _client.GetAliasesAsync(a => a.Index(index.GetIndex(employee.CreatedUtc)));
                _logger.Trace(() => aliasesResponse.GetRequest());
                Assert.True(aliasesResponse.IsValid);
                Assert.Equal(1, aliasesResponse.Indices.Count);
                aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                aliases.Sort();
                Assert.Equal(GetExpectedEmployeeDailyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

                employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractDays(35)));
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                existsResponse = await _client.IndexExistsAsync(index.GetIndex(employee.CreatedUtc));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                aliasesResponse = await _client.GetAliasesAsync(a => a.Index(index.GetIndex(employee.CreatedUtc)));
                _logger.Trace(() => aliasesResponse.GetRequest());
                Assert.True(aliasesResponse.IsValid);
                Assert.Equal(1, aliasesResponse.Indices.Count);
                aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                aliases.Sort();
                Assert.Equal(GetExpectedEmployeeDailyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));
            }
        }
        
        [Fact]
        public async Task MonthlyAliasMaxAge() {
            SystemClock.UtcNowFunc = () => DateTime.UtcNow.EndOfMonth();
            var index = new MonthlyEmployeeIndex(_client, 1, Log);
            index.MaxIndexAge = TimeSpan.FromDays(90);
            index.Delete();

            using (new DisposableAction(() => index.Delete())) {
                index.Configure();
                var repository = new EmployeeRepository(_client, index.Employee, _cache, Log.CreateLogger<EmployeeRepository>());

                var utcNow = SystemClock.UtcNow;
                var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow));
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                var existsResponse = await _client.IndexExistsAsync(index.GetIndex(employee.CreatedUtc));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                var aliasesResponse = await _client.GetAliasesAsync(a => a.Index(index.GetIndex(employee.CreatedUtc)));
                _logger.Trace(() => aliasesResponse.GetRequest());
                Assert.True(aliasesResponse.IsValid);
                Assert.Equal(1, aliasesResponse.Indices.Count);
                var aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                aliases.Sort();
                Assert.Equal(GetExpectedEmployeeMonthlyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

                employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractDays(2)));
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                existsResponse = await _client.IndexExistsAsync(index.GetIndex(employee.CreatedUtc));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                aliasesResponse = await _client.GetAliasesAsync(a => a.Index(index.GetIndex(employee.CreatedUtc)));
                _logger.Trace(() => aliasesResponse.GetRequest());
                Assert.True(aliasesResponse.IsValid);
                Assert.Equal(1, aliasesResponse.Indices.Count);
                aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                aliases.Sort();
                Assert.Equal(GetExpectedEmployeeMonthlyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

                employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractDays(35)));
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                existsResponse = await _client.IndexExistsAsync(index.GetIndex(employee.CreatedUtc));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                aliasesResponse = await _client.GetAliasesAsync(a => a.Index(index.GetIndex(employee.CreatedUtc)));
                _logger.Trace(() => aliasesResponse.GetRequest());
                Assert.True(aliasesResponse.IsValid);
                Assert.Equal(1, aliasesResponse.Indices.Count);
                aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                aliases.Sort();
                Assert.Equal(GetExpectedEmployeeMonthlyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));
            }
        }

        [Fact]
        public async Task DailyIndexMaxAge() {
            var index = new DailyEmployeeIndex(_client, 1, Log);
            index.MaxIndexAge = TimeSpan.FromDays(1);
            index.Delete();
            
            using (new DisposableAction(() => index.Delete())) {
                index.Configure();

                var utcNow = SystemClock.UtcNow;
                index.EnsureIndex(utcNow);
                var existsResponse = await _client.IndexExistsAsync(index.GetIndex(utcNow));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                Assert.Throws<ArgumentException>(() => index.EnsureIndex(utcNow.SubtractDays(1)));
                existsResponse = await _client.IndexExistsAsync(index.GetIndex(utcNow.SubtractDays(1)));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.False(existsResponse.Exists);
            }
        }


        [Fact]
        public async Task MonthlyIndexMaxAge() {
            var index = new MonthlyEmployeeIndex(_client, 1, Log);
            index.MaxIndexAge = TimeSpan.FromDays(31);
            index.Delete();

            using (new DisposableAction(() => index.Delete())) {
                index.Configure();

                var utcNow = SystemClock.UtcNow;
                index.EnsureIndex(utcNow);
                var existsResponse = await _client.IndexExistsAsync(index.GetIndex(utcNow));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                Assert.Throws<ArgumentException>(() => index.EnsureIndex(utcNow.SubtractDays(35)));
                existsResponse = await _client.IndexExistsAsync(index.GetIndex(utcNow.SubtractDays(35)));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.False(existsResponse.Exists);
            }
        }
        
        private string GetExpectedEmployeeDailyAliases(DailyIndex index, DateTime utcNow, DateTime indexDateUtc) {
            double totalDays = utcNow.Date.Subtract(indexDateUtc.Date).TotalDays;
            var aliases = new List<string> { index.Name, index.GetIndex(indexDateUtc) };
            if (totalDays <= 30)
                aliases.Add($"{index.Name}-last30days");
            if (totalDays <= 7)
                aliases.Add($"{index.Name}-last7days");
            if (totalDays <= 1)
                aliases.Add($"{index.Name}-today");

            aliases.Sort();
            return String.Join(", ", aliases);
        }

        private string GetExpectedEmployeeMonthlyAliases(DailyIndex index, DateTime utcNow, DateTime indexDateUtc) {
            double totalMonths = utcNow.StartOfMonth().Subtract(indexDateUtc.StartOfMonth()).GetTotalMonths();
            var aliases = new List<string> { index.Name, index.GetIndex(indexDateUtc) };
            if (totalMonths <= 1) {
                aliases.Add($"{index.Name}-last30days");
                aliases.Add($"{index.Name}-last7days");
                aliases.Add($"{index.Name}-today");
            }

            if (totalMonths <= 2)
                aliases.Add($"{index.Name}-last60days");
            
            aliases.Sort();

            return String.Join(", ", aliases);
        }

        private string ToJson(object data) {
            return Encoding.Default.GetString(_client.Serializer.Serialize(data));
        }
    }
}