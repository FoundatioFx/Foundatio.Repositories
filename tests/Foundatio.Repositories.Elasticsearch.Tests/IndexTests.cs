using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Utility;
using Nest;
using Nito.AsyncEx;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class IndexTests : ElasticRepositoryTestBase {
        public IndexTests(ITestOutputHelper output) : base(output) {
            Log.SetLogLevel<EmployeeRepository>(LogLevel.Warning);
            RemoveDataAsync(configureIndexes: false).GetAwaiter().GetResult();
        }

        [Fact]
        public void CannotAddInvalidTypeToDailyIndex() {
            Assert.Throws<ArgumentException>(() => new DailyEmployeeIndexWithWrongEmployeeType(_configuration, 1));
        }

        [Theory]
        [MemberData("AliasesDatesToCheck")]
        public async Task CanCreateDailyAliases(DateTime utcNow) {
            SystemClock.Test.SetFixedTime(utcNow);
            var index = new DailyEmployeeIndex(_configuration, 1);
            await index.DeleteAsync();
            
            using (new DisposableAction(() => index.DeleteAsync().GetAwaiter().GetResult())) {
                await index.ConfigureAsync();
                var repository = new EmployeeRepository(index.Employee);

                for (int i = 0; i < 35; i += 5) {
                    var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractDays(i)));
                    Assert.NotNull(employee?.Id);

                    Assert.Equal(1, await index.GetCurrentVersionAsync());
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

        [Theory]
        [MemberData("AliasesDatesToCheck")]
        public async Task CanCreateMonthlyAliases(DateTime utcNow) {
            SystemClock.Test.SetFixedTime(utcNow);
            var index = new MonthlyEmployeeIndex(_configuration, 1);
            await index.DeleteAsync();
            
            using (new DisposableAction(() => index.DeleteAsync().GetAwaiter().GetResult())) {
                await index.ConfigureAsync();
                var repository = new EmployeeRepository(index.Employee);

                for (int i = 0; i < 4; i++) {
                    var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractMonths(i)));
                    Assert.NotNull(employee?.Id);

                    Assert.Equal(1, await index.GetCurrentVersionAsync());
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

        public static IEnumerable<object[]> AliasesDatesToCheck => new List<object[]> {
            new object[] { new DateTime(2016, 2, 29, 0, 0, 0, DateTimeKind.Utc) },
            new object[] { new DateTime(2016, 8, 31, 0, 0, 0, DateTimeKind.Utc) },
            new object[] { new DateTime(2016, 9, 1, 0, 0, 0, DateTimeKind.Utc) },
            new object[] { DateTime.UtcNow }
        }.ToArray();

        [Fact(Skip = "This will only work if the mapping is manually updated.")]
        public async Task CanReindexSameIndex() {
            var index = new EmployeeIndex(_configuration);
            await index.DeleteAsync();

            using (new DisposableAction(() => index.DeleteAsync().GetAwaiter().GetResult())) {
                await index.ConfigureAsync();
                Assert.True(_client.IndexExists(index.Name).Exists);

                var repository = new EmployeeRepository(index.Employee);
                var employee = await repository.AddAsync(EmployeeGenerator.Default);
                await _client.RefreshAsync();
                Assert.NotNull(employee?.Id);

                var countResponse = await _client.CountAsync(d => d.Index(index.Name));
                _logger.Trace(() => countResponse.GetRequest());
                Assert.True(countResponse.IsValid);
                Assert.Equal(1, countResponse.Count);

                var mappingResponse = await _client.GetMappingAsync<Employee>(m => m.Index(index.Name));
                _logger.Trace(() => mappingResponse.GetRequest());
                Assert.True(mappingResponse.IsValid);
                Assert.NotNull(mappingResponse.Mappings);

                var newIndex = new EmployeeIndexWithYearsEmployed(_configuration);
                await newIndex.ReindexAsync();

                countResponse = await _client.CountAsync(d => d.Index(index.Name));
                _logger.Trace(() => countResponse.GetRequest());
                Assert.True(countResponse.IsValid);
                Assert.Equal(1, countResponse.Count);

                string version1Mappings = ToJson(mappingResponse.Mappings);
                mappingResponse = await _client.GetMappingAsync<Employee>(m => m.Index(index.Name));
                _logger.Trace(() => mappingResponse.GetRequest());
                Assert.True(mappingResponse.IsValid);
                Assert.NotNull(mappingResponse.Mappings);
                Assert.NotEqual(version1Mappings, ToJson(mappingResponse.Mappings));
            }
        }
        
        [Fact]
        public async Task CanResumeReindex() {
            const int numberOfEmployeesToCreate = 2000;

            var version1Index = new VersionedEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new VersionedEmployeeIndex(_configuration, 2);
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                Assert.True(_client.IndexExists(version1Index.VersionedName).Exists);

                var version1Repository = new EmployeeRepository(version1Index.Employee);
                await version1Repository.AddAsync(EmployeeGenerator.GenerateEmployees(numberOfEmployeesToCreate));
                await _client.RefreshAsync();

                var countResponse = await _client.CountAsync(d => d.Index(version1Index.Name));
                _logger.Trace(() => countResponse.GetRequest());
                Assert.True(countResponse.IsValid);
                Assert.Equal(numberOfEmployeesToCreate, countResponse.Count);

                Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);

                    await Assert.ThrowsAsync<ApplicationException>(async () => await version2Index.ReindexAsync((progress, message) => {
                        _logger.Info("Reindex Progress {0}%: {1}", progress, message);
                        // TODO: Need to make this so it happens randomly in the middle of a batch
                        if (progress >= 45)
                            throw new ApplicationException("Random Error");

                        return Task.CompletedTask;
                    }));

                    Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
                    await version2Index.ReindexAsync();

                    var aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version2Index.VersionedName, aliasResponse.Indices.First().Key);

                    Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

                    countResponse = await _client.CountAsync(d => d.Index(version2Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(numberOfEmployeesToCreate, countResponse.Count);

                    Assert.False(_client.IndexExists(d => d.Index(version1Index.VersionedName)).Exists);
                }
            }
        }


        [Fact]
        public async Task CanReindexVersionedIndex() {
            var version1Index = new VersionedEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new VersionedEmployeeIndex(_configuration, 2);
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                Assert.True(_client.IndexExists(version1Index.VersionedName).Exists);

                var indexes = _client.GetIndicesPointingToAlias(version1Index.Name);
                Assert.Equal(1, indexes.Count);

                var aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version1Index.Name));
                _logger.Trace(() => aliasResponse.GetRequest());
                Assert.True(aliasResponse.IsValid);
                Assert.Equal(1, aliasResponse.Indices.Count);
                Assert.Equal(version1Index.VersionedName, aliasResponse.Indices.First().Key);

                var version1Repository = new EmployeeRepository(version1Index.Employee);
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Default);
                await _client.RefreshAsync();
                Assert.NotNull(employee?.Id);

                var countResponse = await _client.CountAsync(d => d.Index(version1Index.Name));
                _logger.Trace(() => countResponse.GetRequest());
                Assert.True(countResponse.IsValid);
                Assert.Equal(1, countResponse.Count);

                Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);

                    // Make sure we can write to the index still. Should go to the old index until after the reindex is complete.
                    var version2Repository = new EmployeeRepository(version2Index.Employee);
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

                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

                    // alias should still point to the old version until reindex
                    aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version1Index.VersionedName, aliasResponse.Indices.First().Key);

                    await version2Index.ReindexAsync();

                    aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version2Index.VersionedName, aliasResponse.Indices.First().Key);

                    Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

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
            var version1Index = new VersionedEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new VersionedEmployeeIndex(_configuration, 2);
            version2Index.DiscardIndexesOnReindex = false;
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                var version1Repository = new EmployeeRepository(version1Index.Employee);

                var utcNow = SystemClock.UtcNow;
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow));
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();

                    await version2Index.ReindexAsync();

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
            var version1Index = new VersionedEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new VersionedEmployeeIndex(_configuration, 2);
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                Assert.True(_client.IndexExists(version1Index.VersionedName).Exists);

                var version1Repository = new EmployeeRepository(version1Index.Employee);
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Default);
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);

                    // swap the alias so we write to v1 and v2 and try to reindex.
                    await _client.AliasAsync(x => x
                        .Remove(a => a.Alias(version1Index.Name).Index(version1Index.VersionedName))
                        .Add(a => a.Alias(version2Index.Name).Index(version2Index.VersionedName)));

                    var version2Repository = new EmployeeRepository(version2Index.Employee);
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

                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

                    // alias should still point to the old version until reindex
                    var aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version1Index.VersionedName, aliasResponse.Indices.First().Key);

                    await version2Index.ReindexAsync();

                    aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version2Index.VersionedName, aliasResponse.Indices.First().Key);

                    Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

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
            var version1Index = new VersionedEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new VersionedEmployeeIndex(_configuration, 2);
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                Assert.True(_client.IndexExists(version1Index.VersionedName).Exists);

                var repository = new EmployeeRepository(version1Index.Employee);
                var employee = await repository.AddAsync(EmployeeGenerator.Default);
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);
                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

                    // alias should still point to the old version until reindex
                    var aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version1Index.VersionedName, aliasResponse.Indices.First().Key);

                    var countdown = new AsyncCountdownEvent(1);
                    var reindexTask = version2Index.ReindexAsync((progress, message) => {
                        _logger.Info($"Reindex Progress {progress}%: {message}");
                        if (progress == 95) {
                            countdown.Signal();
                            SystemClock.Sleep(1000);
                        }

                        return Task.CompletedTask;
                    });

                    // Wait until the first reindex pass is done.
                    await countdown.WaitAsync();
                    Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
                    await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: SystemClock.UtcNow));
                    employee.Name = "Updated";
                    await repository.SaveAsync(employee);

                    // Resume after everythings been indexed.
                    await reindexTask;
                    aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version2Index.VersionedName, aliasResponse.Indices.First().Key);

                    Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

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
            var version1Index = new VersionedEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new VersionedEmployeeIndex(_configuration, 2);
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                Assert.True(_client.IndexExists(version1Index.VersionedName).Exists);

                var repository = new EmployeeRepository(version1Index.Employee);
                var employee = await repository.AddAsync(EmployeeGenerator.Default);
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);
                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

                    // alias should still point to the old version until reindex
                    var aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version1Index.VersionedName, aliasResponse.Indices.First().Key);

                    var countdown = new AsyncCountdownEvent(1);
                    var reindexTask = version2Index.ReindexAsync((progress, message) => {
                        _logger.Info($"Reindex Progress {progress}%: {message}");
                        if (progress == 95) {
                            countdown.Signal();
                            SystemClock.Sleep(1000);
                        }

                        return Task.CompletedTask;
                    });

                    // Wait until the first reindex pass is done.
                    await countdown.WaitAsync();
                    Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
                    await repository.RemoveAllAsync();
                    await _client.RefreshAsync();

                    // Resume after everythings been indexed.
                    await reindexTask;
                    aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Alias(version2Index.Name));
                    Assert.True(aliasResponse.IsValid, aliasResponse.GetErrorMessage());
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version2Index.VersionedName, aliasResponse.Indices.First().Key);

                    Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

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
            var version1Index = new DailyEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new DailyEmployeeIndex(_configuration, 2);
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                var version1Repository = new EmployeeRepository(version1Index.Employee);

                var utcNow = SystemClock.UtcNow;
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow));
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

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

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());
                    var version2Repository = new EmployeeRepository(version2Index.Employee);

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

                    await version2Index.ReindexAsync();

                    Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

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
            var version1Index = new DailyEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new DailyEmployeeIndex(_configuration, 2);
            version2Index.DiscardIndexesOnReindex = false;
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                var version1Repository = new EmployeeRepository(version1Index.Employee);

                var utcNow = SystemClock.UtcNow;
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow));
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();

                    await version2Index.ReindexAsync();

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

        [Fact(Skip = "TODO: Implement reindexing monthly into daily indexes")]
        public Task CanReindexMonthlyIntoDailyIndex() {
            throw new NotImplementedException();
        }

        [Fact]
        public async Task GetByDateBasedIndex() {
            await _configuration.DailyLogEvents.ConfigureAsync();

            var indexes = await _client.GetIndicesPointingToAliasAsync(_configuration.DailyLogEvents.Name);
            Assert.Equal(0, indexes.Count);

            var alias = await _client.GetAliasAsync(descriptor => descriptor.Alias(_configuration.DailyLogEvents.Name));
            _logger.Trace(() => alias.GetRequest());
            Assert.False(alias.IsValid);
            Assert.Equal(0, alias.Indices.Count);

            var utcNow = SystemClock.UtcNow;
            var repository = new DailyLogEventRepository(_configuration);
            var logEvent = await repository.AddAsync(LogEventGenerator.Generate(createdUtc: utcNow));
            Assert.NotNull(logEvent?.Id);

            logEvent = await repository.AddAsync(LogEventGenerator.Generate(createdUtc: utcNow.SubtractDays(1)));
            Assert.NotNull(logEvent?.Id);

            await _client.RefreshAsync();
            alias = await _client.GetAliasAsync(descriptor => descriptor.Alias(_configuration.DailyLogEvents.Name));
            _logger.Trace(() => alias.GetRequest());
            Assert.True(alias.IsValid);
            Assert.Equal(2, alias.Indices.Count);

            indexes = await _client.GetIndicesPointingToAliasAsync(_configuration.DailyLogEvents.Name);
            Assert.Equal(2, indexes.Count);

            await repository.RemoveAllAsync();
            await _client.RefreshAsync();

            Assert.Equal(0, await repository.CountAsync());
        }

        [Fact]
        public async Task MaintainWillCreateAliasOnVersionedIndex() {
            var version1Index = new VersionedEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new VersionedEmployeeIndex(_configuration, 2);
            await version2Index.DeleteAsync();

            // Indexes don't exist yet so the current version will be the index version.
            Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
            Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                Assert.True(_client.IndexExists(version1Index.VersionedName).Exists);
                Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);
                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

                    // delete all aliases
                    await _configuration.Cache.RemoveAllAsync();
                    await DeleteAliases(version1Index.VersionedName);
                    await DeleteAliases(version2Index.VersionedName);

                    await _client.RefreshAsync();
                    var aliasesResponse = await _client.GetAliasesAsync(a => a.Indices(version1Index.VersionedName, version2Index.VersionedName));
                    Assert.Equal(0, aliasesResponse.Indices.SelectMany(i => i.Value).Count());

                    // Indexes exist but no alias so the oldest index version will be used.
                    Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

                    await version1Index.MaintainAsync();
                    aliasesResponse = await _client.GetAliasesAsync(a => a.Indices(version1Index.VersionedName));
                    Assert.Equal(1, aliasesResponse.Indices.Single().Value.Count);
                    aliasesResponse = await _client.GetAliasesAsync(a => a.Indices(version2Index.VersionedName));
                    Assert.Equal(0, aliasesResponse.Indices.Single().Value.Count);

                    Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());
                }
            }
        }

        [Fact]
        public async Task MaintainWillCreateAliasesOnTimeSeriesIndex() {
            SystemClock.Test.SetFixedTime(SystemClock.UtcNow);
            var version1Index = new DailyEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new DailyEmployeeIndex(_configuration, 2);
            await version2Index.DeleteAsync();

            // Indexes don't exist yet so the current version will be the index version.
            Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
            Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                await version1Index.EnsureIndexAsync(SystemClock.UtcNow);
                Assert.True(_client.IndexExists(version1Index.GetVersionedIndex(SystemClock.UtcNow)).Exists);
                Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

                // delete all aliases
                await _configuration.Cache.RemoveAllAsync();
                await DeleteAliases(version1Index.GetVersionedIndex(SystemClock.UtcNow));

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    await version2Index.EnsureIndexAsync(SystemClock.UtcNow);
                    Assert.True(_client.IndexExists(version2Index.GetVersionedIndex(SystemClock.UtcNow)).Exists);
                    Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

                    // delete all aliases
                    await _configuration.Cache.RemoveAllAsync();
                    await DeleteAliases(version2Index.GetVersionedIndex(SystemClock.UtcNow));

                    await _client.RefreshAsync();
                    var aliasesResponse = await _client.GetAliasesAsync(a => a.Indices(version1Index.GetVersionedIndex(SystemClock.UtcNow), version2Index.GetVersionedIndex(SystemClock.UtcNow)));
                    Assert.Equal(0, aliasesResponse.Indices.SelectMany(i => i.Value).Count());

                    // Indexes exist but no alias so the oldest index version will be used.
                    Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

                    await version1Index.MaintainAsync();
                    aliasesResponse = await _client.GetAliasesAsync(a => a.Indices(version1Index.GetVersionedIndex(SystemClock.UtcNow)));
                    Assert.Equal(version1Index.Aliases.Count + 1, aliasesResponse.Indices.Single().Value.Count);
                    aliasesResponse = await _client.GetAliasesAsync(a => a.Indices(version2Index.GetVersionedIndex(SystemClock.UtcNow)));
                    Assert.Equal(0, aliasesResponse.Indices.Single().Value.Count);

                    Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());
                }
            }
        }

        private async Task DeleteAliases(string index) {
            var aliasesResponse = await _client.GetAliasesAsync(a => a.Indices(index));
            var aliases = aliasesResponse.Indices.Single(a => a.Key == index).Value.Select(a => a.Name).ToList();
            foreach (var alias in aliases) {
                await _client.DeleteAliasAsync(new DeleteAliasRequest(index, alias));
            }
        }

        [Fact]
        public async Task MaintainDailyIndexes() {
            var index = new DailyEmployeeIndex(_configuration, 1);
            await index.DeleteAsync();

            using (new DisposableAction(() => index.DeleteAsync().GetAwaiter().GetResult())) {
                await index.ConfigureAsync();
                var repository = new EmployeeRepository(index.Employee);

                SystemClock.Test.AddTime(TimeSpan.FromDays(15));
                var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: SystemClock.UtcNow));
                Assert.NotNull(employee?.Id);
                await _client.RefreshAsync();

                await index.MaintainAsync();
                Assert.Equal(1, await index.GetCurrentVersionAsync());
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

                SystemClock.Test.AddTime(TimeSpan.FromDays(9));
                index.MaxIndexAge = TimeSpan.FromDays(10);
                await index.MaintainAsync();
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

                SystemClock.Instance = DefaultSystemClock.Instance;
                await index.MaintainAsync();
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
            SystemClock.Test.SetFixedTime(new DateTime(2016, 8, 31, 0, 0, 0, DateTimeKind.Utc));
            var index = new MonthlyEmployeeIndex(_configuration, 1);
            index.MaxIndexAge = SystemClock.UtcNow.EndOfMonth() - SystemClock.UtcNow.SubtractMonths(4).StartOfMonth();
            await index.DeleteAsync();

            var utcNow = SystemClock.UtcNow;
            using (new DisposableAction(() => index.DeleteAsync().GetAwaiter().GetResult())) {
                await index.ConfigureAsync();
                var repository = new EmployeeRepository(index.Employee);

                for (int i = 0; i < 4; i++) {
                    var created = utcNow.SubtractMonths(i);
                    var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: created));
                    Assert.NotNull(employee?.Id);

                    Assert.Equal(1, await index.GetCurrentVersionAsync());
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

                await index.MaintainAsync();

                for (int i = 0; i < 4; i++) {
                    var created = utcNow.SubtractMonths(i);
                    var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: created));
                    Assert.NotNull(employee?.Id);

                    Assert.Equal(1, await index.GetCurrentVersionAsync());
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
        public async Task MaintainOnlyOldIndexes() {
            SystemClock.Test.SetFixedTime(SystemClock.UtcNow.EndOfYear());

            var index = new MonthlyEmployeeIndex(_configuration, 1);
            index.MaxIndexAge = SystemClock.UtcNow.EndOfMonth() - SystemClock.UtcNow.SubtractMonths(12).StartOfMonth();

            await index.EnsureIndexAsync(SystemClock.UtcNow.SubtractMonths(12));
            var existsResponse = await _client.IndexExistsAsync(index.GetIndex(SystemClock.UtcNow.SubtractMonths(12)));
            _logger.Trace(() => existsResponse.GetRequest());
            Assert.True(existsResponse.IsValid);
            Assert.True(existsResponse.Exists);

            index.MaxIndexAge = SystemClock.UtcNow.EndOfMonth() - SystemClock.UtcNow.StartOfMonth();

            await index.MaintainAsync();
            existsResponse = await _client.IndexExistsAsync(index.GetIndex(SystemClock.UtcNow.SubtractMonths(12)));
            _logger.Trace(() => existsResponse.GetRequest());
            Assert.True(existsResponse.IsValid);
            Assert.False(existsResponse.Exists);
        }
        
        [Fact]
        public async Task MaintainOnlyOldIndexesWithNoExistingAliases() {
            SystemClock.Test.SetFixedTime(SystemClock.UtcNow.EndOfYear());

            var index = new MonthlyEmployeeIndex(_configuration, 1);
            index.MaxIndexAge = SystemClock.UtcNow.EndOfMonth() - SystemClock.UtcNow.SubtractMonths(12).StartOfMonth();

            await index.EnsureIndexAsync(SystemClock.UtcNow.SubtractMonths(12));
            var existsResponse = await _client.IndexExistsAsync(index.GetIndex(SystemClock.UtcNow.SubtractMonths(12)));
            _logger.Trace(() => existsResponse.GetRequest());
            Assert.True(existsResponse.IsValid);
            Assert.True(existsResponse.Exists);

            index.MaxIndexAge = SystemClock.UtcNow.EndOfMonth() - SystemClock.UtcNow.StartOfMonth();
            await DeleteAliases(index.GetVersionedIndex(SystemClock.UtcNow.SubtractMonths(12)));

            await index.MaintainAsync();
            existsResponse = await _client.IndexExistsAsync(index.GetIndex(SystemClock.UtcNow.SubtractMonths(12)));
            _logger.Trace(() => existsResponse.GetRequest());
            Assert.True(existsResponse.IsValid);
            Assert.False(existsResponse.Exists);
        }
        
        [Fact]
        public async Task MaintainOnlyOldIndexesWithPartialAliases() {
            SystemClock.Test.SetFixedTime(SystemClock.UtcNow.EndOfYear());

            var index = new MonthlyEmployeeIndex(_configuration, 1);
            index.MaxIndexAge = SystemClock.UtcNow.EndOfMonth() - SystemClock.UtcNow.SubtractMonths(12).StartOfMonth();

            await index.EnsureIndexAsync(SystemClock.UtcNow.SubtractMonths(11));
            await index.EnsureIndexAsync(SystemClock.UtcNow.SubtractMonths(12));
            var existsResponse = await _client.IndexExistsAsync(index.GetIndex(SystemClock.UtcNow.SubtractMonths(12)));
            _logger.Trace(() => existsResponse.GetRequest());
            Assert.True(existsResponse.IsValid);
            Assert.True(existsResponse.Exists);

            index.MaxIndexAge = SystemClock.UtcNow.EndOfMonth() - SystemClock.UtcNow.StartOfMonth();
            await DeleteAliases(index.GetVersionedIndex(SystemClock.UtcNow.SubtractMonths(12)));

            await index.MaintainAsync();
            existsResponse = await _client.IndexExistsAsync(index.GetIndex(SystemClock.UtcNow.SubtractMonths(12)));
            _logger.Trace(() => existsResponse.GetRequest());
            Assert.True(existsResponse.IsValid);
            Assert.False(existsResponse.Exists);
        }

        [Theory]
        [MemberData("AliasesDatesToCheck")]
        public async Task DailyAliasMaxAge(DateTime utcNow) {
            SystemClock.Test.SetFixedTime(utcNow);
            var index = new DailyEmployeeIndex(_configuration, 1);
            index.MaxIndexAge = TimeSpan.FromDays(45);
            await index.DeleteAsync();

            using (new DisposableAction(() => index.DeleteAsync().GetAwaiter().GetResult())) {
                await index.ConfigureAsync();
                var version1Repository = new EmployeeRepository(index.Employee);
                
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

        [Theory]
        [MemberData("AliasesDatesToCheck")]
        public async Task MonthlyAliasMaxAge(DateTime utcNow) {
            SystemClock.Test.SetFixedTime(utcNow);
            var index = new MonthlyEmployeeIndex(_configuration, 1);
            index.MaxIndexAge = TimeSpan.FromDays(90);
            await index.DeleteAsync();

            using (new DisposableAction(() => index.DeleteAsync().GetAwaiter().GetResult())) {
                await index.ConfigureAsync();
                var repository = new EmployeeRepository(index.Employee);
                
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

        [Theory]
        [MemberData("AliasesDatesToCheck")]
        public async Task DailyIndexMaxAge(DateTime utcNow) {
            SystemClock.Test.SetFixedTime(utcNow);

            var index = new DailyEmployeeIndex(_configuration, 1);
            index.MaxIndexAge = TimeSpan.FromDays(1);
            await index.DeleteAsync();

            using (new DisposableAction(() => index.DeleteAsync().GetAwaiter().GetResult())) {
                await index.ConfigureAsync();
                
                await index.EnsureIndexAsync(utcNow);
                var existsResponse = await _client.IndexExistsAsync(index.GetIndex(utcNow));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                await index.EnsureIndexAsync(utcNow.SubtractDays(1));
                existsResponse = await _client.IndexExistsAsync(index.GetIndex(utcNow.SubtractDays(1)));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                await Assert.ThrowsAsync<ArgumentException>(async () => await index.EnsureIndexAsync(utcNow.SubtractDays(2)));
                existsResponse = await _client.IndexExistsAsync(index.GetIndex(utcNow.SubtractDays(2)));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.False(existsResponse.Exists);
            }
        }

        [Theory]
        [MemberData("AliasesDatesToCheck")]
        public async Task MonthlyIndexMaxAge(DateTime utcNow) {
            SystemClock.Test.SetFixedTime(utcNow);

            var index = new MonthlyEmployeeIndex(_configuration, 1);
            index.MaxIndexAge = SystemClock.UtcNow.EndOfMonth() - SystemClock.UtcNow.StartOfMonth();
            await index.DeleteAsync();

            using (new DisposableAction(() => index.DeleteAsync().GetAwaiter().GetResult())) {
                await index.ConfigureAsync();
                
                await index.EnsureIndexAsync(utcNow);
                var existsResponse = await _client.IndexExistsAsync(index.GetIndex(utcNow));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                await index.EnsureIndexAsync(utcNow.Subtract(index.MaxIndexAge.GetValueOrDefault()));
                existsResponse = await _client.IndexExistsAsync(index.GetIndex(utcNow.Subtract(index.MaxIndexAge.GetValueOrDefault())));
                _logger.Trace(() => existsResponse.GetRequest());
                Assert.True(existsResponse.IsValid);
                Assert.True(existsResponse.Exists);

                var endOfTwoMonthsAgo = utcNow.SubtractMonths(2).EndOfMonth();
                await Assert.ThrowsAsync<ArgumentException>(async () => await index.EnsureIndexAsync(endOfTwoMonthsAgo));
                existsResponse = await _client.IndexExistsAsync(index.GetIndex(endOfTwoMonthsAgo));
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
            var aliases = new List<string> { index.Name, index.GetIndex(indexDateUtc) };
            if (new DateTimeRange(utcNow.SubtractDays(1).StartOfMonth(), utcNow.EndOfMonth()).Contains(indexDateUtc))
                aliases.Add($"{index.Name}-today");

            if (new DateTimeRange(utcNow.SubtractDays(7).StartOfMonth(), utcNow.EndOfMonth()).Contains(indexDateUtc))
                aliases.Add($"{index.Name}-last7days");

            if (new DateTimeRange(utcNow.SubtractDays(30).StartOfMonth(), utcNow.EndOfMonth()).Contains(indexDateUtc))
                aliases.Add($"{index.Name}-last30days");

            if (new DateTimeRange(utcNow.SubtractDays(60).StartOfMonth(), utcNow.EndOfMonth()).Contains(indexDateUtc))
                aliases.Add($"{index.Name}-last60days");

            aliases.Sort();
            return String.Join(", ", aliases);
        }

        private string ToJson(object data) {
            return Encoding.Default.GetString(_client.Serializer.Serialize(data));
        }
    }
}