using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Nest;
using Nito.AsyncEx;
using Xunit;
using Xunit.Abstractions;
using LogLevel = Foundatio.Logging.LogLevel;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class ReindexTests : ElasticRepositoryTestBase {
        public ReindexTests(ITestOutputHelper output) : base(output) {
            Log.SetLogLevel<EmployeeRepository>(LogLevel.Warning);
            RemoveDataAsync(configureIndexes: false).GetAwaiter().GetResult();
        }

        [Fact(Skip = "This will only work if the mapping is manually updated.")]
        public async Task CanReindexSameIndex() {
            var index = new EmployeeIndex(_configuration);
            await index.DeleteAsync();

            using (new DisposableAction(() => index.DeleteAsync().GetAwaiter().GetResult())) {
                await index.ConfigureAsync();
                Assert.True(_client.IndexExists(index.Name).Exists);

                var repository = new EmployeeRepository(index.Employee);
                var employee = await repository.AddAsync(EmployeeGenerator.Default);
                await _client.RefreshAsync(Indices.All);
                Assert.NotNull(employee?.Id);

                var countResponse = await _client.CountAsync<Employee>(d => d.Index(index.Name));
                _logger.Trace(() => countResponse.GetRequest());
                Assert.True(countResponse.IsValid);
                Assert.Equal(1, countResponse.Count);

                var mappingResponse = await _client.GetMappingAsync<Employee>(m => m.Index(index.Name));
                _logger.Trace(() => mappingResponse.GetRequest());
                Assert.True(mappingResponse.IsValid);
                Assert.NotNull(mappingResponse.Mappings);

                var newIndex = new EmployeeIndexWithYearsEmployed(_configuration);
                await newIndex.ReindexAsync();

                countResponse = await _client.CountAsync<Employee>(d => d.Index(index.Name));
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
                await _client.RefreshAsync(Indices.All);

                var countResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.Name));
                _logger.Trace(() => countResponse.GetRequest());
                Assert.True(countResponse.IsValid);
                Assert.Equal(numberOfEmployeesToCreate, countResponse.Count);

                Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);

                    // Throw error before second repass.
                    await Assert.ThrowsAsync<ApplicationException>(async () => await version2Index.ReindexAsync((progress, message) => {
                        _logger.Info("Reindex Progress {0}%: {1}", progress, message);
                        if (progress == 91)
                            throw new ApplicationException("Random Error");

                        return Task.CompletedTask;
                    }));

                    Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

                    // Add a document and ensure it resumes from this document.
                    await version1Repository.AddAsync(EmployeeGenerator.Generate(ObjectId.GenerateNewId(SystemClock.UtcNow.AddMinutes(1)).ToString()));
                    await _client.RefreshAsync(Indices.All);
                    await version2Index.ReindexAsync();

                    var aliasResponse = await _client.GetAliasAsync(d => d.Name(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version2Index.VersionedName, aliasResponse.Indices.First().Key);

                    Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version2Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(numberOfEmployeesToCreate + 1, countResponse.Count);

                    Assert.False((await _client.IndexExistsAsync(version1Index.VersionedName)).Exists);
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
                Assert.Equal(1, indexes.Count());

                var aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Name(version1Index.Name));
                _logger.Trace(() => aliasResponse.GetRequest());
                Assert.True(aliasResponse.IsValid);
                Assert.Equal(1, aliasResponse.Indices.Count);
                Assert.Equal(version1Index.VersionedName, aliasResponse.Indices.First().Key);

                var version1Repository = new EmployeeRepository(version1Index.Employee);
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Default);
                await _client.RefreshAsync(Indices.All);
                Assert.NotNull(employee?.Id);

                var countResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.Name));
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
                    await _client.RefreshAsync(Indices.All);

                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(2, countResponse.Count);

                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version2Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(0, countResponse.Count);

                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

                    // alias should still point to the old version until reindex
                    aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Name(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version1Index.VersionedName, aliasResponse.Indices.First().Key);

                    await version2Index.ReindexAsync();

                    aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Name(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version2Index.VersionedName, aliasResponse.Indices.First().Key);

                    Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version2Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(2, countResponse.Count);

                    Assert.False((await _client.IndexExistsAsync(version1Index.VersionedName)).Exists);

                    employee = await version2Repository.AddAsync(EmployeeGenerator.Default);
                    await _client.RefreshAsync(Indices.All);
                    Assert.NotNull(employee?.Id);

                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version2Index.Name));
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
                await _client.RefreshAsync(Indices.All);

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();

                    await version2Index.ReindexAsync();

                    var existsResponse = await _client.IndexExistsAsync(version1Index.VersionedName);
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);

                    var mappingResponse = await _client.GetMappingAsync<Employee>(m => m.Index(version1Index.VersionedName));
                    _logger.Trace(() => mappingResponse.GetRequest());
                    Assert.True(mappingResponse.IsValid);
                    Assert.NotNull(mappingResponse.Mappings);

                    existsResponse = await _client.IndexExistsAsync(version2Index.VersionedName);
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
                await _client.RefreshAsync(Indices.All);

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);

                    // swap the alias so we write to v1 and v2 and try to reindex.
                    await _client.AliasAsync(x => x
                        .Remove(a => a.Alias(version1Index.Name).Index(version1Index.VersionedName))
                        .Add(a => a.Alias(version2Index.Name).Index(version2Index.VersionedName)));

                    var version2Repository = new EmployeeRepository(version2Index.Employee);
                    await version2Repository.AddAsync(EmployeeGenerator.Generate());
                    await _client.RefreshAsync(Indices.All);

                    var countResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(1, countResponse.Count);

                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version2Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(1, countResponse.Count);

                    // swap back the alias
                    await _client.AliasAsync(x => x
                        .Remove(a => a.Alias(version2Index.Name).Index(version2Index.VersionedName))
                        .Add(a => a.Alias(version1Index.Name).Index(version1Index.VersionedName)));

                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

                    // alias should still point to the old version until reindex
                    var aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Name(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version1Index.VersionedName, aliasResponse.Indices.First().Key);

                    await version2Index.ReindexAsync();

                    aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Name(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version2Index.VersionedName, aliasResponse.Indices.First().Key);

                    Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version2Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(2, countResponse.Count);

                    Assert.False((await _client.IndexExistsAsync(version1Index.VersionedName)).Exists);
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
                await _client.RefreshAsync(Indices.All);

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);
                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

                    // alias should still point to the old version until reindex
                    var aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Name(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version1Index.VersionedName, aliasResponse.Indices.First().Key);

                    var countdown = new AsyncCountdownEvent(1);
                    var reindexTask = version2Index.ReindexAsync((progress, message) => {
                        _logger.Info($"Reindex Progress {progress}%: {message}");
                        if (progress == 91) {
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
                    aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Name(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version2Index.VersionedName, aliasResponse.Indices.First().Key);

                    Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

                    var countResponse = await _client.CountAsync<Employee>(d => d.Index(version2Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(2, countResponse.Count);

                    Assert.Equal(employee, await repository.GetByIdAsync(employee.Id));
                    Assert.False((await _client.IndexExistsAsync(version1Index.VersionedName)).Exists);
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
                await _client.RefreshAsync(Indices.All);

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);
                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

                    // alias should still point to the old version until reindex
                    var aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Name(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version1Index.VersionedName, aliasResponse.Indices.First().Key);

                    var countdown = new AsyncCountdownEvent(1);
                    var reindexTask = version2Index.ReindexAsync((progress, message) => {
                        _logger.Info($"Reindex Progress {progress}%: {message}");
                        if (progress == 91) {
                            countdown.Signal();
                            SystemClock.Sleep(1000);
                        }

                        return Task.CompletedTask;
                    });

                    // Wait until the first reindex pass is done.
                    await countdown.WaitAsync();
                    Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
                    await repository.RemoveAllAsync();
                    await _client.RefreshAsync(Indices.All);

                    // Resume after everythings been indexed.
                    await reindexTask;
                    aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Name(version2Index.Name));
                    Assert.True(aliasResponse.IsValid, aliasResponse.GetErrorMessage());
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version2Index.VersionedName, aliasResponse.Indices.First().Key);

                    Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

                    var countResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.ApiCall.HttpStatusCode == 404, countResponse.GetErrorMessage());
                    Assert.Equal(0, countResponse.Count);

                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version2Index.VersionedName));
                    _logger.Trace(() => countResponse.GetRequest());
                    Assert.True(countResponse.IsValid, countResponse.GetErrorMessage());
                    Assert.Equal(1, countResponse.Count);

                    Assert.Equal(employee, await repository.GetByIdAsync(employee.Id));
                    Assert.False((await _client.IndexExistsAsync(version1Index.VersionedName)).Exists);
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
                await _client.RefreshAsync(Indices.All);

                Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

                var aliasCountResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.Name));
                _logger.Trace(() => aliasCountResponse.GetRequest());
                Assert.True(aliasCountResponse.IsValid);
                Assert.Equal(1, aliasCountResponse.Count);

                var indexCountResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.GetIndex(utcNow)));
                _logger.Trace(() => indexCountResponse.GetRequest());
                Assert.True(indexCountResponse.IsValid);
                Assert.Equal(1, indexCountResponse.Count);

                indexCountResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.GetVersionedIndex(utcNow, 1)));
                _logger.Trace(() => indexCountResponse.GetRequest());
                Assert.True(indexCountResponse.IsValid);
                Assert.Equal(1, indexCountResponse.Count);

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());
                    var version2Repository = new EmployeeRepository(version2Index.Employee);

                    // Make sure we write to the old index.
                    await version2Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow));
                    await _client.RefreshAsync(Indices.All);

                    aliasCountResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.Name));
                    _logger.Trace(() => aliasCountResponse.GetRequest());
                    Assert.True(aliasCountResponse.IsValid);
                    Assert.Equal(2, aliasCountResponse.Count);

                    indexCountResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.GetVersionedIndex(utcNow, 1)));
                    _logger.Trace(() => indexCountResponse.GetRequest());
                    Assert.True(indexCountResponse.IsValid);
                    Assert.Equal(2, indexCountResponse.Count);

                    var existsResponse = await _client.IndexExistsAsync(version2Index.GetVersionedIndex(utcNow, 2));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.False(existsResponse.Exists);

                    // alias should still point to the old version until reindex
                    var aliasesResponse = await _client.GetAliasAsync(a => a.Index(version1Index.GetIndex(employee.CreatedUtc)));
                    _logger.Trace(() => aliasesResponse.GetRequest());
                    Assert.True(aliasesResponse.IsValid);
                    Assert.Equal(version1Index.GetVersionedIndex(employee.CreatedUtc, 1), aliasesResponse.Indices.Single().Key);

                    var aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                    aliases.Sort();
                    Assert.Equal(GetExpectedEmployeeDailyAliases(version1Index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

                    await version2Index.ReindexAsync();

                    Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

                    aliasesResponse = await _client.GetAliasAsync(a => a.Index(version1Index.GetIndex(employee.CreatedUtc)));
                    _logger.Trace(() => aliasesResponse.GetRequest());
                    Assert.True(aliasesResponse.IsValid);
                    Assert.Equal(version1Index.GetVersionedIndex(employee.CreatedUtc, 2), aliasesResponse.Indices.Single().Key);

                    aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                    aliases.Sort();
                    Assert.Equal(GetExpectedEmployeeDailyAliases(version1Index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

                    existsResponse = await _client.IndexExistsAsync(version1Index.GetVersionedIndex(utcNow, 1));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.False(existsResponse.Exists);

                    existsResponse = await _client.IndexExistsAsync(version2Index.GetVersionedIndex(utcNow, 2));
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
                await _client.RefreshAsync(Indices.All);

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();

                    await version2Index.ReindexAsync();

                    var existsResponse = await _client.IndexExistsAsync(version1Index.GetVersionedIndex(utcNow, 1));
                    _logger.Trace(() => existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);

                    var mappingResponse = await _client.GetMappingAsync<Employee>(m => m.Index(version1Index.GetVersionedIndex(utcNow, 1)));
                    _logger.Trace(() => mappingResponse.GetRequest());
                    Assert.True(mappingResponse.IsValid);
                    Assert.NotNull(mappingResponse.Mappings);

                    existsResponse = await _client.IndexExistsAsync(version2Index.GetVersionedIndex(utcNow, 2));
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

        private string ToJson(object data) {
            return _client.Serializer.SerializeToString(data);
        }
    }
}