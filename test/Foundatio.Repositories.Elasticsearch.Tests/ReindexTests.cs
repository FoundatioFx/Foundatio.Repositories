using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Foundatio.Logging;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Nest;
using Foundatio.AsyncEx;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class ReindexTests : ElasticRepositoryTestBase {
        public ReindexTests(ITestOutputHelper output) : base(output) {
            Log.SetLogLevel<EmployeeRepository>(LogLevel.Warning);
            RemoveDataAsync(configureIndexes: false).GetAwaiter().GetResult();
        }

        [Fact(Skip = "This will only work if the mapping is manually updated.")]
        public async Task CanReindexSameIndexAsync() {
            var index = new EmployeeIndex(_configuration);
            await index.DeleteAsync();

            using (new DisposableAction(() => index.DeleteAsync().GetAwaiter().GetResult())) {
                await index.ConfigureAsync();
                Assert.True(_client.IndexExists(index.Name).Exists);

                var repository = new EmployeeRepository(index.Employee);
                var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
                Assert.NotNull(employee?.Id);

                var countResponse = await _client.CountAsync<Employee>(d => d.Index(index.Name));
                _logger.LogTrace(countResponse.GetRequest());
                Assert.True(countResponse.IsValid);
                Assert.Equal(1, countResponse.Count);

                var mappingResponse = await _client.GetMappingAsync<Employee>(m => m.Index(index.Name));
                _logger.LogTrace(mappingResponse.GetRequest());
                Assert.True(mappingResponse.IsValid);
                Assert.NotNull(mappingResponse.Mappings);

                var newIndex = new EmployeeIndexWithYearsEmployed(_configuration);
                await newIndex.ReindexAsync();

                countResponse = await _client.CountAsync<Employee>(d => d.Index(index.Name));
                _logger.LogTrace(countResponse.GetRequest());
                Assert.True(countResponse.IsValid);
                Assert.Equal(1, countResponse.Count);

                string version1Mappings = ToJson(mappingResponse.Mappings);
                mappingResponse = await _client.GetMappingAsync<Employee>(m => m.Index(index.Name));
                _logger.LogTrace(mappingResponse.GetRequest());
                Assert.True(mappingResponse.IsValid);
                Assert.NotNull(mappingResponse.Mappings);
                Assert.NotEqual(version1Mappings, ToJson(mappingResponse.Mappings));
            }
        }

        [Fact]
        public async Task CanResumeReindexAsync() {
            const int numberOfEmployeesToCreate = 2000;

            var version1Index = new VersionedEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new VersionedEmployeeIndex(_configuration, 2);
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                Assert.True(_client.IndexExists(version1Index.VersionedName).Exists);

                var version1Repository = new EmployeeRepository(version1Index.Employee);
                await version1Repository.AddAsync(EmployeeGenerator.GenerateEmployees(numberOfEmployeesToCreate), o => o.ImmediateConsistency());

                var countResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.Name));
                _logger.LogTrace(countResponse.GetRequest());
                Assert.True(countResponse.IsValid);
                Assert.Equal(numberOfEmployeesToCreate, countResponse.Count);
                Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);

                    // Throw error before second repass.
                    await Assert.ThrowsAsync<ApplicationException>(async () => await version2Index.ReindexAsync((progress, message) => {
                        _logger.LogInformation("Reindex Progress {0}%: {1}", progress, message);
                        if (progress == 91)
                            throw new ApplicationException("Random Error");

                        return Task.CompletedTask;
                    }));

                    Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

                    // Add a document and ensure it resumes from this document.
                    await version1Repository.AddAsync(EmployeeGenerator.Generate(ObjectId.GenerateNewId(SystemClock.UtcNow.AddMinutes(1)).ToString()), o => o.ImmediateConsistency());
                    await version2Index.ReindexAsync();

                    var aliasResponse = await _client.GetAliasAsync(d => d.Name(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version2Index.VersionedName, aliasResponse.Indices.First().Key);

                    Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version2Index.VersionedName));
                    _logger.LogTrace(countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(numberOfEmployeesToCreate + 1, countResponse.Count);

                    Assert.False((await _client.IndexExistsAsync(version1Index.VersionedName)).Exists);
                }
            }
        }

        [Fact]
        public async Task CanHandleReindexFailureAsync() {
            var version1Index = new VersionedEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new VersionedEmployeeIndex(_configuration, 2);
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                Assert.True(_client.IndexExists(version1Index.VersionedName).Exists);

                var version1Repository = new EmployeeRepository(version1Index.Employee);
                await version1Repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());

                var countResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.Name));
                _logger.LogTrace(countResponse.GetRequest());
                Assert.True(countResponse.IsValid);
                Assert.Equal(1, countResponse.Count);
                Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    //Create invalid mappings
                    var response = await _client.CreateIndexAsync(version2Index.VersionedName, d => d.Mappings(m => m.Map<Employee>(map => map
                        .Dynamic(false)
                        .Properties(p => p
                            .Number(f => f.Name(e => e.Id))
                        ))));
                    _logger.LogTrace(response.GetRequest());

                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);
                    Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

                    await version2Index.ReindexAsync();
                    await version2Index.Configuration.Client.RefreshAsync(Indices.All);

                    var aliasResponse = await _client.GetAliasAsync(d => d.Name(version2Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.True(aliasResponse.Indices.ContainsKey(version1Index.VersionedName));

                    var indexResponse = await _client.CatIndicesAsync(d => d.Index(Indices.Index("employees-*")));
                    Assert.NotNull(indexResponse.Records.FirstOrDefault(r => r.Index == version1Index.VersionedName));
                    Assert.NotNull(indexResponse.Records.FirstOrDefault(r => r.Index == version2Index.VersionedName));
                    Assert.NotNull(indexResponse.Records.FirstOrDefault(r => r.Index == $"{version2Index.VersionedName}-error"));

                    Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.VersionedName));
                    _logger.LogTrace(countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(1, countResponse.Count);

                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version2Index.VersionedName));
                    _logger.LogTrace(countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(0, countResponse.Count);

                    countResponse = await _client.CountAsync<object>(d => d.Index($"{version2Index.VersionedName}-error").Type("failures"));
                    _logger.LogTrace(countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(1, countResponse.Count);
                }
            }
        }

        [Fact]
        public async Task CanReindexVersionedIndexAsync() {
            var version1Index = new VersionedEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new VersionedEmployeeIndex(_configuration, 2);
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                Assert.True(_client.IndexExists(version1Index.VersionedName).Exists);

                var indexes = _client.GetIndicesPointingToAlias(version1Index.Name);
                Assert.Single(indexes);

                var aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Name(version1Index.Name));
                _logger.LogTrace(aliasResponse.GetRequest());
                Assert.True(aliasResponse.IsValid);
                Assert.Equal(1, aliasResponse.Indices.Count);
                Assert.Equal(version1Index.VersionedName, aliasResponse.Indices.First().Key);

                var version1Repository = new EmployeeRepository(version1Index.Employee);
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
                Assert.NotNull(employee?.Id);

                var countResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.Name));
                _logger.LogTrace(countResponse.GetRequest());
                Assert.True(countResponse.IsValid);
                Assert.Equal(1, countResponse.Count);

                Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);

                    // Make sure we can write to the index still. Should go to the old index until after the reindex is complete.
                    var version2Repository = new EmployeeRepository(version2Index.Employee);
                    await version2Repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());

                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.VersionedName));
                    _logger.LogTrace(countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(2, countResponse.Count);

                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version2Index.VersionedName));
                    _logger.LogTrace(countResponse.GetRequest());
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
                    _logger.LogTrace(countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(2, countResponse.Count);

                    Assert.False((await _client.IndexExistsAsync(version1Index.VersionedName)).Exists);

                    employee = await version2Repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
                    Assert.NotNull(employee?.Id);

                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version2Index.Name));
                    _logger.LogTrace(countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(3, countResponse.Count);
                }
            }
        }

        [Fact]
        public async Task CanReindexVersionedIndexWithCorrectMappingsAsync() {
            var version1Index = new VersionedEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new VersionedEmployeeIndex(_configuration, 2) { DiscardIndexesOnReindex = false };
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                var version1Repository = new EmployeeRepository(version1Index.Employee);

                var utcNow = SystemClock.UtcNow;
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
                Assert.NotNull(employee?.Id);

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();

                    await version2Index.ReindexAsync();

                    var existsResponse = await _client.IndexExistsAsync(version1Index.VersionedName);
                    _logger.LogTrace(existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);

                    var mappingResponse = await _client.GetMappingAsync<Employee>(m => m.Index(version1Index.VersionedName));
                    _logger.LogTrace(mappingResponse.GetRequest());
                    Assert.True(mappingResponse.IsValid);
                    Assert.NotNull(mappingResponse.Mappings);

                    existsResponse = await _client.IndexExistsAsync(version2Index.VersionedName);
                    _logger.LogTrace(existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);

                    string version1Mappings = ToJson(mappingResponse.Mappings);
                    mappingResponse = await _client.GetMappingAsync<Employee>(m => m.Index(version1Index.VersionedName));
                    _logger.LogTrace(mappingResponse.GetRequest());
                    Assert.True(mappingResponse.IsValid);
                    Assert.NotNull(mappingResponse.Mappings);
                    Assert.Equal(version1Mappings, ToJson(mappingResponse.Mappings).Replace("-v2", "-v1"));
                }
            }
        }

        [Fact]
        public async Task CanReindexVersionedIndexWithReindexScriptAsync() {
            var version1Index = new VersionedEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version20Index = new VersionedEmployeeIndex(_configuration, 20) { DiscardIndexesOnReindex = false };
            await version20Index.DeleteAsync();

            var version21Index = new VersionedEmployeeIndex(_configuration, 21) { DiscardIndexesOnReindex = false };
            await version21Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                var version1Repository = new EmployeeRepository(version1Index.Employee);

                var utcNow = SystemClock.UtcNow;
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
                Assert.NotNull(employee?.Id);

                using (new DisposableAction(() => version20Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version20Index.ConfigureAsync();
                    await version20Index.ReindexAsync();

                    var version20Repository = new EmployeeRepository(version20Index.Employee);
                    var result = await version20Repository.GetByIdAsync(employee.Id);
                    Assert.Equal("scripted", result.CompanyName);

                    using (new DisposableAction(() => version21Index.DeleteAsync().GetAwaiter().GetResult())) {
                        await version21Index.ConfigureAsync();
                        await version21Index.ReindexAsync();

                        var version21Repository = new EmployeeRepository(version21Index.Employee);
                        result = await version21Repository.GetByIdAsync(employee.Id);
                        Assert.Equal("typed script", result.CompanyName);
                    }
                }
            }

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                var version1Repository = new EmployeeRepository(version1Index.Employee);

                var utcNow = SystemClock.UtcNow;
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
                Assert.NotNull(employee?.Id);

                using (new DisposableAction(() => version21Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version21Index.ConfigureAsync();
                    await version21Index.ReindexAsync();

                    var version21Repository = new EmployeeRepository(version21Index.Employee);
                    var result = await version21Repository.GetByIdAsync(employee.Id);
                    Assert.Equal("typed script", result.CompanyName);
                }
            }
        }

        [Fact]
        public async Task HandleFailureInReindexScriptAsync() {
            var version1Index = new VersionedEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();


            var version22Index = new VersionedEmployeeIndex(_configuration, 22) { DiscardIndexesOnReindex = false };
            await version22Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                var version1Repository = new EmployeeRepository(version1Index.Employee);

                var utcNow = SystemClock.UtcNow;
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
                Assert.NotNull(employee?.Id);

                using (new DisposableAction(() => version22Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version22Index.ConfigureAsync();
                    await version22Index.ReindexAsync();

                    var aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Name(version1Index.Name));
                    Assert.True(aliasResponse.IsValid);
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version1Index.VersionedName, aliasResponse.Indices.First().Key);
                }
            }
        }

        [Fact]
        public async Task CanReindexVersionedIndexWithDataInBothIndexesAsync() {
            var version1Index = new VersionedEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new VersionedEmployeeIndex(_configuration, 2);
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                Assert.True(_client.IndexExists(version1Index.VersionedName).Exists);

                var version1Repository = new EmployeeRepository(version1Index.Employee);
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
                Assert.NotNull(employee?.Id);

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    Assert.True(_client.IndexExists(version2Index.VersionedName).Exists);

                    // swap the alias so we write to v1 and v2 and try to reindex.
                    await _client.AliasAsync(x => x
                        .Remove(a => a.Alias(version1Index.Name).Index(version1Index.VersionedName))
                        .Add(a => a.Alias(version2Index.Name).Index(version2Index.VersionedName)));

                    var version2Repository = new EmployeeRepository(version2Index.Employee);
                    await version2Repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());

                    var countResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.VersionedName));
                    _logger.LogTrace(countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(1, countResponse.Count);

                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version2Index.VersionedName));
                    _logger.LogTrace(countResponse.GetRequest());
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

                    await _client.RefreshAsync(Indices.All);
                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version2Index.VersionedName));
                    _logger.LogTrace(countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(2, countResponse.Count);

                    Assert.False((await _client.IndexExistsAsync(version1Index.VersionedName)).Exists);
                }
            }
        }

        [Fact]
        public async Task CanReindexVersionedIndexWithUpdatedDocsAsync() {
            var version1Index = new VersionedEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new VersionedEmployeeIndex(_configuration, 2);
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                Assert.True(_client.IndexExists(version1Index.VersionedName).Exists);

                var repository = new EmployeeRepository(version1Index.Employee);
                var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
                Assert.NotNull(employee?.Id);

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
                        _logger.LogInformation($"Reindex Progress {progress}%: {message}");
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

                    await _client.RefreshAsync(Indices.All);
                    var countResponse = await _client.CountAsync<Employee>(d => d.Index(version2Index.VersionedName));
                    _logger.LogTrace(countResponse.GetRequest());
                    Assert.True(countResponse.IsValid);
                    Assert.Equal(2, countResponse.Count);

                    var result = await repository.GetByIdAsync(employee.Id);
                    Assert.Equal(ToJson(employee), ToJson(result));
                    Assert.False((await _client.IndexExistsAsync(version1Index.VersionedName)).Exists);
                }
            }
        }

        [Fact]
        public async Task CanReindexVersionedIndexWithDeletedDocsAsync() {
            var version1Index = new VersionedEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new VersionedEmployeeIndex(_configuration, 2);
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                Assert.True(_client.IndexExists(version1Index.VersionedName).Exists);

                var repository = new EmployeeRepository(version1Index.Employee);
                var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
                Assert.NotNull(employee?.Id);

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
                        _logger.LogInformation($"Reindex Progress {progress}%: {message}");
                        if (progress == 91) {
                            countdown.Signal();
                            SystemClock.Sleep(1000);
                        }

                        return Task.CompletedTask;
                    });

                    // Wait until the first reindex pass is done.
                    await countdown.WaitAsync();
                    Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
                    await repository.RemoveAllAsync(o => o.ImmediateConsistency());

                    // Resume after everythings been indexed.
                    await reindexTask;
                    aliasResponse = await _client.GetAliasAsync(descriptor => descriptor.Name(version2Index.Name));
                    Assert.True(aliasResponse.IsValid, aliasResponse.GetErrorMessage());
                    Assert.Equal(1, aliasResponse.Indices.Count);
                    Assert.Equal(version2Index.VersionedName, aliasResponse.Indices.First().Key);

                    Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

                    var countResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.VersionedName));
                    _logger.LogTrace(countResponse.GetRequest());
                    Assert.True(countResponse.ApiCall.HttpStatusCode == 404, countResponse.GetErrorMessage());
                    Assert.Equal(0, countResponse.Count);

                    countResponse = await _client.CountAsync<Employee>(d => d.Index(version2Index.VersionedName));
                    _logger.LogTrace(countResponse.GetRequest());
                    Assert.True(countResponse.IsValid, countResponse.GetErrorMessage());
                    Assert.Equal(1, countResponse.Count);

                    Assert.Equal(employee, await repository.GetByIdAsync(employee.Id));
                    Assert.False((await _client.IndexExistsAsync(version1Index.VersionedName)).Exists);
                }
            }
        }

        [Fact]
        public async Task CanReindexTimeSeriesIndexAsync() {
            var version1Index = new DailyEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new DailyEmployeeIndex(_configuration, 2);
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                var version1Repository = new EmployeeRepository(version1Index.Employee);

                var utcNow = SystemClock.UtcNow;
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
                Assert.NotNull(employee?.Id);

                Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

                var aliasCountResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.Name));
                _logger.LogTrace(aliasCountResponse.GetRequest());
                Assert.True(aliasCountResponse.IsValid);
                Assert.Equal(1, aliasCountResponse.Count);

                var indexCountResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.GetIndex(utcNow)));
                _logger.LogTrace(indexCountResponse.GetRequest());
                Assert.True(indexCountResponse.IsValid);
                Assert.Equal(1, indexCountResponse.Count);

                indexCountResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.GetVersionedIndex(utcNow, 1)));
                _logger.LogTrace(indexCountResponse.GetRequest());
                Assert.True(indexCountResponse.IsValid);
                Assert.Equal(1, indexCountResponse.Count);

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();
                    Assert.Equal(1, await version2Index.GetCurrentVersionAsync());
                    var version2Repository = new EmployeeRepository(version2Index.Employee);

                    // Make sure we write to the old index.
                    await version2Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());

                    aliasCountResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.Name));
                    _logger.LogTrace(aliasCountResponse.GetRequest());
                    Assert.True(aliasCountResponse.IsValid);
                    Assert.Equal(2, aliasCountResponse.Count);

                    indexCountResponse = await _client.CountAsync<Employee>(d => d.Index(version1Index.GetVersionedIndex(utcNow, 1)));
                    _logger.LogTrace(indexCountResponse.GetRequest());
                    Assert.True(indexCountResponse.IsValid);
                    Assert.Equal(2, indexCountResponse.Count);

                    var existsResponse = await _client.IndexExistsAsync(version2Index.GetVersionedIndex(utcNow, 2));
                    _logger.LogTrace(existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.False(existsResponse.Exists);

                    // alias should still point to the old version until reindex
                    var aliasesResponse = await _client.GetAliasAsync(a => a.Index(version1Index.GetIndex(employee.CreatedUtc)));
                    _logger.LogTrace(aliasesResponse.GetRequest());
                    Assert.True(aliasesResponse.IsValid);
                    Assert.Equal(version1Index.GetVersionedIndex(employee.CreatedUtc, 1), aliasesResponse.Indices.Single().Key);

                    var aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                    aliases.Sort();
                    Assert.Equal(GetExpectedEmployeeDailyAliases(version1Index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

                    await version2Index.ReindexAsync();

                    Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
                    Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

                    aliasesResponse = await _client.GetAliasAsync(a => a.Index(version1Index.GetIndex(employee.CreatedUtc)));
                    _logger.LogTrace(aliasesResponse.GetRequest());
                    Assert.True(aliasesResponse.IsValid);
                    Assert.Equal(version1Index.GetVersionedIndex(employee.CreatedUtc, 2), aliasesResponse.Indices.Single().Key);

                    aliases = aliasesResponse.Indices.Values.Single().Select(s => s.Name).ToList();
                    aliases.Sort();
                    Assert.Equal(GetExpectedEmployeeDailyAliases(version1Index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

                    existsResponse = await _client.IndexExistsAsync(version1Index.GetVersionedIndex(utcNow, 1));
                    _logger.LogTrace(existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.False(existsResponse.Exists);

                    existsResponse = await _client.IndexExistsAsync(version2Index.GetVersionedIndex(utcNow, 2));
                    _logger.LogTrace(existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);
                }
            }
        }

        [Fact]
        public async Task CanReindexTimeSeriesIndexWithCorrectMappingsAsync() {
            var version1Index = new DailyEmployeeIndex(_configuration, 1);
            await version1Index.DeleteAsync();

            var version2Index = new DailyEmployeeIndex(_configuration, 2) { DiscardIndexesOnReindex = false };
            await version2Index.DeleteAsync();

            using (new DisposableAction(() => version1Index.DeleteAsync().GetAwaiter().GetResult())) {
                await version1Index.ConfigureAsync();
                var version1Repository = new EmployeeRepository(version1Index.Employee);

                var utcNow = SystemClock.UtcNow;
                var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
                Assert.NotNull(employee?.Id);

                using (new DisposableAction(() => version2Index.DeleteAsync().GetAwaiter().GetResult())) {
                    await version2Index.ConfigureAsync();

                    await version2Index.ReindexAsync();

                    var existsResponse = await _client.IndexExistsAsync(version1Index.GetVersionedIndex(utcNow, 1));
                    _logger.LogTrace(existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);

                    var mappingResponse = await _client.GetMappingAsync<Employee>(m => m.Index(version1Index.GetVersionedIndex(utcNow, 1)));
                    _logger.LogTrace(mappingResponse.GetRequest());
                    Assert.True(mappingResponse.IsValid);
                    Assert.NotNull(mappingResponse.Mappings);

                    existsResponse = await _client.IndexExistsAsync(version2Index.GetVersionedIndex(utcNow, 2));
                    _logger.LogTrace(existsResponse.GetRequest());
                    Assert.True(existsResponse.IsValid);
                    Assert.True(existsResponse.Exists);

                    string version1Mappings = ToJson(mappingResponse.Mappings);
                    mappingResponse = await _client.GetMappingAsync<Employee>(m => m.Index(version1Index.GetVersionedIndex(utcNow, 2)));
                    _logger.LogTrace(mappingResponse.GetRequest());
                    Assert.True(mappingResponse.IsValid);
                    Assert.NotNull(mappingResponse.Mappings);
                    Assert.Equal(version1Mappings, ToJson(mappingResponse.Mappings).Replace("-v2", "-v1"));
                }
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

        private string ToJson(object data) {
            return _client.Serializer.SerializeToString(data);
        }
    }
}