using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Logging;
using Foundatio.Logging.Xunit;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Repositories.Utility;
using Nest;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public class RepositoryTests : TestWithLoggingBase {
        private readonly InMemoryCacheClient _cache = new InMemoryCacheClient();
        private readonly IQueue<WorkItemData> _workItemQueue = new InMemoryQueue<WorkItemData>();
        private readonly MyAppElasticConfiguration _elasticConfiguration;
        private readonly EmployeeRepository _employeeRepository;
        private readonly DailyLogEventRepository _dailyRepository;
        private readonly IElasticClient _client;

        public RepositoryTests(ITestOutputHelper output): base(output) {
            //Log.MinimumLevel = LogLevel.Trace;

            _elasticConfiguration = new MyAppElasticConfiguration(_workItemQueue, _cache, Log);
            _client = _elasticConfiguration.Client;
            _employeeRepository = new EmployeeRepository(_elasticConfiguration, _cache, Log.CreateLogger<EmployeeRepository>());
            _dailyRepository = new DailyLogEventRepository(_elasticConfiguration, _cache, Log.CreateLogger<DailyLogEventRepository>());
        }

        [Fact]
        public async Task CanUseVersionedIndex() {
            await RemoveDataAsync();

            var version1EmployeeIndex = new VersionedEmployeeIndex(_client, 1, Log);
            var version1EmployeeRepository = new EmployeeRepository(_client, version1EmployeeIndex.Employee, _cache, Log.CreateLogger<EmployeeRepository>());
            var version2EmployeeIndex = new VersionedEmployeeIndex(_client, 2, Log);
            var version2EmployeeRepository = new EmployeeRepository(_client, version1EmployeeIndex.Employee, _cache, Log.CreateLogger<EmployeeRepository>());

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

            Assert.Equal(1, version1EmployeeIndex.GetVersion());
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

            Assert.Equal(2, version2EmployeeIndex.GetVersion());

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
            await RemoveDataAsync();

            var indexes = await _client.GetIndicesPointingToAliasAsync(_elasticConfiguration.DailyLogEvents.Name);
            Assert.Equal(0, indexes.Count);

            var alias = await _client.GetAliasAsync(descriptor => descriptor.Alias(_elasticConfiguration.DailyLogEvents.Name));
            Assert.False(alias.IsValid);
            Assert.Equal(0, alias.Indices.Count);

            var logEvent = await _dailyRepository.AddAsync(LogEventGenerator.Default);
            Assert.NotNull(logEvent?.Id);

            logEvent = await _dailyRepository.AddAsync(LogEventGenerator.Generate(createdUtc: DateTime.Now.SubtractDays(1)));
            Assert.NotNull(logEvent?.Id);

            await _client.RefreshAsync();
            alias = await _client.GetAliasAsync(descriptor => descriptor.Alias(_elasticConfiguration.DailyLogEvents.Name));
            Assert.True(alias.IsValid);
            Assert.Equal(2, alias.Indices.Count);

            indexes = await _client.GetIndicesPointingToAliasAsync(_elasticConfiguration.DailyLogEvents.Name);
            Assert.Equal(2, indexes.Count);

            await _dailyRepository.RemoveAllAsync();
            await _client.RefreshAsync();

            Assert.Equal(0, await _dailyRepository.CountAsync());
        }

        [Fact]
        public async Task AddWithDefaultGeneratedIdAsync() {
            await RemoveDataAsync();

            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
            Assert.NotNull(employee?.Id);
            Assert.Equal(EmployeeGenerator.Default.Name, employee.Name);
            Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
            Assert.Equal(EmployeeGenerator.Default.CompanyName, employee.CompanyName);
            Assert.Equal(EmployeeGenerator.Default.CompanyId, employee.CompanyId);
        }

        [Fact]
        public async Task AddWithExistingIdAsync() {
            await RemoveDataAsync();

            string id = ObjectId.GenerateNewId().ToString();
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(id));
            Assert.Equal(id, employee?.Id);
        }

        [Fact]
        public async Task SaveAsync() {
            await RemoveDataAsync();

            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
            Assert.NotNull(employee?.Id);
            Assert.Equal(EmployeeGenerator.Default.Name, employee.Name);

            employee.Name = Guid.NewGuid().ToString();

            var result = await _employeeRepository.SaveAsync(employee);
            Assert.Equal(employee.Name, result?.Name);
        }

        [Fact]
        public async Task AddDuplicateAsync() {
            await RemoveDataAsync();

            string id = ObjectId.GenerateNewId().ToString();
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(id));
            Assert.Equal(id, employee?.Id);

            employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(id));
            Assert.Equal(id, employee?.Id);
        }

        [Fact]
        public async Task SetCreatedAndModifiedTimesAsync() {
            await RemoveDataAsync();

            DateTime nowUtc = DateTime.UtcNow;
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
            Assert.True(employee.CreatedUtc >= nowUtc);
            Assert.True(employee.UpdatedUtc >= nowUtc);

            DateTime createdUtc = employee.CreatedUtc;
            DateTime updatedUtc = employee.UpdatedUtc;

            employee.Name = Guid.NewGuid().ToString();
            await Task.Delay(100);
            employee = await _employeeRepository.SaveAsync(employee);
            Assert.Equal(createdUtc, employee.CreatedUtc);
            Assert.True(updatedUtc < employee.UpdatedUtc, $"Previous UpdatedUtc: {updatedUtc} Current UpdatedUtc: {employee.UpdatedUtc}");
        }

        [Fact]
        public async Task CannotSetFutureCreatedAndModifiedTimesAsync() {
            await RemoveDataAsync();

            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(createdUtc: DateTime.MaxValue, updatedUtc: DateTime.MaxValue));
            Assert.True(employee.CreatedUtc != DateTime.MaxValue);
            Assert.True(employee.UpdatedUtc != DateTime.MaxValue);
            
            employee.CreatedUtc = DateTime.MaxValue;
            employee.UpdatedUtc = DateTime.MaxValue;

            employee = await _employeeRepository.SaveAsync(employee);
            Assert.True(employee.CreatedUtc != DateTime.MaxValue);
            Assert.True(employee.UpdatedUtc != DateTime.MaxValue);
        }

        [Fact]
        public async Task CanGetByIds() {
            await RemoveDataAsync();

            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate());
            Assert.NotNull(employee.Id);

            var result = await _employeeRepository.GetByIdAsync(employee.Id);
            Assert.NotNull(result);
            Assert.Equal(employee.Id, result.Id);
            
            var employee2 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate());
            Assert.NotNull(employee2.Id);

            var results = await _employeeRepository.GetByIdsAsync(new [] { employee.Id, employee2.Id });
            Assert.NotNull(results);
            Assert.Equal(2, results.Total);
        }

        [Fact]
        public async Task CanAddToCacheAsync() {
            await RemoveDataAsync();

            Assert.Equal(0, _cache.Count);
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, addToCache: true);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);

            var cachedResult = await _employeeRepository.GetByIdAsync(employee.Id, useCache: true);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(1, _cache.Hits);
            Assert.Equal(employee.ToJson(), cachedResult.ToJson());
        }

        [Fact]
        public async Task CanSaveToCacheAsync() {
            await RemoveDataAsync();

            Assert.Equal(0, _cache.Count);
            var employee = await _employeeRepository.SaveAsync(EmployeeGenerator.Generate(ObjectId.GenerateNewId().ToString()), addToCache: true);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);

            var cachedResult = await _employeeRepository.GetByIdAsync(employee.Id, useCache: true);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(1, _cache.Hits);
            Assert.Equal(employee.ToJson(), cachedResult.ToJson());
        }
        
        [Fact]
        public async Task GetFromCacheAsync() {
            await RemoveDataAsync();

            var employees = new List<Employee> { EmployeeGenerator.Default, EmployeeGenerator.Generate() };

            Assert.Equal(0, _cache.Count);
            await _employeeRepository.AddAsync(employees);
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            
            var cachedResult = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), useCache: true);
            Assert.NotNull(cachedResult);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);

            cachedResult = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), useCache: true);
            Assert.NotNull(cachedResult);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(2, _cache.Hits);

            await _employeeRepository.GetByIdAsync(employees.First().Id, useCache: true);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(3, _cache.Hits);
        }

        [Fact]
        public async Task GetByIdsFromCacheAsync() {
            await RemoveDataAsync();

            Assert.Equal(0, _cache.Count);
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);

            var cachedResult = await _employeeRepository.GetByIdAsync(employee.Id, useCache: true);
            Assert.NotNull(cachedResult);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(employee.ToJson(), cachedResult.ToJson());

            cachedResult = await _employeeRepository.GetByIdAsync(employee.Id, useCache: true);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(1, _cache.Hits);
            Assert.Equal(employee.ToJson(), cachedResult.ToJson());
        }

        [Fact]
        public async Task GetByAgeAsync() {
            await RemoveDataAsync();
            
            var employee19 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19));
            var employee20 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20));
            await _elasticConfiguration.Client.RefreshAsync();

            var result = await _employeeRepository.GetByAgeAsync(employee19.Age);
            Assert.Equal(employee19.ToJson(), result.ToJson());

            var results = await _employeeRepository.GetAllByAgeAsync(employee20.Age);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee20.ToJson(), results.Documents.First().ToJson());
        }

        [Fact]
        public async Task GetByCompanyAsync() {
            await RemoveDataAsync();

            var employee1 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19, companyId: EmployeeGenerator.DefaultCompanyId));
            var employee2 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20));
            await _elasticConfiguration.Client.RefreshAsync();

            var result = await _employeeRepository.GetByCompanyAsync(employee1.CompanyId);
            Assert.Equal(employee1.ToJson(), result.ToJson());

            var results = await _employeeRepository.GetAllByCompanyAsync(employee1.CompanyId);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee1.ToJson(), results.Documents.First().ToJson());
            
            Assert.Equal(1, await _employeeRepository.GetCountByCompanyAsync(employee1.CompanyId));
            await _employeeRepository.RemoveAsync(employee1, false);
            await _elasticConfiguration.Client.RefreshAsync();
            Assert.Equal(1, await _employeeRepository.CountAsync());
            Assert.Equal(0, await _employeeRepository.GetCountByCompanyAsync(employee1.CompanyId));
        }

        private async Task RemoveDataAsync() {
            await _cache.RemoveAllAsync();
            _elasticConfiguration.DeleteIndexes();
            _elasticConfiguration.ConfigureIndexes();
            await _elasticConfiguration.Client.RefreshAsync();
        }
    }
}