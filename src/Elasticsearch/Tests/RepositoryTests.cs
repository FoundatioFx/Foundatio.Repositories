using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Jobs;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Queries;
using Foundatio.Repositories.Utility;
using Nest;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public class RepositoryTests {
        private readonly InMemoryCacheClient _cache = new InMemoryCacheClient();
        private readonly IElasticClient _client;
        private readonly IQueue<WorkItemData> _workItemQueue = new InMemoryQueue<WorkItemData>();
        private readonly ElasticConfiguration _configuration;
        private readonly EmployeeIndex _employeeIndex = new EmployeeIndex();
        private readonly ElasticQueryBuilder _queryBuilder = new ElasticQueryBuilder();
        private readonly EmployeeRepository _repository;

        public RepositoryTests() {
            _queryBuilder.RegisterDefaults();
            _queryBuilder.Register(new AgeQueryBuilder());
            _queryBuilder.Register(new CompanyQueryBuilder());

            _configuration = new ElasticConfiguration(_workItemQueue, _cache);
            _client = _configuration.GetClient(new[] { new Uri(ConfigurationManager.ConnectionStrings["ElasticConnectionString"].ConnectionString) });
            _repository = new EmployeeRepository(new ElasticRepositoryConfiguration<Employee>(_client, _employeeIndex.Employee, _queryBuilder, null, _cache));
        }
        
        //[Fact]
        //public async Task GetByDateBasedIndex() {
        //    await RemoveDataAsync();

        //    var indexes = await _client.GetIndicesPointingToAliasAsync(_monthlyEmployeeIndex.AliasName);
        //    Assert.Equal(0, indexes.Count);
            
        //    var alias = await _client.GetAliasAsync(descriptor => descriptor.Alias(_monthlyEmployeeIndex.AliasName));
        //    Assert.False(alias.IsValid);
        //    Assert.Equal(0, alias.Indices.Count);

        //    var employee = await _monthlyRepository.AddAsync(EmployeeGenerator.Default);
        //    Assert.NotNull(employee?.Id);
            
        //    employee = await _monthlyRepository.AddAsync(EmployeeGenerator.Generate(startDate: DateTimeOffset.Now.SubtractMonths(1)));
        //    Assert.NotNull(employee?.Id);

        //    await _client.RefreshAsync();
        //    alias = await _client.GetAliasAsync(descriptor => descriptor.Alias(_monthlyEmployeeIndex.AliasName));
        //    Assert.True(alias.IsValid);
        //    Assert.Equal(2, alias.Indices.Count);
            
        //    indexes = await _client.GetIndicesPointingToAliasAsync(_monthlyEmployeeIndex.AliasName);
        //    Assert.Equal(2, indexes.Count);
        //}

        [Fact]
        public async Task AddWithDefaultGeneratedIdAsync() {
            await RemoveDataAsync();

            var employee = await _repository.AddAsync(EmployeeGenerator.Default);
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
            var employee = await _repository.AddAsync(EmployeeGenerator.Generate(id));
            Assert.Equal(id, employee?.Id);
        }

        [Fact]
        public async Task SaveAsync() {
            await RemoveDataAsync();

            var employee = await _repository.AddAsync(EmployeeGenerator.Default);
            Assert.NotNull(employee?.Id);
            Assert.Equal(EmployeeGenerator.Default.Name, employee.Name);

            employee.Name = Guid.NewGuid().ToString();

            var result = await _repository.SaveAsync(employee);
            Assert.Equal(employee.Name, result?.Name);
        }

        [Fact]
        public async Task AddDuplicateAsync() {
            await RemoveDataAsync();

            string id = ObjectId.GenerateNewId().ToString();
            var employee = await _repository.AddAsync(EmployeeGenerator.Generate(id));
            Assert.Equal(id, employee?.Id);

            employee = await _repository.AddAsync(EmployeeGenerator.Generate(id));
            Assert.Equal(id, employee?.Id);
        }

        [Fact]
        public async Task SetCreatedAndModifiedTimesAsync() {
            await RemoveDataAsync();

            DateTime nowUtc = DateTime.UtcNow;
            var employee = await _repository.AddAsync(EmployeeGenerator.Default);
            Assert.True(employee.CreatedUtc >= nowUtc);
            Assert.True(employee.UpdatedUtc >= nowUtc);

            DateTime createdUtc = employee.CreatedUtc;
            DateTime updatedUtc = employee.UpdatedUtc;

            employee.Name = Guid.NewGuid().ToString();
            employee = await _repository.SaveAsync(employee);
            Assert.Equal(createdUtc, employee.CreatedUtc);
            Assert.True(updatedUtc < employee.UpdatedUtc, $"Previous UpdatedUtc: {updatedUtc} Current UpdatedUtc: {employee.UpdatedUtc}");
        }

        [Fact]
        public async Task CannotSetFutureCreatedAndModifiedTimesAsync() {
            await RemoveDataAsync();

            var employee = await _repository.AddAsync(EmployeeGenerator.Generate(createdUtc: DateTime.MaxValue, updatedUtc: DateTime.MaxValue));
            Assert.True(employee.CreatedUtc != DateTime.MaxValue);
            Assert.True(employee.UpdatedUtc != DateTime.MaxValue);
            
            employee.CreatedUtc = DateTime.MaxValue;
            employee.UpdatedUtc = DateTime.MaxValue;

            employee = await _repository.SaveAsync(employee);
            Assert.True(employee.CreatedUtc != DateTime.MaxValue);
            Assert.True(employee.UpdatedUtc != DateTime.MaxValue);
        }

        [Fact]
        public async Task CanGetByIds() {
            var employee = await _repository.AddAsync(EmployeeGenerator.Generate());
            Assert.NotNull(employee.Id);

            var result = await _repository.GetByIdAsync(employee.Id);
            Assert.NotNull(result);
            Assert.Equal(employee.Id, result.Id);
            
            var employee2 = await _repository.AddAsync(EmployeeGenerator.Generate());
            Assert.NotNull(employee2.Id);

            var results = await _repository.GetByIdsAsync(new [] { employee.Id, employee2.Id });
            Assert.NotNull(results);
            Assert.Equal(2, results.Total);
        }

        [Fact]
        public async Task CanAddToCacheAsync() {
            await RemoveDataAsync();

            Assert.Equal(0, _cache.Count);
            var employee = await _repository.AddAsync(EmployeeGenerator.Default, addToCache: true);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);

            var cachedResult = await _repository.GetByIdAsync(employee.Id, useCache: true);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(1, _cache.Hits);
            Assert.Equal(employee.ToJson(), cachedResult.ToJson());
        }

        [Fact]
        public async Task CanSaveToCacheAsync() {
            await RemoveDataAsync();

            Assert.Equal(0, _cache.Count);
            var employee = await _repository.SaveAsync(EmployeeGenerator.Generate(ObjectId.GenerateNewId().ToString()), addToCache: true);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);

            var cachedResult = await _repository.GetByIdAsync(employee.Id, useCache: true);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(1, _cache.Hits);
            Assert.Equal(employee.ToJson(), cachedResult.ToJson());
        }
        
        [Fact]
        public async Task GetFromCacheAsync() {
            await RemoveDataAsync();

            var employees = new List<Employee> { EmployeeGenerator.Default, EmployeeGenerator.Generate() };

            Assert.Equal(0, _cache.Count);
            await _repository.AddAsync(employees);
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            
            var cachedResult = await _repository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), useCache: true);
            Assert.NotNull(cachedResult);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);

            cachedResult = await _repository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), useCache: true);
            Assert.NotNull(cachedResult);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(2, _cache.Hits);

            await _repository.GetByIdAsync(employees.First().Id, useCache: true);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(3, _cache.Hits);
        }

        [Fact]
        public async Task GetByIdsFromCacheAsync() {
            await RemoveDataAsync();

            Assert.Equal(0, _cache.Count);
            var employee = await _repository.AddAsync(EmployeeGenerator.Default);
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);

            var cachedResult = await _repository.GetByIdAsync(employee.Id, useCache: true);
            Assert.NotNull(cachedResult);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(employee.ToJson(), cachedResult.ToJson());

            cachedResult = await _repository.GetByIdAsync(employee.Id, useCache: true);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(1, _cache.Hits);
            Assert.Equal(employee.ToJson(), cachedResult.ToJson());
        }

        [Fact]
        public async Task GetByAgeAsync() {
            await RemoveDataAsync();
            
            var employee19 = await _repository.AddAsync(EmployeeGenerator.Generate(age: 19));
            var employee20 = await _repository.AddAsync(EmployeeGenerator.Generate(age: 20));
            await _client.RefreshAsync();

            var result = await _repository.GetByAgeAsync(employee19.Age);
            Assert.Equal(employee19.ToJson(), result.ToJson());

            var results = await _repository.GetAllByAgeAsync(employee20.Age);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee20.ToJson(), results.Documents.First().ToJson());
        }

        [Fact]
        public async Task GetByCompanyAsync() {
            await RemoveDataAsync();

            var company19 = await _repository.AddAsync(EmployeeGenerator.Generate(age: 19, companyId: EmployeeGenerator.DefaultCompanyId));
            var company20 = await _repository.AddAsync(EmployeeGenerator.Generate(age: 20));
            await _client.RefreshAsync();

            var result = await _repository.GetByCompanyAsync(company19.CompanyId);
            Assert.Equal(company19.ToJson(), result.ToJson());

            var results = await _repository.GetAllByCompanyAsync(company20.CompanyId);
            Assert.Equal(1, results.Total);
            Assert.Equal(company20.ToJson(), results.Documents.First().ToJson());
            
            Assert.Equal(1, await _repository.GetCountByCompanyAsync(company20.CompanyId));
            await _repository.RemoveAsync(company20, false);
            Assert.Equal(0, await _repository.GetCountByCompanyAsync(company20.CompanyId));
        }

        private async Task RemoveDataAsync() {
            await _cache.RemoveAllAsync();
            _configuration.DeleteIndexes(_client);
            _configuration.ConfigureIndexes(_client);
            await _client.RefreshAsync();
        }
    }
}