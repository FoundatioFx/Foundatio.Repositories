using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Jobs;
using Foundatio.Logging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class QueryTests : ElasticRepositoryTestBase {
        private readonly EmployeeRepository _employeeRepository;
        private readonly IQueue<WorkItemData> _workItemQueue = new InMemoryQueue<WorkItemData>();

        public QueryTests(ITestOutputHelper output) : base(output) {
            _employeeRepository = new EmployeeRepository(MyAppConfiguration, _cache, Log.CreateLogger<EmployeeRepository>());

            RemoveDataAsync().GetAwaiter().GetResult();
        }

        protected override ElasticConfiguration GetElasticConfiguration() {
            return new MyAppElasticConfiguration(_workItemQueue, _cache, Log);
        }

        private MyAppElasticConfiguration MyAppConfiguration => _configuration as MyAppElasticConfiguration;
        
        [Fact]
        public async Task GetByAgeAsync() {
            var employee19 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19));
            var employee20 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20));
            await _client.RefreshAsync();

            var result = await _employeeRepository.GetByAgeAsync(employee19.Age);
            Assert.Equal(employee19.ToJson(), result.ToJson());

            var results = await _employeeRepository.GetAllByAgeAsync(employee20.Age);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee20.ToJson(), results.Documents.First().ToJson());
        }

        [Fact]
        public async Task GetByCompanyAsync() {
            var employee1 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19, companyId: EmployeeGenerator.DefaultCompanyId));
            var employee2 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20));
            await _client.RefreshAsync();

            var result = await _employeeRepository.GetByCompanyAsync(employee1.CompanyId);
            Assert.Equal(employee1.ToJson(), result.ToJson());

            var results = await _employeeRepository.GetAllByCompanyAsync(employee1.CompanyId);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee1.ToJson(), results.Documents.First().ToJson());

            Assert.Equal(1, await _employeeRepository.GetCountByCompanyAsync(employee1.CompanyId));
            await _employeeRepository.RemoveAsync(employee1, false);
            await _client.RefreshAsync();
            Assert.Equal(1, await _employeeRepository.CountAsync());
            Assert.Equal(0, await _employeeRepository.GetCountByCompanyAsync(employee1.CompanyId));
        }
    }
}