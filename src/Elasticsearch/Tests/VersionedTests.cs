using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Jobs;
using Foundatio.Logging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class VersionedTests : ElasticRepositoryTestBase {
        private readonly EmployeeRepository _employeeRepository;
        private readonly IQueue<WorkItemData> _workItemQueue = new InMemoryQueue<WorkItemData>();

        public VersionedTests(ITestOutputHelper output) : base(output) {
            _employeeRepository = new EmployeeRepository(MyAppConfiguration, _cache, Log.CreateLogger<EmployeeRepository>());
            
            RemoveDataAsync().GetAwaiter().GetResult();
        }

        protected override ElasticConfiguration GetElasticConfiguration() {
            return new MyAppElasticConfiguration(_workItemQueue, _cache, Log);
        }

        private MyAppElasticConfiguration MyAppConfiguration => _configuration as MyAppElasticConfiguration;

        [Fact]
        public async Task Add() {
            var employee = EmployeeGenerator.Default;
            Assert.Equal(0, employee.Version);

            employee = await _employeeRepository.AddAsync(employee);
            Assert.NotNull(employee?.Id);
            Assert.Equal(1, employee.Version);
            
            Assert.Equal(employee, await _employeeRepository.GetByIdAsync(employee.Id));
        }

        [Fact]
        public async Task AddAndIngoreHighVersion() {
            var employee = EmployeeGenerator.Generate();
            employee.Version = 5;

            employee = await _employeeRepository.AddAsync(employee);
            Assert.NotNull(employee?.Id);
            Assert.Equal(1, employee.Version);

            Assert.Equal(employee, await _employeeRepository.GetByIdAsync(employee.Id));
        }

        [Fact]
        public async Task AddCollection() {
            var employee = EmployeeGenerator.Default;
            Assert.Equal(0, employee.Version);

            var employees = new List<Employee> { employee, EmployeeGenerator.Generate() };
            await _employeeRepository.AddAsync(employees);
            Assert.Equal(1, employee.Version);

            var result = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToList());
            Assert.Equal(2, result.Documents.Count);
            Assert.Equal(employees, result.Documents);
        }
        
        [Fact]
        public async Task Save() {
            var employee = EmployeeGenerator.Default;
            Assert.Equal(0, employee.Version);

            await _employeeRepository.AddAsync(new List<Employee> { employee });
            Assert.Equal(1, employee.Version);

            employee = await _employeeRepository.GetByIdAsync(employee.Id);
            var employeeCopy = await _employeeRepository.GetByIdAsync(employee.Id);
            Assert.Equal(employee, employeeCopy);
            Assert.Equal(1, employee.Version);

            employee.CompanyName = employeeCopy.CompanyName = "updated";

            employee = await _employeeRepository.SaveAsync(employee);
            Assert.Equal(employeeCopy.Version + 1, employee.Version);

            long version = employeeCopy.Version;
            await Assert.ThrowsAsync<ApplicationException>(async () => await _employeeRepository.SaveAsync(employeeCopy));
            Assert.Equal(version, employeeCopy.Version);

            await Assert.ThrowsAsync<ApplicationException>(async () => await _employeeRepository.SaveAsync(employeeCopy));
            Assert.Equal(version, employeeCopy.Version);

            Assert.Equal(employee, await _employeeRepository.GetByIdAsync(employee.Id));
        }
        
        [Fact]
        public async Task SaveWithHigherVersion() {
            var employee = EmployeeGenerator.Default;
            Assert.Equal(0, employee.Version);

            await _employeeRepository.AddAsync(new List<Employee> { employee });
            Assert.Equal(1, employee.Version);

            employee.Version = 5;
            await Assert.ThrowsAsync<ApplicationException>(async () => await _employeeRepository.SaveAsync(employee));
        }

        [Fact]
        public async Task SaveCollection() {
            var employee1 = EmployeeGenerator.Default;
            Assert.Equal(0, employee1.Version);

            var employee2 = EmployeeGenerator.Generate();
            await _employeeRepository.AddAsync(new List<Employee> { employee1, employee2 });
            Assert.Equal(1, employee1.Version);
            Assert.Equal(1, employee2.Version);

            employee1 = await _employeeRepository.GetByIdAsync(employee1.Id);
            var employeeCopy = await _employeeRepository.GetByIdAsync(employee1.Id);
            Assert.Equal(employee1, employeeCopy);
            Assert.Equal(1, employee1.Version);

            employee1.CompanyName = employeeCopy.CompanyName = "updated";
            
            await _employeeRepository.SaveAsync(new List<Employee> { employee1, employee2 });
            Assert.Equal(employeeCopy.Version + 1, employee1.Version);
            Assert.Equal(2, employee2.Version);

            await Assert.ThrowsAsync<ApplicationException>(async () => await _employeeRepository.SaveAsync(new List<Employee> { employeeCopy, employee2 }));
            Assert.NotEqual(employeeCopy.Version, employee1.Version);
            Assert.Equal(3, employee2.Version);

            await Assert.ThrowsAsync<ApplicationException>(async () => await _employeeRepository.SaveAsync(new List<Employee> { employeeCopy, employee2 }));
            Assert.NotEqual(employeeCopy.Version, employee1.Version);
            Assert.Equal(4, employee2.Version);

            Assert.Equal(employee2, await _employeeRepository.GetByIdAsync(employee2.Id));
        }

        // TODO need versioning tests for index many when getParent & getindex == null; // This should never be the case.
        // TODO need versioning tests for parent / child docs.
        // TODO UpdateAll version tests.
        // TODO: FindAs version tests
    }
}