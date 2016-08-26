using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class VersionedTests : ElasticRepositoryTestBase {
        private readonly EmployeeRepository _employeeRepository;

        public VersionedTests(ITestOutputHelper output) : base(output) {
            _employeeRepository = new EmployeeRepository(_configuration, _cache, Log.CreateLogger<EmployeeRepository>());
            
            RemoveDataAsync().GetAwaiter().GetResult();
        }
        
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
        public async Task AddAndIgnoreHighVersion() {
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

        [Fact]
        public async Task UpdateAllWithSinglePageOfData() {
            var utcNow = SystemClock.UtcNow;
            var employees = new List<Employee> {
                EmployeeGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1), companyId: "1"),
                EmployeeGenerator.Generate(createdUtc: utcNow, companyId: "1"),
                EmployeeGenerator.Generate(createdUtc: utcNow, companyId: "2"),
            };

            await _employeeRepository.AddAsync(employees);
            await _client.RefreshAsync();
            Assert.True(employees.All(e => e.Version == 1));
            
            Assert.Equal(2, await _employeeRepository.UpdateCompanyNameByCompanyAsync("1", "Test Company"));
            await _client.RefreshAsync();

            var results = await _employeeRepository.GetAllByCompanyAsync("1");
            Assert.Equal(2, results.Documents.Count);
            foreach (var document in results.Documents) {
                Assert.Equal(2, document.Version);
                Assert.Equal("1", document.CompanyId);
                Assert.Equal("Test Company", document.CompanyName);
            }

            results = await _employeeRepository.GetAllByCompanyAsync("2");
            Assert.Equal(1, results.Documents.Count);
            Assert.Equal(employees.First(e => e.CompanyId == "2"), results.Documents.First());

            var company2Employees = results.Documents.ToList();
            long company2EmployeesVersion = company2Employees.First().Version;
            Assert.Equal(1, await _employeeRepository.IncrementYearsEmployeed(company2Employees.Select(e => e.Id).ToArray()));
            await _client.RefreshAsync();

            results = await _employeeRepository.GetAllByCompanyAsync("2");
            Assert.Equal(company2Employees.First().YearsEmployed + 1, results.Documents.First().YearsEmployed);
            
            await Assert.ThrowsAsync<ApplicationException>(async () => await _employeeRepository.SaveAsync(company2Employees));
            Assert.Equal(company2EmployeesVersion, company2Employees.First().Version);
        }

        [Fact]
        public async Task UpdateAllWithNoData() {
            Assert.Equal(0, await _employeeRepository.UpdateCompanyNameByCompanyAsync("1", "Test Company"));
        }

        [Fact]
        public async Task CanUsePaging() {
            const int NUMBER_OF_EMPLOYEES = 1000;
            const int PAGE_SIZE = 100;
            Log.MinimumLevel = LogLevel.Warning;

            var employees = EmployeeGenerator.GenerateEmployees(NUMBER_OF_EMPLOYEES, companyId: "1");
            await _employeeRepository.AddAsync(employees);

            await _client.RefreshAsync();
            Assert.Equal(NUMBER_OF_EMPLOYEES, await _employeeRepository.CountAsync());

            var results = await _employeeRepository.GetAllAsync(null, PAGE_SIZE);
            Assert.True(results.HasMore);

            var viewedIds = new HashSet<string>();
            int pagedRecords = 0;
            do {
                Assert.Equal(PAGE_SIZE, results.Documents.Count);
                Assert.Equal(NUMBER_OF_EMPLOYEES, results.Total);
                Assert.False(results.Hits.Any(h => viewedIds.Contains(h.Id)));
                viewedIds.AddRange(results.Hits.Select(h => h.Id));

                pagedRecords += results.Documents.Count;
            } while (await results.NextPageAsync());

            Assert.False(results.HasMore);
            Assert.True(employees.All(e => viewedIds.Contains(e.Id)));
            Assert.Equal(NUMBER_OF_EMPLOYEES, pagedRecords);
        }

        [Fact]
        public async Task CanUseSnapshotPaging() {
            const int NUMBER_OF_EMPLOYEES = 100;
            const int PAGE_SIZE = 10;

            Log.MinimumLevel = LogLevel.Warning;
            var employees = EmployeeGenerator.GenerateEmployees(NUMBER_OF_EMPLOYEES, companyId: "1");
            await _employeeRepository.AddAsync(employees);
            Log.MinimumLevel = LogLevel.Trace;

            await _client.RefreshAsync();
            Assert.Equal(NUMBER_OF_EMPLOYEES, await _employeeRepository.CountAsync());

            var results = await _employeeRepository.GetAllAsync(null, new ElasticPagingOptions().WithLimit(PAGE_SIZE).UseSnapshotPaging());
            Assert.True(results.HasMore);

            var viewedIds = new HashSet<string>();
            var newEmployees = new List<Employee>();
            int pagedRecords = 0;
            do {
                Assert.Equal(PAGE_SIZE, results.Documents.Count);
                Assert.Equal(NUMBER_OF_EMPLOYEES, results.Total);
                Assert.False(results.Hits.Any(h => viewedIds.Contains(h.Id)));
                viewedIds.AddRange(results.Hits.Select(h => h.Id));

                Assert.False(newEmployees.Any(d => viewedIds.Contains(d.Id)));

                pagedRecords += results.Documents.Count;
                newEmployees.Add(await _employeeRepository.AddAsync(EmployeeGenerator.Generate(companyId: "1")));
                await _client.RefreshAsync();
            } while (await results.NextPageAsync());

            Assert.False(results.HasMore);
            Assert.True(employees.All(e => viewedIds.Contains(e.Id)));
            Assert.Equal(NUMBER_OF_EMPLOYEES, pagedRecords);
        }

        [Fact]
        public async Task CanUseSnapshotWithScrollId() {
            const int NUMBER_OF_EMPLOYEES = 100;
            const int PAGE_SIZE = 10;

            Log.MinimumLevel = LogLevel.Warning;
            var employees = EmployeeGenerator.GenerateEmployees(NUMBER_OF_EMPLOYEES, companyId: "1");
            await _employeeRepository.AddAsync(employees);
            Log.MinimumLevel = LogLevel.Trace;

            await _client.RefreshAsync();
            Assert.Equal(NUMBER_OF_EMPLOYEES, await _employeeRepository.CountAsync());

            var results = await _employeeRepository.GetAllAsync(null, new ElasticPagingOptions().WithLimit(PAGE_SIZE).UseSnapshotPaging());
            Assert.True(results.HasMore);

            var viewedIds = new HashSet<string>();
            var newEmployees = new List<Employee>();
            int pagedRecords = 0;
            do {
                Assert.Equal(PAGE_SIZE, results.Documents.Count);
                Assert.Equal(NUMBER_OF_EMPLOYEES, results.Total);
                Assert.False(results.Hits.Any(h => viewedIds.Contains(h.Id)));
                viewedIds.AddRange(results.Hits.Select(h => h.Id));

                Assert.False(newEmployees.Any(d => viewedIds.Contains(d.Id)));

                pagedRecords += results.Documents.Count;
                newEmployees.Add(await _employeeRepository.AddAsync(EmployeeGenerator.Generate(companyId: "1")));
                await _client.RefreshAsync();

                results = await _employeeRepository.GetAllAsync(null, new ElasticPagingOptions().WithScrollId(results));
            } while (results != null && results.Hits.Count > 0);

            Assert.False(results.HasMore);
            Assert.True(employees.All(e => viewedIds.Contains(e.Id)));
            Assert.Equal(NUMBER_OF_EMPLOYEES, pagedRecords);
        }

        [Fact]
        public async Task CanUsePagingWithOddNumber() {
            const int NUMBER_OF_EMPLOYEES = 67;
            const int PAGE_SIZE = 12;
            Log.MinimumLevel = LogLevel.Warning;

            var employees = EmployeeGenerator.GenerateEmployees(NUMBER_OF_EMPLOYEES, companyId: "1");
            await _employeeRepository.AddAsync(employees);

            await _client.RefreshAsync();
            Assert.Equal(NUMBER_OF_EMPLOYEES, await _employeeRepository.CountAsync());

            var results = await _employeeRepository.GetAllAsync(null, PAGE_SIZE);
            Assert.True(results.HasMore);

            var viewedIds = new HashSet<string>();
            int pagedRecords = 0;
            do {
                Assert.Equal(Math.Min(PAGE_SIZE, NUMBER_OF_EMPLOYEES - pagedRecords), results.Documents.Count);
                Assert.Equal(NUMBER_OF_EMPLOYEES, results.Total);
                Assert.False(results.Hits.Any(h => viewedIds.Contains(h.Id)));
                viewedIds.AddRange(results.Hits.Select(h => h.Id));

                pagedRecords += results.Documents.Count;
            } while (await results.NextPageAsync());

            Assert.False(results.HasMore);
            Assert.True(employees.All(e => viewedIds.Contains(e.Id)));
            Assert.Equal(NUMBER_OF_EMPLOYEES, pagedRecords);
        }

        [Fact]
        public async Task UpdateAllWithPageLimit() {
            const int NUMBER_OF_EMPLOYEES = 100;
            Log.MinimumLevel = LogLevel.Warning;

            await _employeeRepository.AddAsync(EmployeeGenerator.GenerateEmployees(NUMBER_OF_EMPLOYEES, companyId: "1"));

            await _client.RefreshAsync();
            Assert.Equal(NUMBER_OF_EMPLOYEES, await _employeeRepository.UpdateCompanyNameByCompanyAsync("1", "Test Company", limit: 100));

            await _client.RefreshAsync();
            var results = await _employeeRepository.GetAllByCompanyAsync("1", NUMBER_OF_EMPLOYEES);
            Assert.Equal(NUMBER_OF_EMPLOYEES, results.Documents.Count);
            foreach (var document in results.Documents) {
                Assert.Equal(2, document.Version);
                Assert.Equal("1", document.CompanyId);
                Assert.Equal("Test Company", document.CompanyName);
            }
        }
        
        [Fact]
        public async Task UpdateAllWithNoPageLimit() {
            const int NUMBER_OF_EMPLOYEES = 100;
            await _employeeRepository.AddAsync(EmployeeGenerator.GenerateEmployees(NUMBER_OF_EMPLOYEES, companyId: "1"));

            await _client.RefreshAsync();
            Assert.Equal(NUMBER_OF_EMPLOYEES, await _employeeRepository.UpdateCompanyNameByCompanyAsync("1", "Test Company"));

            await _client.RefreshAsync();
            var results = await _employeeRepository.GetAllByCompanyAsync("1", paging: NUMBER_OF_EMPLOYEES);
            Assert.Equal(NUMBER_OF_EMPLOYEES, results.Documents.Count);
            foreach (var document in results.Documents) {
                Assert.Equal(2, document.Version);
                Assert.Equal("1", document.CompanyId);
                Assert.Equal("Test Company", document.CompanyName);
            }
        }

        // TODO: need versioning tests for index many when getParent & getindex == null; // This should never be the case.
        // TODO: need versioning tests for parent / child docs.
        // TODO: FindAs version tests
    }
}