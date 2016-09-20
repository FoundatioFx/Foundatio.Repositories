using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Repositories.Models;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class QueryTests : ElasticRepositoryTestBase {
        private readonly DailyLogEventRepository _dailyRepository;
        private readonly EmployeeRepository _employeeRepository;

        public QueryTests(ITestOutputHelper output) : base(output) {
            _dailyRepository = new DailyLogEventRepository(_configuration);
            _employeeRepository = new EmployeeRepository(_configuration);

            RemoveDataAsync().GetAwaiter().GetResult();
        }
        
        [Fact]
        public async Task GetByAgeAsync() {
            var employee19 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19));
            var employee20 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20));
            await _client.RefreshAsync();

            var results = await _employeeRepository.GetAllByAgeAsync(employee19.Age);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee19, results.Documents.First());

            results = await _employeeRepository.GetAllByAgeAsync(employee20.Age);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee20, results.Documents.First());
        }

        [Fact]
        public async Task GetByCompanyAsync() {
            var employee1 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19, companyId: EmployeeGenerator.DefaultCompanyId));
            var employee2 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20));
            await _client.RefreshAsync();
            
            var results = await _employeeRepository.GetAllByCompanyAsync(employee1.CompanyId);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee1, results.Documents.First());

            results = await _employeeRepository.GetAllByCompanyAsync(employee2.CompanyId);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee2, results.Documents.First());

            Assert.Equal(1, await _employeeRepository.GetCountByCompanyAsync(employee1.CompanyId));
            await _employeeRepository.RemoveAsync(employee1, false);
            await _client.RefreshAsync();
            Assert.Equal(1, await _employeeRepository.CountAsync());
            Assert.Equal(0, await _employeeRepository.GetCountByCompanyAsync(employee1.CompanyId));
        }
        
        [Fact]
        public async Task GetByMissingFieldAsync() {
            var employee1 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(companyId: EmployeeGenerator.DefaultCompanyId));
            var employee2 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(companyName: "Acme", name: "blake", companyId: EmployeeGenerator.DefaultCompanyId));

            await _client.RefreshAsync();

            // non analyzed field 
            var results = await _employeeRepository.GetNumberOfEmployeesWithMissingCompanyName(employee1.CompanyId);
            Assert.Equal(1, results.Total);

            // analyzed field
            results = await _employeeRepository.GetNumberOfEmployeesWithMissingName(employee1.CompanyId);
            Assert.Equal(1, results.Total);
        }
        
        [Fact]
        public async Task GetByCompanyWithIncludedFields() {
            var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"));
            Assert.NotNull(log?.Id);

            await _client.RefreshAsync();
            var results = await _dailyRepository.GetByCompanyAsync(log.CompanyId);
            Assert.Equal(1, results.Documents.Count);
            Assert.Equal(log, results.Documents.First());
            
            results = await _dailyRepository.GetPartialByCompanyAsync(log.CompanyId);
            Assert.Equal(1, results.Documents.Count);
            var companyLog = results.Documents.First();
            Assert.Equal(log.Id, companyLog.Id);
            Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
            Assert.Null(companyLog.Message);
            Assert.Null(companyLog.CompanyId);
        }
        
        [Fact]
        public async Task GetAgeByFilter() {
            await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19, companyId: EmployeeGenerator.DefaultCompanyId));
            await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20));

            await _client.RefreshAsync();
            var results = await GetByFilterAsync("age:19");
            Assert.Equal(1, results.Total);
            Assert.True(results.Documents.All(d => d.Age == 19));
            
            results = await GetByFilterAsync("age:>19");
            Assert.Equal(1, results.Total);
            Assert.True(results.Documents.All(d => d.Age > 19));
            
            results = await GetByFilterAsync("age:<19");
            Assert.Equal(0, results.Total);
        }

        [Fact]
        public async Task GetWithNoField() {
            await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19, companyId: EmployeeGenerator.DefaultCompanyId, name: "Blake Niemyjski"));

            await _client.RefreshAsync();
            var results = await GetByFilterAsync("blake");
            Assert.Equal(1, results.Total);
            Assert.True(results.Documents.All(d => d.Name == "Blake Niemyjski"));
        }

        /// <summary>
        /// Name field is Analyzed
        /// </summary>
        [Fact]
        public async Task GetNameByFilter() {
            var employeeEric = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(name: "Eric J. Smith"));
            var employeeBlake = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(name: "Blake Niemyjski"));

            await _client.RefreshAsync();
            var results = await GetByFilterAsync(null, "name:blake");
            Assert.Equal(1, results.Total);
            Assert.True(results.Documents.All(d => d.Name == employeeBlake.Name));

            results = await GetByFilterAsync(null, "name:\"Blake Niemyjski\"");
            Assert.Equal(1, results.Total);
            Assert.True(results.Documents.All(d => d.Name == employeeBlake.Name));

            results = await GetByFilterAsync(null, "name:Niemy* name:eric");
            Assert.Equal(2, results.Total);
            Assert.True(results.Hits.All(h => h.Score < 1));

            results = await GetByFilterAsync(null, "name:J*");
            Assert.Equal(1, results.Total);
            Assert.True(results.Documents.All(d => d.Name == employeeEric.Name));
            
            results = await GetByFilterAsync(null, "name:*");
            Assert.Equal(2, results.Total);

            await Assert.ThrowsAsync<FormatException>(async () => {
                await GetByFilterAsync(null, "name:");
            });

            // In this example we want to search a quoted string (E.G., GET /url).
            results = await GetByFilterAsync(null, "name:\"Blake /profile.url\"");
            Assert.Equal(0, results.Total);
        }

        /// <summary>
        /// Company field is NotAnalyzed
        /// </summary>
        [Fact]
        public async Task GetCompanyByFilter() {
            var employeeEric = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(name: "Eric J. Smith", companyName: "Exceptionless Test Company"));
            var employeeBlake = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(name: "Blake Niemyjski", companyName: "Exceptionless"));

            Log.SetLogLevel<EmployeeRepository>(LogLevel.Trace);

            await _client.RefreshAsync();
            var results = await GetByFilterAsync("company_name:Exceptionless");
            Assert.Equal(1, results.Total);
            Assert.True(results.Documents.All(d => d.Name == employeeBlake.Name));

            results = await GetByFilterAsync("company_name:\"Exceptionless\"");
            Assert.Equal(1, results.Total);
            Assert.True(results.Documents.All(d => d.Name == employeeBlake.Name));

            results = await GetByFilterAsync(null, "company_name:e*");
            Assert.Equal(0, results.Total);

            await Assert.ThrowsAsync<FormatException>(async () => {
                await GetByFilterAsync(null, "company_name:");
            });
        }

        private Task<FindResults<Employee>> GetByFilterAsync(string filter, string criteria = null) {
            Log.SetLogLevel<EmployeeRepository>(LogLevel.Trace);
            return _employeeRepository.SearchAsync(null, filter, criteria);
        }
    }
}