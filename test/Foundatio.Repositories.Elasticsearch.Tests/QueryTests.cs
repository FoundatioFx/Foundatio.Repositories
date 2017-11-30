using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

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
            var employee19 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19), o => o.ImmediateConsistency());
            var employee20 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

            var results = await _employeeRepository.GetAllByAgeAsync(employee19.Age);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee19, results.Documents.First());

            results = await _employeeRepository.GetAllByAgeAsync(employee20.Age);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee20, results.Documents.First());
        }

        [Fact]
        public async Task GetByCompanyAsync() {
            var employee1 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19, companyId: EmployeeGenerator.DefaultCompanyId), o => o.ImmediateConsistency());
            var employee2 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

            var results = await _employeeRepository.GetAllByCompanyAsync(employee1.CompanyId);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee1, results.Documents.First());

            results = await _employeeRepository.GetAllByCompanyAsync(employee2.CompanyId);
            Assert.Equal(1, results.Total);
            Assert.Equal(employee2, results.Documents.First());

            results = await _employeeRepository.GetAllByCompaniesWithFieldEqualsAsync(new string[] { employee1.CompanyId });
            Assert.Equal(1, results.Total);

            results = await _employeeRepository.GetAllByCompaniesWithFieldEqualsAsync(new string[] { employee1.CompanyId, employee2.CompanyId });
            Assert.Equal(2, results.Total);

            Assert.Equal(1, await _employeeRepository.GetCountByCompanyAsync(employee1.CompanyId));
            await _employeeRepository.RemoveAsync(employee1, o => o.Cache().ImmediateConsistency());

           await  _employeeRepository.GetByQueryAsync(q => q.FieldCondition(e => e.Age, ComparisonOperator.Equals, 12));
            Assert.Equal(1, await _employeeRepository.CountAsync());
            Assert.Equal(0, await _employeeRepository.GetCountByCompanyAsync(employee1.CompanyId));
        }

        [Fact]
        public async Task GetByMissingFieldAsync() {
            var employee1 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(companyId: EmployeeGenerator.DefaultCompanyId), o => o.ImmediateConsistency());
            var employee2 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(companyName: "Acme", name: "blake", companyId: EmployeeGenerator.DefaultCompanyId), o => o.ImmediateConsistency());

            // non analyzed field
            var results = await _employeeRepository.GetNumberOfEmployeesWithMissingCompanyName(employee1.CompanyId);
            Assert.Equal(1, results.Total);

            // analyzed field
            results = await _employeeRepository.GetNumberOfEmployeesWithMissingName(employee1.CompanyId);
            Assert.Equal(1, results.Total);
        }

        [Fact]
        public async Task GetByCompanyWithIncludedFields() {
            var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());
            Assert.NotNull(log?.Id);

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
            await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19, companyId: EmployeeGenerator.DefaultCompanyId), o => o.ImmediateConsistency());
            await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

            var results = await _employeeRepository.GetByFilterAsync("age:19");
            Assert.Equal(1, results.Total);
            Assert.True(results.Documents.All(d => d.Age == 19));

            results = await _employeeRepository.GetByFilterAsync("age:>19");
            Assert.Equal(1, results.Total);
            Assert.True(results.Documents.All(d => d.Age > 19));

            results = await _employeeRepository.GetByFilterAsync("age:<19");
            Assert.Equal(0, results.Total);
        }

        [Fact]
        public async Task GetWithNoField() {
            await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19, companyId: EmployeeGenerator.DefaultCompanyId, name: "Blake Niemyjski"), o => o.ImmediateConsistency());

            var results = await _employeeRepository.GetByFilterAsync("blake");
            Assert.Equal(1, results.Total);
            Assert.True(results.Documents.All(d => d.Name == "Blake Niemyjski"));
        }

        /// <summary>
        /// Name field is Analyzed
        /// </summary>
        [Fact]
        public async Task GetNameByFilter() {
            var employeeEric = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(name: "Eric J. Smith"), o => o.ImmediateConsistency());
            var employeeBlake = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(name: "Blake Niemyjski"), o => o.ImmediateConsistency());

            var results = await _employeeRepository.GetByCriteriaAsync("name:blake");
            Assert.Equal(1, results.Total);
            Assert.True(results.Documents.All(d => d.Name == employeeBlake.Name));

            results = await _employeeRepository.GetByCriteriaAsync("name:\"Blake Niemyjski\"");
            Assert.Equal(1, results.Total);
            Assert.True(results.Documents.All(d => d.Name == employeeBlake.Name));

            results = await _employeeRepository.GetByCriteriaAsync("name:Niemy* name:eric");
            Assert.Equal(2, results.Total);

            results = await _employeeRepository.GetByCriteriaAsync("name:J*");
            Assert.Equal(1, results.Total);
            Assert.True(results.Documents.All(d => d.Name == employeeEric.Name));

            results = await _employeeRepository.GetByCriteriaAsync("name:*");
            Assert.Equal(2, results.Total);
            Assert.Equal(2, results.Hits.Sum(h => h.Score));

            await Assert.ThrowsAsync<FormatException>(async () => {
                await _employeeRepository.GetByCriteriaAsync( "name:");
            });

            // In this example we want to search a quoted string (E.G., GET /url).
            results = await _employeeRepository.GetByCriteriaAsync("name:\"Blake /profile.url\"");
            Assert.Equal(0, results.Total);
        }

        /// <summary>
        /// Company field is NotAnalyzed
        /// </summary>
        [Fact]
        public async Task GetCompanyByFilter() {
            var employeeEric = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(name: "Eric J. Smith", companyName: "Exceptionless Test Company"), o => o.ImmediateConsistency());
            var employeeBlake = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(name: "Blake Niemyjski", companyName: "Exceptionless"), o => o.ImmediateConsistency());

            Log.SetLogLevel<EmployeeRepository>(LogLevel.Trace);

            var results = await _employeeRepository.GetByFilterAsync("companyName:Exceptionless");
            Assert.Equal(1, results.Total);
            Assert.True(results.Documents.All(d => d.Name == employeeBlake.Name));

            results = await _employeeRepository.GetByFilterAsync("companyName:\"Exceptionless\"");
            Assert.Equal(1, results.Total);
            Assert.True(results.Documents.All(d => d.Name == employeeBlake.Name));

            results = await _employeeRepository.GetByCriteriaAsync("companyName:e*");
            Assert.Equal(0, results.Total);

            await Assert.ThrowsAsync<FormatException>(async () => {
                await _employeeRepository.GetByCriteriaAsync("companyName:");
            });
        }
    }
}