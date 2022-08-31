using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using System;
using System.Linq;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Utility;
using Xunit;
using Xunit.Abstractions;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;
using Foundatio.Parsers.LuceneQueries;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class QueryTests : ElasticRepositoryTestBase {
    private readonly ILogEventRepository _dailyRepository;
    private readonly IEmployeeRepository _employeeRepository;

    public QueryTests(ITestOutputHelper output) : base(output) {
        _dailyRepository = new DailyLogEventRepository(_configuration);
        _employeeRepository = new EmployeeRepository(_configuration);
    }

    public override async Task InitializeAsync() {
        await base.InitializeAsync();
        await RemoveDataAsync();
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
        Log.MinimumLevel = LogLevel.Trace;

        var employee1 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19, companyId: EmployeeGenerator.DefaultCompanyId), o => o.ImmediateConsistency());
        var employee2 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20, name: "Eric J. Smith", employmentType: EmploymentType.Contract), o => o.ImmediateConsistency());

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

        var result = await  _employeeRepository.FindAsync(q => q.FieldEquals(e => e.Age, 12));
        Assert.Empty(result.Documents);

        result = await _employeeRepository.FindAsync(q => q.FieldEquals(e => e.Name, "Eric J. Smith"));
        Assert.Single(result.Documents);

        result = await _employeeRepository.FindAsync(q => q.FieldEquals(e => e.Name, "Eric"));
        Assert.Empty(result.Documents);

        result = await _employeeRepository.FindAsync(q => q.FieldEquals(e => e.EmploymentType, EmploymentType.Contract));
        Assert.Single(result.Documents);

        result = await _employeeRepository.FindAsync(q => q.FieldEquals(e => e.EmploymentType, EmploymentType.Contract, EmploymentType.FullTime));
        Assert.Single(result.Documents);

        result = await _employeeRepository.FindAsync(q => q.FieldEquals(e => e.EmploymentType, new[] { EmploymentType.Contract, EmploymentType.FullTime }));
        Assert.Single(result.Documents);

        result = await _employeeRepository.FindAsync(q => q.FieldCondition(e => e.Name, ComparisonOperator.Contains, "Eric"));
        Assert.Single(result.Documents);

        var query = new RepositoryQuery<Employee>();
        query.FieldEquals(e => e.Age, 12);
        result = await  _employeeRepository.FindAsync(q => query);
        Assert.Empty(result.Documents);
        
        Assert.Equal(1, await _employeeRepository.CountAsync());
        Assert.Equal(0, await _employeeRepository.GetCountByCompanyAsync(employee1.CompanyId));

        employee1 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19, companyId: EmployeeGenerator.DefaultCompanyId), o => o.ImmediateConsistency());

        query = new RepositoryQuery<Employee>();
        query.FieldEquals(e => e.Name, null);
        result = await _employeeRepository.FindAsync(q => query);
        Assert.Single(result.Documents);
        Assert.Null(result.Documents.Single().Name);

        query = new RepositoryQuery<Employee>();
        query.FieldEmpty(e => e.Name);
        result = await _employeeRepository.FindAsync(q => query);
        Assert.Single(result.Documents);
        Assert.Null(result.Documents.Single().Name);

        query = new RepositoryQuery<Employee>();
        query.FieldNotEquals(e => e.Name, null);
        result = await _employeeRepository.FindAsync(q => query);
        Assert.Single(result.Documents);
        Assert.NotNull(result.Documents.Single().Name);

        query = new RepositoryQuery<Employee>();
        query.FieldHasValue(e => e.Name);
        result = await _employeeRepository.FindAsync(q => query);
        Assert.Single(result.Documents);
        Assert.NotNull(result.Documents.Single().Name);
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

        var results = await _dailyRepository.FindAsync(q => q.Company(log.CompanyId));
        Assert.Equal(1, results.Documents.Count);
        Assert.Equal(log, results.Documents.First());
        
        results = await _dailyRepository.FindAsync(q => q.Company(log.CompanyId).Include(e => e.Id).Include(l => l.CreatedUtc));
        Assert.Equal(1, results.Documents.Count);
        var companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Null(companyLog.Message);
        Assert.Null(companyLog.CompanyId);
    }

    [Fact]
    public async Task GetByCompanyWithIncludeMask() {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());
        Assert.NotNull(log?.Id);

        var results = await _dailyRepository.FindAsync(q => q.Company(log.CompanyId));
        Assert.Equal(1, results.Documents.Count);
        Assert.Equal(log, results.Documents.First());

        results = await _dailyRepository.FindAsync(q => q.Company(log.CompanyId).IncludeMask("iD,Createdutc"), o => o.QueryLogLevel(LogLevel.Warning));
        Assert.Equal(1, results.Documents.Count);
        var companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Null(companyLog.Message);
        Assert.Null(companyLog.CompanyId);
    }

    [Fact]
    public async Task CanHandleIncludeWithWrongCasing() {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test", stuff: "stuff"), o => o.ImmediateConsistency());
        Assert.NotNull(log?.Id);

        var results = await _dailyRepository.FindAsync(q => q.IncludeMask("meTa(sTuFf)  ,  CreaTedUtc"), o => o.Include(e => e.Id).QueryLogLevel(LogLevel.Warning));
        Assert.Equal(1, results.Documents.Count);
        var companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Null(companyLog.Message);
        Assert.Null(companyLog.CompanyId);

        results = await _dailyRepository.FindAsync(q => q.Include(e => e.Id).Include("createdUtc"), o => o.QueryLogLevel(LogLevel.Warning));
        Assert.Equal(1, results.Documents.Count);
        companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Null(companyLog.Message);
        Assert.Null(companyLog.CompanyId);
    }

    [Fact]
    public async Task CanHandleExcludeWithWrongCasing() {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());
        Assert.NotNull(log?.Id);

        var results = await _dailyRepository.FindAsync(q => q.Exclude("CreatedUtc"));
        Assert.Equal(1, results.Documents.Count);
        var companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.NotEqual(log.CreatedUtc, companyLog.CreatedUtc);

        results = await _dailyRepository.FindAsync(q => q.Exclude("createdUtc"));
        Assert.Equal(1, results.Documents.Count);
        companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.NotEqual(log.CreatedUtc, companyLog.CreatedUtc);
    }

    [Fact]
    public async Task CanHandleIncludeAndExclude() {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test", stuff: "stuff"), o => o.ImmediateConsistency());
        Assert.NotNull(log?.Id);

        var results = await _dailyRepository.FindAsync(q => q.Exclude(e => e.Date).Include(e => e.Id).Include("createdUtc"), o => o.QueryLogLevel(LogLevel.Warning));
        Assert.Equal(1, results.Documents.Count);
        var companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Equal(default, companyLog.Date);
        Assert.Null(companyLog.Message);
        Assert.Null(companyLog.CompanyId);
    }

    [Fact]
    public async Task GetByCompanyWithExcludeMask() {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());
        Assert.NotNull(log?.Id);

        var results = await _dailyRepository.FindAsync(q => q.ExcludeMask("CREATEDUtc"), o => o.ExcludeMask("MessAge").QueryLogLevel(LogLevel.Warning));
        Assert.Equal(1, results.Documents.Count);
        var companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.NotEqual(log.CreatedUtc, companyLog.CreatedUtc);

        results = await _dailyRepository.FindAsync(q => q.Company(log.CompanyId).ExcludeMask("Createdutc"), o => o.QueryLogLevel(LogLevel.Warning));
        Assert.Equal(1, results.Documents.Count);
        companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.NotEqual(log.CreatedUtc, companyLog.CreatedUtc);
    }

    [Fact]
    public async Task GetByCreatedDate() {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test", createdUtc: SystemClock.UtcNow), o => o.ImmediateConsistency());
        Assert.NotNull(log?.Id);

        var results = await _dailyRepository.GetByDateRange(SystemClock.UtcNow.SubtractDays(1), SystemClock.UtcNow.AddDays(1));
        Assert.Equal(log, results.Documents.Single());
        
        results = await _dailyRepository.GetByDateRange(SystemClock.UtcNow.SubtractDays(1), DateTime.MaxValue);
        Assert.Equal(log, results.Documents.Single());
        
        results = await _dailyRepository.GetByDateRange(DateTime.MinValue, SystemClock.UtcNow.AddDays(1));
        Assert.Equal(log, results.Documents.Single());
        
        results = await _dailyRepository.GetByDateRange(DateTime.MinValue, DateTime.MaxValue);
        Assert.Equal(log, results.Documents.Single());
        
        results = await _dailyRepository.GetByDateRange(SystemClock.UtcNow.AddDays(1), DateTime.MaxValue);
        Assert.Empty(results.Documents);
    }

    [Fact]
    public async Task GetAgeByFilter() {
        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19, companyId: EmployeeGenerator.DefaultCompanyId), o => o.ImmediateConsistency());
        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

        var results = await _employeeRepository.FindAsync(q => q.FilterExpression("age:19"));
        Assert.Equal(1, results.Total);
        Assert.True(results.Documents.All(d => d.Age == 19));
        
        results = await _employeeRepository.FindAsync(q => q.FilterExpression("age:>18 AND age:<=19"));
        Assert.Equal(1, results.Total);
        
        results = await _employeeRepository.FindAsync(q => q.FilterExpression("age:[18..19]"));
        Assert.Equal(1, results.Total);

        results = await _employeeRepository.FindAsync(q => q.FilterExpression("age:>19"));
        Assert.Equal(1, results.Total);
        Assert.True(results.Documents.All(d => d.Age > 19));

        results = await _employeeRepository.FindAsync(q => q.FilterExpression("age:<19"));
        Assert.Equal(0, results.Total);
        
        results = await _employeeRepository.FindAsync(q => q.FilterExpression("_missing_:age"));
        Assert.Equal(0, results.Total);

        results = await _employeeRepository.FindAsync(q => q.FilterExpression("_exists_:age"));
        Assert.Equal(2, results.Total);
    }

    [Fact]
    public async Task GetWithNoField() {
        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19, companyId: EmployeeGenerator.DefaultCompanyId, name: "Blake Niemyjski"), o => o.ImmediateConsistency());

        var results = await _employeeRepository.FindAsync(q => q.FilterExpression("blake"));
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

        var results = await _employeeRepository.FindAsync(q => q.SearchExpression("name:blake"));
        Assert.Equal(1, results.Total);
        Assert.True(results.Documents.All(d => d.Name == employeeBlake.Name));

        results = await _employeeRepository.FindAsync(q => q.SearchExpression("name:\"Blake Niemyjski\""));
        Assert.Equal(1, results.Total);
        Assert.True(results.Documents.All(d => d.Name == employeeBlake.Name));

        results = await _employeeRepository.FindAsync(q => q.SearchExpression("name:Niemy* name:eric"));
        Assert.Equal(2, results.Total);

        results = await _employeeRepository.FindAsync(q => q.SearchExpression("name:J*"));
        Assert.Equal(1, results.Total);
        Assert.True(results.Documents.All(d => d.Name == employeeEric.Name));

        await Assert.ThrowsAsync<QueryValidationException>(async () => {
            await _employeeRepository.FindAsync(q => q.SearchExpression("name:"));
        });

        // In this example we want to search a quoted string (E.G., GET /url).
        results = await _employeeRepository.FindAsync(q => q.SearchExpression("name:\"Blake /profile.url\""));
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

        var results = await _employeeRepository.FindAsync(q => q.FilterExpression("companyName:Exceptionless"));
        Assert.Equal(1, results.Total);
        Assert.True(results.Documents.All(d => d.Name == employeeBlake.Name));

        results = await _employeeRepository.FindAsync(q => q.FilterExpression("companyName:\"Exceptionless\""));
        Assert.Equal(1, results.Total);
        Assert.True(results.Documents.All(d => d.Name == employeeBlake.Name));

        results = await _employeeRepository.FindAsync(q => q.SearchExpression("companyName:e*"));
        Assert.Equal(0, results.Total);

        await Assert.ThrowsAsync<QueryValidationException>(async () => {
            await _employeeRepository.FindAsync(q => q.SearchExpression("companyName:"));
        });
    }

    [Fact]
    public async Task GetByEmailAddressFilter() {
        var findResult = await _employeeRepository.GetByEmailAddressAsync(EmployeeGenerator.Default.EmailAddress);
        Assert.Null(findResult);
        Assert.Equal(1, _cache.Writes);
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(2, _cache.Misses); // one for soft deleted ids

        // missing value should be cached
        findResult = await _employeeRepository.GetByEmailAddressAsync(EmployeeGenerator.Default.EmailAddress);
        Assert.DoesNotContain(Log.LogEntries, l => l.LogLevel == LogLevel.Error);
        Assert.Null(findResult);
        Assert.Equal(1, _cache.Writes);
        Assert.Equal(1, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.Cache());
        Assert.NotNull(employee?.Id);
        Assert.NotNull(employee.EmailAddress);
        Assert.Equal(3, _cache.Writes);
        Assert.Equal(2, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        await _employeeRepository.SaveAsync(employee, o => o.Cache());
        Assert.Equal(2, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        Assert.Equal(employee, await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache()));
        Assert.Equal(2, _cache.Count);
        Assert.Equal(2, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        var idsResult = await _employeeRepository.GetByIdsAsync(new[] { employee.Id }, o => o.Cache());
        Assert.Equal(employee, idsResult.Single());
        Assert.Equal(2, _cache.Count);
        Assert.Equal(3, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        findResult = await _employeeRepository.GetByEmailAddressAsync(employee.EmailAddress);
        Assert.Equal(employee, findResult.Document);
        Assert.Equal(2, _cache.Count);
        Assert.Equal(4, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        await _employeeRepository.InvalidateCacheAsync(employee);
        Assert.Equal(0, _cache.Count);
        Assert.Equal(4, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        findResult = await _employeeRepository.GetByEmailAddressAsync(employee.EmailAddress);
        Assert.Null(findResult);
        Assert.Equal(1, _cache.Count);
        Assert.Equal(4, _cache.Hits);
        Assert.Equal(4, _cache.Misses);
    }
}
