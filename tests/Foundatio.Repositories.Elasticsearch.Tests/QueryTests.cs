using System;
using System.Linq;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Xunit;
using FieldQueryValidationException = Foundatio.Repositories.Exceptions.QueryValidationException;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;
using ParserQueryValidationException = Foundatio.Parsers.LuceneQueries.QueryValidationException;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class QueryTests : ElasticRepositoryTestBase
{
    private readonly ILogEventRepository _dailyRepository;
    private readonly ILogEventRepository _dailyWithCompanyDefaultExcludeRepository;
    private readonly ILogEventRepository _dailyWithRequiredCompanyRepository;
    private readonly ILogEventRepository _dailyWithRequiredCompanyAndDefaultExcludeRepository;
    private readonly IEmployeeRepository _employeeRepository;

    public QueryTests(ITestOutputHelper output) : base(output)
    {
        _dailyRepository = new DailyLogEventRepository(_configuration);
        _dailyWithCompanyDefaultExcludeRepository = new DailyLogEventWithCompanyDefaultExcludeRepository(_configuration);
        _dailyWithRequiredCompanyRepository = new DailyLogEventWithRequiredCompanyRepository(_configuration);
        _dailyWithRequiredCompanyAndDefaultExcludeRepository = new DailyLogEventWithRequiredCompanyAndDefaultExcludeRepository(_configuration);
        _employeeRepository = new EmployeeRepository(_configuration);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    [Fact]
    public async Task GetByAgeAsync()
    {
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
    public async Task GetByCompanyAsync()
    {
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

        var result = await _employeeRepository.FindAsync(q => q.FieldEquals(e => e.Age, 12));
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
        result = await _employeeRepository.FindAsync(q => query);
        Assert.Empty(result.Documents);

        Assert.Equal(1, await _employeeRepository.CountAsync());
        Assert.Equal(0, await _employeeRepository.GetCountByCompanyAsync(employee1.CompanyId));

        employee1 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19, companyId: EmployeeGenerator.DefaultCompanyId), o => o.ImmediateConsistency());

        query = new RepositoryQuery<Employee>();
        query.FieldEquals(e => e.Name, null!);
        result = await _employeeRepository.FindAsync(q => query);
        Assert.Single(result.Documents);
        Assert.Null(result.Documents.Single().Name);

        query = new RepositoryQuery<Employee>();
        query.FieldEmpty(e => e.Name);
        result = await _employeeRepository.FindAsync(q => query);
        Assert.Single(result.Documents);
        Assert.Null(result.Documents.Single().Name);

        query = new RepositoryQuery<Employee>();
        query.FieldNotEquals(e => e.Name, null!);
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
    public async Task FieldConditionContainsNull_WithNullValue_BehavesAsIsEmpty()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 19, name: "Eric J. Smith"),
            EmployeeGenerator.Generate(age: 20)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldCondition(e => e.Name, ComparisonOperator.Contains, (object)null!));

        // Assert — null Contains rewrites to IsEmpty, so finds the employee without a name
        Assert.Single(result.Documents);
        Assert.Null(result.Documents.Single().Name);
    }

    [Fact]
    public async Task FieldConditionNotContainsNull_WithNullValue_BehavesAsHasValue()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 19, name: "Eric J. Smith"),
            EmployeeGenerator.Generate(age: 20)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldCondition(e => e.Name, ComparisonOperator.NotContains, (object)null!));

        // Assert — null NotContains rewrites to HasValue, so finds the employee with a name
        Assert.Single(result.Documents);
        Assert.NotNull(result.Documents.Single().Name);
    }

    [Fact]
    public async Task GetByMissingFieldAsync()
    {
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
    public async Task GetByCompanyWithIncludedFields()
    {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());
        Assert.NotNull(log);
        Assert.NotNull(log.Id);

        var results = await _dailyRepository.FindAsync(q => q.Company(log.CompanyId));
        Assert.Single(results.Documents);
        Assert.Equal(log, results.Documents.First());

        results = await _dailyRepository.FindAsync(q => q.Company(log.CompanyId).Include(e => e.Id).Include(l => l.CreatedUtc));
        Assert.Single(results.Documents);
        var companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Null(companyLog.Message);
        Assert.Null(companyLog.CompanyId);
    }

    [Fact]
    public async Task GetByCompanyWithIncludeMask()
    {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());
        Assert.NotNull(log);
        Assert.NotNull(log.Id);

        var results = await _dailyRepository.FindAsync(q => q.Company(log.CompanyId));
        Assert.Single(results.Documents);
        Assert.Equal(log, results.Documents.First());

        results = await _dailyRepository.FindAsync(q => q.Company(log.CompanyId).IncludeMask("iD,Createdutc"), o => o.QueryLogLevel(LogLevel.Warning));
        Assert.Single(results.Documents);
        var companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Null(companyLog.Message);
        Assert.Null(companyLog.CompanyId);
    }

    [Fact]
    public async Task CanHandleIncludeWithWrongCasing()
    {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test", stuff: "stuff"), o => o.ImmediateConsistency());
        Assert.NotNull(log);
        Assert.NotNull(log.Id);

        var results = await _dailyRepository.FindAsync(q => q.IncludeMask("meTa(sTuFf)  ,  CreaTedUtc"), o => o.Include(e => e.Id).QueryLogLevel(LogLevel.Warning));
        Assert.Single(results.Documents);
        var companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Null(companyLog.Message);
        Assert.Null(companyLog.CompanyId);

        results = await _dailyRepository.FindAsync(q => q.Include(e => e.Id).Include("createdUtc"), o => o.QueryLogLevel(LogLevel.Warning));
        Assert.Single(results.Documents);
        companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Null(companyLog.Message);
        Assert.Null(companyLog.CompanyId);
    }

    [Fact]
    public async Task CanHandleExcludeWithWrongCasing()
    {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());
        Assert.NotNull(log);
        Assert.NotNull(log.Id);

        var results = await _dailyRepository.FindAsync(q => q.Exclude("CreatedUtc"));
        Assert.Single(results.Documents);
        var companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Equal(log.CompanyId, companyLog.CompanyId);
        Assert.Equal(log.Message, companyLog.Message);

        results = await _dailyRepository.FindAsync(q => q.Exclude("createdUtc"));
        Assert.Single(results.Documents);
        companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Equal(log.CompanyId, companyLog.CompanyId);
        Assert.Equal(log.Message, companyLog.Message);
    }

    [Fact]
    public async Task CanHandleIncludeAndExclude()
    {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test", stuff: "stuff"), o => o.ImmediateConsistency());
        Assert.NotNull(log);
        Assert.NotNull(log.Id);

        var results = await _dailyRepository.FindAsync(q => q.Exclude(e => e.Date).Include(e => e.Id).Include("createdUtc"), o => o.QueryLogLevel(LogLevel.Warning));
        Assert.Single(results.Documents);
        var companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Equal(default, companyLog.Date);
        Assert.Null(companyLog.Message);
        Assert.Null(companyLog.CompanyId);
        Assert.Null(companyLog.Meta);
    }

    [Fact]
    public async Task CanHandleIncludeAndExcludeOnGetById()
    {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test", stuff: "stuff"), o => o.ImmediateConsistency());
        Assert.NotNull(log);
        Assert.NotNull(log.Id);

        var companyLog = await _dailyRepository.GetByIdAsync(log!.Id, o => o.QueryLogLevel(LogLevel.Warning).Exclude(e => e.Date).Include(e => e.Id).Include("createdUtc"));
        Assert.NotNull(companyLog);
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Equal(default, companyLog.Date);
        Assert.Null(companyLog.Message);
        Assert.Null(companyLog.CompanyId);
        Assert.Null(companyLog.Meta);
    }

    [Fact]
    public async Task CanHandleIncludeAndExcludeOnGetByIdWithCaching()
    {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test", stuff: "stuff"), o => o.ImmediateConsistency());
        Assert.NotNull(log);
        Assert.NotNull(log.Id);

        Assert.Equal(1, _cache.Misses);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(1, _cache.Writes);
        Assert.Collection(_cache.Items, c => Assert.StartsWith("alias:daily-logevents-", c.Key));

        const string cacheKey = "company:1234567890";
        var companyLog = await _dailyRepository.GetByIdAsync(log!.Id, o => o.QueryLogLevel(LogLevel.Warning).Exclude(e => e.Date).Include(e => e.Id).Include("createdUtc").Cache(cacheKey));
        Assert.NotNull(companyLog);
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Equal(default, companyLog.Date);
        Assert.Null(companyLog.Message);
        Assert.Null(companyLog.CompanyId);
        Assert.Null(companyLog.Meta);

        Assert.Equal(2, _cache.Misses);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(2, _cache.Writes);
        Assert.Collection(_cache.Items, c => Assert.StartsWith("alias:daily-logevents-", c.Key), c => Assert.Equal($"LogEvent:{cacheKey}", c.Key));

        // Ensure cache hit by cache key.
        companyLog = await _dailyRepository.GetByIdAsync(log!.Id, o => o.QueryLogLevel(LogLevel.Warning).Exclude(e => e.Date).Include(e => e.Id).Include("createdUtc").Cache(cacheKey));
        Assert.NotNull(companyLog);
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Equal(default, companyLog.Date);
        Assert.Null(companyLog.Message);
        Assert.Null(companyLog.CompanyId);
        Assert.Null(companyLog.Meta);

        Assert.Equal(2, _cache.Misses);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(2, _cache.Writes);
        Assert.Collection(_cache.Items, c => Assert.StartsWith("alias:daily-logevents-", c.Key), c => Assert.Equal($"LogEvent:{cacheKey}", c.Key));
    }

    [Fact]
    public async Task CanHandleIncludeAndExcludeOnGetByIds()
    {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test", stuff: "stuff"), o => o.ImmediateConsistency());
        Assert.NotNull(log);
        Assert.NotNull(log.Id);

        var results = await _dailyRepository.GetByIdsAsync([log!.Id], o => o.QueryLogLevel(LogLevel.Warning).Exclude(e => e.Date).Include(e => e.Id).Include("createdUtc"));
        Assert.Single(results);
        var companyLog = results.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Equal(default, companyLog.Date);
        Assert.Null(companyLog.Message);
        Assert.Null(companyLog.CompanyId);
        Assert.Null(companyLog.Meta);
    }

    [Fact]
    public async Task CanHandleIncludeAndExcludeOnGetByIdsWithCaching()
    {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test", stuff: "stuff"), o => o.ImmediateConsistency());
        Assert.NotNull(log);
        Assert.NotNull(log.Id);

        Assert.Equal(1, _cache.Misses);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(1, _cache.Writes);
        Assert.Collection(_cache.Items, c => Assert.StartsWith("alias:daily-logevents-", c.Key));

        const string cacheKey = "company:1234567890";
        var results = await _dailyRepository.GetByIdsAsync([log!.Id], o => o.QueryLogLevel(LogLevel.Warning).Exclude(e => e.Date).Include(e => e.Id).Include("createdUtc").Cache(cacheKey));
        Assert.Single(results);
        var companyLog = results.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Equal(default, companyLog.Date);
        Assert.Null(companyLog.Message);
        Assert.Null(companyLog.CompanyId);
        Assert.Null(companyLog.Meta);

        Assert.Equal(2, _cache.Misses);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(2, _cache.Writes);
        Assert.Collection(_cache.Items, c => Assert.StartsWith("alias:daily-logevents-", c.Key), c => Assert.Equal($"LogEvent:{cacheKey}", c.Key));

        // Ensure cache hit by cache key.
        results = await _dailyRepository.GetByIdsAsync([log!.Id], o => o.QueryLogLevel(LogLevel.Warning).Exclude(e => e.Date).Include(e => e.Id).Include("createdUtc").Cache(cacheKey));
        Assert.Single(results);
        companyLog = results.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
        Assert.Equal(default, companyLog.Date);
        Assert.Null(companyLog.Message);
        Assert.Null(companyLog.CompanyId);
        Assert.Null(companyLog.Meta);

        Assert.Equal(2, _cache.Misses);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(2, _cache.Writes);
        Assert.Collection(_cache.Items, c => Assert.StartsWith("alias:daily-logevents-", c.Key), c => Assert.Equal($"LogEvent:{cacheKey}", c.Key));
    }

    [Fact]
    public async Task GetByCompanyWithIncludeWillOverrideDefaultExclude()
    {
        var log = await _dailyWithCompanyDefaultExcludeRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());
        Assert.NotNull(log);
        Assert.NotNull(log.Id);

        var results = await _dailyWithCompanyDefaultExcludeRepository.FindAsync(q => q.Include(e => e.CompanyId), o => o.QueryLogLevel(LogLevel.Warning));
        Assert.Single(results.Documents);
        var companyLog = results.Documents.Single();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.NotNull(companyLog.CompanyId);

        results = await _dailyWithCompanyDefaultExcludeRepository.FindAsync(q => q.Company(log.CompanyId), o => o.Include(e => e.CompanyId).QueryLogLevel(LogLevel.Warning));
        Assert.Single(results.Documents);
        companyLog = results.Documents.Single();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.NotNull(companyLog.CompanyId);

        results = await _dailyWithCompanyDefaultExcludeRepository.FindAsync(q => q.Company(log.CompanyId), o => o.QueryLogLevel(LogLevel.Warning));
        Assert.Single(results.Documents);
        companyLog = results.Documents.Single();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Null(companyLog.CompanyId);
    }

    [Fact]
    public async Task GetByCompanyWithExcludeMask()
    {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());
        Assert.NotNull(log);
        Assert.NotNull(log.Id);

        var results = await _dailyRepository.FindAsync(q => q.ExcludeMask("CREATEDUtc"), o => o.ExcludeMask("MessAge").QueryLogLevel(LogLevel.Warning));
        Assert.Single(results.Documents);
        var companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);

        results = await _dailyRepository.FindAsync(q => q.Company(log.CompanyId).ExcludeMask("Createdutc"), o => o.QueryLogLevel(LogLevel.Warning));
        Assert.Single(results.Documents);
        companyLog = results.Documents.First();
        Assert.Equal(log.Id, companyLog.Id);
        Assert.Equal(log.CreatedUtc, companyLog.CreatedUtc);
    }

    [Fact]
    public async Task GetByCreatedDate()
    {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test", createdUtc: DateTime.UtcNow), o => o.ImmediateConsistency());
        Assert.NotNull(log);
        Assert.NotNull(log.Id);

        var results = await _dailyRepository.GetByDateRange(DateTime.UtcNow.SubtractDays(1), DateTime.UtcNow.AddDays(1));
        Assert.Equal(log, results.Documents.Single());

        results = await _dailyRepository.GetByDateRange(DateTime.UtcNow.SubtractDays(1), DateTime.MaxValue);
        Assert.Equal(log, results.Documents.Single());

        results = await _dailyRepository.GetByDateRange(DateTime.MinValue, DateTime.UtcNow.AddDays(1));
        Assert.Equal(log, results.Documents.Single());

        results = await _dailyRepository.GetByDateRange(DateTime.MinValue, DateTime.MaxValue);
        Assert.Equal(log, results.Documents.Single());

        results = await _dailyRepository.GetByDateRange(DateTime.UtcNow.AddDays(1), DateTime.MaxValue);
        Assert.Empty(results.Documents);
    }

    [Fact]
    public async Task GetAgeByFilter()
    {
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
    public async Task GetWithNoField()
    {
        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 19, companyId: EmployeeGenerator.DefaultCompanyId, name: "Blake Niemyjski"), o => o.ImmediateConsistency());

        var results = await _employeeRepository.FindAsync(q => q.FilterExpression("blake"));
        Assert.Equal(1, results.Total);
        Assert.True(results.Documents.All(d => d.Name == "Blake Niemyjski"));
    }

    /// <summary>
    /// Name field is Analyzed
    /// </summary>
    [Fact]
    public async Task GetNameByFilter()
    {
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

        await Assert.ThrowsAsync<ParserQueryValidationException>(async () =>
        {
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
    public async Task GetCompanyByFilter()
    {
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

        await Assert.ThrowsAsync<ParserQueryValidationException>(async () =>
        {
            await _employeeRepository.FindAsync(q => q.SearchExpression("companyName:"));
        });
    }

    [Fact]
    public async Task GetByEmailAddressFilter()
    {
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
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);
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

    [Fact]
    public async Task FindAsync_WithIncludesAndRequiredField_ReturnsRequiredFieldInResults()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var results = await _dailyWithRequiredCompanyRepository.FindAsync(q => q.Company(log.CompanyId).Include(e => e.Id).Include(l => l.CreatedUtc));

        // Assert
        Assert.Single(results.Documents);
        var result = results.Documents.First();
        Assert.Equal(log.Id, result.Id);
        Assert.Equal(log.CreatedUtc, result.CreatedUtc);
        Assert.Equal(log.CompanyId, result.CompanyId);
        Assert.Null(result.Message);
    }

    [Fact]
    public async Task FindAsync_WithIncludeMaskAndRequiredField_ReturnsRequiredFieldInResults()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var results = await _dailyWithRequiredCompanyRepository.FindAsync(q => q.Company(log.CompanyId).IncludeMask("id,createdUtc"));

        // Assert
        Assert.Single(results.Documents);
        var result = results.Documents.First();
        Assert.Equal(log.Id, result.Id);
        Assert.Equal(log.CreatedUtc, result.CreatedUtc);
        Assert.Equal(log.CompanyId, result.CompanyId);
        Assert.Null(result.Message);
    }

    [Fact]
    public async Task FindAsync_WithExcludesAndRequiredField_ReturnsRequiredFieldInResults()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var results = await _dailyWithRequiredCompanyRepository.FindAsync(q => q.Company(log.CompanyId).Exclude(e => e.Message));

        // Assert
        Assert.Single(results.Documents);
        var result = results.Documents.First();
        Assert.Equal(log.Id, result.Id);
        Assert.Equal(log.CompanyId, result.CompanyId);
        Assert.Equal(log.CreatedUtc, result.CreatedUtc);
        Assert.Equal(log.Value, result.Value);
        Assert.Null(result.Message);
    }

    [Fact]
    public async Task FindAsync_WithNoFieldRestrictions_DoesNotInjectRequiredField()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var results = await _dailyWithRequiredCompanyRepository.FindAsync(q => q.Company(log.CompanyId));

        // Assert
        Assert.Single(results.Documents);
        Assert.Equal(log, results.Documents.First());
    }

    [Fact]
    public async Task GetByIdAsync_WithIncludesAndRequiredField_ReturnsRequiredFieldInResult()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var result = await _dailyWithRequiredCompanyRepository.GetByIdAsync(log.Id, o => o.Include(e => e.Id).Include(e => e.CreatedUtc));

        // Assert
        Assert.NotNull(result);
        Assert.Equal(log.Id, result.Id);
        Assert.Equal(log.CreatedUtc, result.CreatedUtc);
        Assert.Equal(log.CompanyId, result.CompanyId);
        Assert.Null(result.Message);
    }

    [Fact]
    public async Task GetByIdAsync_WithExcludesAndRequiredField_ReturnsRequiredFieldInResult()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var result = await _dailyWithRequiredCompanyRepository.GetByIdAsync(log.Id, o => o.Exclude(e => e.Message));

        // Assert
        Assert.NotNull(result);
        Assert.Equal(log.Id, result.Id);
        Assert.Equal(log.CompanyId, result.CompanyId);
        Assert.Equal(log.CreatedUtc, result.CreatedUtc);
        Assert.Equal(log.Value, result.Value);
        Assert.Null(result.Message);
    }

    [Fact]
    public async Task GetByIdsAsync_WithIncludesAndRequiredField_ReturnsRequiredFieldInResults()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var results = await _dailyWithRequiredCompanyRepository.GetByIdsAsync([log.Id], o => o.Include(e => e.Id).Include(e => e.CreatedUtc));

        // Assert
        Assert.Single(results);
        var result = results.First();
        Assert.Equal(log.Id, result.Id);
        Assert.Equal(log.CreatedUtc, result.CreatedUtc);
        Assert.Equal(log.CompanyId, result.CompanyId);
        Assert.Null(result.Message);
    }

    [Fact]
    public async Task GetByIdsAsync_WithExcludesAndRequiredField_ReturnsRequiredFieldInResults()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var results = await _dailyWithRequiredCompanyRepository.GetByIdsAsync([log.Id], o => o.Exclude(e => e.Message));

        // Assert
        Assert.Single(results);
        var result = results.First();
        Assert.Equal(log.Id, result.Id);
        Assert.Equal(log.CompanyId, result.CompanyId);
        Assert.Equal(log.CreatedUtc, result.CreatedUtc);
        Assert.Equal(log.Value, result.Value);
        Assert.Null(result.Message);
    }

    [Fact]
    public async Task FindAsync_WithIncludesAndDefaultExclude_ReturnsRequiredFieldOverExclude()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyAndDefaultExcludeRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var results = await _dailyWithRequiredCompanyAndDefaultExcludeRepository.FindAsync(q => q.Company(log.CompanyId).Include(e => e.Id).Include(e => e.CreatedUtc));

        // Assert
        Assert.Single(results.Documents);
        var result = results.Documents.First();
        Assert.Equal(log.Id, result.Id);
        Assert.Equal(log.CreatedUtc, result.CreatedUtc);
        Assert.Equal(log.CompanyId, result.CompanyId);
        Assert.Null(result.Message);
    }

    [Fact]
    public async Task FindAsync_WithOptionsIncludesAndRequiredField_ReturnsRequiredFieldInResults()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var results = await _dailyWithRequiredCompanyRepository.FindAsync(q => q.Company(log.CompanyId), o => o.Include(e => e.Id).Include(e => e.CreatedUtc));

        // Assert
        Assert.Single(results.Documents);
        var result = results.Documents.First();
        Assert.Equal(log.Id, result.Id);
        Assert.Equal(log.CreatedUtc, result.CreatedUtc);
        Assert.Equal(log.CompanyId, result.CompanyId);
        Assert.Null(result.Message);
    }

    [Fact]
    public async Task GetByIdAsync_WithIncludeMaskAndRequiredField_ReturnsRequiredFieldInResult()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var result = await _dailyWithRequiredCompanyRepository.GetByIdAsync(log.Id, o => o.IncludeMask("id,createdUtc"));

        // Assert
        Assert.NotNull(result);
        Assert.Equal(log.Id, result.Id);
        Assert.Equal(log.CreatedUtc, result.CreatedUtc);
        Assert.Equal(log.CompanyId, result.CompanyId);
        Assert.Null(result.Message);
    }

    [Fact]
    public async Task FindAsync_WithExcludeOfRequiredField_IncludesRequiredFieldAnyway()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var results = await _dailyWithRequiredCompanyRepository.FindAsync(q => q.Company(log.CompanyId).Exclude(e => e.CompanyId).Include(e => e.Id));

        // Assert
        Assert.Single(results.Documents);
        var result = results.Documents.First();
        Assert.Equal(log.Id, result.Id);
        Assert.Equal(log.CompanyId, result.CompanyId);
    }

    [Fact]
    public async Task GetByIdAsync_WithExcludeOfRequiredField_IncludesRequiredFieldAnyway()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var result = await _dailyWithRequiredCompanyRepository.GetByIdAsync(log.Id, o => o.Exclude(e => e.CompanyId).Include(e => e.Id));

        // Assert
        Assert.NotNull(result);
        Assert.Equal(log.Id, result.Id);
        Assert.Equal(log.CompanyId, result.CompanyId);
    }

    [Fact]
    public async Task FindAsync_WithQueryIncludesAndRequiredField_ReturnsRequiredFieldViaQueryMethod()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var results = await _dailyWithRequiredCompanyRepository.GetPartialByCompanyAsync(log.CompanyId);

        // Assert
        Assert.Single(results.Documents);
        var result = results.Documents.First();
        Assert.Equal(log.Id, result.Id);
        Assert.Equal(log.CreatedUtc, result.CreatedUtc);
        Assert.Equal(log.CompanyId, result.CompanyId);
        Assert.Null(result.Message);
    }

    [Fact]
    public async Task GetByIdAsync_WithExcludeMaskAndRequiredField_ReturnsRequiredFieldInResult()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var result = await _dailyWithRequiredCompanyRepository.GetByIdAsync(log.Id, o => o.ExcludeMask("message"));

        // Assert
        Assert.NotNull(result);
        Assert.Equal(log.Id, result.Id);
        Assert.Equal(log.CompanyId, result.CompanyId);
        Assert.Equal(log.CreatedUtc, result.CreatedUtc);
        Assert.Equal(log.Value, result.Value);
        Assert.Null(result.Message);
    }

    [Fact]
    public async Task FindAsync_WithOnlyDefaultExcludes_ReturnsAllFieldsExceptDefaultExclude()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyAndDefaultExcludeRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var results = await _dailyWithRequiredCompanyAndDefaultExcludeRepository.FindAsync(q => q.Company(log.CompanyId));

        // Assert
        Assert.Single(results.Documents);
        var result = results.Documents.First();
        Assert.Equal(log.Id, result.Id);
        Assert.Equal(log.CompanyId, result.CompanyId);
        Assert.Equal(log.CreatedUtc, result.CreatedUtc);
        Assert.Null(result.Message);
    }

    [Fact]
    public async Task FindAsync_WithDefaultExcludesAndCallerIncludes_InjectsRequiredField()
    {
        // Arrange
        var log = await _dailyWithRequiredCompanyAndDefaultExcludeRepository.AddAsync(LogEventGenerator.Generate(companyId: "1234567890", message: "test"), o => o.ImmediateConsistency());

        // Act
        var results = await _dailyWithRequiredCompanyAndDefaultExcludeRepository.FindAsync(q => q.Company(log.CompanyId).Include(e => e.Id).Include(e => e.CreatedUtc));

        // Assert
        Assert.Single(results.Documents);
        var result = results.Documents.First();
        Assert.Equal(log.Id, result.Id);
        Assert.Equal(log.CreatedUtc, result.CreatedUtc);
        Assert.Equal(log.CompanyId, result.CompanyId);
        Assert.Null(result.Message);
    }

    [Fact]
    public async Task FieldAnd_AtTopLevel_RequiresAllConditions()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 25, companyName: "Acme"),
            EmployeeGenerator.Generate(age: 30, companyName: "Acme"),
            EmployeeGenerator.Generate(age: 25, companyName: "Globex")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldAnd(g => g
            .FieldEquals(f => f.CompanyName, "Acme")
            .FieldEquals(f => f.Age, 25)
        ));

        // Assert
        Assert.Equal(1, result.Total);
        Assert.Equal("Acme", result.Documents.First().CompanyName);
        Assert.Equal(25, result.Documents.First().Age);
    }

    [Fact]
    public async Task FieldConditionIf_WithFuncConditionReturnsFalse_ReturnsAllDocuments()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18, companyName: "Acme"),
            EmployeeGenerator.Generate(age: 25, companyName: "Globex")
        ], o => o.ImmediateConsistency());

        // Act
        string? nullValue = null;
        var result = await _employeeRepository.FindAsync(q => q
            .FieldEqualsIf(e => e.CompanyName, nullValue, condition: v => v is not null));

        // Assert
        Assert.Equal(2, result.Total);
    }

    [Fact]
    public async Task FieldConditionIf_WithFuncCondition_AppliesWhenFuncReturnsTrue()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18, companyName: "Acme"),
            EmployeeGenerator.Generate(age: 25, companyName: "Globex")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q
            .FieldEqualsIf(e => e.CompanyName, "Acme", condition: v => !String.IsNullOrEmpty(v?.ToString())));

        // Assert
        Assert.Equal(1, result.Total);
        Assert.Equal("Acme", result.Documents.First().CompanyName);
    }

    [Fact]
    public async Task FieldContainsIf_WhenConditionFalse_ReturnsAllDocuments()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(name: "Eric J. Smith"),
            EmployeeGenerator.Generate(name: "Blake Niemyjski")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldContainsIf(e => e.Name, "Eric", false));

        // Assert
        Assert.Equal(2, result.Total);
    }

    [Fact]
    public async Task FieldContains_OnKeywordField_ThrowsQueryValidationException()
    {
        // Arrange
        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(companyName: "Exceptionless"), o => o.ImmediateConsistency());

        // Act & Assert
        var ex = await Assert.ThrowsAsync<FieldQueryValidationException>(async () =>
        {
            await _employeeRepository.FindAsync(q => q.FieldContains(e => e.CompanyName, "Exception"));
        });

        Assert.Contains("non-analyzed", ex.Message);
        Assert.Contains("FieldEquals", ex.Message);
    }

    [Fact]
    public async Task FieldContains_WithMultipleTokens_MatchesAllTokensOrderIndependent()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(name: "Eric J. Smith"),
            EmployeeGenerator.Generate(name: "Blake Niemyjski")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldContains(e => e.Name, "Smith Eric"));

        // Assert
        Assert.Single(result.Documents);
        Assert.Equal("Eric J. Smith", result.Documents.First().Name);
    }

    [Fact]
    public async Task FieldContains_WithPartialToken_DoesNotMatch()
    {
        // Arrange
        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(name: "Eric J. Smith"), o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldContains(e => e.Name, "Er"));

        // Assert
        Assert.Empty(result.Documents);
    }

    [Fact]
    public async Task FieldContains_WithSingleToken_MatchesAnalyzedField()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(name: "Eric J. Smith"),
            EmployeeGenerator.Generate(name: "Blake Niemyjski")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldContains(e => e.Name, "Eric"));

        // Assert
        Assert.Single(result.Documents);
        Assert.Equal("Eric J. Smith", result.Documents.First().Name);
    }

    [Fact]
    public async Task FieldEmptyIf_WhenConditionFalse_ReturnsAllDocuments()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(name: "Eric"),
            EmployeeGenerator.Generate(age: 20)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldEmptyIf(e => e.Name, false));

        // Assert
        Assert.Equal(2, result.Total);
    }

    [Fact]
    public async Task FieldEmptyIf_WhenConditionTrue_FiltersToDocumentsWithoutValue()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(name: "Eric"),
            EmployeeGenerator.Generate(age: 20)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldEmptyIf(e => e.Name, true));

        // Assert
        Assert.Single(result.Documents);
        Assert.Null(result.Documents.First().Name);
    }

    [Fact]
    public async Task FieldEqualsIf_WhenConditionFalse_ReturnsAllDocuments()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18, companyName: "Acme"),
            EmployeeGenerator.Generate(age: 25, companyName: "Globex")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q
            .FieldEqualsIf(e => e.CompanyName, "Acme", false));

        // Assert
        Assert.Equal(2, result.Total);
    }

    [Fact]
    public async Task FieldEqualsIf_WhenConditionTrue_FiltersDocuments()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18, companyName: "Acme"),
            EmployeeGenerator.Generate(age: 25, companyName: "Globex")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q
            .FieldEqualsIf(e => e.CompanyName, "Acme", true));

        // Assert
        Assert.Equal(1, result.Total);
        Assert.Equal("Acme", result.Documents.First().CompanyName);
    }

    [Fact]
    public async Task FieldEquals_OnAnalyzedFieldWithKeyword_ResolvesToKeyword()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(name: "Eric"),
            EmployeeGenerator.Generate(name: "Blake"),
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q
            .FieldEquals(e => e.Name, "Eric"));

        // Assert
        Assert.Equal(1, result.Total);
        Assert.Equal("Eric", result.Documents.First().Name);
    }

    [Fact]
    public async Task FieldEquals_OnAnalyzedFieldWithNoKeyword_ThrowsQueryValidationException()
    {
        // Arrange
        var employee = EmployeeGenerator.Generate(name: "Eric");
        employee.PhoneNumbers = [new PhoneInfo { Number = "555-1234" }];
        await _employeeRepository.AddAsync(employee, o => o.ImmediateConsistency());

        // Act & Assert
        var ex = await Assert.ThrowsAsync<FieldQueryValidationException>(async () =>
        {
            await _employeeRepository.FindAsync(q => q
                .FieldEquals(e => e.PhoneNumbers[0].Number, "555-1234"));
        });

        Assert.Contains("analyzed text field", ex.Message);
        Assert.Contains(".keyword", ex.Message);
    }

    [Fact]
    public async Task FieldEquals_OnIsDeletedFalseWithActiveOnlyMode_DoesNotThrow()
    {
        // Arrange — isDeleted=false with ActiveOnly is redundant but not contradictory
        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q
            .FieldEquals(e => e.IsDeleted, false));

        // Assert
        Assert.Equal(1, result.Total);
    }

    [Fact]
    public async Task FieldEquals_OnIsDeletedWithActiveOnlyMode_ThrowsQueryValidationException()
    {
        // Arrange
        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

        // Act & Assert
        var ex = await Assert.ThrowsAsync<FieldQueryValidationException>(async () =>
        {
            await _employeeRepository.FindAsync(q => q.FieldEquals(e => e.IsDeleted, true));
        });

        Assert.Contains("isDeleted", ex.Message);
        Assert.Contains("ActiveOnly", ex.Message);
        Assert.Contains("IncludeSoftDeletes", ex.Message);
    }

    [Fact]
    public async Task FieldEquals_OnIsDeletedWithSoftDeleteModeAll_DoesNotThrow()
    {
        // Arrange
        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q
            .FieldEquals(e => e.IsDeleted, false),
            o => o.SoftDeleteMode(SoftDeleteQueryMode.All));

        // Assert
        Assert.Equal(1, result.Total);
    }

    [Fact]
    public async Task FieldGreaterThanIf_WhenConditionFalse_ReturnsAllDocuments()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18),
            EmployeeGenerator.Generate(age: 25)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldGreaterThanIf(e => e.Age, 18, false));

        // Assert
        Assert.Equal(2, result.Total);
    }

    [Fact]
    public async Task FieldGreaterThanIf_WhenConditionTrue_FiltersDocuments()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18),
            EmployeeGenerator.Generate(age: 25)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldGreaterThanIf(e => e.Age, 18, true));

        // Assert
        Assert.Equal(1, result.Total);
    }

    [Fact]
    public async Task FieldGreaterThanOrEqual_WithDouble_ReturnsMatchingDocuments()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18),
            EmployeeGenerator.Generate(age: 25)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldGreaterThanOrEqual(e => e.DecimalAge, 25.0));

        // Assert
        Assert.Equal(1, result.Total);
    }

    [Fact]
    public async Task FieldGreaterThanOrEqual_WithIntAge_ReturnsMatchingAndOlder()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 17),
            EmployeeGenerator.Generate(age: 18),
            EmployeeGenerator.Generate(age: 25)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldGreaterThanOrEqual(e => e.Age, 18));

        // Assert
        Assert.Equal(2, result.Total);
        Assert.True(result.Documents.All(d => d.Age >= 18));
    }

    [Fact]
    public async Task FieldGreaterThan_WithDateTime_ReturnsNewerDocuments()
    {
        // Arrange
        var cutoff = DateTime.UtcNow.SubtractDays(5);
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 20, createdUtc: DateTime.UtcNow.SubtractDays(10)),
            EmployeeGenerator.Generate(age: 25, createdUtc: DateTime.UtcNow.SubtractDays(1))
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldGreaterThan(e => e.CreatedUtc, cutoff));

        // Assert
        Assert.Equal(1, result.Total);
    }

    [Fact]
    public async Task FieldGreaterThan_WithIntAge_ReturnsOlderEmployees()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18),
            EmployeeGenerator.Generate(age: 19),
            EmployeeGenerator.Generate(age: 25)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldGreaterThan(e => e.Age, 18));

        // Assert
        Assert.Equal(2, result.Total);
        Assert.True(result.Documents.All(d => d.Age > 18));
    }

    [Fact]
    public async Task FieldGreaterThan_WithLongValue_UsesLongRangeQuery()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18),
            EmployeeGenerator.Generate(age: 25),
            EmployeeGenerator.Generate(age: 30)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q
            .FieldGreaterThan(e => e.Age, (long)25));

        // Assert
        Assert.Equal(1, result.Total);
        Assert.True(result.Documents.All(d => d.Age > 25));
    }

    [Fact]
    public async Task FieldGreaterThan_WithStringOnAnalyzedFieldWithKeyword_ResolvesToKeyword()
    {
        // Arrange — Name is text with .keyword sub-field; string range should auto-resolve to keyword
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(name: "Alpha"),
            EmployeeGenerator.Generate(name: "Beta"),
            EmployeeGenerator.Generate(name: "Gamma")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(
            q => q.FieldGreaterThan(e => e.Name, "Beta"));

        // Assert
        Assert.Equal(1, result.Total);
        Assert.Equal("Gamma", result.Documents.First().Name);
    }

    [Fact]
    public async Task FieldGreaterThan_WithStringOnAnalyzedFieldWithNoKeyword_ThrowsQueryValidationException()
    {
        // Arrange
        var employee = EmployeeGenerator.Generate(name: "Eric");
        employee.PhoneNumbers = [new PhoneInfo { Number = "555-1234" }];
        await _employeeRepository.AddAsync(employee, o => o.ImmediateConsistency());

        // Act & Assert
        var ex = await Assert.ThrowsAsync<FieldQueryValidationException>(async () =>
        {
            await _employeeRepository.FindAsync(q => q
                .FieldGreaterThan(e => e.PhoneNumbers[0].Number, "500"));
        });

        Assert.Contains("analyzed text field", ex.Message);
        Assert.Contains(".keyword", ex.Message);
    }

    [Fact]
    public async Task FieldGreaterThan_WithStringOnKeywordField_ReturnsMatchingDocuments()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(companyName: "Alpha"),
            EmployeeGenerator.Generate(companyName: "Beta"),
            EmployeeGenerator.Generate(companyName: "Gamma")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(
            q => q.FieldGreaterThan(e => e.CompanyName, "Beta"));

        // Assert
        Assert.Equal(1, result.Total);
        Assert.Equal("Gamma", result.Documents.First().CompanyName);
    }

    [Fact]
    public async Task FieldHasValueIf_WhenConditionFalse_ReturnsAllDocuments()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(name: "Eric"),
            EmployeeGenerator.Generate(age: 20)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldHasValueIf(e => e.Name, false));

        // Assert
        Assert.Equal(2, result.Total);
    }

    [Fact]
    public async Task FieldHasValueIf_WhenConditionTrue_FiltersToDocumentsWithValue()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(name: "Eric"),
            EmployeeGenerator.Generate(age: 20)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldHasValueIf(e => e.Name, true));

        // Assert
        Assert.Single(result.Documents);
        Assert.NotNull(result.Documents.First().Name);
    }

    [Fact]
    public async Task FieldLessThanOrEqual_WithDateTimeOffset_ReturnsOlderDocuments()
    {
        // Arrange
        var cutoff = DateTimeOffset.UtcNow.AddDays(5);
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 20, nextReview: DateTimeOffset.UtcNow.AddDays(1)),
            EmployeeGenerator.Generate(age: 25, nextReview: DateTimeOffset.UtcNow.AddDays(10))
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldLessThanOrEqual(e => e.NextReview, cutoff));

        // Assert
        Assert.Equal(1, result.Total);
    }

    [Fact]
    public async Task FieldLessThanOrEqual_WithIntAge_ReturnsMatchingAndYounger()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18),
            EmployeeGenerator.Generate(age: 25),
            EmployeeGenerator.Generate(age: 30)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldLessThanOrEqual(e => e.Age, 25));

        // Assert
        Assert.Equal(2, result.Total);
        Assert.True(result.Documents.All(d => d.Age <= 25));
    }

    [Fact]
    public async Task FieldLessThanOrEqual_WithLongValue_UsesLongRangeQuery()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18),
            EmployeeGenerator.Generate(age: 25),
            EmployeeGenerator.Generate(age: 30)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q
            .FieldLessThanOrEqual(e => e.Age, (long)25));

        // Assert
        Assert.Equal(2, result.Total);
        Assert.True(result.Documents.All(d => d.Age <= 25));
    }

    [Fact]
    public async Task FieldLessThanOrEqual_WithStringOnKeywordField_ReturnsMatchingDocuments()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(companyName: "Alpha"),
            EmployeeGenerator.Generate(companyName: "Beta"),
            EmployeeGenerator.Generate(companyName: "Gamma")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(
            q => q.FieldLessThanOrEqual(e => e.CompanyName, "Beta"));

        // Assert
        Assert.Equal(2, result.Total);
        Assert.True(result.Documents.All(d => String.Compare(d.CompanyName, "Beta", StringComparison.Ordinal) <= 0));
    }

    [Fact]
    public async Task FieldLessThan_WithIntAge_ReturnsYoungerEmployees()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18),
            EmployeeGenerator.Generate(age: 25),
            EmployeeGenerator.Generate(age: 30)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldLessThan(e => e.Age, 25));

        // Assert
        Assert.Equal(1, result.Total);
        Assert.True(result.Documents.All(d => d.Age < 25));
    }

    [Fact]
    public async Task FieldNotContains_OnKeywordField_ThrowsQueryValidationException()
    {
        // Arrange
        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(companyName: "Exceptionless"), o => o.ImmediateConsistency());

        // Act & Assert
        var ex = await Assert.ThrowsAsync<FieldQueryValidationException>(async () =>
        {
            await _employeeRepository.FindAsync(q => q.FieldNotContains(e => e.CompanyName, "Exception"));
        });

        Assert.Contains("non-analyzed", ex.Message);
        Assert.Contains("FieldNotEquals", ex.Message);
    }

    [Fact]
    public async Task FieldNotContains_WithMatchingToken_ExcludesDocuments()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(name: "Eric J. Smith"),
            EmployeeGenerator.Generate(name: "Blake Niemyjski")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldNotContains(e => e.Name, "Eric"));

        // Assert
        Assert.Single(result.Documents);
        Assert.Equal("Blake Niemyjski", result.Documents.First().Name);
    }

    [Fact]
    public async Task FieldNotEqualsIf_WhenConditionTrue_ExcludesMatchingDocuments()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18, companyName: "Acme"),
            EmployeeGenerator.Generate(age: 25, companyName: "Globex")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q
            .FieldNotEqualsIf(e => e.CompanyName, "Acme", true));

        // Assert
        Assert.Equal(1, result.Total);
        Assert.Equal("Globex", result.Documents.First().CompanyName);
    }

    [Fact]
    public async Task FieldNotEquals_WithNonNullValue_ExcludesMatchingDocuments()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18, companyName: "Acme"),
            EmployeeGenerator.Generate(age: 25, companyName: "Globex"),
            EmployeeGenerator.Generate(age: 30, companyName: "Acme")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q
            .FieldNotEquals(e => e.CompanyName, "Acme"));

        // Assert
        Assert.Equal(1, result.Total);
        Assert.Equal("Globex", result.Documents.First().CompanyName);
    }

    [Fact]
    public async Task FieldNot_WithMultipleConditions_ExcludesDocumentsMatchingAny()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18, companyName: "Active"),
            EmployeeGenerator.Generate(age: 25, companyName: "Inactive"),
            EmployeeGenerator.Generate(age: 30, companyName: "Pending")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldNot(g => g
            .FieldEquals(f => f.CompanyName, "Active")
            .FieldEquals(f => f.CompanyName, "Inactive")
        ));

        // Assert
        Assert.Equal(1, result.Total);
        Assert.Equal("Pending", result.Documents.First().CompanyName);
    }

    [Fact]
    public async Task FieldNot_WithSingleCondition_ExcludesMatchingDocuments()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18, companyName: "Active"),
            EmployeeGenerator.Generate(age: 25, companyName: "Inactive"),
            EmployeeGenerator.Generate(age: 30, companyName: "Active")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldNot(g => g
            .FieldEquals(f => f.CompanyName, "Active")
        ));

        // Assert
        Assert.Equal(1, result.Total);
        Assert.Equal("Inactive", result.Documents.First().CompanyName);
    }

    [Fact]
    public async Task FieldOr_WithBuilderApi_SupportsDynamicConditionalGroups()
    {
        // Arrange
        var companyId = "test-company";
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18, companyId: companyId),
            EmployeeGenerator.Generate(age: 25, companyId: "other"),
            EmployeeGenerator.Generate(age: 30, companyId: "another")
        ], o => o.ImmediateConsistency());

        var includeAgeFilter = true;
        var group = FieldConditionGroup<Employee>.Or();
        group.FieldEquals(f => f.CompanyId, companyId);
        if (includeAgeFilter)
            group.FieldEquals(f => f.Age, 25);

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldOr(group));

        // Assert
        Assert.Equal(2, result.Total);
    }

    [Fact]
    public async Task FieldOr_WithEmptyLambda_ReturnsAllDocuments()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18),
            EmployeeGenerator.Generate(age: 25)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldOr(g => { }));

        // Assert
        Assert.Equal(2, result.Total);
    }

    [Fact]
    public async Task FieldOr_WithFieldEmpty_MatchesDocumentsWithOrWithoutValue()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18, companyName: "TestCo"),
            EmployeeGenerator.Generate(age: 25),
            EmployeeGenerator.Generate(age: 30, companyName: "OtherCo")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldOr(g => g
            .FieldEquals(f => f.CompanyName, "TestCo")
            .FieldEmpty(f => f.CompanyName)
        ));

        // Assert
        Assert.Equal(2, result.Total);
    }

    [Fact]
    public async Task FieldOr_WithMixedRangeAndEquals_MatchesBothConditions()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18),
            EmployeeGenerator.Generate(age: 25, companyName: "Special"),
            EmployeeGenerator.Generate(age: 30)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldOr(g => g
            .FieldGreaterThan(f => f.Age, 28)
            .FieldEquals(f => f.CompanyName, "Special")
        ));

        // Assert
        Assert.Equal(2, result.Total);
    }

    [Fact]
    public async Task FieldOr_WithNestedFieldAnd_ProducesCorrectBoolQuery()
    {
        // Arrange
        var companyId = "test-company";
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18, companyId: companyId, companyName: "TestCo"),
            EmployeeGenerator.Generate(age: 25, companyId: companyId, companyName: "OtherCo"),
            EmployeeGenerator.Generate(age: 30, companyId: "other-company")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldOr(g => g
            .FieldEquals(f => f.Age, 30)
            .FieldAnd(g2 => g2
                .FieldEquals(f => f.CompanyId, companyId)
                .FieldEquals(f => f.CompanyName, "TestCo")
            )
        ));

        // Assert
        Assert.Equal(2, result.Total);
    }

    [Fact]
    public async Task FieldOr_WithNullRangeInsideGroup_ThrowsQueryValidationException()
    {
        // Arrange
        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

        // Act & Assert
        var ex = await Assert.ThrowsAsync<FieldQueryValidationException>(async () =>
        {
            await _employeeRepository.FindAsync(q => q.FieldOr(g => g
                .FieldEquals(f => f.CompanyName, "Acme")
                .FieldCondition(f => f.Age, ComparisonOperator.GreaterThan, (object)null!)
            ));
        });

        Assert.Contains("null value", ex.Message);
        Assert.Contains("GreaterThan", ex.Message);
    }

    [Fact]
    public async Task FieldOr_WithSingleCondition_UnwrapsWithoutBoolQuery()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18),
            EmployeeGenerator.Generate(age: 25)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldOr(g => g
            .FieldEquals(f => f.Age, 18)
        ));

        // Assert
        Assert.Equal(1, result.Total);
    }

    [Fact]
    public async Task FieldOr_WithTwoConditions_MatchesEitherCondition()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 18, companyId: "company1"),
            EmployeeGenerator.Generate(age: 25, companyId: "company2"),
            EmployeeGenerator.Generate(age: 30, companyId: "company3")
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q.FieldOr(g => g
            .FieldEquals(f => f.Age, 18)
            .FieldEquals(f => f.Age, 25)
        ));

        // Assert
        Assert.Equal(2, result.Total);
        Assert.True(result.Documents.All(d => d.Age is 18 or 25));
    }

    [Fact]
    public async Task FieldRange_WithCollectionValue_ThrowsQueryValidationException()
    {
        // Arrange
        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

        // Act & Assert
        var ex = await Assert.ThrowsAsync<FieldQueryValidationException>(async () =>
        {
            await _employeeRepository.FindAsync(q => q.FieldCondition(e => e.Age, ComparisonOperator.GreaterThan, new object[] { 1, 2 }));
        });

        Assert.Contains("collection value", ex.Message);
    }

    [Fact]
    public async Task FieldRange_WithCombinedBounds_ReturnsDocumentsInRange()
    {
        // Arrange
        await _employeeRepository.AddAsync([
            EmployeeGenerator.Generate(age: 15),
            EmployeeGenerator.Generate(age: 20),
            EmployeeGenerator.Generate(age: 25),
            EmployeeGenerator.Generate(age: 30)
        ], o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.FindAsync(q => q
            .FieldGreaterThanOrEqual(e => e.Age, 20)
            .FieldLessThan(e => e.Age, 30));

        // Assert
        Assert.Equal(2, result.Total);
        Assert.True(result.Documents.All(d => d.Age >= 20 && d.Age < 30));
    }

    [Fact]
    public async Task FieldRange_WithNullValue_ThrowsQueryValidationException()
    {
        // Arrange
        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

        // Act & Assert
        var ex = await Assert.ThrowsAsync<FieldQueryValidationException>(async () =>
        {
            await _employeeRepository.FindAsync(q => q.FieldGreaterThan(e => e.Age, null!));
        });

        Assert.Contains("null value", ex.Message);
        Assert.Contains("GreaterThan", ex.Message);
    }

    [Fact]
    public async Task FieldRange_WithUnsupportedType_ThrowsQueryValidationException()
    {
        // Arrange
        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

        // Act & Assert
        var ex = await Assert.ThrowsAsync<FieldQueryValidationException>(async () =>
        {
            await _employeeRepository.FindAsync(q => q
                .FieldGreaterThan(e => e.Age, new Guid("00000000-0000-0000-0000-000000000001")));
        });

        Assert.Contains("unsupported type", ex.Message);
        Assert.Contains("Guid", ex.Message);
    }
}

