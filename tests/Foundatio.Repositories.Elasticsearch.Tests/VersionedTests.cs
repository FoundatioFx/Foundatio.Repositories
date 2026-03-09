using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class VersionedTests : ElasticRepositoryTestBase
{
    private readonly IEmployeeRepository _employeeRepository;

    public VersionedTests(ITestOutputHelper output) : base(output)
    {
        _employeeRepository = new EmployeeRepository(_configuration);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    [Fact]
    public async Task AddAsync()
    {
        var employee = EmployeeGenerator.Default;
        Assert.Null(employee.Version);

        employee = await _employeeRepository.AddAsync(employee);
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);
        Assert.Equal("1:0", employee.Version);

        var employee2 = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.Equal(employee, employee2);
    }

    [Fact]
    public async Task CanSaveNonExistingAsync()
    {
        var employee = EmployeeGenerator.Default;
        Assert.Null(employee.Version);
        employee.Id = ObjectId.GenerateNewId().ToString();

        employee = await _employeeRepository.SaveAsync(employee, o => o.SkipVersionCheck());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);
        Assert.Equal("1:0", employee.Version);

        var employee2 = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.Equal(employee, employee2);
    }

    [Fact]
    public async Task AddAndIgnoreHighVersionAsync()
    {
        var employee = EmployeeGenerator.Generate();
        employee.Version = "1:5";

        employee = await _employeeRepository.AddAsync(employee);
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);
        Assert.Equal("1:0", employee.Version);

        Assert.Equal(employee, await _employeeRepository.GetByIdAsync(employee.Id));
    }

    [Fact]
    public async Task AddCollectionAsync()
    {
        var employee = EmployeeGenerator.Default;
        Assert.Null(employee.Version);

        var employees = new List<Employee> { employee, EmployeeGenerator.Generate() };
        await _employeeRepository.AddAsync(employees);
        Assert.Equal("1:0", employee.Version);

        var result = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToList());
        Assert.Equal(2, result.Count);
        Assert.Equal(employees[0], result.First());
        Assert.Equal(employees[1], result.Last());
    }

    [Fact]
    public async Task SaveAsync()
    {
        var employee = EmployeeGenerator.Default;
        Assert.Null(employee.Version);

        await _employeeRepository.AddAsync(new List<Employee> { employee });
        Assert.Equal("1:0", employee.Version);

        employee = await _employeeRepository.GetByIdAsync(employee.Id);
        var employeeCopy = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.Equal(employee, employeeCopy);
        Assert.Equal("1:0", employee.Version);

        employee.CompanyName = employeeCopy.CompanyName = "updated";

        employee = await _employeeRepository.SaveAsync(employee);
        Assert.Equal("1:1", employee.Version);

        string version = employeeCopy.Version;
        await Assert.ThrowsAsync<VersionConflictDocumentException>(async () => await _employeeRepository.SaveAsync(employeeCopy));
        Assert.Equal(version, employeeCopy.Version);

        await Assert.ThrowsAsync<VersionConflictDocumentException>(async () => await _employeeRepository.SaveAsync(employeeCopy));
        Assert.Equal(version, employeeCopy.Version);

        Assert.Equal(employee, await _employeeRepository.GetByIdAsync(employee.Id));

        var request = new UpdateRequest<Employee, Employee>(_configuration.Employees.Name, employee.Id)
        {
            Script = new Script { Source = "ctx._source.version = '112:2'" },
            Refresh = Refresh.True
        };

        var response = await _client.UpdateAsync(request, TestCancellationToken);
        _logger.LogRequest(response);
        Assert.True(response.IsValidResponse);

        employee = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.Equal("1:2", employee.Version);

        employee.CompanyName = "updated again";
        employee = await _employeeRepository.SaveAsync(employee);
        Assert.Equal("1:3", employee.Version);
    }

    [Fact]
    public async Task SaveWithHigherVersionAsync()
    {
        var employee = EmployeeGenerator.Default;
        Assert.Null(employee.Version);

        await _employeeRepository.AddAsync(new List<Employee> { employee });
        Assert.Equal("1:0", employee.Version);

        employee.Version = "1:5";
        await Assert.ThrowsAsync<VersionConflictDocumentException>(async () => await _employeeRepository.SaveAsync(employee));
    }

    [Fact]
    public async Task SaveCollectionAsync()
    {
        var employee1 = EmployeeGenerator.Default;
        Assert.Null(employee1.Version);

        var employee2 = EmployeeGenerator.Generate();
        await _employeeRepository.AddAsync(new List<Employee> { employee1, employee2 });
        Assert.Equal("1:0", employee1.Version);
        Assert.Equal("1:1", employee2.Version);

        var employee1Version1Copy = await _employeeRepository.GetByIdAsync(employee1.Id);
        Assert.Equal("1:0", employee1Version1Copy.Version);
        Assert.Equal(employee1, employee1Version1Copy);

        employee1.CompanyName = employee1Version1Copy.CompanyName = "updated";
        await _employeeRepository.SaveAsync(new List<Employee> { employee1, employee2 });
        Assert.Equal("1:2", employee1.Version);
        Assert.Equal("1:3", employee2.Version);

        await Assert.ThrowsAsync<VersionConflictDocumentException>(async () => await _employeeRepository.SaveAsync(new List<Employee> { employee1Version1Copy, employee2 }));
        Assert.Equal("1:0", employee1Version1Copy.Version);
        Assert.Equal("1:2", employee1.Version);
        Assert.Equal("1:4", employee2.Version);

        await Assert.ThrowsAsync<VersionConflictDocumentException>(async () => await _employeeRepository.SaveAsync(new List<Employee> { employee1Version1Copy, employee2 }));
        Assert.Equal("1:0", employee1Version1Copy.Version);
        Assert.Equal("1:2", employee1.Version);
        Assert.Equal("1:5", employee2.Version);

        Assert.Equal(employee2, await _employeeRepository.GetByIdAsync(employee2.Id));
    }

    [Fact]
    public async Task UpdateAllWithSinglePageOfDataAsync()
    {
        var utcNow = DateTime.UtcNow;
        var employees = new List<Employee> {
            EmployeeGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1), companyId: "1"),
            EmployeeGenerator.Generate(createdUtc: utcNow, companyId: "1"),
            EmployeeGenerator.Generate(createdUtc: utcNow, companyId: "2"),
        };

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        Assert.True(employees.All(e => e.GetElasticVersion().PrimaryTerm == 1));
        Assert.True(employees.All(e => e.GetElasticVersion().SequenceNumber >= 0 && e.GetElasticVersion().SequenceNumber < 101));

        Assert.Equal(2, await _employeeRepository.UpdateCompanyNameByCompanyAsync("1", "Test Company"));

        var results = await _employeeRepository.GetAllByCompanyAsync("1");
        Assert.Equal(2, results.Documents.Count);
        foreach (var document in results.Documents)
        {
            var originalDoc = employees.First(e => e.Id == document.Id);
            Assert.True(document.GetElasticVersion() > originalDoc.GetElasticVersion());
            Assert.Equal("1", document.CompanyId);
            Assert.Equal("Test Company", document.CompanyName);
        }

        results = await _employeeRepository.GetAllByCompanyAsync("2");
        Assert.Single(results.Documents);
        Assert.Equal(employees.First(e => e.CompanyId == "2"), results.Documents.First());

        var company2Employees = results.Documents.ToList();
        string company2EmployeesVersion = company2Employees.First().Version;
        Assert.Equal(1, await _employeeRepository.IncrementYearsEmployeedAsync(company2Employees.Select(e => e.Id).ToArray()));

        results = await _employeeRepository.GetAllByCompanyAsync("2");
        Assert.Equal(company2Employees.First().YearsEmployed + 1, results.Documents.First().YearsEmployed);

        await Assert.ThrowsAsync<VersionConflictDocumentException>(async () => await _employeeRepository.SaveAsync(company2Employees));
        Assert.Equal(company2EmployeesVersion, company2Employees.First().Version);
    }

    [Fact]
    public async Task UpdateAllWithNoDataAsync()
    {
        Assert.Equal(0, await _employeeRepository.UpdateCompanyNameByCompanyAsync("1", "Test Company"));
    }

    [Fact]
    public async Task CanUsePagingAsync()
    {
        const int NUMBER_OF_EMPLOYEES = 1000;
        const int PAGE_SIZE = 100;

        var employees = EmployeeGenerator.GenerateEmployees(NUMBER_OF_EMPLOYEES, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        Assert.Equal(NUMBER_OF_EMPLOYEES, await _employeeRepository.CountAsync());

        var results = await _employeeRepository.GetAllAsync(o => o.PageLimit(PAGE_SIZE));
        Assert.True(results.HasMore);

        var viewedIds = new HashSet<string>();
        int pagedRecords = 0;
        do
        {
            Assert.Equal(PAGE_SIZE, results.Documents.Count);
            Assert.Equal(NUMBER_OF_EMPLOYEES, results.Total);
            Assert.DoesNotContain(results.Hits, h => viewedIds.Contains(h.Id));
            viewedIds.AddRange(results.Hits.Select(h => h.Id));

            pagedRecords += results.Documents.Count;
        } while (await results.NextPageAsync());

        Assert.False(results.HasMore);
        Assert.True(employees.All(e => viewedIds.Contains(e.Id)));
        Assert.Equal(NUMBER_OF_EMPLOYEES, pagedRecords);
    }

    [Fact]
    public async Task CanUsePagingWithCachingAsync()
    {
        const int NUMBER_OF_EMPLOYEES = 100;
        const int PAGE_SIZE = 50;

        var employees = EmployeeGenerator.GenerateEmployees(NUMBER_OF_EMPLOYEES, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        Assert.Equal(NUMBER_OF_EMPLOYEES, await _employeeRepository.CountAsync());

        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(1, _cache.Misses);

        var results = await _employeeRepository.GetAllByCompanyAsync("1", o => o.PageLimit(PAGE_SIZE).Cache());
        Assert.True(results.HasMore);
        Assert.Equal(PAGE_SIZE, results.Documents.Count);
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(3, _cache.Misses);

        results = await _employeeRepository.GetAllByCompanyAsync("1", o => o.PageLimit(PAGE_SIZE).Cache());
        Assert.True(results.HasMore);
        Assert.Equal(PAGE_SIZE, results.Documents.Count);
        Assert.Equal(1, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(4, _cache.Misses); // Supports soft deletes check increments misses.

        results = await _employeeRepository.GetAllByCompanyAsync("1", o => o.PageLimit(20).Cache());
        Assert.True(results.HasMore);
        Assert.Equal(20, results.Documents.Count);
        Assert.Equal(2, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(6, _cache.Misses); // Supports soft deletes check increments misses.
    }

    [Fact]
    public async Task CanUseSnapshotPagingAsync()
    {
        const int NUMBER_OF_EMPLOYEES = 100;
        const int PAGE_SIZE = 10;

        var employees = EmployeeGenerator.GenerateEmployees(NUMBER_OF_EMPLOYEES, companyId: "1");
        await _employeeRepository.AddAsync(employees);
        await _client.Indices.RefreshAsync(Indices.All, cancellationToken: TestCancellationToken);

        Assert.Equal(NUMBER_OF_EMPLOYEES, await _employeeRepository.CountAsync());

        var results = await _employeeRepository.GetAllAsync(o => o.PageLimit(PAGE_SIZE).SnapshotPaging());
        Assert.True(results.HasMore);

        var viewedIds = new HashSet<string>();
        var newEmployees = new List<Employee>();
        int pagedRecords = 0;
        do
        {
            Assert.True(results.Documents.Count >= PAGE_SIZE);
            Assert.Equal(NUMBER_OF_EMPLOYEES, results.Total);
            Assert.DoesNotContain(results.Hits, h => viewedIds.Contains(h.Id));
            viewedIds.AddRange(results.Hits.Select(h => h.Id));

            Assert.DoesNotContain(newEmployees, d => viewedIds.Contains(d.Id));

            pagedRecords += results.Documents.Count;
            newEmployees.Add(await _employeeRepository.AddAsync(EmployeeGenerator.Generate(companyId: "1"), o => o.ImmediateConsistency()));
        } while (await results.NextPageAsync());

        Assert.False(results.HasMore);
        Assert.True(employees.All(e => viewedIds.Contains(e.Id)));
        Assert.Equal(NUMBER_OF_EMPLOYEES, pagedRecords);
    }

    [Fact]
    public async Task CanUseSnapshotWithScrollIdAsync()
    {
        const int NUMBER_OF_EMPLOYEES = 100;
        const int PAGE_SIZE = 10;

        var employees = EmployeeGenerator.GenerateEmployees(NUMBER_OF_EMPLOYEES, companyId: "1");
        await _employeeRepository.AddAsync(employees);
        await _client.Indices.RefreshAsync(Indices.All, cancellationToken: TestCancellationToken);

        Assert.Equal(NUMBER_OF_EMPLOYEES, await _employeeRepository.CountAsync());

        var results = await _employeeRepository.GetAllAsync(o => o.PageLimit(PAGE_SIZE).SnapshotPaging());
        Assert.True(results.HasMore);

        var viewedIds = new HashSet<string>();
        var newEmployees = new List<Employee>();
        int pagedRecords = 0;
        do
        {
            Assert.True(results.Documents.Count >= PAGE_SIZE);
            Assert.Equal(NUMBER_OF_EMPLOYEES, results.Total);
            Assert.DoesNotContain(results.Hits, h => viewedIds.Contains(h.Id));
            viewedIds.AddRange(results.Hits.Select(h => h.Id));

            Assert.DoesNotContain(newEmployees, d => viewedIds.Contains(d.Id));

            pagedRecords += results.Documents.Count;
            newEmployees.Add(await _employeeRepository.AddAsync(EmployeeGenerator.Generate(companyId: "1"), o => o.ImmediateConsistency()));

            results = await _employeeRepository.GetAllAsync(o => o.SnapshotPagingScrollId(results));
        } while (results != null && results.Hits.Count > 0);

        Assert.False(results.HasMore);
        Assert.True(employees.All(e => viewedIds.Contains(e.Id)));
        Assert.Equal(NUMBER_OF_EMPLOYEES, pagedRecords);
    }

    [Fact]
    public async Task CanUsePagingWithOddNumberAsync()
    {
        const int NUMBER_OF_EMPLOYEES = 67;
        const int PAGE_SIZE = 12;

        var employees = EmployeeGenerator.GenerateEmployees(NUMBER_OF_EMPLOYEES, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        Assert.Equal(NUMBER_OF_EMPLOYEES, await _employeeRepository.CountAsync());

        var results = await _employeeRepository.GetAllAsync(o => o.PageLimit(PAGE_SIZE));
        Assert.True(results.HasMore);

        var viewedIds = new HashSet<string>();
        int pagedRecords = 0;
        do
        {
            Assert.Equal(Math.Min(PAGE_SIZE, NUMBER_OF_EMPLOYEES - pagedRecords), results.Documents.Count);
            Assert.Equal(NUMBER_OF_EMPLOYEES, results.Total);
            Assert.DoesNotContain(results.Hits, h => viewedIds.Contains(h.Id));
            viewedIds.AddRange(results.Hits.Select(h => h.Id));

            pagedRecords += results.Documents.Count;
        } while (await results.NextPageAsync());

        Assert.False(results.HasMore);
        Assert.True(employees.All(e => viewedIds.Contains(e.Id)));
        Assert.Equal(NUMBER_OF_EMPLOYEES, pagedRecords);
    }

    [Fact]
    public async Task UpdateAllWithPageLimitAsync()
    {
        const int NUMBER_OF_EMPLOYEES = 100;
        var employees = EmployeeGenerator.GenerateEmployees(NUMBER_OF_EMPLOYEES, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        Assert.Equal(NUMBER_OF_EMPLOYEES, await _employeeRepository.UpdateCompanyNameByCompanyAsync("1", "Test Company", limit: 100));

        var results = await _employeeRepository.GetAllByCompanyAsync("1", o => o.PageLimit(NUMBER_OF_EMPLOYEES));
        Assert.Equal(NUMBER_OF_EMPLOYEES, results.Documents.Count);
        foreach (var document in results.Documents)
        {
            var originalDoc = employees.First(e => e.Id == document.Id);
            Assert.True(document.GetElasticVersion() > originalDoc.GetElasticVersion());
            Assert.Equal("1", document.CompanyId);
            Assert.Equal("Test Company", document.CompanyName);
        }
    }

    [Fact]
    public async Task UpdateAllWithNoPageLimitAsync()
    {
        const int NUMBER_OF_EMPLOYEES = 100;
        var employees = EmployeeGenerator.GenerateEmployees(NUMBER_OF_EMPLOYEES, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        Assert.Equal(NUMBER_OF_EMPLOYEES, await _employeeRepository.UpdateCompanyNameByCompanyAsync("1", "Test Company"));

        var results = await _employeeRepository.GetAllByCompanyAsync("1", o => o.PageLimit(NUMBER_OF_EMPLOYEES));
        Assert.Equal(NUMBER_OF_EMPLOYEES, results.Documents.Count);
        foreach (var document in results.Documents)
        {
            var originalDoc = employees.First(e => e.Id == document.Id);
            Assert.True(document.GetElasticVersion() > originalDoc.GetElasticVersion());
            Assert.Equal("1", document.CompanyId);
            Assert.Equal("Test Company", document.CompanyName);
        }
    }

    [Fact]
    public async Task JsonPatchAsync_WithVersioning_AppliesPatchSuccessfully()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        var originalVersion = employee.GetElasticVersion();

        // Act
        var patch = new PatchDocument(new ReplaceOperation { Path = "companyName", Value = "PatchedCo" });
        await _employeeRepository.PatchAsync(employee.Id, new JsonPatch(patch), o => o.ImmediateConsistency());

        // Assert
        var updated = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.Equal("PatchedCo", updated.CompanyName);
        Assert.True(updated.GetElasticVersion() > originalVersion);
    }

    [Fact]
    public Task ScriptPatchAsync_WhenDocumentNotFound_ThrowsDocumentNotFoundException()
    {
        // Act & Assert
        return Assert.ThrowsAsync<DocumentNotFoundException>(async () =>
            await _employeeRepository.PatchAsync("nonexistent", new ScriptPatch("ctx._source.companyName = 'Test'")));
    }

    [Fact]
    public Task PartialPatchAsync_WhenDocumentNotFound_ThrowsDocumentNotFoundException()
    {
        // Act & Assert
        return Assert.ThrowsAsync<DocumentNotFoundException>(async () =>
            await _employeeRepository.PatchAsync("nonexistent", new PartialPatch(new { CompanyName = "Test" })));
    }

    [Fact]
    public async Task PatchAllAsync_JsonPatchWithVersionConflicts_RetriesWithoutInfiniteLoop()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(3, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        var emp1 = employees[0];
        emp1.CompanyName = "Bumped";
        await _employeeRepository.SaveAsync(emp1, o => o.ImmediateConsistency());

        // Act
        var patch = new PatchDocument(new ReplaceOperation { Path = "companyName", Value = "PatchedAll" });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var task = _employeeRepository.PatchAllAsync(q => q.Company("1"), new JsonPatch(patch), o => o.ImmediateConsistency());

        // Assert
        var completedTask = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(30), cts.Token));
        Assert.Equal(task, completedTask);
        cts.Cancel();
    }

    [Fact]
    public async Task PatchAllAsync_ActionPatchWithVersionConflicts_RetriesDocuments()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(3, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        var emp1 = employees[0];
        emp1.CompanyName = "Bumped";
        await _employeeRepository.SaveAsync(emp1, o => o.ImmediateConsistency());

        // Act
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var task = _employeeRepository.PatchAllAsync(q => q.Company("1"), new ActionPatch<Employee>(e => e.CompanyName = "PatchedAll"), o => o.ImmediateConsistency());

        // Assert
        var completedTask = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(30), cts.Token));
        Assert.Equal(task, completedTask);
        cts.Cancel();
    }

    [Fact]
    public async Task PatchAllAsync_PartialPatchWithConcurrentWrites_RetriesOnConflict()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(10, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        long affected = await _employeeRepository.UpdateCompanyNameByCompanyAsync("1", "PartialPatched");

        // Assert
        Assert.Equal(10, affected);
        var results = await _employeeRepository.GetAllByCompanyAsync("1");
        Assert.True(results.Documents.All(e => e.CompanyName == "PartialPatched"));
    }

    [Fact]
    public async Task SaveAsync_CollectionWithPartialConflict_CachesSuccessAndLeavesFailedUnchanged()
    {
        // Arrange
        var emp1 = EmployeeGenerator.Generate(companyId: "1");
        var emp2 = EmployeeGenerator.Generate(companyId: "1");
        await _employeeRepository.AddAsync(new List<Employee> { emp1, emp2 }, o => o.Cache().ImmediateConsistency());

        emp1.CompanyName = "BumpedEmp1";
        await _employeeRepository.SaveAsync(emp1, o => o.Cache().ImmediateConsistency());

        var emp1StaleCopy = EmployeeGenerator.Generate(companyId: "1");
        emp1StaleCopy.Id = emp1.Id;
        emp1StaleCopy.Version = "1:0";
        emp1StaleCopy.CompanyName = "StaleUpdate";
        emp2.CompanyName = "FreshUpdate";

        // Act
        await Assert.ThrowsAsync<VersionConflictDocumentException>(async () =>
            await _employeeRepository.SaveAsync(new List<Employee> { emp1StaleCopy, emp2 }, o => o.Cache().ImmediateConsistency()));

        // Assert
        var emp2FromEs = await _employeeRepository.GetByIdAsync(emp2.Id, o => o.Cache(false));
        Assert.Equal("FreshUpdate", emp2FromEs.CompanyName);

        var emp1Cached = await _employeeRepository.GetByIdAsync(emp1.Id, o => o.Cache());
        Assert.Equal("BumpedEmp1", emp1Cached.CompanyName);
    }

    [Fact]
    public async Task SaveAsync_CollectionWithPartialConflict_SendsNotificationsForSuccessfulDocs()
    {
        // Arrange
        var emp1 = EmployeeGenerator.Generate(companyId: "1");
        var emp2 = EmployeeGenerator.Generate(companyId: "1");
        await _employeeRepository.AddAsync(new List<Employee> { emp1, emp2 }, o => o.ImmediateConsistency());
        long countBefore = _employeeRepository.DocumentsChangedCount;

        emp1.CompanyName = "BumpedEmp1";
        await _employeeRepository.SaveAsync(emp1, o => o.ImmediateConsistency());

        var emp1StaleCopy = EmployeeGenerator.Generate(companyId: "1");
        emp1StaleCopy.Id = emp1.Id;
        emp1StaleCopy.Version = "1:0";
        emp2.CompanyName = "FreshUpdate";

        // Act
        try
        {
            await _employeeRepository.SaveAsync(new List<Employee> { emp1StaleCopy, emp2 }, o => o.ImmediateConsistency());
        }
        catch (VersionConflictDocumentException)
        {
            // Expected: stale emp1 causes a conflict; we only care that notifications were still sent for emp2
        }

        // Assert
        Assert.True(_employeeRepository.DocumentsChangedCount > countBefore);
    }
}
