using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Utility;
using Foundatio.Repositories.Models;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

/// <summary>
/// Optimistic concurrency, retry budgets and conflict handling across every write operation.
/// </summary>
public sealed class ConcurrencyTests : ElasticRepositoryTestBase
{
    private readonly IEmployeeRepository _employeeRepository;

    public ConcurrencyTests(ITestOutputHelper output) : base(output)
    {
        _employeeRepository = new EmployeeRepository(_configuration);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    public static TheoryData<string> PatchTypes => new() { "JsonPatch", "ActionPatch", "PartialPatch", "ScriptPatch" };

    private static IPatchOperation CreatePatch(string patchType)
    {
        return patchType switch
        {
            "JsonPatch" => new JsonPatch(new PatchDocument(new ReplaceOperation { Path = "companyName", Value = "Patched" })),
            "ActionPatch" => new ActionPatch<Employee>(e => e.CompanyName = "Patched"),
            "PartialPatch" => new PartialPatch(new { CompanyName = "Patched" }),
            "ScriptPatch" => new ScriptPatch("ctx._source.companyName = 'Patched';"),
            _ => throw new ArgumentOutOfRangeException(nameof(patchType))
        };
    }

    /// <summary>
    /// Keeps incrementing <c>yearsEmployed</c> with version-checked read-modify-write saves until cancelled, so every
    /// increment that is acknowledged must survive. Returns the number of acknowledged increments per id.
    /// </summary>
    private Task<Dictionary<string, int>> StartIncrementingWriterAsync(IReadOnlyList<string> ids, CancellationToken cancellationToken)
    {
        return Task.Run(async () =>
        {
            var acknowledged = ids.ToDictionary(id => id, _ => 0);
            while (!cancellationToken.IsCancellationRequested)
            {
                foreach (string id in ids)
                {
                    if (cancellationToken.IsCancellationRequested)
                        break;

                    var employee = await _employeeRepository.GetByIdAsync(id, o => o.Cache(false));
                    if (employee is null)
                        continue;

                    employee.YearsEmployed++;
                    try
                    {
                        await _employeeRepository.SaveAsync(employee, o => o.Cache(false));
                        acknowledged[id]++;
                    }
                    catch (VersionConflictDocumentException)
                    {
                    }
                }
            }

            return acknowledged;
        }, cancellationToken);
    }

    [Theory]
    [MemberData(nameof(PatchTypes))]
    public async Task PatchAllAsync_WithConcurrentWriters_LosesNoUpdates(string patchType)
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(40, companyId: "1", yearsEmployed: 0);
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var hotIds = employees.Take(10).Select(e => e.Id).ToList();

        using var writerCancellation = new CancellationTokenSource();
        var writer = StartIncrementingWriterAsync(hotIds, writerCancellation.Token);
        await Task.Delay(100, TestCancellationToken);

        // Act
        long patched = await _employeeRepository.PatchAllAsync(q => q.Company("1"), CreatePatch(patchType), o => o.PageLimit(10).ImmediateConsistency());
        await writerCancellation.CancelAsync();
        var acknowledged = await writer;

        // Assert
        Assert.Equal(40, patched);
        var results = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), o => o.Cache(false));
        Assert.Equal(40, results.Count);
        Assert.All(results, e => Assert.Equal("Patched", e.CompanyName));
        foreach (string id in hotIds)
            Assert.Equal(acknowledged[id], results.Single(e => e.Id == id).YearsEmployed);

        // Whether a writer collides with a batch is timing-dependent, so conflicts aren't asserted here. They are covered
        // deterministically by PatchAllAsync_WhenEveryDocumentChangesAfterRead_RetriesInOneGroupedRound.
        Assert.True(acknowledged.Values.Sum() > 0);
    }

    [Theory]
    [MemberData(nameof(PatchTypes))]
    public async Task PatchAsyncIds_WithConcurrentWriters_LosesNoUpdates(string patchType)
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(10, companyId: "1", yearsEmployed: 0);
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var ids = employees.Select(e => e.Id).ToList();

        using var writerCancellation = new CancellationTokenSource();
        var writer = StartIncrementingWriterAsync(ids, writerCancellation.Token);
        await Task.Delay(100, TestCancellationToken);

        // Act
        long patched = await _employeeRepository.PatchAsync(ids.ToArray(), CreatePatch(patchType), o => o.ImmediateConsistency());
        await writerCancellation.CancelAsync();
        var acknowledged = await writer;

        // Assert
        Assert.Equal(10, patched);
        var results = await _employeeRepository.GetByIdsAsync(ids.ToArray(), o => o.Cache(false));
        Assert.All(results, e =>
        {
            Assert.Equal("Patched", e.CompanyName);
            Assert.Equal(acknowledged[e.Id], e.YearsEmployed);
        });
    }

    [Fact]
    public async Task RemoveAllAsync_WithConcurrentWriters_RemovesEveryDocument()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(200, companyId: "1", yearsEmployed: 0);
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        using var writerCancellation = new CancellationTokenSource();
        var writer = StartIncrementingWriterAsync(employees.Take(20).Select(e => e.Id).ToList(), writerCancellation.Token);
        await Task.Delay(100, TestCancellationToken);

        // Act
        long removed;
        try
        {
            removed = await _employeeRepository.RemoveAllAsync(q => q.Company("1"), o => o.ImmediateConsistency());
        }
        finally
        {
            await writerCancellation.CancelAsync();
            await writer;
        }

        // Assert
        Assert.True(removed >= 200 - 20);
        Assert.Equal(0, await _employeeRepository.CountAsync(q => q.Company("1"), o => o.ImmediateConsistency()));
    }

    private (EmployeeRepository Repository, FaultInjectingRequestInvoker Invoker, MyAppElasticConfiguration Configuration) CreateFaultInjectingRepository()
    {
        var invoker = new FaultInjectingRequestInvoker(new HttpRequestInvoker());
        var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log) { RequestInvoker = invoker };
        return (new EmployeeRepository(configuration), invoker, configuration);
    }

    private Task BumpYearsEmployedAsync(string companyId)
    {
        return _client.UpdateByQueryAsync<Employee>(_configuration.Employees.Name, u => u
            .Query(q => q.Term(t => t.Field("companyId").Value(companyId)))
            .Script(s => s.Source("ctx._source.yearsEmployed += 1;"))
            .Refresh(), TestCancellationToken);
    }

    [Theory]
    [InlineData("JsonPatch")]
    [InlineData("ActionPatch")]
    public async Task PatchAllAsync_WhenEveryDocumentChangesAfterRead_RetriesInOneGroupedRound(string patchType)
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(20, companyId: "1", yearsEmployed: 0);
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.BeforeNextBulk(async () => await BumpYearsEmployedAsync("1"));

        // Act
        long patched = await repository.PatchAllAsync(q => q.Company("1"), CreatePatch(patchType), o => o.ImmediateConsistency());

        // Assert
        Assert.Equal(20, patched);
        Assert.Equal(2, invoker.CountRequests(HttpMethod.POST, "_bulk"));
        Assert.Contains(Log.LogEntries, l => l.LogLevel == LogLevel.Warning && l.Message.Contains("Version conflict during"));
        Assert.Equal(1, invoker.CountRequests(HttpMethod.POST, "_mget"));
        var results = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), o => o.Cache(false));
        Assert.All(results, e =>
        {
            Assert.Equal("Patched", e.CompanyName);
            Assert.Equal(1, e.YearsEmployed);
        });
    }

    [Fact]
    public async Task PatchAllAsync_WithRetryZeroAndConflicts_FailsFastAndReportsIds()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(5, companyId: "1", yearsEmployed: 0);
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.BeforeNextBulk(async () => await BumpYearsEmployedAsync("1"));

        // Act
        var exception = await Assert.ThrowsAsync<VersionConflictDocumentException>(() =>
            repository.PatchAllAsync(q => q.Company("1"), CreatePatch("JsonPatch"), o => o.Retry(0).ImmediateConsistency()));

        // Assert
        Assert.Equal(1, invoker.CountRequests(HttpMethod.POST, "_bulk"));
        Assert.Equal(0, invoker.CountRequests(HttpMethod.POST, "_mget"));
        Assert.Contains("5 still conflicting", exception.Message);
        Assert.Contains(employees[0].Id, exception.Message);
        var results = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), o => o.Cache(false));
        Assert.All(results, e =>
        {
            Assert.NotEqual("Patched", e.CompanyName);
            Assert.Equal(1, e.YearsEmployed);
        });
    }

    [Fact]
    public async Task PatchAllAsync_WithSkipVersionCheck_OverwritesConcurrentChange()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(5, companyId: "1", yearsEmployed: 0);
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.BeforeNextBulk(async () => await BumpYearsEmployedAsync("1"));

        // Act
        long patched = await repository.PatchAllAsync(q => q.Company("1"), CreatePatch("JsonPatch"), o => o.SkipVersionCheck().ImmediateConsistency());

        // Assert
        Assert.Equal(5, patched);
        Assert.Equal(1, invoker.CountRequests(HttpMethod.POST, "_bulk"));
        var results = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), o => o.Cache(false));
        Assert.All(results, e =>
        {
            Assert.Equal("Patched", e.CompanyName);
            Assert.Equal(0, e.YearsEmployed);
        });
    }

    [Fact]
    public async Task PatchAllAsync_WhenDocumentDeletedMidBatch_SkipsIt()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(5, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.BeforeNextBulk(async () => await _client.DeleteAsync(new Elastic.Clients.Elasticsearch.DeleteRequest(_configuration.Employees.Name, employees[0].Id) { Refresh = Elastic.Clients.Elasticsearch.Refresh.True }, TestCancellationToken));

        // Act
        long patched = await repository.PatchAllAsync(q => q.Company("1"), CreatePatch("JsonPatch"), o => o.ImmediateConsistency());

        // Assert
        Assert.Equal(4, patched);
        Assert.Null(await _employeeRepository.GetByIdAsync(employees[0].Id, o => o.Cache(false)));
        Assert.Equal(4, await _employeeRepository.CountAsync(q => q.Company("1"), o => o.ImmediateConsistency()));
    }

    [Fact]
    public async Task PatchAllAsync_WithPartialPatchOnMultipleBatches_NotifiesAndCallsBackForEveryDocument()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(25, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var updatedIds = new List<string>();

        // Act
        long patched = await _employeeRepository.PatchAllAsync(q => q.Company("1"), CreatePatch("PartialPatch"), o => o.PageLimit(10).ImmediateConsistency().UpdateCallback(ids => updatedIds.AddRange(ids)));

        // Assert
        Assert.Equal(25, patched);
        Assert.Equal(employees.Select(e => e.Id).Order(), updatedIds.Order());
    }

    [Theory]
    [InlineData("JsonPatch")]
    [InlineData("ActionPatch")]
    [InlineData("PartialPatch")]
    [InlineData("ScriptPatch")]
    public async Task PatchAsync_WithStaleIfVersion_ThrowsVersionConflict(string patchType)
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        string staleVersion = employee.Version;
        employee.Name = "changed";
        await _employeeRepository.SaveAsync(employee, o => o.ImmediateConsistency());

        // Act & Assert
        await Assert.ThrowsAsync<VersionConflictDocumentException>(() => _employeeRepository.PatchAsync(employee.Id, CreatePatch(patchType), o => o.IfVersion(staleVersion)));
        var current = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.NotNull(current);
        Assert.Equal(employee.Version, current.Version);
        Assert.NotEqual("Patched", current.CompanyName);
    }

    [Theory]
    [InlineData("JsonPatch")]
    [InlineData("ActionPatch")]
    [InlineData("PartialPatch")]
    [InlineData("ScriptPatch")]
    public async Task PatchAsync_WithCurrentIfVersion_AppliesPatch(string patchType)
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());

        // Act
        bool modified = await _employeeRepository.PatchAsync(employee.Id, CreatePatch(patchType), o => o.IfVersion(employee.Version));

        // Assert
        Assert.True(modified);
        var current = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.NotNull(current);
        Assert.Equal("Patched", current.CompanyName);
        Assert.Equal("1:1", current.Version);
    }

    [Fact]
    public Task PatchAsyncIds_WithIfVersion_ThrowsArgumentException()
    {
        return Assert.ThrowsAsync<ArgumentException>(() => _employeeRepository.PatchAsync(new[] { "1", "2" }, CreatePatch("ScriptPatch"), o => o.IfVersion("1:0")));
    }

    [Fact]
    public async Task RemoveAsync_WithStaleDocument_ThrowsVersionConflictAndKeepsDocument()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        var stale = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.NotNull(stale);
        employee.Name = "changed";
        await _employeeRepository.SaveAsync(employee, o => o.ImmediateConsistency());

        // Act & Assert
        await Assert.ThrowsAsync<VersionConflictDocumentException>(() => _employeeRepository.RemoveAsync(stale, o => o.ImmediateConsistency()));
        Assert.NotNull(await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false)));

        var other = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());
        await Assert.ThrowsAsync<VersionConflictDocumentException>(() => _employeeRepository.RemoveAsync(new[] { stale, other }, o => o.ImmediateConsistency()));
        Assert.Null(await _employeeRepository.GetByIdAsync(other.Id, o => o.Cache(false)));
        Assert.NotNull(await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false)));
    }

    [Fact]
    public async Task RemoveAsync_WithStaleDocumentAndSkipVersionCheck_RemovesDocument()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        var stale = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.NotNull(stale);
        employee.Name = "changed";
        await _employeeRepository.SaveAsync(employee, o => o.ImmediateConsistency());

        // Act
        await _employeeRepository.RemoveAsync(stale, o => o.SkipVersionCheck().ImmediateConsistency());

        // Assert
        Assert.Null(await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false)));
    }

    [Fact]
    public async Task RemoveAsync_ByIdWithStaleCache_RemovesDocument()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.Cache().ImmediateConsistency());
        await _client.UpdateAsync<Employee, object>(_configuration.Employees.Name, employee.Id, u => u.Doc(new { name = "changed outside the repository" }).Refresh(Elastic.Clients.Elasticsearch.Refresh.True), TestCancellationToken);

        // Act
        await _employeeRepository.RemoveAsync(employee.Id, o => o.ImmediateConsistency());

        // Assert
        Assert.Null(await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false)));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SaveAsync_WithStaleVersionAndSkipVersionCheck_OverwritesDocument(bool useVersionCheckFalse)
    {
        // Arrange
        var employees = new List<Employee> { EmployeeGenerator.Generate(), EmployeeGenerator.Generate() };
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var stale = (await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), o => o.Cache(false))).ToList();
        foreach (var employee in employees)
            employee.Name = "fresh";
        await _employeeRepository.SaveAsync(employees, o => o.ImmediateConsistency());

        // Act
        stale[0].Name = "single";
        await _employeeRepository.SaveAsync(stale[0], o => useVersionCheckFalse ? o.VersionCheck(false) : o.SkipVersionCheck());
        foreach (var employee in stale)
            employee.Name = "bulk";
        await _employeeRepository.SaveAsync(stale, o => (useVersionCheckFalse ? o.VersionCheck(false) : o.SkipVersionCheck()).ImmediateConsistency());

        // Assert
        var results = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), o => o.Cache(false));
        Assert.All(results, e => Assert.Equal("bulk", e.Name));
        await Assert.ThrowsAsync<VersionConflictDocumentException>(() => _employeeRepository.SaveAsync(employees[0], o => o.VersionCheck(true)));
    }

    [Fact]
    public async Task SaveAsync_WithoutVersion_ThrowsUnlessVersionCheckSkipped()
    {
        // Arrange
        var employee = EmployeeGenerator.Generate(id: ObjectId.GenerateNewId().ToString());

        // Act & Assert
        var exception = await Assert.ThrowsAsync<DocumentException>(() => _employeeRepository.SaveAsync(employee));
        Assert.Contains("has no version", exception.Message);
        await _employeeRepository.SaveAsync(employee, o => o.SkipVersionCheck());
        Assert.Equal("1:0", employee.Version);
    }

    [Theory]
    [InlineData(429)]
    [InlineData(503)]
    public async Task SaveAsync_WithTransientFailures_RetriesAndSucceeds(int statusCode)
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.FailNext(2, statusCode, HttpMethod.PUT, $"/_doc/{employee.Id}");

        // Act
        employee.Name = "saved after retries";
        await repository.SaveAsync(employee, o => o.Cache(false));

        // Assert
        Assert.Equal(2, invoker.InjectedCount);
        Assert.Equal(3, invoker.CountRequests(HttpMethod.PUT, $"/_doc/{employee.Id}"));
        Assert.Equal("1:1", employee.Version);
    }

    [Fact]
    public async Task SaveAsync_WithTransientFailureAndRetryZero_FailsFast()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.FailNext(1, 503, HttpMethod.PUT, $"/_doc/{employee.Id}");

        // Act & Assert
        employee.Name = "not saved";
        var exception = await Assert.ThrowsAnyAsync<DocumentException>(() => repository.SaveAsync(employee, o => o.Cache(false).Retry(0)));
        Assert.IsNotType<VersionConflictDocumentException>(exception);
        Assert.Equal(1, invoker.CountRequests(HttpMethod.PUT, $"/_doc/{employee.Id}"));
    }

    [Fact]
    public async Task SaveAsync_WithVersionConflict_IsNotRetried()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        var stale = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.NotNull(stale);
        await _employeeRepository.SaveAsync(employee, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;

        // Act & Assert
        await Assert.ThrowsAsync<VersionConflictDocumentException>(() => repository.SaveAsync(stale, o => o.Cache(false)));
        Assert.Equal(1, invoker.CountRequests(HttpMethod.PUT, $"/_doc/{employee.Id}"));
    }

    [Theory]
    [InlineData("JsonPatch")]
    [InlineData("ScriptPatch")]
    public async Task PatchAllAsync_WithTransientBulkFailure_RetriesBatch(string patchType)
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(5, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.FailNext(1, 429, HttpMethod.POST, "_bulk");

        // Act
        long patched = await repository.PatchAllAsync(q => q.Company("1"), CreatePatch(patchType), o => o.ImmediateConsistency());

        // Assert
        Assert.Equal(5, patched);
        Assert.Equal(1, invoker.InjectedCount);
        var results = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), o => o.Cache(false));
        Assert.All(results, e => Assert.Equal("Patched", e.CompanyName));
    }

    [Fact]
    public async Task AddAsync_CollectionWithTransientBulkFailure_RetriesAndAddsAll()
    {
        // Arrange
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.FailNext(1, 503, HttpMethod.POST, "_bulk");
        var employees = EmployeeGenerator.GenerateEmployees(3);

        // Act
        await repository.AddAsync(employees, o => o.ImmediateConsistency());

        // Assert
        Assert.Equal(3, await _employeeRepository.CountAsync(o => o.ImmediateConsistency()));
        Assert.All(employees, e => Assert.NotNull(e.Version));
    }

    [Fact]
    public async Task PatchAsync_ScriptPatchWithTransientFailure_Retries()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.FailNext(1, 429, HttpMethod.POST, $"/_update/{employee.Id}");

        // Act
        bool modified = await repository.PatchAsync(employee.Id, CreatePatch("ScriptPatch"));

        // Assert
        Assert.True(modified);
        Assert.Equal(2, invoker.CountRequests(HttpMethod.POST, $"/_update/{employee.Id}"));
    }

    [Fact]
    public async Task PatchAsync_JsonPatchWithConcurrentWritersAndRetryZero_FailsFast()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.BeforeNextBulk(async () => await _client.UpdateAsync<Employee, object>(_configuration.Employees.Name, employee.Id, u => u.Doc(new { name = "concurrent" }), TestCancellationToken));

        // Act & Assert
        await Assert.ThrowsAsync<VersionConflictDocumentException>(() => repository.PatchAsync(employee.Id, CreatePatch("JsonPatch"), o => o.Retry(0)));
        Assert.Equal(1, invoker.CountRequests(HttpMethod.POST, "_bulk"));

        // With the default budget the same race is retried and the patch lands on top of the concurrent change.
        invoker.BeforeNextBulk(async () => await _client.UpdateAsync<Employee, object>(_configuration.Employees.Name, employee.Id, u => u.Doc(new { name = "concurrent again" }), TestCancellationToken));
        Assert.True(await repository.PatchAsync(employee.Id, CreatePatch("JsonPatch")));
        var current = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.NotNull(current);
        Assert.Equal("Patched", current.CompanyName);
        Assert.Equal("concurrent again", current.Name);
    }

    [Fact]
    public async Task PatchAllAsync_ScriptPatchWithoutCache_UsesBulkUpdatesAndRunsScriptOncePerDocument()
    {
        // Arrange
        var repository = new EmployeeRepository(_configuration.Employees);
        var employees = EmployeeGenerator.GenerateEmployees(15, companyId: "1", yearsEmployed: 0);
        await repository.AddAsync(employees, o => o.ImmediateConsistency());
        var (faultRepository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;

        // Act
        long patched = await faultRepository.PatchAllAsync(q => q.Company("1"), new ScriptPatch("ctx._source.yearsEmployed += 1;"), o => o.Cache(false).PageLimit(10).ImmediateConsistency());

        // Assert
        Assert.Equal(15, patched);
        Assert.Equal(0, invoker.CountRequests(HttpMethod.POST, "_update_by_query"));
        var results = await repository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), o => o.Cache(false));
        Assert.All(results, e => Assert.Equal(1, e.YearsEmployed));
    }

    private sealed class NoCacheEmployeeRepository : EmployeeRepository
    {
        public NoCacheEmployeeRepository(MyAppElasticConfiguration configuration) : base(configuration.Employees)
        {
            DisableCache();
        }
    }

    [Fact]
    public async Task PatchAllAsync_WithUseUpdateByQueryAndNoConflicts_PatchesAll()
    {
        // Arrange
        var repository = new NoCacheEmployeeRepository(_configuration);
        var employees = EmployeeGenerator.GenerateEmployees(10, companyId: "1", yearsEmployed: 0);
        await repository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        long patched = await repository.PatchAllAsync(q => q.Company("1"), new ScriptPatch("ctx._source.yearsEmployed += 1;"), o => o.UseUpdateByQuery().ImmediateConsistency());

        // Assert
        Assert.Equal(10, patched);
        var results = await repository.GetByIdsAsync(employees.Select(e => e.Id).ToArray());
        Assert.All(results, e => Assert.Equal(1, e.YearsEmployed));
    }

    [Fact]
    public async Task PatchAllAsync_ScriptPatchDefaultPath_DoesNotSkipConcurrentlyChangedDocuments()
    {
        // Arrange
        var repository = new NoCacheEmployeeRepository(_configuration);
        var employees = EmployeeGenerator.GenerateEmployees(30, companyId: "1", yearsEmployed: 0);
        await repository.AddAsync(employees, o => o.ImmediateConsistency());

        using var writerCancellation = new CancellationTokenSource();
        var writer = StartIncrementingWriterAsync(employees.Take(10).Select(e => e.Id).ToList(), writerCancellation.Token);
        await Task.Delay(100, TestCancellationToken);

        // Act
        long patched = await repository.PatchAllAsync(q => q.Company("1"), new ScriptPatch("ctx._source.companyName = 'Patched';"), o => o.PageLimit(10).ImmediateConsistency());
        await writerCancellation.CancelAsync();
        var acknowledged = await writer;

        // Assert
        Assert.Equal(30, patched);
        var results = await repository.GetByIdsAsync(employees.Select(e => e.Id).ToArray());
        Assert.All(results, e =>
        {
            Assert.Equal("Patched", e.CompanyName);
            if (acknowledged.TryGetValue(e.Id, out int increments))
                Assert.Equal(increments, e.YearsEmployed);
        });
    }

    /// <summary>
    /// Reindexing doesn't preserve <c>_seq_no</c>/<c>_primary_term</c>, and the version check only compares that pair, so a
    /// version captured before the reindex is rejected only when the new index assigned a different pair. The pair the new
    /// index assigns depends on the reindexer's passes, so this test doesn't assume it: it asserts the guaranteed rule
    /// (conflict if and only if the pairs differ) for documents saved a different number of times before the reindex. When
    /// the pairs match, the stale copy silently overwrites the migrated document, which is why callers must re-read after a
    /// reindex.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(3)]
    public async Task SaveAsync_AfterReindex_WithPreReindexVersion_ConflictsOnlyWhenSequencePairDiffers(int savesBeforeReindex)
    {
        // Arrange
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        var version2Index = new VersionedEmployeeIndex(_configuration, 2);
        await _configuration.Employees.DeleteAsync();
        await version1Index.DeleteAsync();
        await version2Index.DeleteAsync();
        try
        {
            await version1Index.ConfigureAsync();
            var repository = new EmployeeRepository(version1Index);
            var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
            for (int i = 0; i < savesBeforeReindex; i++)
            {
                employee.Name = $"update {i}";
                await repository.SaveAsync(employee, o => o.ImmediateConsistency());
            }

            string preReindexVersion = employee.Version;
            Assert.Equal($"1:{savesBeforeReindex}", preReindexVersion);

            await version2Index.ConfigureAsync();
            await version2Index.ReindexAsync();
            await _client.Indices.RefreshAsync(cancellationToken: TestCancellationToken);

            // Act
            var reindexedRepository = new EmployeeRepository(version2Index);
            var reindexed = await reindexedRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
            Assert.NotNull(reindexed);
            employee.Name = "stale";

            // Assert
            Assert.NotNull(reindexed.Version);
            if (preReindexVersion != reindexed.Version)
            {
                await Assert.ThrowsAsync<VersionConflictDocumentException>(() => reindexedRepository.SaveAsync(employee, o => o.Cache(false)));
            }
            else
            {
                await reindexedRepository.SaveAsync(employee, o => o.Cache(false).ImmediateConsistency());
                var overwritten = await reindexedRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
                Assert.Equal("stale", overwritten?.Name);
            }

            var source = await _client.GetAsync<Dictionary<string, object?>>(new Elastic.Clients.Elasticsearch.GetRequest(version2Index.Name, employee.Id), TestCancellationToken);
            Assert.True(source.Found);
            Assert.DoesNotContain("version", source.Source!.Keys);
        }
        finally
        {
            await version1Index.DeleteAsync();
            await version2Index.DeleteAsync();
        }
    }

    [Fact]
    public async Task Writes_OnIndexWithSequenceNumbersDisabled_ThrowActionableErrors()
    {
        // Arrange
        var index = new SequenceNumbersDisabledEmployeeIndex(_configuration);
        await index.DeleteAsync();
        try
        {
            await index.ConfigureAsync();
            var repository = new EmployeeRepository(index);

            // Act
            var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
            var loaded = await repository.GetByIdAsync(employee.Id, o => o.Cache(false));
            var found = await repository.FindAsync(q => q.Id(employee.Id), o => o.ImmediateConsistency());

            // Assert
            Assert.Null(employee.Version);
            Assert.NotNull(loaded);
            Assert.Null(loaded.Version);
            Assert.Null(Assert.Single(found.Hits).Version);

            loaded.Name = "unversioned save";
            var saveException = await Assert.ThrowsAsync<DocumentException>(() => repository.SaveAsync(loaded, o => o.Cache(false)));
            Assert.Contains("has no version", saveException.Message);

            await repository.SaveAsync(loaded, o => o.Cache(false).SkipVersionCheck().ImmediateConsistency());
            await Assert.ThrowsAsync<DocumentVersioningNotSupportedException>(() => repository.PatchAsync(employee.Id, new ScriptPatch("ctx._source.name = 'x';")));
            await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync(employee.Id, new JsonPatch(new PatchDocument(new ReplaceOperation { Path = "name", Value = "x" }))));
            Assert.True(await repository.PatchAsync(employee.Id, new JsonPatch(new PatchDocument(new ReplaceOperation { Path = "name", Value = "skip" })), o => o.SkipVersionCheck().ImmediateConsistency()));

            var current = await repository.GetByIdAsync(employee.Id, o => o.Cache(false));
            Assert.NotNull(current);
            Assert.Equal("skip", current.Name);
        }
        finally
        {
            await index.DeleteAsync();
        }
    }

    [Theory]
    [InlineData("PartialPatch", true, 409)]
    [InlineData("PartialPatch", true, 400)]
    [InlineData("PartialPatch", false, 409)]
    [InlineData("PartialPatch", false, 400)]
    [InlineData("ScriptPatch", true, 409)]
    [InlineData("ScriptPatch", true, 400)]
    [InlineData("ScriptPatch", false, 409)]
    [InlineData("ScriptPatch", false, 400)]
    public async Task PatchAllAsync_WhenBatchItemFails_ProcessesEveryBatchAndReportsFailure(string patchType, bool cacheEnabled, int status)
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(12, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var invoker = new FaultInjectingRequestInvoker(new HttpRequestInvoker());
        using var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log) { RequestInvoker = invoker };
        IEmployeeRepository repository = cacheEnabled ? new EmployeeRepository(configuration) : new NoCacheEmployeeRepository(configuration);
        var ids = employees.Select(e => e.Id).ToArray();
        if (cacheEnabled)
            Assert.Equal(12, (await repository.GetByIdsAsync(ids, o => o.Cache())).Count);

        _messageBus.ResetMessagesSent();
        var updatedIds = new List<string>();
        invoker.FailFirstItemOfNextBulk(status, status is 409 ? "version_conflict_engine_exception" : "illegal_argument_exception", "injected failure");

        // Act
        Task Patch() => repository.PatchAllAsync(q => q.Company("1"), CreatePatch(patchType), o => o.PageLimit(5).ImmediateConsistency().UpdateCallback(updatedIds.AddRange));
        var exception = status is 409
            ? await Assert.ThrowsAsync<VersionConflictDocumentException>(Patch)
            : await Assert.ThrowsAsync<DocumentException>(Patch);

        // Assert
        string failedId = Assert.Single(invoker.FailedBulkItemIds);
        Assert.Contains(failedId, exception.Message);
        Assert.Equal(3, invoker.CountRequests(HttpMethod.POST, "_bulk"));
        Assert.Equal(11, updatedIds.Count);
        Assert.DoesNotContain(failedId, updatedIds);
        Assert.Equal(11, _messageBus.MessagesSent);

        foreach (string id in updatedIds)
        {
            var current = await repository.GetByIdAsync(id, o => o.Cache());
            Assert.NotNull(current);
            Assert.Equal("Patched", current.CompanyName);
        }
    }

    [Fact]
    public async Task PatchAllAsync_ScriptPatchWithCacheWhenScriptFailsForOneDocument_PatchesTheRestAndThrows()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(12, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        string failId = employees[3].Id;
        var script = new ScriptPatch("if (ctx._id == params.failId) { throw new IllegalArgumentException('boom'); } ctx._source.companyName = 'Patched';")
        {
            Params = new Dictionary<string, object> { ["failId"] = failId }
        };
        var updatedIds = new List<string>();

        // Act
        var exception = await Assert.ThrowsAsync<DocumentException>(() =>
            _employeeRepository.PatchAllAsync(q => q.Company("1"), script, o => o.PageLimit(5).ImmediateConsistency().UpdateCallback(updatedIds.AddRange)));

        // Assert
        Assert.Contains(failId, exception.Message);
        Assert.Equal(11, updatedIds.Count);
        var results = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), o => o.Cache(false));
        Assert.All(results, e => Assert.Equal(e.Id == failId ? employees[3].CompanyName : "Patched", e.CompanyName));
    }
}