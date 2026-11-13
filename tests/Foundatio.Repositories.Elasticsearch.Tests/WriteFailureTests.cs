using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Xunit;
using HttpMethod = Elastic.Transport.HttpMethod;

namespace Foundatio.Repositories.Elasticsearch.Tests;

/// <summary>
/// How every write reports failures: what is retried, which exception wins, and that successes are always notified and
/// invalidated before the failure is thrown.
/// </summary>
public sealed class WriteFailureTests : ElasticRepositoryTestBase
{
    private readonly IEmployeeRepository _employeeRepository;

    public WriteFailureTests(ITestOutputHelper output) : base(output)
    {
        _employeeRepository = new EmployeeRepository(_configuration);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    private (EmployeeRepository Repository, FaultInjectingRequestInvoker Invoker, MyAppElasticConfiguration Configuration) CreateFaultInjectingRepository()
    {
        var invoker = new FaultInjectingRequestInvoker(new HttpRequestInvoker());
        var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log) { RequestInvoker = invoker };
        return (new EmployeeRepository(configuration), invoker, configuration);
    }

    private Task ChangeNameOutsideRepositoryAsync(string id, string name)
    {
        return _client.UpdateAsync<Employee, object>(_configuration.Employees.Name, id, u => u.Doc(new { name }).Refresh(Elastic.Clients.Elasticsearch.Refresh.True), TestCancellationToken);
    }

    [Theory]
    [InlineData(502)]
    [InlineData(504)]
    public async Task SaveAsync_WithGatewayError_IsNotRetried(int statusCode)
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.FailNext(1, statusCode, HttpMethod.PUT, $"/_doc/{employee.Id}");

        // Act
        employee.Name = "not saved";
        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.SaveAsync(employee, o => o.Cache(false)));

        // Assert
        Assert.Contains(employee.Id, exception.Message);
        Assert.Equal(1, invoker.CountRequests(HttpMethod.PUT, $"/_doc/{employee.Id}"));
    }

    [Theory]
    [InlineData(502)]
    [InlineData(504)]
    public async Task PatchAllAsync_WithGatewayErrorOnBulk_IsNotRetried(int statusCode)
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(3, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.FailNext(1, statusCode, HttpMethod.POST, "_bulk");

        // Act & Assert
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(q => q.Company("1"), new ActionPatch<Employee>(e => e.CompanyName = "Patched"), o => o.ImmediateConsistency()));
        Assert.Equal(1, invoker.CountRequests(HttpMethod.POST, "_bulk"));
    }

    [Fact]
    public async Task SaveAsync_WithTimeoutAfterSend_IsNotRetried()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.ThrowNext(1, new TaskCanceledException("The request timed out", new TimeoutException()), HttpMethod.PUT, $"/_doc/{employee.Id}");

        // Act
        employee.Name = "not saved";
        await Assert.ThrowsAsync<DocumentException>(() => repository.SaveAsync(employee, o => o.Cache(false)));

        // Assert
        Assert.Equal(1, invoker.CountRequests(HttpMethod.PUT, $"/_doc/{employee.Id}"));
    }

    [Fact]
    public async Task SaveAsync_WithConnectionRefused_RetriesBecauseNothingWasSent()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.ThrowNext(1, new HttpRequestException(HttpRequestError.ConnectionError, "Connection refused"), HttpMethod.PUT, $"/_doc/{employee.Id}");

        // Act
        employee.Name = "saved after reconnect";
        await repository.SaveAsync(employee, o => o.Cache(false));

        // Assert
        Assert.Equal(2, invoker.CountRequests(HttpMethod.PUT, $"/_doc/{employee.Id}"));
        Assert.Equal("1:1", employee.Version);
    }

    [Theory]
    [InlineData("JsonPatch")]
    [InlineData("ScriptPatch")]
    public async Task PatchAsync_WithIfVersionAndTransientFailureThenConcurrentWrite_ThrowsConflict(string patchType)
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        string? expected = employee.Version;
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        string path = patchType is "JsonPatch" ? "_bulk" : $"/_update/{employee.Id}";
        invoker.FailNext(1, 429, HttpMethod.POST, path, () => ChangeNameOutsideRepositoryAsync(employee.Id, "concurrent"));
        IPatchOperation patch = patchType is "JsonPatch"
            ? new JsonPatch(new PatchDocument(new ReplaceOperation { Path = "companyName", Value = "Patched" }))
            : new ScriptPatch("ctx._source.companyName = 'Patched';");

        // Act
        await Assert.ThrowsAsync<VersionConflictDocumentException>(() => repository.PatchAsync(employee.Id, patch, o => o.IfVersion(expected)));

        // Assert
        Assert.Equal(2, invoker.CountRequests(HttpMethod.POST, path));
        Assert.True(invoker.CountRequests(HttpMethod.POST, "_mget") <= 1, "An expected version must never re-read the document.");
        var current = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.NotNull(current);
        Assert.Equal("concurrent", current.Name);
        Assert.NotEqual("Patched", current.CompanyName);
    }

    [Fact]
    public async Task PatchAsync_WithIfVersionOnNonVersionedEntity_ChecksTheVersion()
    {
        // Arrange
        var repository = new IdentityRepository(_configuration);
        var identity = await repository.AddAsync(IdentityGenerator.Generate(), o => o.ImmediateConsistency());
        var response = await _client.GetAsync<Identity>(new Elastic.Clients.Elasticsearch.GetRequest(_configuration.Identities.Name, identity.Id), TestCancellationToken);
        string version = $"{response.PrimaryTerm}:{response.SeqNo}";
        await _client.IndexAsync(identity, i => i.Index(_configuration.Identities.Name).Id(identity.Id).Refresh(Elastic.Clients.Elasticsearch.Refresh.True), TestCancellationToken);

        // Act & Assert
        await Assert.ThrowsAsync<VersionConflictDocumentException>(() => repository.PatchAsync(identity.Id, new ActionPatch<Identity>(i => i.Id = i.Id), o => o.IfVersion(version)));
        await Assert.ThrowsAsync<VersionConflictDocumentException>(() => repository.PatchAsync(identity.Id, new ScriptPatch("ctx._source.id = ctx._source.id;"), o => o.IfVersion(version)));
        await Assert.ThrowsAsync<ArgumentException>(() => repository.PatchAsync(identity.Id, new ScriptPatch("ctx._source.id = ctx._source.id;"), o => o.IfVersion(version).SkipVersionCheck()));
        await Assert.ThrowsAsync<ArgumentException>(() => repository.PatchAsync(new[] { identity.Id }, new ScriptPatch("ctx._source.id = ctx._source.id;"), o => o.IfVersion(version)));
    }

    [Fact]
    public async Task PatchAsync_JsonPatchTargetingVersion_IsRejected()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        var patch = new JsonPatch(new PatchDocument(new ReplaceOperation { Path = "/version", Value = "9:9" }));

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _employeeRepository.PatchAsync(employee.Id, patch));
        await Assert.ThrowsAsync<ArgumentException>(() => _employeeRepository.PatchAllAsync(q => q.Id(employee.Id), patch));
    }

    [Fact]
    public async Task PatchAsync_JsonPatchWithTransientReadFailure_RetriesTheRead()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.FailNext(1, 503, HttpMethod.POST, "_mget");

        // Act
        bool modified = await repository.PatchAsync(employee.Id, new JsonPatch(new PatchDocument(new ReplaceOperation { Path = "companyName", Value = "Patched" })));

        // Assert
        Assert.True(modified);
        Assert.Equal(2, invoker.CountRequests(HttpMethod.POST, "_mget"));
    }

    [Fact]
    public async Task PatchAsyncIds_WithMultiGetItemError_ReportsErrorNotMissing()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(3);
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.FailFirstDocOfNextMultiGet("no_shard_available_action_exception", "shard unavailable");

        // Act
        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync(employees.Select(e => e.Id).ToArray(), new ActionPatch<Employee>(e => e.CompanyName = "Patched"), o => o.ImmediateConsistency()));

        // Assert
        Assert.IsNotType<DocumentNotFoundException>(exception);
        Assert.Contains("shard unavailable", exception.Message);
        var results = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), o => o.Cache(false));
        Assert.Equal(2, results.Count(e => e.CompanyName == "Patched"));
    }

    [Fact]
    public async Task GetByIdsAsync_WithMultiGetItemError_ThrowsInsteadOfReturningMissing()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(2);
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        await repository.CountAsync(o => o.ImmediateConsistency());
        invoker.FailFirstDocOfNextMultiGet("no_shard_available_action_exception", "shard unavailable");

        // Act & Assert
        await Assert.ThrowsAsync<DocumentException>(() => repository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), o => o.Cache(false)));
    }

    [Fact]
    public async Task PatchAsyncIds_WhenOneScriptFails_NotifiesTheOthersAndThrows()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(4);
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        string failId = employees[1].Id;
        var script = new ScriptPatch("if (ctx._id == params.failId) { throw new IllegalArgumentException('boom'); } ctx._source.companyName = 'Patched';")
        {
            Params = new Dictionary<string, object> { ["failId"] = failId }
        };
        _messageBus.ResetMessagesSent();

        // Act
        var exception = await Assert.ThrowsAsync<DocumentException>(() => _employeeRepository.PatchAsync(employees.Select(e => e.Id).ToArray(), script, o => o.ImmediateConsistency()));

        // Assert
        Assert.Contains(failId, exception.Message);
        Assert.Equal(3, _messageBus.MessagesSent);
        var results = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), o => o.Cache(false));
        Assert.All(results, e => Assert.Equal(e.Id == failId ? employees[1].CompanyName : "Patched", e.CompanyName));
    }

    [Fact]
    public async Task PatchAllAsync_WhenRepositoryDisposedDuringRetry_ThrowsCancelledAfterNotifyingSuccesses()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(4, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.FailFirstItemOfNextBulk(429, "es_rejected_execution_exception", "rejected");
        invoker.BeforeNextBulk(() =>
        {
            invoker.BeforeNextBulk(() =>
            {
                repository.Dispose();
                return Task.CompletedTask;
            });
            return Task.CompletedTask;
        });
        var updatedIds = new List<string>();

        // Act
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => repository.PatchAllAsync(q => q.Company("1"), new ActionPatch<Employee>(e => e.CompanyName = "Patched"), o => o.ImmediateConsistency().UpdateCallback(updatedIds.AddRange)));

        // Assert
        Assert.Equal(3, updatedIds.Count);
        Assert.DoesNotContain(Assert.Single(invoker.FailedBulkItemIds), updatedIds);
    }

    [Fact]
    public async Task RemoveAsync_WithConcurrentDeleteForSingleAndBulk_CountsAsRemoved()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(3);
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        foreach (var employee in employees)
            await _client.DeleteAsync(new Elastic.Clients.Elasticsearch.DeleteRequest(_configuration.Employees.Name, employee.Id) { Refresh = Elastic.Clients.Elasticsearch.Refresh.True }, TestCancellationToken);

        var removed = new List<string>();
        var changed = new List<string>();
        _employeeRepository.DocumentsRemoved.AddSyncHandler((o, args) => removed.AddRange(args.Documents.Select(d => d.Id)));
        _employeeRepository.DocumentsChanged.AddSyncHandler((o, args) => changed.AddRange(args.Documents.Select(d => d.Value.Id)));
        _messageBus.ResetMessagesSent();

        // Act & Assert
        await _employeeRepository.RemoveAsync(employees[0], o => o.ImmediateConsistency());
        await _employeeRepository.RemoveAsync(employees.Skip(1).ToList(), o => o.ImmediateConsistency());

        Assert.Empty(removed);
        Assert.Empty(changed);
        Assert.Equal(0, _messageBus.MessagesSent);
    }

    [Fact]
    public async Task RemoveAsync_WithSomeAlreadyDeletedDocuments_RaisesRemovedOnlyForDocumentsItDeleted()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(3);
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        await _client.DeleteAsync(new Elastic.Clients.Elasticsearch.DeleteRequest(_configuration.Employees.Name, employees[1].Id) { Refresh = Elastic.Clients.Elasticsearch.Refresh.True }, TestCancellationToken);

        var removed = new List<string>();
        var changed = new List<string>();
        _employeeRepository.DocumentsRemoved.AddSyncHandler((o, args) => removed.AddRange(args.Documents.Select(d => d.Id)));
        _employeeRepository.DocumentsChanged.AddSyncHandler((o, args) => changed.AddRange(args.Documents.Select(d => d.Value.Id)));
        _messageBus.ResetMessagesSent();

        // Act
        await _employeeRepository.RemoveAsync(employees, o => o.ImmediateConsistency());

        // Assert
        Assert.Equal(new[] { employees[0].Id, employees[2].Id }.Order(), removed.Order());
        Assert.Equal(new[] { employees[0].Id, employees[2].Id }.Order(), changed.Order());
        Assert.Equal(2, _messageBus.MessagesSent);
        Assert.Equal(0, await _employeeRepository.CountAsync());
    }

    [Fact]
    public async Task RemoveAsync_WithAlreadyDeletedDocument_InvalidatesCache()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache()));
        await _client.DeleteAsync(new Elastic.Clients.Elasticsearch.DeleteRequest(_configuration.Employees.Name, employee.Id) { Refresh = Elastic.Clients.Elasticsearch.Refresh.True }, TestCancellationToken);
        Assert.NotNull(await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache()));

        // Act
        await _employeeRepository.RemoveAsync(employee, o => o.ImmediateConsistency());

        // Assert
        Assert.Null(await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache()));
    }

    private async Task<(EmployeeRepository Repository, MyAppElasticConfiguration Configuration)> CreateRepositoryWithoutIndexAsync()
    {
        await _configuration.DeleteIndexesAsync();
        var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log);
        return (new EmployeeRepository(configuration), configuration);
    }

    private async Task<bool> EmployeesIndexExistsAsync()
    {
        var response = await _client.Indices.ExistsAsync(_configuration.Employees.Name, cancellationToken: TestCancellationToken);
        return response.Exists;
    }

    [Fact]
    public async Task RemoveAsync_WithVersionedDocumentWithoutVersion_DoesNotCreateIndex()
    {
        // Arrange
        var (repository, configuration) = await CreateRepositoryWithoutIndexAsync();
        using var _ = configuration;
        var employee = EmployeeGenerator.Generate(id: ObjectId.GenerateNewId().ToString());

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.RemoveAsync(employee));

        // Assert
        Assert.False(await EmployeesIndexExistsAsync());
    }

    [Fact]
    public async Task RemoveAsync_WithCacheKey_DoesNotCreateIndex()
    {
        // Arrange
        var (repository, configuration) = await CreateRepositoryWithoutIndexAsync();
        using var _ = configuration;
        var employee = EmployeeGenerator.Generate(id: ObjectId.GenerateNewId().ToString());

        // Act
        await Assert.ThrowsAsync<ArgumentException>(() => repository.RemoveAsync(employee, o => o.CacheKey("key").SkipVersionCheck()));

        // Assert
        Assert.False(await EmployeesIndexExistsAsync());
    }

    [Fact]
    public async Task RemoveAsync_WhenRemovingHandlerThrows_DoesNotCreateIndex()
    {
        // Arrange
        var (repository, configuration) = await CreateRepositoryWithoutIndexAsync();
        using var _ = configuration;
        repository.DocumentsRemoving.AddSyncHandler((o, args) => throw new InvalidOperationException("vetoed"));
        var employee = EmployeeGenerator.Generate(id: ObjectId.GenerateNewId().ToString());

        // Act
        await Assert.ThrowsAsync<InvalidOperationException>(() => repository.RemoveAsync(employee, o => o.SkipVersionCheck()));

        // Assert
        Assert.False(await EmployeesIndexExistsAsync());
    }

    [Fact]
    public async Task PatchAsync_WithInvalidExpectedVersion_DoesNotCreateIndex()
    {
        // Arrange
        var (repository, configuration) = await CreateRepositoryWithoutIndexAsync();
        using var _ = configuration;
        string id = ObjectId.GenerateNewId().ToString();
        var patch = new PartialPatch(new { companyName = "Patched" });

        // Act
        await Assert.ThrowsAsync<ArgumentException>(() => repository.PatchAsync(id, patch, o => o.IfVersion("not-a-version")));
        await Assert.ThrowsAsync<ArgumentException>(() => repository.PatchAsync(id, patch, o => o.IfVersion("1:1").SkipVersionCheck()));

        // Assert
        Assert.False(await EmployeesIndexExistsAsync());
    }

    private sealed class NoCacheEmployeeRepository : EmployeeRepository
    {
        public NoCacheEmployeeRepository(MyAppElasticConfiguration configuration) : base(configuration.Employees)
        {
            DisableCache();
        }
    }

    /// <summary>No change handlers and no cache, so RemoveAllAsync uses delete-by-query.</summary>
    private sealed class PlainEmployeeRepository : ElasticRepositoryBase<Employee>
    {
        public PlainEmployeeRepository(MyAppElasticConfiguration configuration) : base(configuration.Employees)
        {
            DisableCache();
        }
    }

    private const string DeleteByQueryWithConflicts = """{"took":1,"timed_out":false,"total":5,"deleted":3,"batches":1,"version_conflicts":2,"noops":0,"retries":{"bulk":0,"search":0},"throttled_millis":0,"requests_per_second":-1.0,"throttled_until_millis":0,"failures":[]}""";

    [Fact]
    public async Task RemoveAllAsync_WhenLaterPassFails_NotifiesRemovedAndThrows()
    {
        // Arrange
        var invoker = new FaultInjectingRequestInvoker(new HttpRequestInvoker());
        using var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log) { RequestInvoker = invoker };
        var repository = new PlainEmployeeRepository(configuration);
        await repository.CountAsync();
        invoker.RespondNext(1, 200, DeleteByQueryWithConflicts, HttpMethod.POST, "_delete_by_query");
        invoker.FailNext(1, 500, HttpMethod.POST, "_delete_by_query");
        _messageBus.ResetMessagesSent();

        // Act
        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.RemoveAllAsync(q => q.Company("1")));

        // Assert
        Assert.IsNotType<VersionConflictDocumentException>(exception);
        Assert.True(_messageBus.MessagesSent > 0, "Documents removed by the first pass must be notified.");
        Assert.Equal(2, invoker.CountRequests(HttpMethod.POST, "_delete_by_query"));
    }

    [Fact]
    public async Task RemoveAllAsync_WhenRepositoryDisposedBetweenPasses_ThrowsCancelledNotConflict()
    {
        // Arrange
        var invoker = new FaultInjectingRequestInvoker(new HttpRequestInvoker());
        using var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log) { RequestInvoker = invoker };
        var repository = new PlainEmployeeRepository(configuration);
        await repository.CountAsync();
        invoker.RespondNext(1, 200, DeleteByQueryWithConflicts, HttpMethod.POST, "_delete_by_query", () =>
        {
            repository.Dispose();
            return Task.CompletedTask;
        });

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => repository.RemoveAllAsync(q => q.Company("1")));
        Assert.Equal(1, invoker.CountRequests(HttpMethod.POST, "_delete_by_query"));
    }

    [Fact]
    public async Task PatchAllAsync_WithUpdateByQueryWhenTaskIsNotFound_ThrowsInsteadOfReportingZero()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(3, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var invoker = new FaultInjectingRequestInvoker(new HttpRequestInvoker());
        using var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log) { RequestInvoker = invoker };
        var repository = new NoCacheEmployeeRepository(configuration);
        invoker.FailNext(1, 404, HttpMethod.GET, "_tasks/");
        int changedEvents = 0;
        repository.DocumentsChanged.AddHandler((_, args) =>
        {
            if (args.ChangeType is ChangeType.Saved)
                changedEvents++;

            return Task.CompletedTask;
        });

        // Act & Assert
        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(q => q.Company("1"), new ScriptPatch("ctx._source.yearsEmployed += 1;"), o => o.UseUpdateByQuery()));
        Assert.Contains("not found", exception.Message);
        Assert.Equal(1, changedEvents);
    }

    [Fact]
    public async Task PatchAllAsync_WithUpdateByQueryWhenSubmitFails_DoesNotReportChanges()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(3, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var invoker = new FaultInjectingRequestInvoker(new HttpRequestInvoker());
        using var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log) { RequestInvoker = invoker };
        var repository = new NoCacheEmployeeRepository(configuration);
        invoker.FailNext(1, 400, HttpMethod.POST, "_update_by_query");
        int changedEvents = 0;
        repository.DocumentsChanged.AddHandler((_, _) =>
        {
            changedEvents++;
            return Task.CompletedTask;
        });

        // Act & Assert
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(q => q.Company("1"), new ScriptPatch("ctx._source.yearsEmployed += 1;"), o => o.UseUpdateByQuery()));
        Assert.Equal(0, changedEvents);
    }

    [Fact]
    public async Task PatchAllAsync_WithUpdateByQueryWhenDisposedWhileWaiting_ThrowsCancelled()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(3, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var invoker = new FaultInjectingRequestInvoker(new HttpRequestInvoker());
        using var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log) { RequestInvoker = invoker };
        var repository = new NoCacheEmployeeRepository(configuration);
        invoker.FailNext(1, 503, HttpMethod.GET, "_tasks/", () =>
        {
            repository.Dispose();
            return Task.CompletedTask;
        });

        int changedEvents = 0;
        repository.DocumentsChanged.AddHandler((_, args) =>
        {
            if (args.ChangeType is ChangeType.Saved)
                changedEvents++;

            return Task.CompletedTask;
        });

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => repository.PatchAllAsync(q => q.Company("1"), new ScriptPatch("ctx._source.yearsEmployed += 1;"), o => o.UseUpdateByQuery()));
        Assert.Equal(1, changedEvents);
    }

    [Fact]
    public async Task PatchAllAsync_WhenConflictReReadFails_NotifiesSuccessesAndThrows()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(4, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.BeforeNextBulk(() => ChangeNameOutsideRepositoryAsync(employees[0].Id, "concurrent"));
        invoker.FailNext(1, 500, HttpMethod.POST, "_mget");
        var updatedIds = new List<string>();

        // Act
        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(q => q.Company("1"), new ActionPatch<Employee>(e => e.CompanyName = "Patched"), o => o.ImmediateConsistency().UpdateCallback(updatedIds.AddRange)));

        // Assert
        Assert.Contains(employees[0].Id, exception.Message);
        Assert.Equal(employees.Skip(1).Select(e => e.Id).Order(), updatedIds.Order());
    }

    [Fact]
    public async Task PatchAllAsync_WhenPatchThrowsOnRetry_NotifiesSuccessesAndThrows()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(4, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var (repository, invoker, configuration) = CreateFaultInjectingRepository();
        using var _ = configuration;
        invoker.BeforeNextBulk(() => ChangeNameOutsideRepositoryAsync(employees[0].Id, "concurrent"));
        var patch = new ActionPatch<Employee>(e =>
        {
            if (e.Name == "concurrent")
                throw new InvalidOperationException("patch failed on the re-read document");
            e.CompanyName = "Patched";
        });
        var updatedIds = new List<string>();

        // Act
        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(q => q.Company("1"), patch, o => o.ImmediateConsistency().UpdateCallback(updatedIds.AddRange)));

        // Assert
        Assert.Contains("patch failed on the re-read document", exception.Message);
        Assert.Equal(3, updatedIds.Count);
        Assert.DoesNotContain(employees[0].Id, updatedIds);
    }

    [Fact]
    public async Task PatchAsyncIds_ScriptPatchWithPipelineWhenRepositoryDisposedBetweenDocuments_NotifiesPatchedDocumentsAndThrowsCancelled()
    {
        // Arrange
        const string pipelineId = "write-failure-tests-dispose-pipeline";
        var pipeline = await _client.Ingest.PutPipelineAsync(pipelineId, p => p.Processors(pr => pr.Lowercase(l => l.Field("name"))), TestCancellationToken);
        Assert.True(pipeline.IsValidResponse);
        try
        {
            var invoker = new FaultInjectingRequestInvoker(new HttpRequestInvoker());
            using var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log) { RequestInvoker = invoker };
            var repository = new EmployeeWithPipelineRepository(configuration.Employees, pipelineId);
            var employees = EmployeeGenerator.GenerateEmployees(3, companyId: "1");
            await repository.AddAsync(employees, o => o.ImmediateConsistency());
            invoker.BeforeNthRequest(2, HttpMethod.POST, "_update_by_query", () =>
            {
                repository.Dispose();
                return Task.CompletedTask;
            });
            var updatedIds = new List<string>();
            _messageBus.ResetMessagesSent();

            // Act
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => repository.PatchAsync(employees.Select(e => e.Id).ToArray(), new ScriptPatch("ctx._source.companyName = 'Patched';"), o => o.ImmediateConsistency().UpdateCallback(updatedIds.AddRange)));

            // Assert
            Assert.Equal([employees[0].Id], updatedIds);
            Assert.Equal(1, _messageBus.MessagesSent);
        }
        finally
        {
            await _client.Ingest.DeletePipelineAsync(pipelineId, cancellationToken: TestCancellationToken);
        }
    }

    [Fact]
    public async Task PatchAllAsync_ScriptPatchWithPipelineWhenOneDocumentFails_PatchesTheRestAndThrows()
    {
        // Arrange
        const string pipelineId = "write-failure-tests-pipeline";
        var pipeline = await _client.Ingest.PutPipelineAsync(pipelineId, p => p.Processors(pr => pr.Lowercase(l => l.Field("name"))), TestCancellationToken);
        Assert.True(pipeline.IsValidResponse);
        try
        {
            var repository = new EmployeeWithPipelineRepository(_configuration.Employees, pipelineId);
            var employees = EmployeeGenerator.GenerateEmployees(4, companyId: "1");
            await repository.AddAsync(employees, o => o.ImmediateConsistency());
            string failId = employees[2].Id;
            var script = new ScriptPatch("if (ctx._id == params.failId) { throw new IllegalArgumentException('boom'); } ctx._source.companyName = 'Patched';")
            {
                Params = new Dictionary<string, object> { ["failId"] = failId }
            };
            var updatedIds = new List<string>();

            // Act
            var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(q => q.Company("1"), script, o => o.ImmediateConsistency().UpdateCallback(updatedIds.AddRange)));

            // Assert
            Assert.Contains(failId, exception.Message);
            Assert.Equal(3, updatedIds.Count);
            var results = await repository.GetByIdsAsync(employees.Select(e => e.Id).ToArray(), o => o.Cache(false));
            Assert.All(results, e => Assert.Equal(e.Id == failId ? employees[2].CompanyName : "Patched", e.CompanyName));
        }
        finally
        {
            await _client.Ingest.DeletePipelineAsync(pipelineId, cancellationToken: TestCancellationToken);
        }
    }

    [Fact]
    public async Task IsMissingDocumentConflict_MatchesTheReasonElasticsearchReturns()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        await _client.DeleteAsync(new Elastic.Clients.Elasticsearch.DeleteRequest(_configuration.Employees.Name, employee.Id) { Refresh = Elastic.Clients.Elasticsearch.Refresh.True }, TestCancellationToken);
        var version = ElasticDocumentVersion.Parse(employee.Version);

        // Act
        var response = await _client.DeleteAsync(new Elastic.Clients.Elasticsearch.DeleteRequest(_configuration.Employees.Name, employee.Id) { IfSeqNo = version.SequenceNumber, IfPrimaryTerm = version.PrimaryTerm }, TestCancellationToken);
        var changed = await _client.IndexAsync(employee, i => i.Index(_configuration.Employees.Name).Id(employee.Id).Refresh(Elastic.Clients.Elasticsearch.Refresh.True), TestCancellationToken);
        var conflict = await _client.DeleteAsync(new Elastic.Clients.Elasticsearch.DeleteRequest(_configuration.Employees.Name, employee.Id) { IfSeqNo = version.SequenceNumber, IfPrimaryTerm = version.PrimaryTerm }, TestCancellationToken);

        // Assert
        Assert.Equal(409, response.ApiCallDetails.HttpStatusCode);
        Assert.True(ElasticWriteErrors.IsMissingDocumentConflict(response.ElasticsearchServerError?.Error?.Reason), response.ElasticsearchServerError?.Error?.Reason);
        Assert.True(changed.IsValidResponse);
        Assert.Equal(409, conflict.ApiCallDetails.HttpStatusCode);
        Assert.False(ElasticWriteErrors.IsMissingDocumentConflict(conflict.ElasticsearchServerError?.Error?.Reason));
    }

    [Fact]
    public async Task PatchAllAsync_WithSameIdInTwoIndexes_PatchesBoth()
    {
        // Arrange
        var index = new MonthlyEmployeeIndex(_configuration, 1);
        await index.DeleteAsync();
        try
        {
            await index.ConfigureAsync();
            var repository = new EmployeeRepository(index);
            string id = ObjectId.GenerateNewId().ToString();
            var now = DateTime.UtcNow;
            string thisMonth = index.GetIndex(now);
            string lastMonth = index.GetIndex(now.AddMonths(-1));
            await index.EnsureIndexAsync(now);
            await index.EnsureIndexAsync(now.AddMonths(-1));
            foreach (string name in new[] { thisMonth, lastMonth })
                await _client.IndexAsync(EmployeeGenerator.Generate(id: id, companyId: "dup"), i => i.Index(name).Id(id).Refresh(Elastic.Clients.Elasticsearch.Refresh.True), TestCancellationToken);

            // Act
            long patched = await repository.PatchAllAsync(q => q.Company("dup"), new ActionPatch<Employee>(e => e.CompanyName = "Patched"), o => o.ImmediateConsistency());

            // Assert
            Assert.Equal(2, patched);
            foreach (string name in new[] { thisMonth, lastMonth })
            {
                var stored = await _client.GetAsync<Employee>(new Elastic.Clients.Elasticsearch.GetRequest(name, id), TestCancellationToken);
                Assert.Equal("Patched", stored.Source?.CompanyName);
            }
        }
        finally
        {
            await index.DeleteAsync();
        }
    }
}
