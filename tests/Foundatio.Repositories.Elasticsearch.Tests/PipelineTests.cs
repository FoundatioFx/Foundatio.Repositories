using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class PipelineTests : ElasticRepositoryTestBase
{
    private const string PipelineId = "employee-lowercase-name";
    private readonly EmployeeWithPipelineRepository _employeeRepository;

    public PipelineTests(ITestOutputHelper output) : base(output)
    {
        _employeeRepository = new EmployeeWithPipelineRepository(_configuration.Employees, PipelineId);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();

        var response = await _client.Ingest.PutPipelineAsync(PipelineId, p => p
            .Description("Lowercases the name field for pipeline tests")
            .Processors(pr => pr.Lowercase(l => l.Field("name"))));
        Assert.True(response.IsValidResponse, $"Failed to create pipeline: {response.ElasticsearchServerError?.Error?.Reason}");

        await RemoveDataAsync();
    }

    [Fact]
    public async Task AddAsync_WithLowercasePipeline_LowercasesName()
    {
        // Arrange
        var employee = EmployeeGenerator.Generate(name: "  BLAKE  ");

        // Act
        employee = await _employeeRepository.AddAsync(employee, o => o.ImmediateConsistency());

        // Assert
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);
        var result = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.NotNull(result);
        Assert.Equal("  blake  ", result.Name);
    }

    [Fact]
    public async Task AddCollectionAsync_WithLowercasePipeline_LowercasesNames()
    {
        // Arrange
        var employees = new List<Employee>
        {
            EmployeeGenerator.Generate(name: "  BLAKE  "),
            EmployeeGenerator.Generate(name: "\tBLAKE  ")
        };

        // Act
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Assert
        var result = await _employeeRepository.GetByIdsAsync(new Ids(employees.Select(e => e.Id)));
        Assert.Equal(2, result.Count);
        Assert.Contains(result, e => String.Equals(e.Name, "  blake  "));
        Assert.Contains(result, e => String.Equals(e.Name, "\tblake  "));
    }

    [Fact]
    public async Task SaveCollectionAsync_WithLowercasePipeline_LowercasesNames()
    {
        // Arrange
        var employee1 = EmployeeGenerator.Generate(id: ObjectId.GenerateNewId().ToString(), name: "Original1");
        var employee2 = EmployeeGenerator.Generate(id: ObjectId.GenerateNewId().ToString(), name: "Original2");
        await _employeeRepository.AddAsync(new List<Employee> { employee1, employee2 }, o => o.ImmediateConsistency());

        // Act
        employee1.Name = "  BLAKE  ";
        employee2.Name = "\tBLAKE  ";
        await _employeeRepository.SaveAsync(new List<Employee> { employee1, employee2 }, o => o.ImmediateConsistency());

        // Assert
        var result = await _employeeRepository.GetByIdsAsync(new List<string> { employee1.Id, employee2.Id });
        Assert.Equal(2, result.Count);
        Assert.Contains(result, e => String.Equals(e.Name, "  blake  "));
        Assert.Contains(result, e => String.Equals(e.Name, "\tblake  "));
    }

    [Fact]
    public async Task JsonPatchAsync_WithLowercasePipeline_LowercasesName()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());

        // Act
        var patch = new PatchDocument(new ReplaceOperation { Path = "name", Value = JsonValue.Create("Patched") });
        await _employeeRepository.PatchAsync(employee.Id, new JsonPatch(patch), o => o.ImmediateConsistency());

        // Assert
        employee = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.NotNull(employee);
        Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
        Assert.Equal("patched", employee.Name);
    }

    [Fact]
    public async Task JsonPatchAllAsync_WithLowercasePipeline_LowercasesNames()
    {
        // Arrange
        var employees = new List<Employee>
        {
            EmployeeGenerator.Generate(companyId: "1", name: "employee1"),
            EmployeeGenerator.Generate(companyId: "1", name: "employee2"),
        };
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        var patch = new PatchDocument(new ReplaceOperation { Path = "name", Value = JsonValue.Create("Patched") });
        await _employeeRepository.PatchAsync(employees.Select(e => e.Id).ToArray(), new JsonPatch(patch), o => o.ImmediateConsistency());

        // Assert
        var results = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToList());
        Assert.Equal(2, results.Count);
        Assert.All(results, e => Assert.Equal("patched", e.Name));
    }

    [Fact]
    public async Task PartialPatchAsync_WithLowercasePipeline_LowercasesName()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());

        // Act
        await _employeeRepository.PatchAsync(employee.Id, new PartialPatch(new { name = "Patched" }), o => o.ImmediateConsistency());

        // Assert
        employee = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.NotNull(employee);
        Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
        Assert.Equal("patched", employee.Name);
    }

    [Fact]
    public async Task PartialPatchAllAsync_WithLowercasePipeline_LowercasesNames()
    {
        // Arrange
        var employees = new List<Employee>
        {
            EmployeeGenerator.Generate(companyId: "1", name: "employee1"),
            EmployeeGenerator.Generate(companyId: "1", name: "employee2"),
        };
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        await _employeeRepository.PatchAsync(employees.Select(e => e.Id).ToArray(), new PartialPatch(new { name = "Patched" }), o => o.ImmediateConsistency());

        // Assert
        var results = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToList());
        Assert.Equal(2, results.Count);
        Assert.All(results, e => Assert.Equal("patched", e.Name));
    }

    [Fact]
    public async Task ScriptPatchAsync_WithLowercasePipeline_LowercasesName()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());

        // Act
        await _employeeRepository.PatchAsync(employee.Id, new ScriptPatch("ctx._source.name = 'Patched';"), o => o.ImmediateConsistency());

        // Assert
        employee = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.NotNull(employee);
        Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
        Assert.Equal("patched", employee.Name);
    }

    [Fact]
    public async Task ScriptPatchAllAsync_WithLowercasePipeline_LowercasesNames()
    {
        // Arrange
        var employees = new List<Employee>
        {
            EmployeeGenerator.Generate(companyId: "1", name: "employee1"),
            EmployeeGenerator.Generate(companyId: "1", name: "employee2"),
        };
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        await _employeeRepository.PatchAsync(employees.Select(e => e.Id).ToArray(), new ScriptPatch("ctx._source.name = 'Patched';"), o => o.ImmediateConsistency());

        // Assert
        var results = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToList());
        Assert.Equal(2, results.Count);
        Assert.All(results, e => Assert.Equal("patched", e.Name));
    }

    [Fact]
    public async Task ScriptPatchAllAsync_ByQueryWithLowercasePipeline_LowercasesNames()
    {
        // Arrange
        var employees = new List<Employee>
        {
            EmployeeGenerator.Generate(companyId: "1", name: "employee1"),
            EmployeeGenerator.Generate(companyId: "1", name: "employee2"),
        };
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        long patched = await _employeeRepository.PatchAllAsync(q => q.Company("1"), new ScriptPatch("ctx._source.name = 'Patched';"), o => o.ImmediateConsistency());

        // Assert
        Assert.Equal(2, patched);
        var results = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToList());
        Assert.Equal(2, results.Count);
        Assert.All(results, e => Assert.Equal("patched", e.Name));
    }

    [Fact]
    public async Task ScriptPatchAsync_WithPipelineAndImmediateConsistency_RefreshesOnceAfterAllDocuments()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(3, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var invoker = new FaultInjectingRequestInvoker(new Elastic.Transport.HttpRequestInvoker());
        using var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log) { RequestInvoker = invoker };
        var repository = new EmployeeWithPipelineRepository(configuration.Employees, PipelineId);

        // Act
        long patched = await repository.PatchAsync(employees.Select(e => e.Id).ToArray(), new ScriptPatch("ctx._source.name = 'Patched';"), o => o.ImmediateConsistency());

        // Assert
        Assert.Equal(3, patched);
        Assert.Equal(3, invoker.CountRequests(Elastic.Transport.HttpMethod.POST, "_update_by_query"));
        Assert.Equal(0, invoker.CountRequests(Elastic.Transport.HttpMethod.POST, "refresh=true"));
        Assert.Equal(1, invoker.CountRequests(Elastic.Transport.HttpMethod.POST, "_refresh"));
        var results = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToList());
        Assert.All(results, e => Assert.Equal("patched", e.Name));
    }

    [Fact]
    public async Task ScriptPatchAsync_WithPipelineWhenOneDocumentFails_RefreshesPatchedDocumentsOnceAndThrows()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(3, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var invoker = new FaultInjectingRequestInvoker(new Elastic.Transport.HttpRequestInvoker());
        using var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log) { RequestInvoker = invoker };
        var repository = new EmployeeWithPipelineRepository(configuration.Employees, PipelineId);
        invoker.FailNext(1, 400, Elastic.Transport.HttpMethod.POST, "_update_by_query");

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync(employees.Select(e => e.Id).ToArray(), new ScriptPatch("ctx._source.name = 'Patched';"), o => o.ImmediateConsistency().Retry(0)));

        // Assert
        Assert.Equal(3, invoker.CountRequests(Elastic.Transport.HttpMethod.POST, "_update_by_query"));
        Assert.Equal(1, invoker.CountRequests(Elastic.Transport.HttpMethod.POST, "_refresh"));
        var results = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToList());
        Assert.Equal(2, results.Count(e => e.Name is "patched"));
    }

    [Fact]
    public async Task ScriptPatchAllAsync_WithPipeline_DoesNotRefreshPerDocument()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(3, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var invoker = new FaultInjectingRequestInvoker(new Elastic.Transport.HttpRequestInvoker());
        using var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log) { RequestInvoker = invoker };
        var repository = new EmployeeWithPipelineRepository(configuration.Employees, PipelineId);

        // Act
        long patched = await repository.PatchAllAsync(q => q.Company("1"), new ScriptPatch("ctx._source.name = 'Patched';"), o => o.ImmediateConsistency());

        // Assert
        Assert.Equal(3, patched);
        Assert.Equal(3, invoker.CountRequests(Elastic.Transport.HttpMethod.POST, "_update_by_query"));
        Assert.Equal(0, invoker.CountRequests(Elastic.Transport.HttpMethod.POST, "refresh=true"));
        Assert.True(invoker.CountRequests(Elastic.Transport.HttpMethod.POST, "_refresh") <= 2, "PatchAllAsync refreshes only around the batch read, never per document.");
    }

    [Fact]
    public Task ScriptPatchAsync_WithPipeline_MissingDocument_ThrowsDocumentNotFound()
    {
        // Arrange
        var nonExistentId = ObjectId.GenerateNewId().ToString();

        // Act & Assert
        return Assert.ThrowsAsync<DocumentNotFoundException>(
            () => _employeeRepository.PatchAsync(nonExistentId, new ScriptPatch("ctx._source.name = 'Patched';"), o => o.ImmediateConsistency()));
    }

    [Fact]
    public async Task ScriptPatchAsync_WithPipeline_NoopScript_ReportsNotModified()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());

        // Act — script sets ctx.op = 'noop' so nothing actually changes
        var modified = await _employeeRepository.PatchAsync(employee.Id, new ScriptPatch("ctx.op = 'noop';"), o => o.ImmediateConsistency());

        // Assert
        Assert.False(modified);
    }

    [Fact]
    public async Task PartialPatchAsync_WithPipeline_NestedObject_ReplacesTopLevel()
    {
        // Arrange — add employee with a name (simulates nested data at top level)
        var employee = EmployeeGenerator.Generate(name: "Original", companyName: "OriginalCo");
        employee = await _employeeRepository.AddAsync(employee, o => o.ImmediateConsistency());

        // Act — partial patch updates only name, not companyName
        await _employeeRepository.PatchAsync(employee.Id, new PartialPatch(new { name = "Updated" }), o => o.ImmediateConsistency());

        // Assert — name is updated, companyName preserved (top-level merge)
        var result = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.NotNull(result);
        Assert.Equal("updated", result.Name); // lowercased by pipeline
        Assert.Equal("OriginalCo", result.CompanyName);
    }

    [Theory]
    [InlineData(409, 0)]
    [InlineData(400, 10)]
    public async Task PartialPatchAllAsync_WithPipelineWhenBatchItemFails_ProcessesEveryBatchAndReportsFailure(int status, int retries)
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(12, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var invoker = new FaultInjectingRequestInvoker(new Elastic.Transport.HttpRequestInvoker());
        using var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log) { RequestInvoker = invoker };
        var repository = new EmployeeWithPipelineRepository(configuration.Employees, PipelineId);
        var updatedIds = new List<string>();
        invoker.FailFirstItemOfNextBulk(status, status is 409 ? "version_conflict_engine_exception" : "illegal_argument_exception", "injected failure");

        // Act
        Task Patch() => repository.PatchAllAsync(q => q.Company("1"), new PartialPatch(new { name = "PATCHED" }), o => o.PageLimit(5).Retry(retries).ImmediateConsistency().UpdateCallback(updatedIds.AddRange));
        var exception = status is 409
            ? await Assert.ThrowsAsync<VersionConflictDocumentException>(Patch)
            : await Assert.ThrowsAsync<DocumentException>(Patch);

        // Assert
        string failedId = Assert.Single(invoker.FailedBulkItemIds);
        Assert.Contains(failedId, exception.Message);
        Assert.Equal(3, invoker.CountRequests(Elastic.Transport.HttpMethod.POST, "_bulk"));
        Assert.Equal(11, updatedIds.Count);
        Assert.DoesNotContain(failedId, updatedIds);
        var results = await _employeeRepository.GetByIdsAsync(updatedIds);
        Assert.All(results, e => Assert.Equal("patched", e.Name));
    }

    [Fact]
    public async Task PartialPatchAllAsync_WithPipelineWhenBatchItemConflicts_RetriesAndPatchesAll()
    {
        // Arrange
        var employees = EmployeeGenerator.GenerateEmployees(12, companyId: "1");
        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var invoker = new FaultInjectingRequestInvoker(new Elastic.Transport.HttpRequestInvoker());
        using var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log) { RequestInvoker = invoker };
        var repository = new EmployeeWithPipelineRepository(configuration.Employees, PipelineId);
        invoker.FailFirstItemOfNextBulk(409, "version_conflict_engine_exception", "injected conflict");

        // Act
        long patched = await repository.PatchAllAsync(q => q.Company("1"), new PartialPatch(new { name = "PATCHED" }), o => o.PageLimit(5).ImmediateConsistency());

        // Assert
        Assert.Equal(12, patched);
        Assert.Equal(4, invoker.CountRequests(Elastic.Transport.HttpMethod.POST, "_bulk"));
        Assert.Equal(1, invoker.CountRequests(Elastic.Transport.HttpMethod.POST, "_mget"));
        var results = await _employeeRepository.GetByIdsAsync(employees.Select(e => e.Id).ToList());
        Assert.All(results, e => Assert.Equal("patched", e.Name));
    }

    public override async ValueTask DisposeAsync()
    {
        await _client.Ingest.DeletePipelineAsync(PipelineId);
        await base.DisposeAsync();
    }
}

internal class EmployeeWithPipelineRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeWithPipelineRepository(IIndex index, string pipelineId) : base(index)
    {
        DefaultPipeline = pipelineId;
    }
}
