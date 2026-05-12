using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Configuration;
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
