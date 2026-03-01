using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
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
    public async Task AddAsync()
    {
        // Arrange & Act
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(name: "  BLAKE  "), o => o.ImmediateConsistency());

        // Assert
        Assert.NotNull(employee?.Id);
        var result = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.Equal("  blake  ", result.Name);
    }

    [Fact]
    public async Task AddCollectionAsync()
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
        Assert.True(result.All(e => String.Equals(e.Name, e.Name.ToLowerInvariant())));
    }

    [Fact]
    public async Task SaveCollectionAsync()
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
        Assert.True(result.All(e => String.Equals(e.Name, e.Name.ToLowerInvariant())));
    }

    [Fact]
    public async Task JsonPatchAsync()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());

        // Act
        var patch = new PatchDocument(new ReplaceOperation { Path = "name", Value = JsonValue.Create("Patched") });
        await _employeeRepository.PatchAsync(employee.Id, new JsonPatch(patch), o => o.ImmediateConsistency());

        // Assert
        employee = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
        Assert.Equal("patched", employee.Name);
    }

    [Fact]
    public async Task JsonPatchAllAsync()
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

    [Fact(Skip = "ES Update API does not support ingest pipelines (elastic/elasticsearch#17895, closed won't-fix)")]
    public async Task PartialPatchAsync()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());

        // Act
        await _employeeRepository.PatchAsync(employee.Id, new PartialPatch(new { name = "Patched" }), o => o.ImmediateConsistency());

        // Assert
        employee = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
        Assert.Equal("patched", employee.Name);
    }

    [Fact(Skip = "ES Update API does not support ingest pipelines (elastic/elasticsearch#17895, closed won't-fix)")]
    public async Task PartialPatchAllAsync()
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

    [Fact(Skip = "ES Update API does not support ingest pipelines (elastic/elasticsearch#17895, closed won't-fix)")]
    public async Task ScriptPatchAsync()
    {
        // Arrange
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());

        // Act
        await _employeeRepository.PatchAsync(employee.Id, new ScriptPatch("ctx._source.name = 'Patched';"), o => o.ImmediateConsistency());

        // Assert
        employee = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
        Assert.Equal("patched", employee.Name);
    }

    [Fact(Skip = "ES Update API does not support ingest pipelines (elastic/elasticsearch#17895, closed won't-fix)")]
    public async Task ScriptPatchAllAsync()
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
