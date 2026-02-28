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
    private readonly EmployeeWithPipelineRepository _repository;

    public PipelineTests(ITestOutputHelper output) : base(output)
    {
        _repository = new EmployeeWithPipelineRepository(_configuration.Employees, PipelineId);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await EnsurePipelineExistsAsync();
        await RemoveDataAsync();
    }

    private async Task EnsurePipelineExistsAsync()
    {
        var response = await _client.Ingest.PutPipelineAsync(PipelineId, p => p
            .Description("Lowercases the name field for pipeline tests")
            .Processors(pr => pr.Lowercase(l => l.Field("name"))));

        Assert.True(response.IsValidResponse, $"Failed to create pipeline: {response.ElasticsearchServerError?.Error?.Reason}");
    }

    [Fact]
    public async Task AddAsync_WithPipeline_TransformsDocument()
    {
        var employee = await _repository.AddAsync(EmployeeGenerator.Generate(name: "  BLAKE NIEMYJSKI  "), o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        var result = await _repository.GetByIdAsync(employee.Id);
        Assert.NotNull(result);
        Assert.Equal("  blake niemyjski  ", result.Name);
    }

    [Fact]
    public async Task AddCollectionAsync_WithPipeline_TransformsAllDocuments()
    {
        var employees = new List<Employee>
        {
            EmployeeGenerator.Generate(name: "BLAKE"),
            EmployeeGenerator.Generate(name: "JOHN DOE")
        };
        await _repository.AddAsync(employees, o => o.ImmediateConsistency());

        var results = await _repository.GetByIdsAsync(employees.Select(e => e.Id).ToList());
        Assert.Equal(2, results.Count);
        Assert.All(results, e => Assert.Equal(e.Name.ToLowerInvariant(), e.Name));
    }

    [Fact]
    public async Task SaveAsync_WithPipeline_TransformsDocument()
    {
        var employee = await _repository.AddAsync(EmployeeGenerator.Generate(name: "original"), o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        employee.Name = "UPDATED NAME";
        await _repository.SaveAsync(employee, o => o.ImmediateConsistency());

        var result = await _repository.GetByIdAsync(employee.Id);
        Assert.NotNull(result);
        Assert.Equal("updated name", result.Name);
    }

    [Fact]
    public async Task JsonPatchAsync_WithPipeline_TransformsDocument()
    {
        var employee = await _repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        var patch = new PatchDocument(new ReplaceOperation { Path = "name", Value = JsonValue.Create("PATCHED") });
        await _repository.PatchAsync(employee.Id, new JsonPatch(patch), o => o.ImmediateConsistency());

        var result = await _repository.GetByIdAsync(employee.Id);
        Assert.NotNull(result);
        Assert.Equal("patched", result.Name);
        Assert.Equal(EmployeeGenerator.Default.Age, result.Age);
    }

    [Fact]
    public async Task JsonPatchAllAsync_WithPipeline_TransformsAllDocuments()
    {
        var employees = new List<Employee>
        {
            EmployeeGenerator.Generate(companyId: "1", name: "employee1"),
            EmployeeGenerator.Generate(companyId: "1", name: "employee2"),
        };
        await _repository.AddAsync(employees, o => o.ImmediateConsistency());

        var patch = new PatchDocument(new ReplaceOperation { Path = "name", Value = JsonValue.Create("PATCHED") });
        await _repository.PatchAsync(employees.Select(e => e.Id).ToArray(), new JsonPatch(patch), o => o.ImmediateConsistency());

        var results = await _repository.GetByIdsAsync(employees.Select(e => e.Id).ToList());
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
