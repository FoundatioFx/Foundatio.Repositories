using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.AsyncEx;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ReindexTests : ElasticRepositoryTestBase
{
    public ReindexTests(ITestOutputHelper output) : base(output)
    {
        Log.SetLogLevel<EmployeeRepository>(LogLevel.Warning);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync(false);
    }

    [Fact(Skip = "This will only work if the mapping is manually updated.")]
    public async Task CanReindexSameIndexAsync()
    {
        var index = new EmployeeIndex(_configuration);
        await index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(index.Name)).Exists);

        IEmployeeRepository repository = new EmployeeRepository(_configuration);
        var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        var countResponse = await _client.CountAsync<Employee>();
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);

        var mappingResponse = await _client.Indices.GetMappingAsync<Employee>();
        _logger.LogRequest(mappingResponse);
        Assert.True(mappingResponse.IsValidResponse);
        Assert.NotNull(mappingResponse.Mappings.Values.FirstOrDefault()?.Mappings);

        var newIndex = new EmployeeIndexWithYearsEmployed(_configuration);
        await newIndex.ReindexAsync();

        countResponse = await _client.CountAsync<Employee>();
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);

        string version1Mappings = ToJson(mappingResponse.Mappings.Values.FirstOrDefault()?.Mappings);
        mappingResponse = await _client.Indices.GetMappingAsync<Employee>();
        _logger.LogRequest(mappingResponse);
        Assert.True(mappingResponse.IsValidResponse);
        Assert.NotNull(mappingResponse.Mappings.Values.FirstOrDefault()?.Mappings);
        Assert.NotEqual(version1Mappings, ToJson(mappingResponse.Mappings.Values.FirstOrDefault()?.Mappings));
    }

    [Fact]
    public async Task CanResumeReindexAsync()
    {
        const int numberOfEmployeesToCreate = 2000;

        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new VersionedEmployeeIndex(_configuration, 2);
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName)).Exists);

        IEmployeeRepository version1Repository = new EmployeeRepository(_configuration);
        await version1Repository.AddAsync(EmployeeGenerator.GenerateEmployees(numberOfEmployeesToCreate), o => o.ImmediateConsistency());

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.Name));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(numberOfEmployeesToCreate, countResponse.Count);
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version2Index.VersionedName)).Exists);

        // Throw error before second repass.
        await Assert.ThrowsAsync<ApplicationException>(async () => await version2Index.ReindexAsync((progress, message) =>
        {
            _logger.LogInformation("Reindex Progress {0}%: {1}", progress, message);
            if (progress == 91)
                throw new ApplicationException("Random Error");

            return Task.CompletedTask;
        }));

        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

        // Add a document and ensure it resumes from this document.
        await version1Repository.AddAsync(EmployeeGenerator.Generate(ObjectId.GenerateNewId(DateTime.UtcNow.AddMinutes(1)).ToString()), o => o.ImmediateConsistency());
        await version2Index.ReindexAsync();

        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name);
        Assert.True(aliasResponse.IsValidResponse);
        Assert.Single(aliasResponse.Aliases);
        Assert.Equal(version2Index.VersionedName, aliasResponse.Aliases.First().Key);

        Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(numberOfEmployeesToCreate + 1, countResponse.Count);

        Assert.False((await _client.Indices.ExistsAsync(version1Index.VersionedName)).Exists);
    }

    [Fact]
    public async Task CanHandleReindexFailureAsync()
    {
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new VersionedEmployeeIndex(_configuration, 2);
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName)).Exists);

        IEmployeeRepository version1Repository = new EmployeeRepository(_configuration);
        await version1Repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.Name));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        //Create invalid mappings
        var response = await _client.Indices.CreateAsync(version2Index.VersionedName, d => d.Mappings<Employee>(map => map
            .Dynamic(DynamicMapping.False)
            .Properties(p => p
                .IntegerNumber(e => e.Id)
            )));
        _logger.LogRequest(response);

        Assert.True((await _client.Indices.ExistsAsync(version2Index.VersionedName)).Exists);
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

        await version2Index.ReindexAsync();
        await version2Index.Configuration.Client.Indices.RefreshAsync(Indices.All);

        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name);
        Assert.True(aliasResponse.IsValidResponse);
        Assert.Single(aliasResponse.Aliases);
        Assert.True(aliasResponse.Aliases.ContainsKey(version1Index.VersionedName));

        // Verify indices exist
        var index1Exists = await _client.Indices.ExistsAsync(version1Index.VersionedName);
        Assert.True(index1Exists.Exists);
        var index2Exists = await _client.Indices.ExistsAsync(version2Index.VersionedName);
        Assert.True(index2Exists.Exists);
        var errorIndexExists = await _client.Indices.ExistsAsync($"{version2Index.VersionedName}-error");
        Assert.True(errorIndexExists.Exists);

        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.VersionedName));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(0, countResponse.Count);

        countResponse = await _client.CountAsync<object>(d => d.Indices($"{version2Index.VersionedName}-error"));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);
    }

    [Fact]
    public async Task CanReindexVersionedIndexAsync()
    {
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new VersionedEmployeeIndex(_configuration, 2);
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName)).Exists);

        var indexes = _client.GetIndicesPointingToAlias(version1Index.Name);
        Assert.Single(indexes);

        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version1Index.Name);
        _logger.LogRequest(aliasResponse);
        Assert.True(aliasResponse.IsValidResponse);
        Assert.Single(aliasResponse.Aliases);
        Assert.Equal(version1Index.VersionedName, aliasResponse.Aliases.First().Key);

        IEmployeeRepository version1Repository = new EmployeeRepository(_configuration);
        var employee = await version1Repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.Name));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);

        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version2Index.VersionedName)).Exists);

        // Make sure we can write to the index still. Should go to the old index until after the reindex is complete.
        IEmployeeRepository version2Repository = new EmployeeRepository(_configuration);
        await version2Repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.VersionedName));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(2, countResponse.Count);

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(0, countResponse.Count);

        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        // alias should still point to the old version until reindex
        aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name);
        Assert.True(aliasResponse.IsValidResponse);
        Assert.Single(aliasResponse.Aliases);
        Assert.Equal(version1Index.VersionedName, aliasResponse.Aliases.First().Key);

        await version2Index.ReindexAsync();

        aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name);
        Assert.True(aliasResponse.IsValidResponse);
        Assert.Single(aliasResponse.Aliases);
        Assert.Equal(version2Index.VersionedName, aliasResponse.Aliases.First().Key);

        Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(2, countResponse.Count);

        Assert.False((await _client.Indices.ExistsAsync(version1Index.VersionedName)).Exists);

        employee = await version2Repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.Name));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(3, countResponse.Count);
    }

    [Fact]
    public async Task CanReindexVersionedIndexWithCorrectMappingsAsync()
    {
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new VersionedEmployeeIndex(_configuration, 2) { DiscardIndexesOnReindex = false };
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        IEmployeeRepository version1Repository = new EmployeeRepository(_configuration);

        var utcNow = DateTime.UtcNow;
        var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();

        await version2Index.ReindexAsync();

        var existsResponse = await _client.Indices.ExistsAsync(version1Index.VersionedName);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        var mappingResponse = await _client.Indices.GetMappingAsync<Employee>(m => m.Indices(version1Index.VersionedName));
        _logger.LogRequest(mappingResponse);
        Assert.True(mappingResponse.IsValidResponse);
        var mappingsV1 = mappingResponse.Mappings[version1Index.VersionedName];
        Assert.NotNull(mappingsV1);

        existsResponse = await _client.Indices.ExistsAsync(version2Index.VersionedName);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);
        string version1Mappings = ToJson(mappingsV1);

        mappingResponse = await _client.Indices.GetMappingAsync<Employee>(m => m.Indices(version2Index.VersionedName));
        _logger.LogRequest(mappingResponse);
        Assert.True(mappingResponse.IsValidResponse);
        var mappingsV2 = mappingResponse.Mappings[version2Index.VersionedName];
        Assert.NotNull(mappingsV2);
        string version2Mappings = ToJson(mappingsV2);
        Assert.Equal(version1Mappings, version2Mappings);
    }

    [Fact]
    public async Task CanReindexVersionedIndexWithReindexScriptAsync()
    {
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        var version20Index = new VersionedEmployeeIndex(_configuration, 20) { DiscardIndexesOnReindex = false };
        await version20Index.DeleteAsync();

        var version21Index = new VersionedEmployeeIndex(_configuration, 21) { DiscardIndexesOnReindex = false };
        await version21Index.DeleteAsync();

        await using (new AsyncDisposableAction(() => version1Index.DeleteAsync()))
        {
            await version1Index.ConfigureAsync();
            IEmployeeRepository version1Repository = new EmployeeRepository(version1Index);

            var utcNow = DateTime.UtcNow;
            var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
            Assert.NotNull(employee?.Id);

            await using AsyncDisposableAction version20Scope = new(() => version20Index.DeleteAsync());
            await version20Index.ConfigureAsync();
            await version20Index.ReindexAsync();

            IEmployeeRepository version20Repository = new EmployeeRepository(version20Index);
            var result = await version20Repository.GetByIdAsync(employee.Id);
            Assert.Equal("scripted", result.CompanyName);

            await using AsyncDisposableAction version21Scope = new(() => version21Index.DeleteAsync());
            await version21Index.ConfigureAsync();
            await version21Index.ReindexAsync();

            IEmployeeRepository version21Repository = new EmployeeRepository(version21Index);
            result = await version21Repository.GetByIdAsync(employee.Id);
            Assert.Equal("typed script", result.CompanyName);
        }

        await using (new AsyncDisposableAction(() => version1Index.DeleteAsync()))
        {
            await version1Index.ConfigureAsync();
            IEmployeeRepository version1Repository = new EmployeeRepository(version1Index);

            var utcNow = DateTime.UtcNow;
            var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
            Assert.NotNull(employee?.Id);

            await using AsyncDisposableAction version21Scope = new(() => version21Index.DeleteAsync());
            await version21Index.ConfigureAsync();
            await version21Index.ReindexAsync();

            IEmployeeRepository version21Repository = new EmployeeRepository(version21Index);
            var result = await version21Repository.GetByIdAsync(employee.Id);
            Assert.Equal("typed script", result.CompanyName);
        }
    }

    [Fact]
    public async Task HandleFailureInReindexScriptAsync()
    {
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        var version22Index = new VersionedEmployeeIndex(_configuration, 22) { DiscardIndexesOnReindex = false };
        await version22Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        IEmployeeRepository version1Repository = new EmployeeRepository(_configuration);

        var utcNow = DateTime.UtcNow;
        var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        await using AsyncDisposableAction version22Scope = new(() => version22Index.DeleteAsync());
        await version22Index.ConfigureAsync();
        await version22Index.ReindexAsync();

        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version1Index.Name);
        Assert.True(aliasResponse.IsValidResponse);
        Assert.Single(aliasResponse.Aliases);
        Assert.Equal(version1Index.VersionedName, aliasResponse.Aliases.First().Key);
    }

    [Fact]
    public async Task CanReindexVersionedIndexWithDataInBothIndexesAsync()
    {
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new VersionedEmployeeIndex(_configuration, 2);
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName)).Exists);

        IEmployeeRepository version1Repository = new EmployeeRepository(_configuration);
        var employee = await version1Repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version2Index.VersionedName)).Exists);

        // swap the alias so we write to v1 and v2 and try to reindex.
        await _client.Indices.UpdateAliasesAsync(x => x.Actions(
            a => a.Remove(r => r.Alias(version1Index.Name).Index(version1Index.VersionedName)),
            a => a.Add(ad => ad.Alias(version2Index.Name).Index(version2Index.VersionedName))));

        IEmployeeRepository version2Repository = new EmployeeRepository(_configuration);
        await version2Repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.VersionedName));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);

        // swap back the alias
        await _client.Indices.UpdateAliasesAsync(x => x.Actions(
            a => a.Remove(r => r.Alias(version2Index.Name).Index(version2Index.VersionedName)),
            a => a.Add(ad => ad.Alias(version1Index.Name).Index(version1Index.VersionedName))));

        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        // alias should still point to the old version until reindex
        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name);
        Assert.True(aliasResponse.IsValidResponse);
        Assert.Single(aliasResponse.Aliases);
        Assert.Equal(version1Index.VersionedName, aliasResponse.Aliases.First().Key);

        await version2Index.ReindexAsync();

        aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name);
        Assert.True(aliasResponse.IsValidResponse);
        Assert.Single(aliasResponse.Aliases);
        Assert.Equal(version2Index.VersionedName, aliasResponse.Aliases.First().Key);

        Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        await _client.Indices.RefreshAsync(Indices.All);
        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(2, countResponse.Count);

        Assert.False((await _client.Indices.ExistsAsync(version1Index.VersionedName)).Exists);
    }

    [Fact]
    public async Task CanReindexVersionedIndexWithUpdatedDocsAsync()
    {
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new VersionedEmployeeIndex(_configuration, 2);
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName)).Exists);

        IEmployeeRepository repository = new EmployeeRepository(_configuration);
        var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version2Index.VersionedName)).Exists);
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        // alias should still point to the old version until reindex
        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name);
        _logger.LogRequest(aliasResponse);
        Assert.True(aliasResponse.IsValidResponse);
        Assert.Single(aliasResponse.Aliases);
        Assert.Equal(version1Index.VersionedName, aliasResponse.Aliases.First().Key);

        var countdown = new AsyncCountdownEvent(1);
        var reindexTask = version2Index.ReindexAsync(async (progress, message) =>
        {
            _logger.LogInformation("Reindex Progress {Progress}%: {Message}", progress, message);
            // Signal after any progress is made (reindex has started processing)
            if (progress > 0 && countdown.CurrentCount > 0)
            {
                countdown.Signal();
                await Task.Delay(1000);
            }
        });

        // Wait until the first reindex pass is done (with timeout to prevent hang).
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await countdown.WaitAsync(cts.Token);
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
        await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: DateTime.UtcNow));
        employee.Name = "Updated";
        await repository.SaveAsync(employee);

        // Resume after everythings been indexed.
        await reindexTask;
        aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name);
        Assert.True(aliasResponse.IsValidResponse);
        Assert.Single(aliasResponse.Aliases);
        Assert.Equal(version2Index.VersionedName, aliasResponse.Aliases.First().Key);

        Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        await _client.Indices.RefreshAsync(Indices.All);
        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(2, countResponse.Count);

        var result = await repository.GetByIdAsync(employee.Id);
        Assert.Equal(ToJson(employee), ToJson(result));
        Assert.False((await _client.Indices.ExistsAsync(version1Index.VersionedName)).Exists);
    }

    [Fact]
    public async Task CanReindexVersionedIndexWithDeletedDocsAsync()
    {
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new VersionedEmployeeIndex(_configuration, 2);
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName)).Exists);

        IEmployeeRepository repository = new EmployeeRepository(_configuration);
        var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version2Index.VersionedName)).Exists);
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        // alias should still point to the old version until reindex
        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name);
        _logger.LogRequest(aliasResponse);
        Assert.True(aliasResponse.IsValidResponse);
        Assert.Single(aliasResponse.Aliases);
        Assert.Equal(version1Index.VersionedName, aliasResponse.Aliases.First().Key);

        var countdown = new AsyncCountdownEvent(1);
        var reindexTask = version2Index.ReindexAsync(async (progress, message) =>
        {
            _logger.LogInformation("Reindex Progress {Progress}%: {Message}", progress, message);
            // Signal after any progress is made (reindex has started processing)
            if (progress > 0 && countdown.CurrentCount > 0)
            {
                countdown.Signal();
                await Task.Delay(1000);
            }
        });

        // Wait until the first reindex pass is done (with timeout to prevent hang).
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await countdown.WaitAsync(cts.Token);
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
        await repository.RemoveAllAsync(o => o.ImmediateConsistency());

        // Resume after everythings been indexed.
        await reindexTask;
        aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name);
        _logger.LogRequest(aliasResponse);
        Assert.True(aliasResponse.IsValidResponse, aliasResponse.GetErrorMessage());
        Assert.Single(aliasResponse.Aliases);
        Assert.Equal(version2Index.VersionedName, aliasResponse.Aliases.First().Key);

        Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.VersionedName));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.ApiCallDetails.HttpStatusCode == 404, countResponse.GetErrorMessage());
        Assert.Equal(0, countResponse.Count);

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName));
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse, countResponse.GetErrorMessage());
        Assert.Equal(1, countResponse.Count);

        Assert.Equal(employee, await repository.GetByIdAsync(employee.Id));
        Assert.False((await _client.Indices.ExistsAsync(version1Index.VersionedName)).Exists);
    }

    [Fact]
    public async Task CanReindexTimeSeriesIndexAsync()
    {
        var version1Index = new DailyEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new DailyEmployeeIndex(_configuration, 2);
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        IEmployeeRepository version1Repository = new EmployeeRepository(version1Index);

        var utcNow = DateTime.UtcNow;
        var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

        var aliasCountResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.Name));
        _logger.LogRequest(aliasCountResponse);
        Assert.True(aliasCountResponse.IsValidResponse);
        Assert.Equal(1, aliasCountResponse.Count);

        var indexCountResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.GetIndex(utcNow)));
        _logger.LogRequest(indexCountResponse);
        Assert.True(indexCountResponse.IsValidResponse);
        Assert.Equal(1, indexCountResponse.Count);

        indexCountResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.GetVersionedIndex(utcNow, 1)));
        _logger.LogRequest(indexCountResponse);
        Assert.True(indexCountResponse.IsValidResponse);
        Assert.Equal(1, indexCountResponse.Count);

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());
        IEmployeeRepository version2Repository = new EmployeeRepository(version2Index);

        // Make sure we write to the old index.
        await version2Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());

        aliasCountResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.Name));
        _logger.LogRequest(aliasCountResponse);
        Assert.True(aliasCountResponse.IsValidResponse);
        Assert.Equal(2, aliasCountResponse.Count);

        indexCountResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.GetVersionedIndex(utcNow, 1)));
        _logger.LogRequest(indexCountResponse);
        Assert.True(indexCountResponse.IsValidResponse);
        Assert.Equal(2, indexCountResponse.Count);

        var existsResponse = await _client.Indices.ExistsAsync(version2Index.GetVersionedIndex(utcNow, 2));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.False(existsResponse.Exists);

        // alias should still point to the old version until reindex
        var aliasesResponse = await _client.Indices.GetAliasAsync((Indices)version1Index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Equal(version1Index.GetVersionedIndex(employee.CreatedUtc, 1), aliasesResponse.Aliases.Single().Key);

        var aliases = aliasesResponse.Aliases.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeDailyAliases(version1Index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

        await version2Index.ReindexAsync();

        Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        aliasesResponse = await _client.Indices.GetAliasAsync((Indices)version1Index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse, aliasesResponse.GetErrorMessage());
        Assert.Equal(version1Index.GetVersionedIndex(employee.CreatedUtc, 2), aliasesResponse.Aliases.Single().Key);

        aliases = aliasesResponse.Aliases.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeDailyAliases(version1Index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

        existsResponse = await _client.Indices.ExistsAsync(version1Index.GetVersionedIndex(utcNow, 1));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.False(existsResponse.Exists);

        existsResponse = await _client.Indices.ExistsAsync(version2Index.GetVersionedIndex(utcNow, 2));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);
    }

    [Fact]
    public async Task CanReindexTimeSeriesIndexWithCorrectMappingsAsync()
    {
        var version1Index = new DailyEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new DailyEmployeeIndex(_configuration, 2) { DiscardIndexesOnReindex = false };
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        IEmployeeRepository version1Repository = new EmployeeRepository(version1Index);

        var utcNow = DateTime.UtcNow;
        var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();

        await version2Index.ReindexAsync();

        var existsResponse = await _client.Indices.ExistsAsync(version1Index.GetVersionedIndex(utcNow, 1));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        string indexV1 = version1Index.GetVersionedIndex(utcNow, 1);
        var mappingResponse = await _client.Indices.GetMappingAsync<Employee>(m => m.Indices(indexV1));
        _logger.LogRequest(mappingResponse);
        Assert.True(mappingResponse.IsValidResponse);
        var mappingsV1 = mappingResponse.Mappings[indexV1];
        Assert.NotNull(mappingsV1);
        string version1Mappings = ToJson(mappingsV1);

        string indexV2 = version2Index.GetVersionedIndex(utcNow, 2);
        existsResponse = await _client.Indices.ExistsAsync(indexV2);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        mappingResponse = await _client.Indices.GetMappingAsync<Employee>(m => m.Indices(indexV2));
        _logger.LogRequest(mappingResponse);
        Assert.True(mappingResponse.IsValidResponse);
        var mappingsV2 = mappingResponse.Mappings[indexV2];
        Assert.NotNull(mappingsV2);
        string version2Mappings = ToJson(mappingsV2);
        Assert.Equal(version1Mappings, version2Mappings);
    }

    private static string GetExpectedEmployeeDailyAliases(IIndex index, DateTime utcNow, DateTime indexDateUtc)
    {
        double totalDays = utcNow.Date.Subtract(indexDateUtc.Date).TotalDays;
        var aliases = new List<string> { index.Name, index.GetIndex(indexDateUtc) };
        if (totalDays <= 30)
            aliases.Add($"{index.Name}-last30days");
        if (totalDays <= 7)
            aliases.Add($"{index.Name}-last7days");
        if (totalDays <= 1)
            aliases.Add($"{index.Name}-today");

        aliases.Sort();
        return String.Join(", ", aliases);
    }

    private string ToJson(object data)
    {
        using var stream = new MemoryStream();
        _client.SourceSerializer.Serialize(data, stream);
        stream.Position = 0;
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
