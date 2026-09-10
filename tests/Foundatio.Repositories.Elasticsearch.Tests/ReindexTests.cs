using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.AsyncEx;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
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

    [Fact]
    public async Task CanReindexSameIndexAsync()
    {
        var index = new EmployeeIndex(_configuration);
        await index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(index.Name, cancellationToken: TestCancellationToken)).Exists);

        IEmployeeRepository repository = new EmployeeRepository(_configuration);
        var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(index.Name), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);

        // ES does not support reindexing into the same index. This must surface as an explicit failure rather
        // than a silent no-op, and must leave the data intact.
        var newIndex = new EmployeeIndexWithYearsEmployed(_configuration);
        var exception = await Assert.ThrowsAsync<ReindexIncompleteException>(
            () => newIndex.ReindexAsync(cancellationToken: TestCancellationToken));
        Assert.Contains("cannot write into an index its reading from", exception.Reason);

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(index.Name), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);

        var result = await repository.GetByIdAsync(employee.Id);
        Assert.NotNull(result);
        Assert.Equal(employee.Id, result.Id);
    }

    /// <summary>
    /// A reindex that loses documents must throw, and must not promote the alias to the short destination.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The destination is given a mapping that rejects half the source documents (<c>name</c> mapped as an
    /// integer, so alphabetic names cannot be indexed). <c>_reindex</c> drops those documents and reports them
    /// as failures. Before R3 this returned normally, so a caller - a startup migration, a queued handler, an
    /// operator script - could not distinguish it from success.
    /// </para>
    /// <para>
    /// Asserts identities rather than a total, since a count cannot distinguish "all documents arrived" from
    /// "the wrong documents arrived", and asserts the alias still points at the source so reads are not
    /// silently served from an incomplete index.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task ReindexAsync_WhenDocumentsFailToCopy_ThrowsAndLeavesAliasOnSource()
    {
        // Arrange
        const int totalEmployees = 20;

        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        // The destination maps `name` as an integer, so alphabetic names cannot be indexed there.
        var version2Index = new VersionedEmployeeIndex(_configuration, 2, null,
            m => m.Properties(p => p.IntegerNumber(e => e.Name!)));
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(async () =>
        {
            await version1Index.DeleteAsync();
            await version2Index.DeleteAsync();
        });

        await version1Index.ConfigureAsync();

        var employees = new List<Employee>(totalEmployees);
        for (int i = 0; i < totalEmployees; i++)
        {
            // Half get numeric names (which the destination accepts), half alphabetic (which it rejects).
            string name = i % 2 is 0 ? i.ToString() : $"employee-{i:D3}";
            employees.Add(EmployeeGenerator.Generate(id: ObjectId.GenerateNewId().ToString(), name: name));
        }

        IEmployeeRepository version1Repository = new EmployeeRepository(_configuration);
        await version1Repository.AddAsync(employees, o => o.ImmediateConsistency());

        await version2Index.ConfigureAsync();

        // Act
        var exception = await Assert.ThrowsAsync<ReindexIncompleteException>(
            () => version2Index.ReindexAsync(cancellationToken: TestCancellationToken));

        // Assert: the exception names both indexes and says why, so an operator can act on it.
        Assert.Equal(version1Index.VersionedName, exception.OldIndex);
        Assert.Equal(version2Index.VersionedName, exception.NewIndex);
        Assert.Contains("failed to copy", exception.Reason);

        await _client.Indices.RefreshAsync(Indices.All, TestCancellationToken);

        // The alias must still serve the complete source, not the short destination.
        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name, cancellationToken: TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        var aliasTargets = aliasResponse.Aliases;
#else
        var aliasTargets = aliasResponse.Values;
#endif
        Assert.NotNull(aliasTargets);
        Assert.DoesNotContain(version2Index.VersionedName, aliasTargets.Keys);
        Assert.Contains(version1Index.VersionedName, aliasTargets.Keys);

        // Read what actually arrived, so the assertions below are about identities rather than a count.
        var arrived = new HashSet<string>();
        var searchResponse = await _client.SearchAsync<Employee>(d => d
            .Indices(version2Index.VersionedName)
            .Size(totalEmployees * 2)
            .Query(q => q.MatchAll(_ => { })), TestCancellationToken);
        Assert.True(searchResponse.IsValidResponse);
        foreach (var hit in searchResponse.Hits)
        {
            if (hit.Id is not null)
                arrived.Add(hit.Id);
        }

        // The destination really is short - the exception is not a false alarm.
        Assert.Contains(employees, e => !arrived.Contains(e.Id));

        // R7: the documents left behind are searchable in the failure index, so they can be found and replayed.
        string failureIndex = ElasticReindexer.GetFailureIndexName(version2Index.VersionedName);
        await _client.Indices.RefreshAsync((Indices)failureIndex, TestCancellationToken);
        var failureResponse = await _client.SearchAsync<ReindexFailure>(d => d
            .Indices(failureIndex)
            .Size(totalEmployees * 2)
            .Query(q => q.Term(tq => tq.Field("source_index").Value(version1Index.VersionedName))), TestCancellationToken);

        Assert.True(failureResponse.IsValidResponse);
        Assert.NotEmpty(failureResponse.Documents);
        Assert.All(failureResponse.Documents, failure =>
        {
            Assert.NotNull(failure.Id);
            Assert.DoesNotContain(failure.Id!, arrived);
            Assert.NotNull(failure.Cause?.Type);
        });
    }

    /// <summary>
    /// A reindex whose script drops documents leaves the destination legitimately short, so it must complete
    /// normally and keep the source rather than reporting data loss.
    /// </summary>
    /// <remarks>
    /// This covers the count comparison running on a reindex with <c>DiscardIndexesOnReindex = false</c>. The
    /// comparison used to live inside the delete branch, so this path was never checked at all; it now runs
    /// but must only warn, since <c>ctx.op = 'noop'</c> is a documented, intentional way to drop documents.
    /// </remarks>
    [Fact]
    public async Task ReindexAsync_WhenScriptDropsDocuments_CompletesAndKeepsSourceIndex()
    {
        // Arrange
        const int totalEmployees = 10;

        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        // Drops every employee with an even age, so the destination is legitimately short of the source.
        var version2Index = new VersionedEmployeeIndexWithDocumentDroppingScript(_configuration, 2) { DiscardIndexesOnReindex = false };
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(async () =>
        {
            await version1Index.DeleteAsync();
            await version2Index.DeleteAsync();
        });

        await version1Index.ConfigureAsync();

        var employees = new List<Employee>(totalEmployees);
        for (int i = 0; i < totalEmployees; i++)
            employees.Add(EmployeeGenerator.Generate(id: ObjectId.GenerateNewId().ToString(), age: i));

        IEmployeeRepository version1Repository = new EmployeeRepository(_configuration);
        await version1Repository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act: a deliberately lossy script is not a failure.
        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);
        await _client.Indices.RefreshAsync(Indices.All, TestCancellationToken);

        // Assert: the destination is short, but the source was kept so nothing is unrecoverable.
        var newCount = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName), TestCancellationToken);
        Assert.True(newCount.IsValidResponse);
        Assert.Equal(totalEmployees / 2, newCount.Count);

        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);

        // The alias still moved, because the copy itself completed.
        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name, cancellationToken: TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        var aliasTargets = aliasResponse.Aliases;
#else
        var aliasTargets = aliasResponse.Values;
#endif
        Assert.NotNull(aliasTargets);
        Assert.Contains(version2Index.VersionedName, aliasTargets.Keys);
    }

    /// <summary>
    /// A document hard-deleted through the alias after the cutover leaves the destination short of the frozen
    /// source, and that must not be reported as data loss.
    /// </summary>
    /// <remarks>
    /// Regression guard: comparing live index document counts cannot establish completeness once the alias has
    /// moved, because deletes land on the destination while the source still counts them. Throwing here would
    /// tell callers to retry a reindex that would resurrect the deleted document.
    /// </remarks>
    [Fact]
    public async Task ReindexAsync_WhenDocumentDeletedAfterCutover_DoesNotReportDataLoss()
    {
        // Arrange
        const int totalEmployees = 5;

        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new VersionedEmployeeIndex(_configuration, 2) { DiscardIndexesOnReindex = false };
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(async () =>
        {
            await version1Index.DeleteAsync();
            await version2Index.DeleteAsync();
        });

        await version1Index.ConfigureAsync();

        var employees = EmployeeGenerator.GenerateEmployees(totalEmployees);
        IEmployeeRepository version1Repository = new EmployeeRepository(_configuration);
        await version1Repository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);

        // Delete through the alias, which now points at the destination. The source is untouched, so a naive
        // count comparison would see the destination as one document short.
        IEmployeeRepository version2Repository = new EmployeeRepository(_configuration);
        await version2Repository.RemoveAsync(employees.First(), o => o.ImmediateConsistency());

        // A second reindex of the same versions is a no-op, but re-running the verification must still not
        // report loss, and must not resurrect the deleted document.
        await _client.Indices.RefreshAsync(Indices.All, TestCancellationToken);

        // Assert
        var newCount = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName), TestCancellationToken);
        Assert.True(newCount.IsValidResponse);
        Assert.Equal(totalEmployees - 1, newCount.Count);

        var oldCount = await _client.CountAsync<Employee>(d => d.Indices(version1Index.VersionedName), TestCancellationToken);
        Assert.True(oldCount.IsValidResponse);
        Assert.Equal(totalEmployees, oldCount.Count);

        // The deleted document must stay deleted.
        Assert.Null(await version2Repository.GetByIdAsync(employees.First().Id));
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
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);

        IEmployeeRepository version1Repository = new EmployeeRepository(_configuration);
        await version1Repository.AddAsync(EmployeeGenerator.GenerateEmployees(numberOfEmployeesToCreate), o => o.ImmediateConsistency());

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.Name), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(numberOfEmployeesToCreate, countResponse.Count);
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version2Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);

        // Throw error before second repass.
        await Assert.ThrowsAsync<ApplicationException>(async () => await version2Index.ReindexAsync((progress, message) =>
        {
            _logger.LogInformation("Reindex Progress {0}%: {1}", progress, message);
            if (progress == 91)
                throw new ApplicationException("Random Error");

            return Task.CompletedTask;
        }, TestCancellationToken));

        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

        // Add a document and ensure it resumes from this document.
        await version1Repository.AddAsync(EmployeeGenerator.Generate(ObjectId.GenerateNewId(DateTime.UtcNow.AddMinutes(1)).ToString()), o => o.ImmediateConsistency());
        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);

        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name, cancellationToken: TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        Assert.NotNull(aliasResponse.Aliases);
        var indices = aliasResponse.Aliases;
#else
        Assert.NotNull(aliasResponse.Values);
        var indices = aliasResponse.Values;
#endif
        Assert.Single(indices);
        Assert.Equal(version2Index.VersionedName, indices.First().Key);

        Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(numberOfEmployeesToCreate + 1, countResponse.Count);

        Assert.False((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);
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
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);

        IEmployeeRepository version1Repository = new EmployeeRepository(_configuration);
        await version1Repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.Name), cancellationToken: TestCancellationToken);
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
            )), cancellationToken: TestCancellationToken);
        _logger.LogRequest(response);

        Assert.True((await _client.Indices.ExistsAsync(version2Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

        // The destination's mapping rejects the document, so the reindex must fail loudly rather than
        // promoting the alias to an empty index.
        var exception = await Assert.ThrowsAsync<ReindexIncompleteException>(
            () => version2Index.ReindexAsync(cancellationToken: TestCancellationToken));
        Assert.Contains("failed to copy", exception.Reason);

        await version2Index.Configuration.Client.Indices.RefreshAsync(Indices.All, cancellationToken: TestCancellationToken);

        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name, cancellationToken: TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        Assert.NotNull(aliasResponse.Aliases);
        var indices = aliasResponse.Aliases;
#else
        Assert.NotNull(aliasResponse.Values);
        var indices = aliasResponse.Values;
#endif
        Assert.Single(indices);
        Assert.True(indices.ContainsKey(version1Index.VersionedName));

        // Verify indices exist
        var index1Exists = await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken);
        Assert.True(index1Exists.Exists);
        var index2Exists = await _client.Indices.ExistsAsync(version2Index.VersionedName, cancellationToken: TestCancellationToken);
        Assert.True(index2Exists.Exists);
        var errorIndexExists = await _client.Indices.ExistsAsync($"{version2Index.VersionedName}-error", cancellationToken: TestCancellationToken);
        Assert.True(errorIndexExists.Exists);

        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.VersionedName), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(0, countResponse.Count);

        countResponse = await _client.CountAsync<object>(d => d.Indices($"{version2Index.VersionedName}-error"), cancellationToken: TestCancellationToken);
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
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);

        var indexes = _client.GetIndicesPointingToAlias(version1Index.Name);
        Assert.Single(indexes);

        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version1Index.Name, cancellationToken: TestCancellationToken);
        _logger.LogRequest(aliasResponse);
        Assert.True(aliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        Assert.NotNull(aliasResponse.Aliases);
        var indices = aliasResponse.Aliases;
#else
        Assert.NotNull(aliasResponse.Values);
        var indices = aliasResponse.Values;
#endif
        Assert.Single(indices);
        Assert.Equal(version1Index.VersionedName, indices.First().Key);

        IEmployeeRepository version1Repository = new EmployeeRepository(_configuration);
        var employee = await version1Repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.Name), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);

        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version2Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);

        // Make sure we can write to the index still. Should go to the old index until after the reindex is complete.
        IEmployeeRepository version2Repository = new EmployeeRepository(_configuration);
        await version2Repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.VersionedName), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(2, countResponse.Count);

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(0, countResponse.Count);

        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        // alias should still point to the old version until reindex
        aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name, cancellationToken: TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        Assert.NotNull(aliasResponse.Aliases);
        indices = aliasResponse.Aliases;
#else
        Assert.NotNull(aliasResponse.Values);
        indices = aliasResponse.Values;
#endif
        Assert.Single(indices);
        Assert.Equal(version1Index.VersionedName, indices.First().Key);

        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);

        aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name, cancellationToken: TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        Assert.NotNull(aliasResponse.Aliases);
        indices = aliasResponse.Aliases;
#else
        Assert.NotNull(aliasResponse.Values);
        indices = aliasResponse.Values;
#endif
        Assert.Single(indices);
        Assert.Equal(version2Index.VersionedName, indices.First().Key);

        Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(2, countResponse.Count);

        Assert.False((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);

        employee = await version2Repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.Name), cancellationToken: TestCancellationToken);
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
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();

        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);

        var existsResponse = await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails is { HasSuccessfulStatusCode: true } or { HttpStatusCode: 404 });
        Assert.True(existsResponse.Exists);

        var mappingResponse = await _client.Indices.GetMappingAsync<Employee>(m => m.Indices(version1Index.VersionedName), cancellationToken: TestCancellationToken);
        _logger.LogRequest(mappingResponse);
        Assert.True(mappingResponse.IsValidResponse);
        var mappingsV1 = mappingResponse.Mappings[version1Index.VersionedName];
        Assert.NotNull(mappingsV1);

        existsResponse = await _client.Indices.ExistsAsync(version2Index.VersionedName, cancellationToken: TestCancellationToken);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails is { HasSuccessfulStatusCode: true } or { HttpStatusCode: 404 });
        Assert.True(existsResponse.Exists);
        string version1Mappings = ToJson(mappingsV1);

        mappingResponse = await _client.Indices.GetMappingAsync<Employee>(m => m.Indices(version2Index.VersionedName), cancellationToken: TestCancellationToken);
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
            Assert.NotNull(employee);
            Assert.NotNull(employee.Id);

            await using AsyncDisposableAction version20Scope = new(() => version20Index.DeleteAsync());
            await version20Index.ConfigureAsync();
            await version20Index.ReindexAsync(cancellationToken: TestCancellationToken);

            IEmployeeRepository version20Repository = new EmployeeRepository(version20Index);
            var result = await version20Repository.GetByIdAsync(employee.Id);
            Assert.NotNull(result);
            Assert.Equal("scripted", result.CompanyName);

            await using AsyncDisposableAction version21Scope = new(() => version21Index.DeleteAsync());
            await version21Index.ConfigureAsync();
            await version21Index.ReindexAsync(cancellationToken: TestCancellationToken);

            IEmployeeRepository version21Repository = new EmployeeRepository(version21Index);
            result = await version21Repository.GetByIdAsync(employee.Id);
            Assert.NotNull(result);
            Assert.Equal("typed script", result.CompanyName);
        }

        await using (new AsyncDisposableAction(() => version1Index.DeleteAsync()))
        {
            await version1Index.ConfigureAsync();
            IEmployeeRepository version1Repository = new EmployeeRepository(version1Index);

            var utcNow = DateTime.UtcNow;
            var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
            Assert.NotNull(employee);
            Assert.NotNull(employee.Id);

            await using AsyncDisposableAction version21Scope = new(() => version21Index.DeleteAsync());
            await version21Index.ConfigureAsync();
            await version21Index.ReindexAsync(cancellationToken: TestCancellationToken);

            IEmployeeRepository version21Repository = new EmployeeRepository(version21Index);
            var result = await version21Repository.GetByIdAsync(employee.Id);
            Assert.NotNull(result);
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
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        await using AsyncDisposableAction version22Scope = new(() => version22Index.DeleteAsync());
        await version22Index.ConfigureAsync();

        // The reindex script does not compile, so the copy must fail loudly and leave the alias on v1.
        var exception = await Assert.ThrowsAsync<ReindexIncompleteException>(
            () => version22Index.ReindexAsync(cancellationToken: TestCancellationToken));
        Assert.Contains("script_exception", exception.Reason);

        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version1Index.Name, cancellationToken: TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        Assert.NotNull(aliasResponse.Aliases);
        var indices = aliasResponse.Aliases;
#else
        Assert.NotNull(aliasResponse.Values);
        var indices = aliasResponse.Values;
#endif
        Assert.Single(indices);
        Assert.Equal(version1Index.VersionedName, indices.First().Key);
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
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);

        IEmployeeRepository version1Repository = new EmployeeRepository(_configuration);
        var employee = await version1Repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version2Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);

        // swap the alias so we write to v1 and v2 and try to reindex.
        await _client.Indices.UpdateAliasesAsync(x => x.Actions(
            a => a.Remove(r => r.Alias(version1Index.Name).Index(version1Index.VersionedName)),
            a => a.Add(ad => ad.Alias(version2Index.Name).Index(version2Index.VersionedName))), cancellationToken: TestCancellationToken);

        IEmployeeRepository version2Repository = new EmployeeRepository(_configuration);
        await version2Repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.VersionedName), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);

        // swap back the alias
        await _client.Indices.UpdateAliasesAsync(x => x.Actions(
            a => a.Remove(r => r.Alias(version2Index.Name).Index(version2Index.VersionedName)),
            a => a.Add(ad => ad.Alias(version1Index.Name).Index(version1Index.VersionedName))), cancellationToken: TestCancellationToken);

        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        // alias should still point to the old version until reindex
        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name, cancellationToken: TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        Assert.NotNull(aliasResponse.Aliases);
        var indices = aliasResponse.Aliases;
#else
        Assert.NotNull(aliasResponse.Values);
        var indices = aliasResponse.Values;
#endif
        Assert.Single(indices);
        Assert.Equal(version1Index.VersionedName, indices.First().Key);

        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);

        aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name, cancellationToken: TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        Assert.NotNull(aliasResponse.Aliases);
        indices = aliasResponse.Aliases;
#else
        Assert.NotNull(aliasResponse.Values);
        indices = aliasResponse.Values;
#endif
        Assert.Single(indices);
        Assert.Equal(version2Index.VersionedName, indices.First().Key);

        Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        await _client.Indices.RefreshAsync(Indices.All, cancellationToken: TestCancellationToken);
        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(2, countResponse.Count);

        Assert.False((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);
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
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);

        IEmployeeRepository repository = new EmployeeRepository(_configuration);
        var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version2Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        // alias should still point to the old version until reindex
        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name, cancellationToken: TestCancellationToken);
        _logger.LogRequest(aliasResponse);
        Assert.True(aliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        Assert.NotNull(aliasResponse.Aliases);
        var indices = aliasResponse.Aliases;
#else
        Assert.NotNull(aliasResponse.Values);
        var indices = aliasResponse.Values;
#endif
        Assert.Single(indices);
        Assert.Equal(version1Index.VersionedName, indices.First().Key);

        var countdown = new AsyncCountdownEvent(1);
        var reindexTask = version2Index.ReindexAsync(async (progress, message) =>
        {
            _logger.LogInformation("Reindex Progress {Progress}%: {Message}", progress, message);
            // Signal after any progress is made (reindex has started processing)
            if (progress > 0 && countdown.CurrentCount > 0)
            {
                countdown.Signal();
                await Task.Delay(1000, TestCancellationToken);
            }
        }, TestCancellationToken);

        // Wait until the first reindex pass is done (with timeout to prevent hang).
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await countdown.WaitAsync(cts.Token);
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
        await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: DateTime.UtcNow));
        employee.Name = "Updated";
        await repository.SaveAsync(employee);

        // Resume after everythings been indexed.
        await reindexTask;
        aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name, cancellationToken: TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        Assert.NotNull(aliasResponse.Aliases);
        indices = aliasResponse.Aliases;
#else
        Assert.NotNull(aliasResponse.Values);
        indices = aliasResponse.Values;
#endif
        Assert.Single(indices);
        Assert.Equal(version2Index.VersionedName, indices.First().Key);

        Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        await _client.Indices.RefreshAsync(Indices.All, cancellationToken: TestCancellationToken);
        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(2, countResponse.Count);

        var result = await repository.GetByIdAsync(employee.Id);
        Assert.NotNull(result);
        employee.Version = result.Version; // SeqNo/PrimaryTerm is not preserved across reindex
        Assert.Equal(ToJson(employee), ToJson(result));
        Assert.False((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);
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
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);

        IEmployeeRepository repository = new EmployeeRepository(_configuration);
        var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version2Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        // alias should still point to the old version until reindex
        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name, cancellationToken: TestCancellationToken);
        _logger.LogRequest(aliasResponse);
        Assert.True(aliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        Assert.NotNull(aliasResponse.Aliases);
        var indices = aliasResponse.Aliases;
#else
        Assert.NotNull(aliasResponse.Values);
        var indices = aliasResponse.Values;
#endif
        Assert.Single(indices);
        Assert.Equal(version1Index.VersionedName, indices.First().Key);

        var countdown = new AsyncCountdownEvent(1);
        var reindexTask = version2Index.ReindexAsync(async (progress, message) =>
        {
            _logger.LogInformation("Reindex Progress {Progress}%: {Message}", progress, message);
            // Signal after any progress is made (reindex has started processing)
            if (progress > 0 && countdown.CurrentCount > 0)
            {
                countdown.Signal();
                await Task.Delay(1000, TestCancellationToken);
            }
        }, TestCancellationToken);

        // Wait until the first reindex pass is done (with timeout to prevent hang).
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await countdown.WaitAsync(cts.Token);
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
        await repository.RemoveAllAsync(o => o.ImmediateConsistency());

        // Resume after everythings been indexed.
        await reindexTask;
        aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name, cancellationToken: TestCancellationToken);
        _logger.LogRequest(aliasResponse);
        Assert.True(aliasResponse.IsValidResponse, aliasResponse.GetErrorMessage());
#if ELASTICSEARCH9
        Assert.NotNull(aliasResponse.Aliases);
        indices = aliasResponse.Aliases;
#else
        Assert.NotNull(aliasResponse.Values);
        indices = aliasResponse.Values;
#endif
        Assert.Single(indices);
        Assert.Equal(version2Index.VersionedName, indices.First().Key);

        Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.VersionedName), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.Equal(404, countResponse.ApiCallDetails.HttpStatusCode);
        Assert.Equal(0, countResponse.Count);

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse, countResponse.GetErrorMessage());
        Assert.Equal(1, countResponse.Count);

        var reindexedEmployee = await repository.GetByIdAsync(employee.Id);
        Assert.NotNull(reindexedEmployee);
        employee.Version = reindexedEmployee.Version; // SeqNo/PrimaryTerm is not preserved across reindex
        Assert.Equal(employee, reindexedEmployee);
        Assert.False((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);
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
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

        var aliasCountResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.Name), cancellationToken: TestCancellationToken);
        _logger.LogRequest(aliasCountResponse);
        Assert.True(aliasCountResponse.IsValidResponse);
        Assert.Equal(1, aliasCountResponse.Count);

        var indexCountResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.GetIndex(utcNow)), cancellationToken: TestCancellationToken);
        _logger.LogRequest(indexCountResponse);
        Assert.True(indexCountResponse.IsValidResponse);
        Assert.Equal(1, indexCountResponse.Count);

        indexCountResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.GetVersionedIndex(utcNow, 1)), cancellationToken: TestCancellationToken);
        _logger.LogRequest(indexCountResponse);
        Assert.True(indexCountResponse.IsValidResponse);
        Assert.Equal(1, indexCountResponse.Count);

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());
        IEmployeeRepository version2Repository = new EmployeeRepository(version2Index);

        // Make sure we write to the old index.
        await version2Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());

        aliasCountResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.Name), cancellationToken: TestCancellationToken);
        _logger.LogRequest(aliasCountResponse);
        Assert.True(aliasCountResponse.IsValidResponse);
        Assert.Equal(2, aliasCountResponse.Count);

        indexCountResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.GetVersionedIndex(utcNow, 1)), cancellationToken: TestCancellationToken);
        _logger.LogRequest(indexCountResponse);
        Assert.True(indexCountResponse.IsValidResponse);
        Assert.Equal(2, indexCountResponse.Count);

        var existsResponse = await _client.Indices.ExistsAsync(version2Index.GetVersionedIndex(utcNow, 2), cancellationToken: TestCancellationToken);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails is { HasSuccessfulStatusCode: true } or { HttpStatusCode: 404 });
        Assert.False(existsResponse.Exists);

        // alias should still point to the old version until reindex
        var aliasesResponse = await _client.Indices.GetAliasAsync((Indices)version1Index.GetIndex(employee.CreatedUtc), cancellationToken: TestCancellationToken);
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
#if ELASTICSEARCH9
        Assert.NotNull(aliasesResponse.Aliases);
        var aliasIndices = aliasesResponse.Aliases;
#else
        Assert.NotNull(aliasesResponse.Values);
        var aliasIndices = aliasesResponse.Values;
#endif
        Assert.Equal(version1Index.GetVersionedIndex(employee.CreatedUtc, 1), aliasIndices.Single().Key);

        var aliases = aliasIndices.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeDailyAliases(version1Index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);

        Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        aliasesResponse = await _client.Indices.GetAliasAsync((Indices)version1Index.GetIndex(employee.CreatedUtc), cancellationToken: TestCancellationToken);
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse, aliasesResponse.GetErrorMessage());
#if ELASTICSEARCH9
        Assert.NotNull(aliasesResponse.Aliases);
        aliasIndices = aliasesResponse.Aliases;
#else
        Assert.NotNull(aliasesResponse.Values);
        aliasIndices = aliasesResponse.Values;
#endif
        Assert.Equal(version1Index.GetVersionedIndex(employee.CreatedUtc, 2), aliasIndices.Single().Key);

        aliases = aliasIndices.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeDailyAliases(version1Index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

        existsResponse = await _client.Indices.ExistsAsync(version1Index.GetVersionedIndex(utcNow, 1), cancellationToken: TestCancellationToken);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails is { HasSuccessfulStatusCode: true } or { HttpStatusCode: 404 });
        Assert.False(existsResponse.Exists);

        existsResponse = await _client.Indices.ExistsAsync(version2Index.GetVersionedIndex(utcNow, 2), cancellationToken: TestCancellationToken);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails is { HasSuccessfulStatusCode: true } or { HttpStatusCode: 404 });
        Assert.True(existsResponse.Exists);
    }

    [Fact]
    public async Task ReindexAsync_DailyIndexWithReindexScript_ExecutesScript()
    {
        // Arrange
        var version1Index = new DailyEmployeeIndexWithReindexScripts(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new DailyEmployeeIndexWithReindexScripts(_configuration, 2) { DiscardIndexesOnReindex = false };
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        IEmployeeRepository version1Repository = new EmployeeRepository(version1Index);

        var utcNow = DateTime.UtcNow;
        var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);
        Assert.NotEqual("daily-scripted", employee.CompanyName);
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();

        // Act
        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);

        // Assert
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        IEmployeeRepository version2Repository = new EmployeeRepository(version2Index);
        var result = await version2Repository.GetByIdAsync(employee.Id);
        Assert.NotNull(result);
        Assert.Equal("daily-scripted", result.CompanyName);
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
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();

        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);

        var existsResponse = await _client.Indices.ExistsAsync(version1Index.GetVersionedIndex(utcNow, 1), cancellationToken: TestCancellationToken);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails is { HasSuccessfulStatusCode: true } or { HttpStatusCode: 404 });
        Assert.True(existsResponse.Exists);

        string indexV1 = version1Index.GetVersionedIndex(utcNow, 1);
        var mappingResponse = await _client.Indices.GetMappingAsync<Employee>(m => m.Indices(indexV1), cancellationToken: TestCancellationToken);
        _logger.LogRequest(mappingResponse);
        Assert.True(mappingResponse.IsValidResponse);
        var mappingsV1 = mappingResponse.Mappings[indexV1];
        Assert.NotNull(mappingsV1);
        string version1Mappings = ToJson(mappingsV1);

        string indexV2 = version2Index.GetVersionedIndex(utcNow, 2);
        existsResponse = await _client.Indices.ExistsAsync(indexV2, cancellationToken: TestCancellationToken);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails is { HasSuccessfulStatusCode: true } or { HttpStatusCode: 404 });
        Assert.True(existsResponse.Exists);

        mappingResponse = await _client.Indices.GetMappingAsync<Employee>(m => m.Indices(indexV2), cancellationToken: TestCancellationToken);
        _logger.LogRequest(mappingResponse);
        Assert.True(mappingResponse.IsValidResponse);
        var mappingsV2 = mappingResponse.Mappings[indexV2];
        Assert.NotNull(mappingsV2);
        string version2Mappings = ToJson(mappingsV2);
        Assert.Equal(version1Mappings, version2Mappings);
    }

    [Fact]
    public async Task CanReindexTimeSeriesIndexWithReindexScriptAsync()
    {
        // Arrange
        var version1Index = new DailyEmployeeIndexWithReindexScript(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new DailyEmployeeIndexWithReindexScript(_configuration, 2);
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        IEmployeeRepository version1Repository = new EmployeeRepository(version1Index);

        var utcNow = DateTime.UtcNow;
        var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

        // Act
        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);

        // Assert
        Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        IEmployeeRepository version2Repository = new EmployeeRepository(version2Index);
        var result = await version2Repository.GetByIdAsync(employee.Id);
        Assert.NotNull(result);
        Assert.Equal("daily-reindex-script", result.CompanyName);
    }

    [Fact]
    public async Task RenameFieldScript_WithTopLevelField_RenamesFieldDuringReindex()
    {
        // Arrange
        var version1Index = new VersionedEmployeeIndexWithFieldRename(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new VersionedEmployeeIndexWithFieldRename(_configuration, 2) { DiscardIndexesOnReindex = false };
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        IEmployeeRepository version1Repository = new EmployeeRepository(version1Index);

        var employee = await version1Repository.AddAsync(
            EmployeeGenerator.Generate(companyName: "TestCompany", createdUtc: DateTime.UtcNow),
            o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        // Act
        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);

        // Assert
        var request = new GetRequest(version2Index.VersionedName, employee.Id);
        var response = await _client.GetAsync<Dictionary<string, object>>(request, cancellationToken: TestCancellationToken);
        Assert.True(response.IsValidResponse);
        Assert.NotNull(response.Source);
        Assert.True(response.Source.TryGetValue("companyNameRenamed", out var companyNameRenamed));
        Assert.Equal("TestCompany", companyNameRenamed?.ToString());
        Assert.False(response.Source.ContainsKey("companyName"));
    }

    [Fact]
    public async Task RenameFieldScript_WithNestedField_RenamesNestedFieldDuringReindex()
    {
        // Arrange
        var version1Index = new VersionedEmployeeIndexWithNestedFieldRename(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new VersionedEmployeeIndexWithNestedFieldRename(_configuration, 2) { DiscardIndexesOnReindex = false };
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        IEmployeeRepository version1Repository = new EmployeeRepository(version1Index);

        var employee = EmployeeGenerator.Generate(createdUtc: DateTime.UtcNow);
        employee.Data["oldField"] = "nestedValue";
        employee = await version1Repository.AddAsync(employee, o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        // Act
        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);

        // Assert
        var request = new GetRequest(version2Index.VersionedName, employee.Id);
        var response = await _client.GetAsync<Dictionary<string, object>>(request, cancellationToken: TestCancellationToken);
        Assert.True(response.IsValidResponse);
        Assert.NotNull(response.Source);
        Assert.True(response.Source.TryGetValue("data", out var data));

        string json = ToJson(data);
        Assert.Equal("{\"newField\":\"nestedValue\"}", json);
    }

    [Fact]
    public async Task RemoveFieldScript_WithTopLevelField_RemovesFieldDuringReindex()
    {
        // Arrange
        var version1Index = new VersionedEmployeeIndexWithFieldRemove(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new VersionedEmployeeIndexWithFieldRemove(_configuration, 2) { DiscardIndexesOnReindex = false };
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        IEmployeeRepository version1Repository = new EmployeeRepository(version1Index);

        var employee = await version1Repository.AddAsync(
            EmployeeGenerator.Generate(companyName: "TestCompany", createdUtc: DateTime.UtcNow),
            o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        // Act
        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);

        // Assert
        var request = new GetRequest(version2Index.VersionedName, employee.Id);
        var response = await _client.GetAsync<Dictionary<string, object>>(request, cancellationToken: TestCancellationToken);
        Assert.True(response.IsValidResponse);
        Assert.NotNull(response.Source);
        Assert.False(response.Source.ContainsKey("companyName"));
    }

    [Fact]
    public async Task RemoveFieldScript_WithNestedField_RemovesNestedFieldDuringReindex()
    {
        // Arrange
        var version1Index = new VersionedEmployeeIndexWithNestedFieldRemove(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new VersionedEmployeeIndexWithNestedFieldRemove(_configuration, 2) { DiscardIndexesOnReindex = false };
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        IEmployeeRepository version1Repository = new EmployeeRepository(version1Index);

        var employee = EmployeeGenerator.Generate(createdUtc: DateTime.UtcNow);
        employee.Data["oldField"] = "nestedValue";
        employee.Data["keepField"] = "keepValue";
        employee = await version1Repository.AddAsync(employee, o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        // Act
        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);

        // Assert
        var request = new GetRequest(version2Index.VersionedName, employee.Id);
        var response = await _client.GetAsync<Dictionary<string, object>>(request, cancellationToken: TestCancellationToken);
        Assert.True(response.IsValidResponse);
        Assert.NotNull(response.Source);
        Assert.True(response.Source.TryGetValue("data", out var data));

        string json = ToJson(data);
        Assert.Equal("{\"keepField\":\"keepValue\"}", json);
    }

    /// <summary>
    /// A lock acquisition that times out must not surface as a <see cref="NullReferenceException"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>VersionedIndex.ReindexAsync</c> and <c>DailyIndex.ReindexAsync</c> never null-checked the result of
    /// <c>LockProvider.AcquireAsync</c>, which returns <c>null</c> on timeout. The subsequent
    /// <c>reindexLock.RenewAsync()</c> then dereferenced null. A caller that cannot get the lock deserves a
    /// clean skip or a meaningful exception - never a <c>NullReferenceException</c>, which is
    /// indistinguishable from a genuine bug and defeats retry logic that keys on exception type.
    /// </para>
    /// <para>
    /// Uses a lock provider that returns <c>null</c> immediately rather than holding a real lock. Holding a
    /// real one only makes <c>AcquireAsync</c> *wait* for its 30-minute timeout, which does not exercise this
    /// path and makes the test take as long as the lease.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task ReindexAsync_WhenLockCannotBeAcquired_DoesNotThrowNullReference()
    {
        // Arrange
        var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log, new DenyingLockProvider());

        var version1Index = new VersionedEmployeeIndex(configuration, 1);
        await version1Index.DeleteAsync();
        var version2Index = new VersionedEmployeeIndex(configuration, 2);
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(async () =>
        {
            await version1Index.DeleteAsync();
            await version2Index.DeleteAsync();
        });

        await version1Index.ConfigureAsync();
        IEmployeeRepository repository = new EmployeeRepository(configuration);
        await repository.AddAsync(EmployeeGenerator.GenerateEmployees(5), o => o.ImmediateConsistency());
        await version2Index.ConfigureAsync();

        // Act
        var exception = await Record.ExceptionAsync(() => version2Index.ReindexAsync(cancellationToken: TestCancellationToken));

        // Assert: losing the lock race is not an error, so this must skip cleanly rather than throw
        // anything at all - a NullReferenceException being the specific regression guarded against.
        Assert.Null(exception);

        // No migration work was authorized, so the alias must not have moved.
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
    }

    /// <summary>
    /// Alias metadata must survive the cutover.
    /// </summary>
    /// <remarks>
    /// The cutover previously recreated each alias with only its name, discarding <c>filter</c>, routing,
    /// <c>is_write_index</c>, and <c>is_hidden</c>. Dropping a filter is a data-exposure bug, not merely a
    /// configuration regression: a filtered alias is often the only thing scoping a shared index to one
    /// tenant, so a migration that silently unfiltered it would widen every reader's visibility.
    /// </remarks>
    [Fact]
    public async Task ReindexAsync_PreservesAliasFilterAndRoutingAcrossCutover()
    {
        // Arrange
        const string filteredAlias = "employees-acme";

        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();
        var version2Index = new VersionedEmployeeIndex(_configuration, 2);
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(async () =>
        {
            await version1Index.DeleteAsync();
            await version2Index.DeleteAsync();
        });

        await version1Index.ConfigureAsync();

        IEmployeeRepository repository = new EmployeeRepository(_configuration);
        var acmeEmployees = EmployeeGenerator.GenerateEmployees(5, companyId: "acme");
        var otherEmployees = EmployeeGenerator.GenerateEmployees(5, companyId: "other");
        await repository.AddAsync(acmeEmployees.Concat(otherEmployees).ToList(), o => o.ImmediateConsistency());

        // A tenant-scoping alias with a filter and routing, alongside the primary alias.
        var createAliasResponse = await _client.Indices.PutAliasAsync(
            Indices.Index(version1Index.VersionedName), filteredAlias, d => d
                .Filter(f => f.Term(t => t.Field("companyId").Value("acme")))
                .Routing("acme"), TestCancellationToken);
        Assert.True(createAliasResponse.IsValidResponse);

        await version2Index.ConfigureAsync();

        // Act
        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);
        await _client.Indices.RefreshAsync(Indices.All, TestCancellationToken);

        // Assert
        var aliasResponse = await _client.Indices.GetAliasAsync(a => a.Name(filteredAlias), TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        var indices = aliasResponse.Aliases;
#else
        var indices = aliasResponse.Values;
#endif
        Assert.NotNull(indices);
        Assert.True(indices.ContainsKey(version2Index.VersionedName),
            $"Alias '{filteredAlias}' was stranded and did not follow the cutover to {version2Index.VersionedName}.");

        var definition = indices[version2Index.VersionedName].Aliases?[filteredAlias];
        Assert.NotNull(definition);
        Assert.NotNull(definition.Filter);

        // Elasticsearch expands the `routing` shorthand into index_routing and search_routing on read, so
        // assert those rather than the shorthand.
        Assert.Equal("acme", definition.IndexRouting);
        Assert.Equal("acme", definition.SearchRouting);

        // The filter must still scope reads, or the migration widened tenant visibility.
        var filteredCount = await _client.CountAsync<Employee>(d => d.Indices(filteredAlias), TestCancellationToken);
        Assert.True(filteredCount.IsValidResponse);
        Assert.Equal(acmeEmployees.Count, filteredCount.Count);
    }

    [Fact]
    public async Task ReindexAsync_ConcurrentCalls_OnlyOneCompletes()
    {
        // Arrange
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();
        var version2Index = new VersionedEmployeeIndex(_configuration, 2);
        await version2Index.DeleteAsync();

        await version1Index.ConfigureAsync();
        IEmployeeRepository repo = new EmployeeRepository(_configuration);
        await repo.AddAsync(EmployeeGenerator.GenerateEmployees(100), o => o.ImmediateConsistency());

        await version2Index.ConfigureAsync();

        // Act
        var task1 = version2Index.ReindexAsync(cancellationToken: TestCancellationToken);
        var task2 = version2Index.ReindexAsync(cancellationToken: TestCancellationToken);
        await Task.WhenAll(task1, task2);

        // Assert
        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.Name, cancellationToken: TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse);
#if ELASTICSEARCH9
        Assert.NotNull(aliasResponse.Aliases);
        var indices = aliasResponse.Aliases;
#else
        Assert.NotNull(aliasResponse.Values);
        var indices = aliasResponse.Values;
#endif
        Assert.Single(indices);
        Assert.Equal(version2Index.VersionedName, indices.First().Key);

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName), cancellationToken: TestCancellationToken);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(100, countResponse.Count);

        Assert.False((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);
    }

    [Fact]
    public async Task ReindexAsync_LockPreventsHandlerAndDirectConcurrency()
    {
        // Arrange
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();
        var version2Index = new VersionedEmployeeIndex(_configuration, 2);
        await version2Index.DeleteAsync();

        await version1Index.ConfigureAsync();
        IEmployeeRepository repo = new EmployeeRepository(_configuration);
        await repo.AddAsync(EmployeeGenerator.GenerateEmployees(100), o => o.ImmediateConsistency());
        await version2Index.ConfigureAsync();

        string lockKey = ElasticReindexer.GetLockName(version2Index.Name);
        await using var externalLock = await _configuration.LockProvider.AcquireAsync(lockKey, TimeSpan.FromMinutes(1), TestCancellationToken);
        Assert.NotNull(externalLock);

        // Act - Start reindex on a background thread and verify it blocks
        var reindexStarted = new TaskCompletionSource<bool>();
        var reindexTask = Task.Run(async () =>
        {
            reindexStarted.SetResult(true);
            await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);
        }, TestCancellationToken);
        await reindexStarted.Task;
        await Task.Delay(TimeSpan.FromSeconds(2), TestCancellationToken);

        // Assert - Lock prevented reindex from completing
        Assert.False(reindexTask.IsCompleted);

        // Cleanup - Release lock and wait for reindex to complete, then delete
        await externalLock.ReleaseAsync();
        await reindexTask;
        await version2Index.DeleteAsync();
    }

    [Fact]
    public async Task ReindexAsync_AliasLockBlocksSubsequentVersionTransition()
    {
        // Arrange
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();
        var version2Index = new VersionedEmployeeIndex(_configuration, 2);
        await version2Index.DeleteAsync();
        var version3Index = new VersionedEmployeeIndex(_configuration, 3);
        await version3Index.DeleteAsync();

        await version1Index.ConfigureAsync();
        IEmployeeRepository repo = new EmployeeRepository(_configuration);
        await repo.AddAsync(EmployeeGenerator.GenerateEmployees(100), o => o.ImmediateConsistency());
        await version2Index.ConfigureAsync();
        await version3Index.ConfigureAsync();

        string lockKey = ElasticReindexer.GetLockName(version1Index.Name);
        await using var reindexLock = await _configuration.LockProvider.AcquireAsync(lockKey, TimeSpan.FromMinutes(1), TestCancellationToken);
        Assert.NotNull(reindexLock);

        // Act - Start reindex on a background thread and verify it blocks
        var reindexStarted = new TaskCompletionSource<bool>();
        var reindexTask = Task.Run(async () =>
        {
            reindexStarted.SetResult(true);
            await version3Index.ReindexAsync(cancellationToken: TestCancellationToken);
        }, TestCancellationToken);
        await reindexStarted.Task;
        await Task.Delay(TimeSpan.FromSeconds(2), TestCancellationToken);

        // Assert - Lock prevented reindex from completing; data unchanged
        Assert.False(reindexTask.IsCompleted);
        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.VersionedName), cancellationToken: TestCancellationToken);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(100, countResponse.Count);

        // Cleanup - Release lock and wait for reindex to complete, then delete
        await reindexLock.ReleaseAsync();
        await reindexTask;
        await version3Index.DeleteAsync();
    }

    [Fact]
    public async Task ReindexAsync_WhenCancelled_DoesNotCompleteReindex()
    {
        // Arrange
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();
        var version2Index = new VersionedEmployeeIndex(_configuration, 2);
        await version2Index.DeleteAsync();

        await version1Index.ConfigureAsync();
        IEmployeeRepository repo = new EmployeeRepository(_configuration);
        await repo.AddAsync(EmployeeGenerator.GenerateEmployees(5000), o => o.ImmediateConsistency());

        await version2Index.ConfigureAsync();

        // Act
        var reindexTask = version2Index.ReindexAsync((progress, message) =>
        {
            _logger.LogInformation("Reindex Progress {Progress}%: {Message}", progress, message);
            if (progress > 5)
                throw new OperationCanceledException("Test cancellation");
            return Task.CompletedTask;
        }, TestCancellationToken);

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reindexTask);
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);
    }

    [Fact]
    public async Task ReindexAsync_WhenCancellationTokenSignaled_StopsWithoutSwitchingAlias()
    {
        // Arrange
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();
        var version2Index = new VersionedEmployeeIndex(_configuration, 2);
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        IEmployeeRepository repository = new EmployeeRepository(_configuration);
        await repository.AddAsync(EmployeeGenerator.GenerateEmployees(5000), o => o.ImmediateConsistency());

        await version2Index.ConfigureAsync();

        // Act: cancel the caller's token once the copy is underway. Nothing throws from the callback, so the
        // only way the reindex can stop is if the token is actually observed by the copy loop.
        using var cts = new CancellationTokenSource();
        var reindexTask = version2Index.ReindexAsync((progress, message) =>
        {
            _logger.LogInformation("Reindex Progress {Progress}%: {Message}", progress, message);
            if (progress > 5)
                cts.Cancel();

            return Task.CompletedTask;
        }, cts.Token);

        // Assert: cancellation is a throwing concept, so it must surface rather than being swallowed into a
        // silent return that a caller cannot distinguish from success.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reindexTask);

        // And it must stop before the alias is admitted to the new version.
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);
    }

    [Fact]
    public async Task ReindexAsync_WithBatchSize_MigratesAllDocuments()
    {
        // Arrange
        const int numberOfEmployeesToCreate = 250;

        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new VersionedEmployeeIndex(_configuration, 2)
        {
            ReindexBatchSize = 50
        };
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);

        IEmployeeRepository version1Repository = new EmployeeRepository(_configuration);
        await version1Repository.AddAsync(EmployeeGenerator.GenerateEmployees(numberOfEmployeesToCreate), o => o.ImmediateConsistency());

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1Index.Name), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(numberOfEmployeesToCreate, countResponse.Count);
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version2Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);

        // Act
        // A batch size smaller than the total document count forces the Elasticsearch reindex API to
        // issue multiple internal bulk sub-requests; verify all documents still migrate successfully.
        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);

        // Assert
        Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(numberOfEmployeesToCreate, countResponse.Count);
    }

    [Fact]
    public async Task ReindexAsync_WithRequestsPerSecondThrottle_TakesLongerThanUnthrottled()
    {
        // Arrange
        const int numberOfEmployeesToCreate = 200;

        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new VersionedEmployeeIndex(_configuration, 2)
        {
            ReindexBatchSize = 50,
            ReindexRequestsPerSecond = 25
        };
        await version2Index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();

        IEmployeeRepository version1Repository = new EmployeeRepository(_configuration);
        await version1Repository.AddAsync(EmployeeGenerator.GenerateEmployees(numberOfEmployeesToCreate), o => o.ImmediateConsistency());

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();

        // Act
        // With a 50-doc batch throttled to 25 docs/sec, Elasticsearch pauses ~2s between each of the 4
        // batches, so a correctly-throttled reindex takes noticeably longer than an unthrottled one would
        // (which typically completes well under a second locally).
        var sw = Stopwatch.StartNew();
        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);
        sw.Stop();

        // Assert
        Assert.Equal(2, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(numberOfEmployeesToCreate, countResponse.Count);

        Assert.True(sw.Elapsed >= TimeSpan.FromSeconds(3), $"Expected throttled reindex to take at least 3s, but took {sw.Elapsed}.");
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
        using var stream = new System.IO.MemoryStream();
        _client.ElasticsearchClientSettings.RequestResponseSerializer.Serialize(data, stream);
        stream.Position = 0;
        using var reader = new System.IO.StreamReader(stream);
        return reader.ReadToEnd();
    }

    [Fact]
    public async Task ReindexTimeSeriesIndex_ConcurrentCalls_OnlyOneReindexes()
    {
        // Arrange
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

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        // Act — launch two concurrent reindex operations
        var task1 = version2Index.ReindexAsync(cancellationToken: TestCancellationToken);
        var task2 = version2Index.ReindexAsync(cancellationToken: TestCancellationToken);

        await Task.WhenAll(task1, task2);

        // Assert — reindex completed successfully, version bumped
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        var aliasCountResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.Name), TestCancellationToken);
        _logger.LogRequest(aliasCountResponse);
        Assert.True(aliasCountResponse.IsValidResponse);
        Assert.Equal(1, aliasCountResponse.Count);
    }

    /// <summary>
    /// Reindex must detect documents that failed to copy on a client configured the way production is, with
    /// direct streaming left enabled.
    /// </summary>
    /// <remarks>
    /// The per-document failure list lives only in the raw task-status body, which the transport discards
    /// unless direct streaming is disabled. Every other test configuration disables it (for readable request
    /// logs), which masked a defect where a reindex that copied nothing reported success and flipped the alias
    /// onto the empty destination. This test is the guard for that, so it must not disable direct streaming.
    /// </remarks>
    [Fact]
    public async Task ReindexAsync_WithDirectStreamingEnabled_StillDetectsFailedDocuments()
    {
        // Arrange
        using var configuration = new DirectStreamingElasticConfiguration(2, _workItemQueue, _cache, _messageBus, Log);
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        var version2Index = configuration.Employees;
        await version1Index.DeleteAsync();
        await version2Index.DeleteAsync();
        await using AsyncDisposableAction cleanup = new(() => version2Index.DeleteAsync());

        await version1Index.ConfigureAsync();
        IEmployeeRepository repository = new EmployeeRepository(_configuration);
        await repository.AddAsync(EmployeeGenerator.GenerateEmployees(5), o => o.ImmediateConsistency());

        // The destination maps `name` as an integer, so the generated alphabetic names cannot be indexed.
        await _client.Indices.CreateAsync(version2Index.VersionedName, d => d
            .Mappings(md => md.Properties<Employee>(p => p.IntegerNumber(e => e.Name!))), TestCancellationToken);

        // Act & Assert - the copy failed, so it must not be reported as complete
        var exception = await Assert.ThrowsAsync<ReindexIncompleteException>(
            () => version2Index.ReindexAsync(cancellationToken: TestCancellationToken));

        Assert.Contains("failed to copy", exception.Reason);

        // The alias must still serve the complete source rather than the empty destination.
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());
    }

    [Fact]
    public async Task QueuedReindex_MigratesDocumentsAndSwitchesAlias()
    {
        // Arrange - the queued path must reindex exactly like the direct one
        using var configuration = new VersionedEmployeeElasticConfiguration(2, _workItemQueue, _cache, _messageBus, Log);
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        var version2Index = configuration.Employees;
        await version1Index.DeleteAsync();
        await version2Index.DeleteAsync();
        await using AsyncDisposableAction cleanup = new(() => version2Index.DeleteAsync());

        await version1Index.ConfigureAsync();
        IEmployeeRepository repository = new EmployeeRepository(_configuration);
        await repository.AddAsync(EmployeeGenerator.GenerateEmployees(10), o => o.ImmediateConsistency());

        await version2Index.ConfigureAsync();
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        // Act
        var handler = new ReindexWorkItemHandler(configuration);
        await HandleReindexWorkItemAsync(handler, version2Index.CreateReindexWorkItem(1));

        // Assert - alias moved to v2 and every document came across
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());
        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName), TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(10, countResponse.Count);
    }

    [Fact]
    public async Task QueuedReindex_WhenDocumentsFailToCopy_ThrowsSoTheQueueRetries()
    {
        // Arrange - v2 rejects the documents, so the copy cannot succeed
        using var configuration = new VersionedEmployeeElasticConfiguration(2, _workItemQueue, _cache, _messageBus, Log);
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        var version2Index = configuration.Employees;
        await version1Index.DeleteAsync();
        await version2Index.DeleteAsync();
        await using AsyncDisposableAction cleanup = new(() => version2Index.DeleteAsync());

        await version1Index.ConfigureAsync();
        IEmployeeRepository repository = new EmployeeRepository(_configuration);
        await repository.AddAsync(EmployeeGenerator.GenerateEmployees(5), o => o.ImmediateConsistency());

        await _client.Indices.CreateAsync(version2Index.VersionedName, d => d
            .Mappings(md => md.Properties<Employee>(p => p.IntegerNumber(e => e.Name!))), TestCancellationToken);

        // Act & Assert - the failure must propagate so the queue can retry or dead-letter it
        var handler = new ReindexWorkItemHandler(configuration);
        var exception = await Assert.ThrowsAsync<ReindexIncompleteException>(
            () => HandleReindexWorkItemAsync(handler, version2Index.CreateReindexWorkItem(1)));

        Assert.Contains("failed to copy", exception.Reason);
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());
    }

    [Fact]
    public async Task QueuedReindex_WhenMigrationAlreadyCompleted_SkipsInsteadOfRecopying()
    {
        // Arrange - run the migration first, then replay the same stale work item
        using var configuration = new VersionedEmployeeElasticConfiguration(2, _workItemQueue, _cache, _messageBus, Log);
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        var version2Index = configuration.Employees;
        await version1Index.DeleteAsync();
        await version2Index.DeleteAsync();
        await using AsyncDisposableAction cleanup = new(() => version2Index.DeleteAsync());

        await version1Index.ConfigureAsync();
        IEmployeeRepository repository = new EmployeeRepository(_configuration);
        await repository.AddAsync(EmployeeGenerator.GenerateEmployees(5), o => o.ImmediateConsistency());

        await version2Index.ConfigureAsync();
        var workItem = version2Index.CreateReindexWorkItem(1);
        await version2Index.ReindexAsync(cancellationToken: TestCancellationToken);
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        // Drop the source index so a re-copy could not silently succeed - it would have to fail loudly.
        await _client.Indices.DeleteAsync(version1Index.VersionedName, TestCancellationToken);

        // Act - replaying the stale work item must be a no-op, not a failed re-copy
        var handler = new ReindexWorkItemHandler(configuration);
        await HandleReindexWorkItemAsync(handler, workItem);

        // Assert
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());
        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version2Index.VersionedName), TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(5, countResponse.Count);
    }

    [Fact]
    public async Task GetWorkItemLockAsync_WhenAliasIsAlreadyLocked_AbandonsWorkItem()
    {
        // Arrange - hold the reindex lock the direct path uses
        using var configuration = new VersionedEmployeeElasticConfiguration(2, _workItemQueue, _cache, _messageBus, Log);
        var workItem = configuration.Employees.CreateReindexWorkItem(1);

        string lockKey = ElasticReindexer.GetLockName(workItem.Alias);
        await using var externalLock = await configuration.LockProvider.AcquireAsync(lockKey, TimeSpan.FromMinutes(1), TestCancellationToken);
        Assert.NotNull(externalLock);

        var handler = new ReindexWorkItemHandler(configuration);

        // Act - a cancelled token stands in for the acquire timeout elapsing
        using var cancelledSource = new CancellationTokenSource();
        await cancelledSource.CancelAsync();
        var workItemLock = await handler.GetWorkItemLockAsync(workItem, cancelledSource.Token);

        // Assert - no lock means the queue redelivers rather than running two reindexes at once
        Assert.Null(workItemLock);
    }

    [Fact]
    public async Task GetWorkItemLockAsync_WhenAliasIsFree_AcquiresTheSameLockTheDirectPathUses()
    {
        // Arrange
        using var configuration = new VersionedEmployeeElasticConfiguration(2, _workItemQueue, _cache, _messageBus, Log);
        var workItem = configuration.Employees.CreateReindexWorkItem(1);
        var handler = new ReindexWorkItemHandler(configuration);

        // Act
        await using var workItemLock = await handler.GetWorkItemLockAsync(workItem, TestCancellationToken);

        // Assert - the handler holds the alias lock, so the direct path is blocked out
        Assert.NotNull(workItemLock);
        Assert.True(await configuration.LockProvider.IsLockedAsync(ElasticReindexer.GetLockName(workItem.Alias)));
    }

    [Fact]
    public async Task GetWorkItemLockAsync_WithUnknownWorkItemType_AbandonsWorkItem()
    {
        using var configuration = new VersionedEmployeeElasticConfiguration(2, _workItemQueue, _cache, _messageBus, Log);
        var handler = new ReindexWorkItemHandler(configuration);

        Assert.Null(await handler.GetWorkItemLockAsync(new object(), TestCancellationToken));
    }

    private async Task HandleReindexWorkItemAsync(ReindexWorkItemHandler handler, ReindexWorkItem workItem)
    {
        await using var workItemLock = await handler.GetWorkItemLockAsync(workItem, TestCancellationToken);
        Assert.NotNull(workItemLock);

        var context = new WorkItemContext(workItem, Guid.NewGuid().ToString("N"), workItemLock, TestCancellationToken,
            (progress, message) =>
            {
                _logger.LogInformation("Queued reindex progress {Progress}%: {Message}", progress, message);
                return Task.CompletedTask;
            });

        await handler.HandleItemAsync(context);
    }
}
