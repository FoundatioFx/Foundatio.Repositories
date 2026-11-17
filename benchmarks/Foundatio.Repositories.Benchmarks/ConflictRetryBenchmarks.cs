using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.MGet;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Benchmarks;

/// <summary>
/// Compares the two ways a batch patch can retry documents that conflicted, both driven through the repository:
/// the grouped retry <c>PatchAllAsync</c> uses (one multi-get and one bulk request for every conflicting document) and
/// the previous per-id retry (the batch is written once, then every conflicting document is patched again on its own
/// with <c>PatchAsync(id)</c>, one read and one write each).
/// </summary>
/// <remarks>
/// <para>Before the first bulk write of each operation a concurrent writer bumps every document with update-by-query, so
/// every document conflicts exactly once in both benchmarks. The documents are reset before each iteration. Untimed
/// cleanup verifies the injection, request count, returned count, and preserved concurrent changes.</para>
/// <para>Requires Elasticsearch. Set <c>ELASTICSEARCH_URL</c> (defaults to <c>http://localhost:9200</c>). Run with
/// <c>dotnet run -c Release --project benchmarks/Foundatio.Repositories.Benchmarks -- --filter *ConflictRetry* --job short</c>.</para>
/// </remarks>
[MemoryDiagnoser]
public class ConflictRetryBenchmarks
{
    private BenchmarkConfiguration _configuration = null!;
    private BenchmarkRepository _repository = null!;
    private List<BenchmarkDocument> _documents = null!;
    private UpdateByQueryResponse? _conflictInjectionResponse;
    private RefreshResponse? _refreshResponse;
    private int _conflictInjections;
    private long _patchedDocuments;
    private int _expectedBulkRequests;
    private static readonly JsonPatch _patch = new(new PatchDocument(new ReplaceOperation { Path = "name", Value = "patched" }));

    [Params(10, 100, 500)]
    public int ConflictingDocuments { get; set; }

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _configuration = new BenchmarkConfiguration();
        await _configuration.Index.DeleteAsync();
        await _configuration.ConfigureIndexesAsync(beginReindexingOutdated: false);
        _repository = new BenchmarkRepository(_configuration.Index);
        _documents = Enumerable.Range(0, ConflictingDocuments).Select(_ => new BenchmarkDocument { Id = ObjectId.GenerateNewId().ToString() }).ToList();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        foreach (var document in _documents)
        {
            document.Name = "original";
            document.Counter = 0;
        }

        _repository.SaveAsync(_documents, o => o.SkipVersionCheck().ImmediateConsistency()).GetAwaiter().GetResult();
        _conflictInjectionResponse = null;
        _refreshResponse = null;
        _conflictInjections = 0;
        _patchedDocuments = -1;
        _expectedBulkRequests = 0;
        _configuration.Invoker.BeforeNextBulk(BumpEveryDocumentAsync);
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        ValidateIterationAsync().GetAwaiter().GetResult();
    }

    private async Task ValidateIterationAsync()
    {
        if (_conflictInjections is not 1
            || _conflictInjectionResponse is not { IsValidResponse: true, TimedOut: false, VersionConflicts: 0, Noops: 0, Deleted: 0 } injection
            || injection.Total != ConflictingDocuments || injection.Updated != ConflictingDocuments
            || injection.Failures is { Count: > 0 })
            throw new InvalidOperationException($"Conflict injection did not update exactly {ConflictingDocuments} documents once.");

        if (_patchedDocuments != ConflictingDocuments)
            throw new InvalidOperationException($"The benchmark reported {_patchedDocuments} patched documents instead of {ConflictingDocuments}.");

        if (_configuration.Invoker.IsBulkHookPending || _configuration.Invoker.BulkRequests != _expectedBulkRequests)
            throw new InvalidOperationException($"The benchmark sent {_configuration.Invoker.BulkRequests} bulk requests instead of {_expectedBulkRequests}.");

        if (_refreshResponse is { } refresh && (!refresh.IsValidResponse || refresh.Shards is not { Failed: 0 }))
            throw new InvalidOperationException("The per-ID benchmark refresh did not complete successfully.");

        var response = await _configuration.Client.MultiGetAsync<BenchmarkDocument>(new MultiGetRequestDescriptor()
            .Docs(_documents.Select(document => new MultiGetOperation(document.Id) { Index = _configuration.Index.Name }).ToList()));
        if (!response.IsValidResponse || response.Docs.Count != _documents.Count)
            throw new InvalidOperationException("The benchmark could not verify every document after patching.");

        int index = 0;
        foreach (var item in response.Docs)
        {
            string expectedId = _documents[index++].Id;
            bool valid = item.Match(
                result => result is { Found: true } && result.Id == expectedId && result.Index == _configuration.Index.Name
                    && result.Source is { Name: "patched", Counter: 1 } document && document.Id == expectedId,
                _ => false);
            if (!valid)
                throw new InvalidOperationException($"Document {expectedId} did not retain both the patch and the concurrent update.");
        }
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        await _configuration.Index.DeleteAsync();
        _repository.Dispose();
        _configuration.Dispose();
    }

    [Benchmark(Baseline = true)]
    public Task<long> PerIdRetry()
    {
        return RecordResultAsync(PerIdRetryAsync(), ConflictingDocuments + 1);
    }

    private async Task<long> PerIdRetryAsync()
    {
        try
        {
            return await _repository.PatchAllAsync(q => q, _patch, o => o.Retry(0).ImmediateConsistency());
        }
        catch (VersionConflictDocumentException)
        {
            // Retried without a refresh per document, then refreshed once, so both benchmarks pay for one refresh.
            long patched = 0;
            foreach (var document in _documents)
            {
                if (await _repository.PatchAsync(document.Id, _patch))
                    patched++;
            }

            _refreshResponse = await _configuration.Client.Indices.RefreshAsync(_configuration.Index.Name);
            return patched;
        }
    }

    [Benchmark]
    public Task<long> GroupedRetry()
    {
        return RecordResultAsync(_repository.PatchAllAsync(q => q, _patch, o => o.ImmediateConsistency()), 2);
    }

    private async Task<long> RecordResultAsync(Task<long> operation, int expectedBulkRequests)
    {
        _expectedBulkRequests = expectedBulkRequests;
        _patchedDocuments = await operation;
        return _patchedDocuments;
    }

    private async Task BumpEveryDocumentAsync()
    {
        _conflictInjections++;
        _conflictInjectionResponse = await _configuration.Client.UpdateByQueryAsync<BenchmarkDocument>(_configuration.Index.Name, u => u
            .Script(s => s.Source("ctx._source.counter += 1;"))
            .Refresh());
    }
}

public sealed class BenchmarkDocument : IIdentity, IVersioned
{
    public string Id { get; set; } = null!;
    public string? Version { get; set; }
    public string Name { get; set; } = null!;
    public int Counter { get; set; }
}

public sealed class BenchmarkIndex(IElasticConfiguration configuration) : Index<BenchmarkDocument>(configuration, "benchmark-conflict-retry")
{
    public override void ConfigureIndex(Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexRequestDescriptor idx)
    {
        base.ConfigureIndex(idx.Settings(s => s.NumberOfShards(1).NumberOfReplicas(0)));
    }
}

public sealed class BenchmarkRepository(IIndex index) : ElasticRepositoryBase<BenchmarkDocument>(index);

public sealed class BenchmarkConfiguration : ElasticConfiguration
{
    public BenchmarkConfiguration()
    {
        AddIndex(Index = new BenchmarkIndex(this));
    }

    public BenchmarkIndex Index { get; }
    public BulkHookRequestInvoker Invoker { get; } = new(new HttpRequestInvoker());

    protected override NodePool CreateConnectionPool()
    {
        return new SingleNodePool(new Uri(Environment.GetEnvironmentVariable("ELASTICSEARCH_URL") ?? "http://localhost:9200"));
    }

    protected override IRequestInvoker CreateRequestInvoker() => Invoker;
}

/// <summary>Runs a callback right before the next bulk request, to inject a concurrent write between read and write.</summary>
public sealed class BulkHookRequestInvoker(IRequestInvoker inner) : IRequestInvoker
{
    private Func<Task>? _beforeNextBulk;
    private int _bulkRequests;

    public ResponseFactory ResponseFactory => inner.ResponseFactory;
    public int BulkRequests => Volatile.Read(ref _bulkRequests);
    public bool IsBulkHookPending => Volatile.Read(ref _beforeNextBulk) is not null;

    public void BeforeNextBulk(Func<Task> action)
    {
        Interlocked.Exchange(ref _bulkRequests, 0);
        _beforeNextBulk = action;
    }

    public async Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
        where TResponse : TransportResponse, new()
    {
        if (endpoint.PathAndQuery.Contains("_bulk", StringComparison.Ordinal))
        {
            Interlocked.Increment(ref _bulkRequests);
            if (Interlocked.Exchange(ref _beforeNextBulk, null) is { } beforeBulk)
                await beforeBulk().ConfigureAwait(false);
        }

        return await inner.RequestAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken).ConfigureAwait(false);
    }

    public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
        where TResponse : TransportResponse, new()
    {
        return inner.Request<TResponse>(endpoint, boundConfiguration, postData);
    }

    public void Dispose() => inner.Dispose();
}
