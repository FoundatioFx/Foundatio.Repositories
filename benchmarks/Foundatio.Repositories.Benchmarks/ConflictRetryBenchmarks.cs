using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Bulk;
using Elastic.Clients.Elasticsearch.Core.MGet;

namespace Foundatio.Repositories.Benchmarks;

/// <summary>
/// Compares the two ways of retrying documents that conflicted in a batch patch: re-reading and re-writing each
/// document on its own (the previous behavior, one GET and one index request per id) versus one grouped round
/// (one multi-get for all ids and one bulk request with <c>if_seq_no</c>/<c>if_primary_term</c>).
/// </summary>
/// <remarks>
/// Requires Elasticsearch. Set <c>ELASTICSEARCH_URL</c> (defaults to <c>http://localhost:9200</c>).
/// Run with <c>dotnet run -c Release --project benchmarks/Foundatio.Repositories.Benchmarks</c>.
/// </remarks>
[MemoryDiagnoser]
public class ConflictRetryBenchmarks
{
    private const string IndexName = "benchmark-conflict-retry";
    private ElasticsearchClient _client = null!;
    private List<string> _ids = null!;

    [Params(10, 100, 500)]
    public int ConflictingDocuments { get; set; }

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        string url = Environment.GetEnvironmentVariable("ELASTICSEARCH_URL") ?? "http://localhost:9200";
        _client = new ElasticsearchClient(new ElasticsearchClientSettings(new Uri(url)));
        await _client.Indices.DeleteAsync(IndexName, d => d.IgnoreUnavailable());
        await _client.Indices.CreateAsync(IndexName, c => c.Settings(s => s.NumberOfShards(1).NumberOfReplicas(0)));

        _ids = Enumerable.Range(0, ConflictingDocuments).Select(i => $"doc-{i}").ToList();
        var bulk = new BulkRequest(IndexName)
        {
            Refresh = Refresh.True,
            Operations = _ids.Select(id => (IBulkOperation)new BulkIndexOperation<BenchmarkDocument>(new BenchmarkDocument { Name = id, Counter = 0 }) { Id = id }).ToList()
        };
        await _client.BulkAsync(bulk);
    }

    [GlobalCleanup]
    public Task GlobalCleanupAsync()
    {
        return _client.Indices.DeleteAsync(IndexName, d => d.IgnoreUnavailable());
    }

    [Benchmark(Baseline = true)]
    public async Task<int> PerIdRetry()
    {
        int written = 0;
        foreach (string id in _ids)
        {
            var response = await _client.GetAsync<BenchmarkDocument>(new GetRequest(IndexName, id));
            var document = response.Source!;
            document.Counter++;

            var indexResponse = await _client.IndexAsync(new IndexRequest<BenchmarkDocument>(document, IndexName, id)
            {
                IfSeqNo = response.SeqNo,
                IfPrimaryTerm = response.PrimaryTerm
            });

            if (indexResponse.IsValidResponse)
                written++;
        }

        return written;
    }

    [Benchmark]
    public async Task<int> GroupedRetry()
    {
        var operations = _ids.Select(id => new MultiGetOperation(id) { Index = IndexName }).ToList();
        var multiGet = await _client.MultiGetAsync<BenchmarkDocument>(new MultiGetRequestDescriptor().Docs(operations));

        var writes = new List<IBulkOperation>(_ids.Count);
        foreach (var item in multiGet.Docs)
        {
            item.Match(result =>
            {
                if (result is not { Found: true, Source: not null })
                    return;

                result.Source.Counter++;
                writes.Add(new BulkIndexOperation<BenchmarkDocument>(result.Source)
                {
                    Id = result.Id,
                    Index = IndexName,
                    IfSequenceNumber = result.SeqNo,
                    IfPrimaryTerm = result.PrimaryTerm
                });
            }, _ => { });
        }

        var bulkResponse = await _client.BulkAsync(new BulkRequest { Operations = writes });
        return bulkResponse.Items.Count(i => i.IsValid);
    }
}

public sealed class BenchmarkDocument
{
    public string Name { get; set; } = null!;
    public int Counter { get; set; }
}
