using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class SaveOriginalCorrelationTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task SaveAsync_WithSameIdInDifferentLocations_PairsOriginalsAndNotifiesOnlySuccesses(bool differentIndexes, bool conflict)
    {
        using var configuration = new TestConfiguration(differentIndexes, conflict);
        using var repository = new TestRepository(configuration, useRouting: !differentIndexes);
        TestDocument[] originals =
        [
            new() { Id = "same", Index = "first", Routing = "first", IsDeleted = false, Version = "1:1" },
            new() { Id = "same", Index = differentIndexes ? "second" : "first", Routing = differentIndexes ? "first" : "second", IsDeleted = true, Version = "1:1" }
        ];
        var updates = originals.Select(d => new TestDocument
        {
            Id = d.Id,
            Index = d.Index,
            Routing = d.Routing,
            Version = d.Version,
            IsDeleted = !d.IsDeleted
        }).ToArray();
        var saving = new List<ModifiedDocument<TestDocument>>();
        var saved = new List<ModifiedDocument<TestDocument>>();
        var changed = new List<ModifiedDocument<TestDocument>>();
        repository.DocumentsSaving.AddSyncHandler((_, args) => saving.AddRange(args.Documents));
        repository.DocumentsSaved.AddSyncHandler((_, args) => saved.AddRange(args.Documents));
        repository.DocumentsChanged.AddSyncHandler((_, args) => changed.AddRange(args.Documents));

        if (conflict)
            await Assert.ThrowsAsync<VersionConflictDocumentException>(() => repository.SaveAsync(updates, o => o.Originals().AddOriginals(originals[0]).AddOriginals(originals[1]).Retry(0)));
        else
            await repository.SaveAsync(updates, o => o.Originals().AddOriginals(originals[0]).AddOriginals(originals[1]).Retry(0));

        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Equal(2, saving.Count);
        for (int i = 0; i < updates.Length; i++)
            Assert.Same(originals[i], Assert.Single(saving, item => ReferenceEquals(item.Value, updates[i])).Original);

        int successful = conflict ? 1 : 2;
        Assert.Equal(successful, saved.Count);
        Assert.Equal(successful, changed.Count);
        Assert.Equal(successful, repository.Notifications.Count);
        for (int i = 0; i < successful; i++)
        {
            Assert.Same(originals[i], Assert.Single(saved, item => ReferenceEquals(item.Value, updates[i])).Original);
            Assert.Same(originals[i], Assert.Single(changed, item => ReferenceEquals(item.Value, updates[i])).Original);
            var notification = Assert.Single(repository.Notifications, item => ReferenceEquals(item.Document, updates[i]));
            Assert.Equal(i is 0 ? ChangeType.Removed : ChangeType.Saved, notification.ChangeType);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SaveAsync_WithAutomaticOriginals_ReadsEachDocumentFromItsIndexAndRouting(bool differentIndexes)
    {
        using var configuration = new TestConfiguration(differentIndexes, conflict: false);
        using var repository = new TestRepository(configuration, useRouting: !differentIndexes);
        TestDocument[] originals =
        [
            new() { Id = "same", Index = "first", Routing = "first", IsDeleted = false, Version = "1:1" },
            new() { Id = "same", Index = differentIndexes ? "second" : "first", Routing = differentIndexes ? "first" : "second", IsDeleted = true, Version = "1:1" }
        ];
        configuration.Invoker.Originals = originals;
        var updates = originals.Select(d => new TestDocument
        {
            Id = d.Id,
            Index = d.Index,
            Routing = d.Routing,
            Version = d.Version,
            IsDeleted = !d.IsDeleted
        }).ToArray();
        var saved = new List<ModifiedDocument<TestDocument>>();
        repository.DocumentsSaved.AddSyncHandler((_, args) => saved.AddRange(args.Documents));

        await repository.SaveAsync(updates, o => o.Originals().Retry(0));

        Assert.Equal(originals.Select(d => (d.Index, d.Id, differentIndexes ? null : d.Routing)), configuration.Invoker.MultiGetDocuments);
        Assert.Equal(2, saved.Count);
        Assert.Equal(2, repository.Notifications.Count);
        foreach (var change in saved)
        {
            Assert.NotNull(change.Original);
            Assert.Equal(change.Value.Index, change.Original.Index);
            Assert.Equal(change.Value.Routing, change.Original.Routing);
            Assert.NotEqual(change.Value.IsDeleted, change.Original.IsDeleted);
        }
        Assert.Equal(ChangeType.Removed, repository.Notifications[0].ChangeType);
        Assert.Equal(ChangeType.Saved, repository.Notifications[1].ChangeType);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SaveAsync_WithPartiallyProvidedOriginals_ReadsOnlyMissingLocationAndRaisesBeforeGet(bool differentIndexes)
    {
        using var configuration = new TestConfiguration(differentIndexes, conflict: false);
        using var repository = new TestRepository(configuration, useRouting: !differentIndexes);
        TestDocument[] originals =
        [
            new() { Id = "same", Index = "first", Routing = "first", IsDeleted = false, Version = "1:1" },
            new() { Id = "same", Index = differentIndexes ? "second" : "first", Routing = differentIndexes ? "first" : "second", IsDeleted = true, Version = "1:1" }
        ];
        configuration.Invoker.Originals = originals;
        var updates = originals.Select(d => new TestDocument
        {
            Id = d.Id,
            Index = d.Index,
            Routing = d.Routing,
            Version = d.Version,
            IsDeleted = !d.IsDeleted
        }).ToArray();
        var saved = new List<ModifiedDocument<TestDocument>>();
        var beforeGetIds = new List<Id>();
        int beforeGetCalls = 0;
        repository.BeforeGet.AddSyncHandler((_, args) =>
        {
            beforeGetCalls++;
            beforeGetIds.AddRange(args.Ids);
        });
        repository.DocumentsSaved.AddSyncHandler((_, args) => saved.AddRange(args.Documents));

        await repository.SaveAsync(updates, o => o.Originals().AddOriginals(originals[0]).Retry(0));

        Assert.Equal((originals[1].Index, originals[1].Id, differentIndexes ? null : originals[1].Routing), Assert.Single(configuration.Invoker.MultiGetDocuments));
        Assert.Equal(1, beforeGetCalls);
        Assert.Equal(new Id(originals[1].Id, differentIndexes ? null : originals[1].Routing), Assert.Single(beforeGetIds));
        Assert.Equal(2, saved.Count);
        Assert.Same(originals[0], Assert.Single(saved, change => ReferenceEquals(change.Value, updates[0])).Original);
        var second = Assert.Single(saved, change => ReferenceEquals(change.Value, updates[1])).Original;
        Assert.NotNull(second);
        Assert.Equal(originals[1].Index, second.Index);
        Assert.Equal(originals[1].Routing, second.Routing);
        Assert.True(second.IsDeleted);
        Assert.Equal(new[] { ChangeType.Removed, ChangeType.Saved }, repository.Notifications.Select(n => n.ChangeType));
    }

    private sealed class TestDocument : IIdentity, IVersioned, ISupportSoftDeletes
    {
        public string Id { get; set; } = null!;
        public string Index { get; set; } = null!;
        public string Routing { get; set; } = null!;
        public string? Version { get; set; }
        public bool IsDeleted { get; set; }
    }

    private sealed class TestRepository : ElasticRepositoryBase<TestDocument>
    {
        public TestRepository(TestConfiguration configuration, bool useRouting) : base(new TestIndex(configuration))
        {
            HasParent = useRouting;
            if (useRouting)
                GetParentIdFunc = document => document.Routing;
            DisableCache();
        }

        public List<(ChangeType ChangeType, TestDocument Document)> Notifications { get; } = [];

        protected override Task PublishChangeTypeMessageAsync(ChangeType changeType, TestDocument? document, IDictionary<string, object?>? data = null, TimeSpan? delay = null)
        {
            if (document is not null)
                Notifications.Add((changeType, document));

            return Task.CompletedTask;
        }
    }

    private sealed class TestIndex : Index<TestDocument>
    {
        public TestIndex(TestConfiguration configuration) : base(configuration, "save-tests")
        {
            HasMultipleIndexes = configuration.DifferentIndexes;
        }

        public override string GetIndex(object target) => target is TestDocument document ? document.Index : "first";
        public override Task EnsureIndexAsync(object? target) => Task.CompletedTask;
    }

    private sealed class TestConfiguration(bool differentIndexes, bool conflict) : ElasticConfiguration
    {
        public bool DifferentIndexes { get; } = differentIndexes;
        public BulkResponseInvoker Invoker { get; } = new(differentIndexes, conflict);
        protected override IRequestInvoker CreateRequestInvoker() => Invoker;
    }

    private sealed class BulkResponseInvoker(bool differentIndexes, bool conflict) : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _responseFactoryOwner = new();
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };
        public ResponseFactory ResponseFactory => _responseFactoryOwner.ResponseFactory;
        public int Requests { get; private set; }
        public IReadOnlyCollection<TestDocument> Originals { get; set; } = [];
        public List<(string Index, string Id, string? Routing)> MultiGetDocuments { get; } = [];

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            Requests++;
            string body;
            if (endpoint.PathAndQuery.Contains("_mapping", StringComparison.Ordinal))
            {
                body = """{"save-tests":{"mappings":{"properties":{"id":{"type":"keyword"},"isDeleted":{"type":"boolean"}}}}}""";
            }
            else if (endpoint.PathAndQuery.Contains("_mget", StringComparison.Ordinal))
            {
                Assert.NotNull(postData);
                using var requestStream = new MemoryStream();
                postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                using var request = JsonDocument.Parse(requestStream.ToArray());
                var documents = new List<object>();
                foreach (var item in request.RootElement.GetProperty("docs").EnumerateArray())
                {
                    string index = item.GetProperty("_index").GetString()!;
                    string id = item.GetProperty("_id").GetString()!;
                    string? routing = item.TryGetProperty("routing", out var value) ? value.GetString() : null;
                    MultiGetDocuments.Add((index, id, routing));
                    var original = Originals.SingleOrDefault(d => d.Index == index && d.Id == id && (differentIndexes || d.Routing == routing));
                    documents.Add(new
                    {
                        _index = index,
                        _id = id,
                        _routing = routing,
                        found = original is not null,
                        _seq_no = 1,
                        _primary_term = 1,
                        _source = original is null ? null : new { id = original.Id, index = original.Index, routing = original.Routing, isDeleted = original.IsDeleted }
                    });
                }
                body = JsonSerializer.Serialize(new { docs = documents });
            }
            else if (endpoint.PathAndQuery.Contains("_search", StringComparison.Ordinal))
            {
                var hits = Originals.Where(d => !d.IsDeleted).Select(d => new
                {
                    _index = d.Index,
                    _id = d.Id,
                    _routing = d.Routing,
                    _seq_no = 1,
                    _primary_term = 1,
                    _source = new { id = d.Id, index = d.Index, routing = d.Routing, isDeleted = d.IsDeleted }
                }).ToArray();
                body = JsonSerializer.Serialize(new { took = 1, timed_out = false, hits = new { total = new { value = hits.Length, relation = "eq" }, hits } });
            }
            else
            {
                Assert.Contains("_bulk", endpoint.PathAndQuery, StringComparison.Ordinal);
                body = GetBulkResponse();
            }

            byte[] bytes = Encoding.UTF8.GetBytes(body);
            using var stream = new MemoryStream(bytes);
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, 200, _headers, stream, "application/json", bytes.Length, null, null);
        }

        private string GetBulkResponse()
        {
            string secondIndex = differentIndexes ? "second" : "first";
            object second = conflict
                ? new { index = new { _index = secondIndex, _id = "same", status = 409, error = new { type = "version_conflict_engine_exception", reason = "The document changed." } } }
                : new { index = new { _index = secondIndex, _id = "same", status = 200, result = "updated", _seq_no = 2, _primary_term = 1 } };
            return JsonSerializer.Serialize(new
            {
                took = 1,
                errors = conflict,
                items = new object[]
                {
                    new { index = new { _index = "first", _id = "same", status = 200, result = "updated", _seq_no = 2, _primary_term = 1 } },
                    second
                }
            });
        }

        public Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
            where TResponse : TransportResponse, new()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Request<TResponse>(endpoint, boundConfiguration, postData));
        }

        public void Dispose() => ((IDisposable)_responseFactoryOwner).Dispose();
    }
}
