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
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Queries;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class PatchReadConsistencyTests
{
    [Theory]
    [InlineData(false, SoftDeleteQueryMode.ActiveOnly)]
    [InlineData(true, SoftDeleteQueryMode.ActiveOnly)]
    [InlineData(false, SoftDeleteQueryMode.DeletedOnly)]
    [InlineData(true, SoftDeleteQueryMode.DeletedOnly)]
    [InlineData(false, SoftDeleteQueryMode.All)]
    [InlineData(true, SoftDeleteQueryMode.All)]
    public async Task JsonAndActionPatch_WithMixedSoftDeleteStates_PatchesOnlyEligibleDocuments(bool action, SoftDeleteQueryMode mode)
    {
        var active = StoredDocument.CreateSingleIndex("active");
        var deleted = StoredDocument.CreateSingleIndex("deleted", isDeleted: true);
        using var configuration = new TestConfiguration([active, deleted]);
        using var repository = new TestRepository(configuration, [active, deleted], singleIndex: true);
        var updatedIds = new List<string>();

        long patched = await repository.PatchAsync(new Ids(active.Document.Id, deleted.Document.Id), CreateValuePatch(action),
            o => o.Retry(0).Notifications(false).SoftDeleteMode(mode).UpdateCallback(updatedIds.AddRange));

        string[] expectedIds = mode switch
        {
            SoftDeleteQueryMode.ActiveOnly => [active.Document.Id],
            SoftDeleteQueryMode.DeletedOnly => [deleted.Document.Id],
            _ => [active.Document.Id, deleted.Document.Id]
        };
        Assert.Equal(expectedIds.Length, patched);
        Assert.Equal(expectedIds.Order(), configuration.Invoker.Writes.Select(w => w.Id).Order());
        Assert.Equal(expectedIds.Order(), updatedIds.Order());
        Assert.All(configuration.Invoker.Writes, write => Assert.Equal("patched", write.Source.GetProperty("value").GetString()));
    }

    [Theory]
    [InlineData(false, SoftDeleteQueryMode.ActiveOnly)]
    [InlineData(true, SoftDeleteQueryMode.ActiveOnly)]
    [InlineData(false, SoftDeleteQueryMode.DeletedOnly)]
    [InlineData(true, SoftDeleteQueryMode.DeletedOnly)]
    public async Task JsonAndActionPatch_WhenEveryDocumentIsFiltered_ReturnsZeroWithoutWritesOrNotifications(bool action, SoftDeleteQueryMode mode)
    {
        bool isDeleted = mode is SoftDeleteQueryMode.ActiveOnly;
        var first = StoredDocument.CreateSingleIndex("first", isDeleted);
        var second = StoredDocument.CreateSingleIndex("second", isDeleted);
        using var configuration = new TestConfiguration([first, second]);
        using var repository = new TestRepository(configuration, [first, second], singleIndex: true);
        int notifications = 0;
        repository.DocumentsChanged.AddHandler((_, _) =>
        {
            notifications++;
            return Task.CompletedTask;
        });
        var updatedIds = new List<string>();

        long patched = await repository.PatchAsync(new Ids(first.Document.Id, second.Document.Id), CreateValuePatch(action),
            o => o.Retry(0).Notifications(false).SoftDeleteMode(mode).UpdateCallback(updatedIds.AddRange));

        Assert.Equal(0, patched);
        Assert.Empty(configuration.Invoker.Writes);
        Assert.Empty(updatedIds);
        Assert.Equal(0, notifications);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task JsonAndActionPatch_WithRepositoryQueryFilter_DoesNotPatchExcludedDocuments(bool action)
    {
        var included = StoredDocument.CreateSingleIndex("included");
        var excluded = StoredDocument.CreateSingleIndex("excluded");
        using var configuration = new TestConfiguration([included, excluded]);
        using var repository = new TestRepository(configuration, [included, excluded], singleIndex: true);
        repository.BeforeQuery.AddHandler((_, args) =>
        {
            args.Query.ExcludedId(excluded.Document.Id);
            return Task.CompletedTask;
        });

        long patched = await repository.PatchAsync(new Ids(included.Document.Id, excluded.Document.Id), CreateValuePatch(action),
            o => o.Retry(0).Notifications(false));

        Assert.Equal(1, patched);
        var write = Assert.Single(configuration.Invoker.Writes);
        Assert.Equal(included.Document.Id, write.Id);
        Assert.Equal("patched", write.Source.GetProperty("value").GetString());
    }

    private static IPatchOperation CreateValuePatch(bool action) => action
        ? new ActionPatch<TestDocument>(document =>
        {
            document.Value = "patched";
            return true;
        })
        : new JsonPatch(new PatchDocument(new ReplaceOperation { Path = "/value", Value = "patched" }));

    [Fact]
    public async Task JsonPatch_WithPublicVersion_PreservesReadTokenForConditionalWrites()
    {
        var first = StoredDocument.Create(1);
        var second = StoredDocument.Create(2);
        using var configuration = new TestConfiguration([first, second]);
        using var repository = new TestRepository(configuration, [first, second]);
        var patch = new JsonPatch(new PatchDocument(
            new ReplaceOperation { Path = "/version", Value = "9:9" },
            new ReplaceOperation { Path = "/value", Value = "patched" }));

        long patched = await repository.PatchAsync(new Ids(first.Document.Id, second.Document.Id), patch, o => o.Retry(0).Notifications(false));

        Assert.Equal(2, patched);
        Assert.Equal(2, configuration.Invoker.Writes.Count);
        foreach (var write in configuration.Invoker.Writes)
        {
            Assert.Equal(1, write.PrimaryTerm);
            Assert.Equal(7, write.SequenceNumber);
            Assert.Equal("patched", write.Source.GetProperty("value").GetString());
            Assert.Equal("1:7", write.Source.GetProperty("version").GetString());
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PipelinePartialPatch_WithFreshKnownAndOptionalCustomIds_ReadsKnownLocationsInRealtime(bool customId)
    {
        var first = StoredDocument.Create(1);
        var second = StoredDocument.Create(2, customId ? "custom-id" : null);
        using var configuration = new TestConfiguration([first, second]);
        using var repository = new TestRepository(configuration, customId ? [second] : []);

        long patched = await repository.PatchAsync(new Ids(first.Document.Id, second.Document.Id), new PartialPatch(new { value = "patched" }),
            o => o.Retry(0).Notifications(false).Include(d => d.Value));

        Assert.Equal(2, patched);
        Assert.Equal(customId ? new[] { second.Document.Id } : [], repository.SearchedIds);
        Assert.Equal(customId ? 1 : 2, configuration.Invoker.ReadLocations.Count);
        Assert.Equal(1, configuration.Invoker.MultiGetRequests);
        Assert.Equal(1, configuration.Invoker.BulkRequests);
        Assert.Contains((first.ReadIndex, first.Document.Id, first.Routing), configuration.Invoker.ReadLocations);
        Assert.Equal(2, configuration.Invoker.Writes.Count);
        foreach (var stored in new[] { first, second })
        {
            var write = Assert.Single(configuration.Invoker.Writes, w => w.Id == stored.Document.Id);
            Assert.Equal(stored.Index, write.Index);
            Assert.Equal("patched", write.Source.GetProperty("value").GetString());
            Assert.Equal(stored.Document.Keep, write.Source.GetProperty("keep").GetString());
            Assert.Equal(7, write.SequenceNumber);
            Assert.Equal(1, write.PrimaryTerm);
        }
    }

    [Fact]
    public async Task PipelinePartialPatch_WithMixedKnownAndUnknownRouting_ReadsKnownRoutingInRealtime()
    {
        var first = StoredDocument.Create(1, routing: "first");
        var second = StoredDocument.Create(2, routing: "second");
        using var configuration = new TestConfiguration([first, second]);
        using var repository = new TestRepository(configuration, [second], hasParent: true);

        long patched = await repository.PatchAsync(new Ids(new Id(first.Document.Id, first.Routing), new Id(second.Document.Id)),
            new PartialPatch(new { value = "patched" }), o => o.Retry(0).Notifications(false));

        Assert.Equal(2, patched);
        Assert.Equal((first.ReadIndex, first.Document.Id, first.Routing), Assert.Single(configuration.Invoker.ReadLocations));
        Assert.Equal(second.Document.Id, Assert.Single(repository.SearchedIds));
        Assert.Equal(new[] { "first", "second" }, configuration.Invoker.Writes.Select(w => w.Routing).Order());
    }

    [Fact]
    public async Task PipelinePartialPatch_WhenDisposedAfterRealtimeRead_DoesNotStartDiscoveryOrWrite()
    {
        var first = StoredDocument.Create(1);
        var second = StoredDocument.Create(2, "custom-id");
        using var configuration = new TestConfiguration([first, second]);
        using var repository = new TestRepository(configuration, [second]);
        configuration.Invoker.AfterMultiGet = repository.Dispose;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => repository.PatchAsync(new Ids(first.Document.Id, second.Document.Id),
            new PartialPatch(new { value = "patched" }), o => o.Retry(0).Notifications(false)));

        Assert.Equal(1, configuration.Invoker.MultiGetRequests);
        Assert.Empty(repository.SearchedIds);
        Assert.Empty(configuration.Invoker.Writes);
    }

    [Theory]
    [InlineData(false, true)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    public async Task PipelinePartialPatch_WithOverlappingRoutedAndUnroutedIds_KeepsRealtimeOutcomeAuthoritative(bool missing, bool searchVisible)
    {
        var first = StoredDocument.Create(1, routing: "first");
        var second = StoredDocument.Create(1, first.Document.Id, "second");
        var stale = first with { Document = new TestDocument { Id = first.Document.Id, Keep = "stale", Value = "before", Version = "1:7" } };
        using var configuration = new TestConfiguration(missing ? [second] : [first, second]);
        using var repository = new TestRepository(configuration, searchVisible ? [stale, second] : [], hasParent: true);
        var ids = new Ids(new Id(first.Document.Id, first.Routing), new Id(first.Document.Id));

        if (missing)
        {
            await Assert.ThrowsAsync<DocumentNotFoundException>(() => repository.PatchAsync(ids, new PartialPatch(new { value = "patched" }),
                o => o.Retry(0).Notifications(false)));
        }
        else
        {
            Assert.Equal(searchVisible ? 2 : 1, await repository.PatchAsync(ids, new PartialPatch(new { value = "patched" }), o => o.Retry(0).Notifications(false)));
        }

        Assert.Single(configuration.Invoker.ReadLocations);
        Assert.Equal(missing || !searchVisible ? 1 : 2, configuration.Invoker.Writes.Count);
        if (searchVisible)
            Assert.Single(configuration.Invoker.Writes, w => w.Routing == "second");
        if (!missing)
            Assert.Equal(first.Document.Keep, Assert.Single(configuration.Invoker.Writes, w => w.Routing == "first").Source.GetProperty("keep").GetString());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PipelinePartialPatch_WithKnownLocations_WritesBoundedBatches(bool rejectFirst)
    {
        var documents = Enumerable.Range(1, 5).Select(month => StoredDocument.Create(month, routing: $"route-{month}")).ToArray();
        using var configuration = new TestConfiguration(documents);
        if (rejectFirst)
            configuration.Invoker.RejectWriteId = documents[0].Document.Id;
        using var repository = new TestRepository(configuration, [], hasParent: true);
        var updatedIds = new List<string>();
        var ids = new Ids(documents.Select(document => new Id(document.Document.Id, document.Routing)));

        Func<Task<long>> patchAsync = async () => await repository.PatchAsync(ids, new PartialPatch(new { value = "patched" }),
            options => options.PageLimit(2).Retry(0).Notifications(false).UpdateCallback(updatedIds.AddRange));

        if (rejectFirst)
        {
            var exception = await Assert.ThrowsAsync<DocumentException>(patchAsync);
            Assert.Contains("1 failed, 4 succeeded", exception.Message, StringComparison.Ordinal);
        }
        else
        {
            Assert.Equal(documents.Length, await patchAsync());
        }

        Assert.Equal(new[] { 2, 2, 1 }, configuration.Invoker.MultiGetSizes);
        Assert.Equal(new[] { 0, 2, 4 }, configuration.Invoker.WritesBeforeMultiGet);
        Assert.Equal(new[] { 2, 2, 1 }, configuration.Invoker.BulkSizes);
        Assert.Empty(repository.SearchedIds);
        Assert.Equal(documents.Select(document => (document.Index, document.Document.Id, document.Routing)),
            configuration.Invoker.Writes.Select(write => (write.Index, write.Id, write.Routing)));
        Assert.Equal(documents.Skip(rejectFirst ? 1 : 0).Select(document => document.Document.Id), updatedIds);
        Assert.All(configuration.Invoker.Writes, write =>
        {
            Assert.Equal("patched", write.Source.GetProperty("value").GetString());
            Assert.Equal(7, write.SequenceNumber);
            Assert.Equal(1, write.PrimaryTerm);
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PipelinePartialPatch_WithRoutedAndUnroutedIdsAcrossBatches_PreservesRealtimeAuthority(bool missing)
    {
        var first = StoredDocument.Create(1, routing: "first");
        var second = StoredDocument.Create(1, first.Document.Id, "second");
        var other = StoredDocument.Create(2, routing: "other");
        var last = StoredDocument.Create(3, routing: "last");
        var stale = first with { Document = new TestDocument { Id = first.Document.Id, Keep = "stale", Version = "1:7" } };
        using var configuration = new TestConfiguration(missing ? [second, other, last] : [first, second, other, last]);
        using var repository = new TestRepository(configuration, [stale, second], hasParent: true) { SearchPageSize = 2 };
        var ids = new Ids(new Id(first.Document.Id, first.Routing), new Id(other.Document.Id, other.Routing),
            new Id(last.Document.Id, last.Routing), new Id(first.Document.Id));

        Func<Task<long>> patchAsync = async () => await repository.PatchAsync(ids, new PartialPatch(new { value = "patched" }),
            options => options.PageLimit(2).Retry(0).Notifications(false));

        if (missing)
            await Assert.ThrowsAsync<DocumentNotFoundException>(patchAsync);
        else
            Assert.Equal(4, await patchAsync());

        Assert.Equal(new[] { 2, 1 }, configuration.Invoker.MultiGetSizes);
        Assert.Equal(missing ? new[] { 2, 1 } : new[] { 2, 2 }, configuration.Invoker.BulkSizes);
        Assert.Equal(first.Document.Id, Assert.Single(repository.SearchedIds));
        Assert.Single(configuration.Invoker.Writes, write => write.Routing == "second");
        Assert.Single(configuration.Invoker.Writes, write => write.Routing == "other");
        Assert.Single(configuration.Invoker.Writes, write => write.Routing == "last");
        if (missing)
        {
            Assert.DoesNotContain(configuration.Invoker.Writes, write => write.Routing == "first");
        }
        else
        {
            var write = Assert.Single(configuration.Invoker.Writes, write => write.Routing == "first");
            Assert.Equal(first.Document.Keep, write.Source.GetProperty("keep").GetString());
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PipelinePartialPatch_WithMultipleDiscoveryPages_WritesEachPageBeforeReadingTheNext(bool includeMissing)
    {
        var documents = Enumerable.Range(1, 5).Select(month => StoredDocument.Create(month, $"custom-{month}", $"route-{month}")).ToArray();
        using var configuration = new TestConfiguration(documents);
        using var repository = new TestRepository(configuration, documents, hasParent: true) { SearchPageSize = 2 };
        var writesBeforePage = new List<int>();
        repository.BeforeSearchPage = () => writesBeforePage.Add(configuration.Invoker.Writes.Count);
        var updatedIds = new List<string>();
        var requestedIds = documents.Select(document => new Id(document.Document.Id, document.Routing));
        var ids = new Ids(includeMissing ? requestedIds.Append(new Id("custom-missing", "missing")) : requestedIds);

        Func<Task<long>> patchAsync = async () => await repository.PatchAsync(ids, new PartialPatch(new { value = "patched" }),
            options => options.PageLimit(2).Retry(0).Notifications(false).UpdateCallback(updatedIds.AddRange));

        if (includeMissing)
            await Assert.ThrowsAsync<DocumentNotFoundException>(patchAsync);
        else
            Assert.Equal(documents.Length, await patchAsync());

        Assert.Empty(configuration.Invoker.ReadLocations);
        Assert.Equal(new[] { 2, 2, 1 }, configuration.Invoker.BulkSizes);
        Assert.Equal(new[] { 0, 2, 4 }, writesBeforePage);
        Assert.Equal(documents.Select(document => document.Document.Id), updatedIds);
        Assert.Equal(documents.Select(document => (document.Index, document.Document.Id, document.Routing)),
            configuration.Invoker.Writes.Select(write => (write.Index, write.Id, write.Routing)));
        Assert.All(configuration.Invoker.Writes, write => Assert.Equal("patched", write.Source.GetProperty("value").GetString()));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task JsonAndActionPatch_WithResolvableIds_PreservesSearchAcrossIndexes(bool action)
    {
        var first = StoredDocument.Create(1);
        var duplicate = StoredDocument.Create(2, first.Document.Id);
        var other = StoredDocument.Create(3);
        using var configuration = new TestConfiguration([first, other]);
        using var repository = new TestRepository(configuration, [first, duplicate, other]);
        IPatchOperation patch = action
            ? new ActionPatch<TestDocument>(d =>
            {
                d.Value = "patched";
                return true;
            })
            : new JsonPatch(new PatchDocument(new ReplaceOperation { Path = "/value", Value = "patched" }));

        long patched = await repository.PatchAsync(new Ids(first.Document.Id, other.Document.Id), patch, o => o.Retry(0).Notifications(false));

        Assert.Equal(3, patched);
        Assert.Empty(configuration.Invoker.ReadLocations);
        Assert.Equal(2, repository.SearchedIds.Count);
        Assert.Equal(3, configuration.Invoker.Writes.Count);
        Assert.Equal(new[] { first.Index, duplicate.Index }, configuration.Invoker.Writes.Where(w => w.Id == first.Document.Id).Select(w => w.Index).Order());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task JsonAndActionPatch_WithMultipleSearchPages_WritesEachPageBeforeReadingTheNext(bool action)
    {
        var documents = Enumerable.Range(1, 5).Select(month => StoredDocument.Create(month, routing: $"route-{month}")).ToArray();
        using var configuration = new TestConfiguration(documents);
        using var repository = new TestRepository(configuration, documents, hasParent: true) { SearchPageSize = 2 };
        var writesBeforePage = new List<int>();
        repository.BeforeSearchPage = () => writesBeforePage.Add(configuration.Invoker.Writes.Count);
        var ids = new Ids(documents.Select(document => new Id(document.Document.Id, document.Routing)));

        long patched = await repository.PatchAsync(ids, CreateValuePatch(action),
            options => options.PageLimit(2).Retry(0).Notifications(false));

        Assert.Equal(documents.Length, patched);
        Assert.Equal(new[] { 2, 2, 1 }, configuration.Invoker.BulkSizes);
        Assert.Equal(new[] { 0, 2, 4 }, writesBeforePage);
        Assert.Equal(documents.Select(document => (document.Index, document.Document.Id, document.Routing)),
            configuration.Invoker.Writes.Select(write => (write.Index, write.Id, write.Routing)));
        Assert.All(configuration.Invoker.Writes, write => Assert.Equal("patched", write.Source.GetProperty("value").GetString()));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task JsonAndActionPatch_WithRoutedAndMissingIdsAcrossPages_PatchesOnlyRequestedLocations(bool action)
    {
        var firstRoute = StoredDocument.CreateSingleIndex("same") with { Routing = "first" };
        var otherRoute = StoredDocument.CreateSingleIndex("same") with { Routing = "other" };
        var other = StoredDocument.CreateSingleIndex("other");
        var deleted = StoredDocument.CreateSingleIndex("deleted", isDeleted: true);
        StoredDocument[] documents = [firstRoute, otherRoute, other, deleted];
        using var configuration = new TestConfiguration(documents);
        using var repository = new TestRepository(configuration, documents, hasParent: true, singleIndex: true) { SearchPageSize = 2 };
        var updatedIds = new List<string>();
        var ids = new Ids(new Id("same", "first"), new Id("other"), new Id("deleted"), new Id("missing"));

        long patched = await repository.PatchAsync(ids, CreateValuePatch(action),
            options => options.PageLimit(2).Retry(0).Notifications(false).UpdateCallback(updatedIds.AddRange));

        Assert.Equal(2, patched);
        Assert.Equal(new[] { 1, 1 }, configuration.Invoker.BulkSizes);
        Assert.Equal(new[] { ("same", "first"), ("other", (string?)null) },
            configuration.Invoker.Writes.Select(write => (write.Id, write.Routing)));
        Assert.Equal(new[] { "same", "other" }, updatedIds);
    }

    [Fact]
    public async Task ActionPatch_WithFailureInAnEarlyPage_NotifiesLaterSuccessesBeforeThrowing()
    {
        var documents = Enumerable.Range(1, 5).Select(month => StoredDocument.Create(month)).ToArray();
        using var configuration = new TestConfiguration(documents);
        configuration.Invoker.RejectWriteId = documents[0].Document.Id;
        using var repository = new TestRepository(configuration, documents) { SearchPageSize = 2 };
        var updatedIds = new List<string>();

        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync(
            new Ids(documents.Select(document => document.Document.Id)), CreateValuePatch(action: true),
            options => options.PageLimit(2).Retry(0).Notifications(false).UpdateCallback(updatedIds.AddRange)));

        Assert.Equal(new[] { 2, 2, 1 }, configuration.Invoker.BulkSizes);
        Assert.Equal(documents.Skip(1).Select(document => document.Document.Id), updatedIds);
        Assert.Contains("1 failed, 4 succeeded", exception.Message, StringComparison.Ordinal);
    }

    private sealed class TestDocument : IIdentity, IVersioned, ISupportSoftDeletes
    {
        public string Id { get; set; } = null!;
        public string? Version { get; set; }
        public string Keep { get; set; } = "preserved";
        public string Value { get; set; } = "before";
        public bool IsDeleted { get; set; }
    }

    private sealed record StoredDocument(string Index, string ReadIndex, TestDocument Document, string? Routing)
    {
        public static StoredDocument CreateSingleIndex(string id, bool isDeleted = false) =>
            new("patch-reads", "patch-reads", new TestDocument { Id = id, IsDeleted = isDeleted, Version = "1:7" }, null);

        public static StoredDocument Create(int month, string? id = null, string? routing = null)
        {
            id ??= ObjectId.GenerateNewId(new DateTime(2026, month, 15, 0, 0, 0, DateTimeKind.Utc)).ToString();
            return new StoredDocument($"patch-reads-v1-2026.{month:00}", $"patch-reads-2026.{month:00}", new TestDocument { Id = id, Keep = $"preserved-{month}-{routing}", Version = "1:7" }, routing);
        }
    }

    private sealed class TestRepository : ElasticRepositoryBase<TestDocument>
    {
        private readonly IReadOnlyList<StoredDocument> _visible;
        public List<string> SearchedIds { get; } = [];
        public int? SearchPageSize { get; init; }
        public Action? BeforeSearchPage { get; set; }

        public TestRepository(TestConfiguration configuration, IReadOnlyList<StoredDocument> visible, bool hasParent = false, bool singleIndex = false)
            : base(singleIndex ? new SingleTestIndex(configuration) : new TestIndex(configuration))
        {
            _visible = visible;
            HasParent = hasParent;
            DefaultPipeline = "test-pipeline";
            DisableCache();
        }

        public override async Task<FindResults<TResult>> FindAsAsync<TResult>(IRepositoryQuery query, ICommandOptions? options = null)
        {
            var ids = query.GetIds();
            SearchedIds.AddRange(ids);
            Assert.NotNull(options);
            Assert.True(options.SafeGetOption<bool>(FullDocumentSourceKey));
            await OnBeforeQueryAsync(query, options, typeof(TResult));
            var hits = _visible.Where(d => ids.Contains(d.Document.Id) && !query.GetExcludedIds().Contains(d.Document.Id) && ShouldReturnDocument(d.Document, options))
                .Select(d => new FindHit<TResult>(d.Document.Id, (TResult)(object)d.Document, 0, "1:7", d.Routing,
                    new DataDictionary { [ElasticDataKeys.Index] = d.Index })).ToList();
            if (SearchPageSize is not { } pageSize)
                return new FindResults<TResult>(hits, hits.Count);

            FindResults<TResult> CreatePage(int offset)
            {
                BeforeSearchPage?.Invoke();
                var results = new FindResults<TResult>(hits.Skip(offset).Take(pageSize), hits.Count,
                    getNextPageFunc: previous => Task.FromResult(CreatePage(previous.Page * pageSize)));
                ((IFindResults<TResult>)results).Page = offset / pageSize + 1;
                ((IFindResults<TResult>)results).HasMore = offset + pageSize < hits.Count;
                return results;
            }

            return CreatePage(0);
        }
    }

    private sealed class SingleTestIndex(TestConfiguration configuration) : Index<TestDocument>(configuration, "patch-reads")
    {
        public override Task EnsureIndexAsync(object? target) => Task.CompletedTask;
    }

    private sealed class TestIndex(TestConfiguration configuration) : MonthlyIndex<TestDocument>(configuration, "patch-reads")
    {
        public override Task EnsureIndexAsync(object? target) => Task.CompletedTask;
    }

    private sealed class TestConfiguration(IReadOnlyList<StoredDocument> documents) : ElasticConfiguration
    {
        public PatchInvoker Invoker { get; } = new(documents);
        protected override IRequestInvoker CreateRequestInvoker() => Invoker;
    }

    private sealed record WrittenDocument(string Index, string Id, string? Routing, long SequenceNumber, long PrimaryTerm, JsonElement Source);

    private sealed class PatchInvoker(IReadOnlyList<StoredDocument> documents) : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _owner = new();
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };
        public ResponseFactory ResponseFactory => _owner.ResponseFactory;
        public List<(string Index, string Id, string? Routing)> ReadLocations { get; } = [];
        public List<WrittenDocument> Writes { get; } = [];
        public int MultiGetRequests { get; private set; }
        public List<int> MultiGetSizes { get; } = [];
        public List<int> WritesBeforeMultiGet { get; } = [];
        public int BulkRequests { get; private set; }
        public List<int> BulkSizes { get; } = [];
        public Action? AfterMultiGet { get; set; }
        public string? RejectWriteId { get; set; }

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            Assert.NotNull(postData);
            using var requestStream = new MemoryStream();
            postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
            string body;
            if (endpoint.PathAndQuery.Contains("_mget", StringComparison.Ordinal))
            {
                MultiGetRequests++;
                using var request = JsonDocument.Parse(requestStream.ToArray());
                Assert.False(request.RootElement.TryGetProperty("_source", out _));
                MultiGetSizes.Add(request.RootElement.GetProperty("docs").GetArrayLength());
                WritesBeforeMultiGet.Add(Writes.Count);
                var docs = new List<object>();
                foreach (var item in request.RootElement.GetProperty("docs").EnumerateArray())
                {
                    string index = item.GetProperty("_index").GetString()!;
                    string id = item.GetProperty("_id").GetString()!;
                    string? routing = item.TryGetProperty("routing", out var route) ? route.GetString() : null;
                    ReadLocations.Add((index, id, routing));
                    var document = documents.SingleOrDefault(d => d.ReadIndex == index && d.Document.Id == id && d.Routing == routing);
                    docs.Add(new
                    {
                        _index = document?.Index ?? index.Replace("patch-reads-", "patch-reads-v1-", StringComparison.Ordinal),
                        _id = id,
                        _routing = routing,
                        found = document is not null,
                        _seq_no = 7,
                        _primary_term = 1,
                        _source = document is null ? null : new { id, keep = document.Document.Keep, value = document.Document.Value, isDeleted = document.Document.IsDeleted }
                    });
                }
                body = JsonSerializer.Serialize(new { docs });
                AfterMultiGet?.Invoke();
            }
            else
            {
                Assert.Contains("_bulk", endpoint.PathAndQuery, StringComparison.Ordinal);
                BulkRequests++;
                string[] lines = Encoding.UTF8.GetString(requestStream.ToArray()).Split('\n', StringSplitOptions.RemoveEmptyEntries);
                BulkSizes.Add(lines.Length / 2);
                var items = new List<object>();
                bool errors = false;
                for (int i = 0; i < lines.Length; i += 2)
                {
                    using var metadata = JsonDocument.Parse(lines[i]);
                    using var source = JsonDocument.Parse(lines[i + 1]);
                    var item = metadata.RootElement.GetProperty("index");
                    string index = item.GetProperty("_index").GetString()!;
                    string id = item.GetProperty("_id").GetString()!;
                    string? routing = item.TryGetProperty("routing", out var route) ? route.GetString() : null;
                    Writes.Add(new WrittenDocument(index, id, routing, item.GetProperty("if_seq_no").GetInt64(), item.GetProperty("if_primary_term").GetInt64(), source.RootElement.Clone()));
                    if (id == RejectWriteId)
                    {
                        errors = true;
                        items.Add(new { index = new { _index = index, _id = id, status = 400, error = new { type = "mapper_parsing_exception", reason = "rejected test document" } } });
                    }
                    else
                        items.Add(new { index = new { _index = index, _id = id, status = 200, result = "updated", _seq_no = 8, _primary_term = 1 } });
                }
                body = JsonSerializer.Serialize(new { took = 1, errors, items });
            }

            byte[] bytes = Encoding.UTF8.GetBytes(body);
            using var stream = new MemoryStream(bytes);
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, 200, _headers, stream, "application/json", bytes.Length, null, null);
        }

        public Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
            where TResponse : TransportResponse, new() => Task.FromResult(Request<TResponse>(endpoint, boundConfiguration, postData));

        public void Dispose() => ((IDisposable)_owner).Dispose();
    }
}
