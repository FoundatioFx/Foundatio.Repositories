using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Xunit;
using HttpMethod = Elastic.Transport.HttpMethod;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class WriteCacheIdentityTests
{
    [Theory]
    [InlineData("Add", false)]
    [InlineData("Add", true)]
    [InlineData("Save", false)]
    [InlineData("Save", true)]
    public async Task CachedWrite_WithSameIdAndRoutingInDifferentIndexes_RetainsEachPhysicalLocation(string operation, bool customCacheKey)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        TestDocument[] documents =
        [
            new() { Id = "same", Index = "first", Routing = "route", Value = "first document", Version = operation is "Save" ? "1:1" : null },
            new() { Id = "same", Index = "second", Routing = "route", Value = "second document", Version = operation is "Save" ? "1:1" : null }
        ];
        var options = new CommandOptions().Cache().Retry(0);
        if (customCacheKey)
            options.CacheKey("selected-documents");

        if (operation is "Add")
            await repository.AddAsync(documents, options);
        else
            await repository.SaveAsync(documents, options);

        var results = await repository.GetByIdsAsync(new Ids(new Id("same", "route")), options);

        Assert.Equal(2, results.Count);
        Assert.Equal(documents.Select(document => document.Value), results.Select(document => document.Value));
        Assert.Equal(new[] { "3:10", "3:20" }, results.Select(document => document.Version));
        Assert.Equal(new[] { ("first", "same", "route"), ("second", "same", "route") }, configuration.Invoker.Writes);
        Assert.Equal(1, configuration.Invoker.Requests);
        var cached = await repository.GetCachedHitsAsync(customCacheKey ? "selected-documents" : "same");
        Assert.Equal(new[] { "first-v1", "second-v1" }, cached.Select(hit => hit.GetIndex()));
    }

    [Theory]
    [InlineData("Add", false)]
    [InlineData("Add", true)]
    [InlineData("Save", false)]
    [InlineData("Save", true)]
    public async Task CachedSingleWrite_ThroughAnAlias_RetainsTheResponsePhysicalIndex(string operation, bool customCacheKey)
    {
        var document = new TestDocument { Id = "one", Index = "first", Routing = "route", Value = "written value", Version = operation is "Save" ? "1:1" : null };
        using var configuration = new TestConfiguration(operation is "Save" ? [document] : []);
        using var repository = new TestRepository(configuration);
        var options = new CommandOptions().Cache().Retry(0);
        if (customCacheKey)
            options.CacheKey("selected-document");

        if (operation is "Add")
            await repository.AddAsync(document, options);
        else
            await repository.SaveAsync(document, options);

        var result = await repository.GetByIdAsync(new Id(document.Id, document.Routing), options);

        Assert.NotNull(result);
        Assert.Equal(document.Value, result.Value);
        Assert.Equal("3:10", result.Version);
        Assert.Equal(1, configuration.Invoker.IndexRequests);
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Empty(configuration.Invoker.Reads);
        var cached = Assert.Single(await repository.GetCachedHitsAsync(customCacheKey ? "selected-document" : document.Id));
        Assert.Equal("first-v1", cached.GetIndex());
        Assert.Equal("route", cached.Routing);
        Assert.Equal("3:10", cached.Version);
    }

    [Theory]
    [InlineData("Add", false, false)]
    [InlineData("Add", true, false)]
    [InlineData("Save", false, false)]
    [InlineData("Save", true, false)]
    [InlineData("Add", false, true)]
    [InlineData("Add", true, true)]
    [InlineData("Save", false, true)]
    [InlineData("Save", true, true)]
    public async Task CachedWrite_WhenPostWriteHandlerSavesTheDocument_PreservesTheNewerVersion(string operation, bool customCacheKey, bool reloadDocument)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        var document = new TestDocument { Id = "one", Index = "first", Routing = "route", Value = "outer write" };
        var options = new CommandOptions().Cache().Retry(0);
        if (customCacheKey)
            options.CacheKey("selected-document");
        if (operation is "Save")
        {
            await repository.AddAsync(document, new CommandOptions().Retry(0));
            if (customCacheKey)
            {
                var original = await repository.GetByIdAsync(new Id(document.Id, document.Routing), options);
                Assert.NotNull(original);
                Assert.Equal("outer write", original.Value);
                Assert.Equal("3:10", original.Version);
                var cached = Assert.Single(await repository.GetCachedHitsAsync("selected-document"));
                Assert.Equal("3:10", cached.Version);
            }
        }

        bool savedFromHandler = false;
        async Task SaveFromHandlerAsync(TestDocument current)
        {
            if (savedFromHandler)
                return;

            savedFromHandler = true;
            Assert.Same(document, current);
            if (reloadDocument)
            {
                var reloaded = await repository.GetByIdAsync(new Id(current.Id, current.Routing), new CommandOptions().Cache(false));
                Assert.NotNull(reloaded);
                current = reloaded;
                Assert.NotSame(document, current);
            }

            current.Value = "nested save";
            await repository.SaveAsync(current, new CommandOptions().Cache(false).Retry(0));
        }
        if (operation is "Add")
            repository.DocumentsAdded.AddHandler((_, args) => SaveFromHandlerAsync(Assert.Single(args.Documents)));
        else
            repository.DocumentsSaved.AddHandler((_, args) => SaveFromHandlerAsync(Assert.Single(args.Documents).Value));

        if (operation is "Add")
            await repository.AddAsync(document, options);
        else
            await repository.SaveAsync(document, options);

        Assert.True(savedFromHandler);
        string latestVersion = operation is "Add" ? "3:20" : "3:30";
        if (!reloadDocument)
            Assert.Equal(latestVersion, document.Version);
        var current = await repository.GetByIdAsync(new Id(document.Id, document.Routing), options);
        Assert.NotNull(current);
        Assert.Equal("nested save", current.Value);
        Assert.Equal(latestVersion, current.Version);

        current.Value = "after nested save";
        await repository.SaveAsync(current, new CommandOptions().Retry(0));

        Assert.Equal(operation is "Add" ? "3:30" : "3:40", current.Version);
        var persisted = await repository.GetByIdAsync(new Id(document.Id, document.Routing), new CommandOptions().Cache(false));
        Assert.NotNull(persisted);
        Assert.Equal(current.Value, persisted.Value);
        Assert.Equal(current.Version, persisted.Version);
    }

    [Theory]
    [InlineData("Add", false)]
    [InlineData("Add", true)]
    [InlineData("Save", false)]
    [InlineData("Save", true)]
    public async Task CachedWrite_WhenFinalKeyEvictionFails_PreservesCallbackFailure(string operation, bool handlerThrows)
    {
        using var cache = new FailingRemovalCache();
        using var configuration = new TestConfiguration(cache: cache);
        using var repository = new TestRepository(configuration);
        var document = new TestDocument { Id = "one", Index = "first", Routing = "route", Value = "persisted" };
        var options = new CommandOptions().Cache().CacheKey("selected-document").Retry(0);
        if (operation is "Save")
            await repository.AddAsync(document, new CommandOptions().Retry(0));

        Task OnWrittenAsync()
        {
            cache.FailRemoval = true;
            return handlerThrows
                ? Task.FromException(new InvalidOperationException("Callback failed."))
                : Task.CompletedTask;
        }
        if (operation is "Add")
            repository.DocumentsAdded.AddHandler((_, _) => OnWrittenAsync());
        else
            repository.DocumentsSaved.AddHandler((_, _) => OnWrittenAsync());

        var exception = await Record.ExceptionAsync(async () =>
        {
            if (operation is "Add")
                await repository.AddAsync(document, options);
            else
                await repository.SaveAsync(document, options);
        });

        if (handlerThrows)
        {
            Assert.IsType<InvalidOperationException>(exception);
            Assert.Equal("Callback failed.", exception.Message);
        }
        else
        {
            Assert.IsType<IOException>(exception);
            Assert.Equal("Cache cleanup failed.", exception.Message);
        }
        Assert.True(cache.FailedRemovals > 0);
    }

    [Theory]
    [InlineData("Add", false)]
    [InlineData("Add", true)]
    [InlineData("Save", false)]
    [InlineData("Save", true)]
    public async Task CachedWrite_WhenHandlerRepopulatesCustomKeyBeforeNestedSave_DoesNotLeaveEarlierSourceCached(string operation, bool handlerThrows)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        var document = new TestDocument { Id = "one", Index = "first", Routing = "route", Value = "outer write" };
        var options = new CommandOptions().Cache().CacheKey("selected-document").Retry(0);
        if (operation is "Save")
            await repository.AddAsync(document, new CommandOptions().Retry(0));

        bool savedFromHandler = false;
        async Task SaveFromHandlerAsync(TestDocument current, ICommandOptions callbackOptions)
        {
            if (savedFromHandler)
                return;

            savedFromHandler = true;
            var reloaded = await repository.GetByIdAsync(new Id(current.Id, current.Routing), callbackOptions);
            Assert.NotNull(reloaded);
            Assert.NotSame(current, reloaded);
            Assert.Equal("outer write", Assert.Single(await repository.GetCachedHitsAsync("selected-document")).Document!.Value);
            reloaded.Value = "nested save";
            await repository.SaveAsync(reloaded, new CommandOptions().Cache(false).Retry(0));
            if (handlerThrows)
                throw new InvalidOperationException("Post-write handler failed after the nested save.");
        }
        if (operation is "Add")
            repository.DocumentsAdded.AddHandler((_, args) => SaveFromHandlerAsync(Assert.Single(args.Documents), args.Options));
        else
            repository.DocumentsSaved.AddHandler((_, args) => SaveFromHandlerAsync(Assert.Single(args.Documents).Value, args.Options));

        if (handlerThrows)
        {
            var exception = operation is "Add"
                ? await Assert.ThrowsAsync<InvalidOperationException>(async () => await repository.AddAsync(document, options))
                : await Assert.ThrowsAsync<InvalidOperationException>(async () => await repository.SaveAsync(document, options));
            Assert.Equal("Post-write handler failed after the nested save.", exception.Message);
        }
        else if (operation is "Add")
        {
            await repository.AddAsync(document, options);
        }
        else
        {
            await repository.SaveAsync(document, options);
        }

        Assert.True(savedFromHandler);
        var result = await repository.GetByIdAsync(new Id(document.Id, document.Routing), options);
        Assert.NotNull(result);
        Assert.Equal("nested save", result.Value);
        Assert.Equal(operation is "Add" ? "3:20" : "3:30", result.Version);
    }

    [Theory]
    [InlineData("Add", false)]
    [InlineData("Add", true)]
    [InlineData("Save", false)]
    [InlineData("Save", true)]
    public async Task CachedWrite_WhenPostWriteHandlerMutatesTheModel_CachesThePersistedSource(string operation, bool customCacheKey)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        var document = new TestDocument { Id = "one", Index = "first", Routing = "route", Value = "persisted source" };
        var options = new CommandOptions().Cache().Retry(0);
        if (customCacheKey)
            options.CacheKey("selected-document");
        if (operation is "Save")
            await repository.AddAsync(document, new CommandOptions().Retry(0));

        if (operation is "Add")
            repository.DocumentsAdded.AddSyncHandler((_, args) => Assert.Single(args.Documents).Value = "unsaved callback change");
        else
            repository.DocumentsSaved.AddSyncHandler((_, args) => Assert.Single(args.Documents).Value.Value = "unsaved callback change");

        if (operation is "Add")
            await repository.AddAsync(document, options);
        else
            await repository.SaveAsync(document, options);

        Assert.Equal("unsaved callback change", document.Value);
        var cached = await repository.GetByIdAsync(new Id(document.Id, document.Routing), options);
        Assert.NotNull(cached);
        Assert.Equal("persisted source", cached.Value);
        Assert.Equal(document.Version, cached.Version);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SaveAsync_WhenPostWriteHandlerThrows_DoesNotLeaveThePreviousSourceCached(bool populateCache)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        var document = new TestDocument { Id = "one", Index = "first", Routing = "route", Value = "old source" };
        await repository.AddAsync(document, new CommandOptions().Cache().Retry(0));
        Assert.Equal("old source", Assert.Single(await repository.GetCachedHitsAsync(document.Id)).Document!.Value);
        document.Value = "committed source";
        repository.DocumentsSaved.AddSyncHandler((_, _) => throw new InvalidOperationException("Post-write handler failed."));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => repository.SaveAsync(document, new CommandOptions().Cache(populateCache).Retry(0)));

        Assert.Equal("Post-write handler failed.", exception.Message);
        var current = await repository.GetByIdAsync(new Id(document.Id, document.Routing), new CommandOptions().Cache());
        Assert.NotNull(current);
        Assert.Equal("committed source", current.Value);
        Assert.Equal("3:20", current.Version);
    }

    [Fact]
    public async Task RemoveAsync_WhenPostWriteHandlerThrows_DoesNotLeaveTheRemovedDocumentCached()
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        var document = new TestDocument { Id = "one", Index = "first", Routing = "route", Value = "removed source" };
        await repository.AddAsync(document, new CommandOptions().Cache().Retry(0));
        repository.DocumentsRemoved.AddSyncHandler((_, _) => throw new InvalidOperationException("Post-remove handler failed."));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => repository.RemoveAsync(document, new CommandOptions().Retry(0)));

        Assert.Equal("Post-remove handler failed.", exception.Message);
        Assert.Null(await repository.GetByIdAsync(new Id(document.Id, document.Routing), new CommandOptions().Cache()));
    }

    [Theory]
    [InlineData("Removed", false)]
    [InlineData("Removed", true)]
    [InlineData("Changed", false)]
    [InlineData("Changed", true)]
    public async Task RemoveAsync_WithSuccessfulAndMissingDocuments_EvictsBothBeforePostWriteHandlers(string eventName, bool handlerThrows)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        TestDocument[] documents =
        [
            new() { Id = "one", Index = "first", Routing = "route", Value = "removed by this call" },
            new() { Id = "two", Index = "first", Routing = "route", Value = "already removed" }
        ];
        await repository.AddAsync(documents, new CommandOptions().Cache().Retry(0));
        configuration.Invoker.RemoveStoredDocument(documents[1]);
        Assert.Equal("already removed", Assert.Single(await repository.GetCachedHitsAsync("two")).Document!.Value);

        int callbacks = 0;
        TestDocument? observedMissing = null;
        var notifiedIds = new List<string>();
        async Task OnRemovedAsync()
        {
            callbacks++;
            observedMissing = await repository.GetByIdAsync(new Id("two", "route"), new CommandOptions().Cache());
            if (handlerThrows)
                throw new InvalidOperationException("Post-remove handler failed.");
        }
        if (eventName is "Removed")
        {
            repository.DocumentsRemoved.AddHandler((_, args) =>
            {
                notifiedIds.AddRange(args.Documents.Select(document => document.Id));
                return OnRemovedAsync();
            });
        }
        else
        {
            repository.DocumentsChanged.AddHandler((_, args) =>
            {
                notifiedIds.AddRange(args.Documents.Select(document => document.Value.Id));
                return OnRemovedAsync();
            });
        }

        var exception = await Record.ExceptionAsync(() => repository.RemoveAsync(documents, new CommandOptions().Retry(0)));

        if (handlerThrows)
        {
            Assert.IsType<InvalidOperationException>(exception);
            Assert.Equal("Post-remove handler failed.", exception.Message);
        }
        else
        {
            Assert.Null(exception);
        }
        Assert.Equal(1, callbacks);
        Assert.Equal(new[] { "one" }, notifiedIds);
        Assert.Null(observedMissing);
        Assert.Null(await repository.GetByIdAsync(new Id("one", "route"), new CommandOptions().Cache()));
        Assert.Null(await repository.GetByIdAsync(new Id("two", "route"), new CommandOptions().Cache()));
    }

    [Theory]
    [InlineData("Removing")]
    [InlineData("Changing")]
    public async Task RemoveAsync_WhenBeforeHandlerChangesLocation_UsesUpdatedTargetForDeleteAndCompletion(string eventName)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration, notificationsEnabled: true);
        var source = new TestDocument { Id = "source", Index = "first", Routing = "source-route", Value = "original location" };
        var target = new TestDocument { Id = "target", Index = "second", Routing = "target-route", Value = "selected by the handler" };
        await repository.AddAsync(new[] { source, target }, new CommandOptions().Cache().Retry(0));
        repository.Notifications.Clear();

        void ChangeLocation(TestDocument document)
        {
            document.Id = target.Id;
            document.Index = target.Index;
            document.Routing = target.Routing;
            document.Version = target.Version;
        }
        if (eventName is "Removing")
            repository.DocumentsRemoving.AddSyncHandler((_, args) => ChangeLocation(Assert.Single(args.Documents)));
        else
            repository.DocumentsChanging.AddSyncHandler((_, args) => ChangeLocation(Assert.Single(args.Documents).Value));
        var removedIds = new List<string>();
        repository.DocumentsRemoved.AddSyncHandler((_, args) => removedIds.AddRange(args.Documents.Select(document => document.Id)));

        await repository.RemoveAsync(source, new CommandOptions().Retry(0));

        Assert.Equal((target.Index, target.Id, target.Routing, target.Version), Assert.Single(configuration.Invoker.Deletes));
        Assert.Equal(new[] { "target" }, removedIds);
        Assert.Equal((ChangeType.Removed, (string?)"target"), Assert.Single(repository.Notifications));
        Assert.False(await repository.CacheContainsAsync("target"));
        var cachedSource = Assert.Single(await repository.GetCachedHitsAsync("source"));
        Assert.Equal("original location", cachedSource.Document!.Value);
        Assert.NotNull(await repository.GetByIdAsync(new Id("source", "source-route"), new CommandOptions().Cache(false)));
        Assert.Null(await repository.GetByIdAsync(new Id("target", "target-route"), new CommandOptions().Cache()));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task GetByIdsAsync_WithCachedRouteAndUnroutedInput_FindsOtherMatchingRoutes(bool customCacheKey, bool readOnlyCache)
    {
        TestDocument[] documents =
        [
            new() { Id = "same", Index = "first", Routing = "first", Value = "first route" },
            new() { Id = "same", Index = "first", Routing = "second", Value = "second route" }
        ];
        using var configuration = new TestConfiguration(documents, hasMultipleIndexes: false);
        using var repository = new TestRepository(configuration);
        var options = new CommandOptions().Cache();
        if (customCacheKey)
            options.CacheKey("selected-documents");
        Assert.NotNull(await repository.GetByIdAsync(new Id("same", "first"), options));
        if (readOnlyCache)
            options.Cache(false).ReadCache();

        var results = await repository.GetByIdsAsync(new Ids(new Id("same", "first"), new Id("same")), options);

        Assert.Equal(documents.Select(document => document.Value), results.Select(document => document.Value));
        Assert.Equal(new Id("same", "first"), Assert.Single(configuration.Invoker.Reads));
        Assert.Equal(1, configuration.Invoker.Searches);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetByIdsAsync_WithOnlyExplicitRoutes_UsesCachedDocuments(bool customCacheKey)
    {
        using var configuration = new TestConfiguration(hasMultipleIndexes: false);
        using var repository = new TestRepository(configuration);
        TestDocument[] documents =
        [
            new() { Id = "same", Index = "first", Routing = "first", Value = "first route" },
            new() { Id = "same", Index = "first", Routing = "second", Value = "second route" }
        ];
        var options = new CommandOptions().Cache().Retry(0);
        if (customCacheKey)
            options.CacheKey("selected-documents");
        await repository.AddAsync(documents, options);

        var results = await repository.GetByIdsAsync(new Ids(new Id("same", "first"), new Id("same", "second")), options);

        Assert.Equal(documents.Select(document => document.Value), results.Select(document => document.Value));
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Empty(configuration.Invoker.Reads);
        Assert.Equal(0, configuration.Invoker.Searches);
    }

    [Theory]
    [InlineData("Add", false)]
    [InlineData("Add", true)]
    [InlineData("Save", false)]
    [InlineData("Save", true)]
    public async Task CachedWrite_WithIngestPipeline_ReturnsTransformedSource(string operation, bool customCacheKey)
    {
        TestDocument[] documents =
        [
            new() { Id = "one", Index = "first", Routing = "route", Value = "FIRST VALUE", Version = operation is "Save" ? "1:1" : null },
            new() { Id = "two", Index = "first", Routing = "route", Value = "SECOND VALUE", Version = operation is "Save" ? "1:1" : null }
        ];
        var originals = operation is "Save"
            ? documents.Select(document => new TestDocument { Id = document.Id, Index = document.Index, Routing = document.Routing, Value = "original", Version = "1:1" }).ToArray()
            : [];
        using var configuration = new TestConfiguration(originals, hasMultipleIndexes: false);
        using var repository = new TestRepository(configuration, "lowercase-value");
        var options = new CommandOptions().Cache().Retry(0);
        if (customCacheKey)
            options.CacheKey("selected-documents");
        foreach (var original in originals)
            Assert.NotNull(await repository.GetByIdAsync(new Id(original.Id, original.Routing), options));

        if (operation is "Add")
            await repository.AddAsync(documents, options);
        else
            await repository.SaveAsync(documents, options);

        foreach (var document in documents.Reverse())
        {
            var result = await repository.GetByIdAsync(new Id(document.Id, document.Routing), options);
            Assert.NotNull(result);
            Assert.Equal(document.Value.ToLowerInvariant(), result.Value);
            Assert.Equal(document.Version, result.Version);
        }
        Assert.Equal(documents.Length, configuration.Invoker.PipelineWrites);
    }

    public sealed class TestDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string Index { get; set; } = null!;
        public string Routing { get; set; } = null!;
        public string Value { get; set; } = null!;
        public string? Version { get; set; }
    }

    private sealed class TestRepository : ElasticRepositoryBase<TestDocument>
    {
        public TestRepository(TestConfiguration configuration, string? pipeline = null, bool notificationsEnabled = false) : base(new TestIndex(configuration))
        {
            HasParent = true;
            GetParentIdFunc = document => document.Routing;
            NotificationsEnabled = notificationsEnabled;
            DefaultPipeline = pipeline;
        }

        public List<(ChangeType ChangeType, string? Id)> Notifications { get; } = [];
        public Task<bool> CacheContainsAsync(string id) => Cache.ExistsAsync(id);

        protected override Task PublishMessageAsync(EntityChanged message, TimeSpan? delay = null)
        {
            Notifications.Add((message.ChangeType, message.Id));
            return Task.CompletedTask;
        }

        public async Task<ICollection<FindHit<TestDocument>>> GetCachedHitsAsync(string key)
        {
            var value = await Cache.GetAsync<ICollection<FindHit<TestDocument>>>(key);
            Assert.True(value.HasValue);
            return value.Value;
        }
    }

    private sealed class TestIndex : Index<TestDocument>
    {
        public TestIndex(TestConfiguration configuration) : base(configuration, "write-cache-tests")
        {
            HasMultipleIndexes = configuration.HasMultipleIndexes;
        }

        public override string GetIndex(object target) => target is TestDocument document ? document.Index : "first";
        public override Task EnsureIndexAsync(object? target) => Task.CompletedTask;
    }

    private sealed class FailingRemovalCache : InMemoryCacheClient, ICacheClient
    {
        public bool FailRemoval { get; set; }
        public int FailedRemovals { get; private set; }

        Task<bool> ICacheClient.RemoveAsync(string key)
        {
            if (!FailRemoval)
                return base.RemoveAsync(key);

            FailedRemovals++;
            return Task.FromException<bool>(new IOException("Cache cleanup failed."));
        }
    }

    private sealed class TestConfiguration(IReadOnlyList<TestDocument>? documents = null, bool hasMultipleIndexes = true, ICacheClient? cache = null) : ElasticConfiguration(cacheClient: cache)
    {
        public bool HasMultipleIndexes { get; } = hasMultipleIndexes;
        public CacheResponseInvoker Invoker { get; } = new(documents ?? []);
        protected override IRequestInvoker CreateRequestInvoker() => Invoker;
    }

    private sealed class CacheResponseInvoker(IReadOnlyList<TestDocument> documents) : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _owner = new();
        private readonly Dictionary<(string Index, string Id, string Routing), TestDocument> _documents = documents.ToDictionary(document => (document.Index, document.Id, document.Routing));
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };
        public ResponseFactory ResponseFactory => _owner.ResponseFactory;
        public int Requests { get; private set; }
        public int IndexRequests { get; private set; }
        public int Searches { get; private set; }
        public int PipelineWrites { get; private set; }
        public List<(string Index, string Id, string Routing)> Writes { get; } = [];
        public List<(string Index, string Id, string Routing, string? Version)> Deletes { get; } = [];
        public List<Id> Reads { get; } = [];

        public void RemoveStoredDocument(TestDocument document)
        {
            Assert.True(_documents.Remove((document.Index, document.Id, document.Routing)));
        }

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            Requests++;
            int statusCode = 200;
            object response;
            if (endpoint.PathAndQuery.Contains("_bulk", StringComparison.Ordinal))
            {
                response = CreateBulkResponse(boundConfiguration, postData);
            }
            else if (endpoint.Method is HttpMethod.PUT && endpoint.PathAndQuery.Contains("/_doc/", StringComparison.Ordinal))
            {
                IndexRequests++;
                var uri = new Uri(new Uri("http://localhost/"), endpoint.PathAndQuery);
                string index = Uri.UnescapeDataString(uri.Segments[1].TrimEnd('/'));
                string id = Uri.UnescapeDataString(uri.Segments[^1]);
                var parameters = uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries)
                    .Select(parameter => parameter.Split('=', 2))
                    .ToDictionary(pair => Uri.UnescapeDataString(pair[0]), pair => Uri.UnescapeDataString(pair[1]));
                string routing = parameters["routing"];
                var key = (index, id, routing);
                _documents.TryGetValue(key, out var stored);
                var storedVersion = ElasticDocumentVersion.Parse(stored?.Version);
                if (parameters.TryGetValue("if_seq_no", out string? sequenceNumber)
                    && (storedVersion.SequenceNumber != Int64.Parse(sequenceNumber)
                        || storedVersion.PrimaryTerm != Int64.Parse(parameters["if_primary_term"])))
                {
                    statusCode = 409;
                    response = new { status = 409, error = new { type = "version_conflict_engine_exception", reason = "The conditional write used a stale version." } };
                }
                else
                {
                    Assert.NotNull(postData);
                    using var requestStream = new MemoryStream();
                    postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                    using var source = JsonDocument.Parse(requestStream.ToArray());
                    string value = source.RootElement.GetProperty("value").GetString()!;
                    Writes.Add(key);
                    _documents[key] = new TestDocument { Id = id, Index = index, Routing = routing, Value = value, Version = $"3:{Writes.Count * 10}" };
                    response = new { _index = $"{index}-v1", _id = id, result = parameters.GetValueOrDefault("op_type") is "create" ? "created" : "updated", _seq_no = Writes.Count * 10, _primary_term = 3, _version = Writes.Count, _shards = new { total = 1, successful = 1, failed = 0 } };
                }
            }
            else if (endpoint.Method is HttpMethod.DELETE && endpoint.PathAndQuery.Contains("/_doc/", StringComparison.Ordinal))
            {
                var uri = new Uri(new Uri("http://localhost/"), endpoint.PathAndQuery);
                string index = Uri.UnescapeDataString(uri.Segments[1].TrimEnd('/'));
                string id = Uri.UnescapeDataString(uri.Segments[^1]);
                var parameters = uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries)
                    .Select(parameter => parameter.Split('=', 2))
                    .ToDictionary(pair => Uri.UnescapeDataString(pair[0]), pair => Uri.UnescapeDataString(pair[1]));
                string routing = parameters["routing"];
                string? version = parameters.TryGetValue("if_seq_no", out string? sequenceNumber)
                    ? $"{parameters["if_primary_term"]}:{sequenceNumber}"
                    : null;
                Deletes.Add((index, id, routing, version));
                bool removed = _documents.Remove((index, id, routing));
                statusCode = removed ? 200 : 404;
                response = new { _index = $"{index}-v1", _id = id, result = removed ? "deleted" : "not_found", _seq_no = 30, _primary_term = 3, _version = 3, _shards = new { total = 1, successful = 1, failed = 0 } };
            }
            else if (endpoint.PathAndQuery.Contains("_search", StringComparison.Ordinal))
            {
                Searches++;
                var hits = _documents.Values.Select(document => new
                {
                    _index = $"{document.Index}-v1",
                    _id = document.Id,
                    _routing = document.Routing,
                    _seq_no = 10,
                    _primary_term = 3,
                    _source = new { id = document.Id, index = document.Index, routing = document.Routing, value = document.Value }
                });
                response = new { took = 1, timed_out = false, hits = new { total = new { value = _documents.Count, relation = "eq" }, hits } };
            }
            else if (endpoint.PathAndQuery.Contains("_mapping", StringComparison.Ordinal))
            {
                response = new Dictionary<string, object> { ["write-cache-tests"] = new { mappings = new { properties = new { id = new { type = "keyword" } } } } };
            }
            else
            {
                Assert.Contains("/_doc/", endpoint.PathAndQuery, StringComparison.Ordinal);
                var uri = new Uri(new Uri("http://localhost/"), endpoint.PathAndQuery);
                string id = Uri.UnescapeDataString(uri.Segments[^1]);
                string? routing = uri.Query.TrimStart('?').Split('&')
                    .FirstOrDefault(value => value.StartsWith("routing=", StringComparison.Ordinal))?["routing=".Length..];
                routing = routing is null ? null : Uri.UnescapeDataString(routing);
                Reads.Add(new Id(id, routing));
                var document = _documents.Values.SingleOrDefault(document => document.Id == id && document.Routing == routing);
                if (document is null)
                {
                    statusCode = 404;
                    response = new { _index = "first-v1", _id = id, found = false };
                }
                else
                {
                    var version = ElasticDocumentVersion.Parse(document.Version ?? "3:10");
                    response = new
                    {
                        _index = $"{document.Index}-v1",
                        _id = id,
                        _routing = routing,
                        found = true,
                        _seq_no = version.SequenceNumber,
                        _primary_term = version.PrimaryTerm,
                        _source = new { id, index = document.Index, routing, value = document.Value }
                    };
                }
            }

            byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(response);
            using var stream = new MemoryStream(bytes);
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, statusCode, _headers, stream, "application/json", bytes.Length, null, null);
        }

        private object CreateBulkResponse(BoundConfiguration boundConfiguration, PostData? postData)
        {
            Assert.NotNull(postData);
            using var requestStream = new MemoryStream();
            postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
            requestStream.Position = 0;
            using var reader = new StreamReader(requestStream);
            var items = new List<object>();
            while (reader.ReadLine() is { } line)
            {
                using var metadata = JsonDocument.Parse(line);
                var operation = Assert.Single(metadata.RootElement.EnumerateObject());
                string index = operation.Value.GetProperty("_index").GetString()!;
                string id = operation.Value.GetProperty("_id").GetString()!;
                string routing = operation.Value.GetProperty("routing").GetString()!;
                if (operation.Name is "delete")
                {
                    bool removed = _documents.Remove((index, id, routing));
                    items.Add(new { delete = new { _index = $"{index}-v1", _id = id, status = removed ? 200 : 404, result = removed ? "deleted" : "not_found", _seq_no = 30, _primary_term = 3 } });
                    continue;
                }

                Writes.Add((index, id, routing));
                string? documentJson = reader.ReadLine();
                Assert.NotNull(documentJson);
                using var source = JsonDocument.Parse(documentJson);
                string value = source.RootElement.GetProperty("value").GetString()!;
                if (operation.Value.TryGetProperty("pipeline", out var pipeline) && pipeline.GetString() is "lowercase-value")
                {
                    value = value.ToLowerInvariant();
                    PipelineWrites++;
                }
                _documents[(index, id, routing)] = new TestDocument { Id = id, Index = index, Routing = routing, Value = value, Version = $"3:{Writes.Count * 10}" };
                items.Add(new Dictionary<string, object>
                {
                    [operation.Name] = new
                    {
                        _index = $"{index}-v1",
                        _id = id,
                        status = operation.Name is "create" ? 201 : 200,
                        result = operation.Name is "create" ? "created" : "updated",
                        _seq_no = Writes.Count * 10,
                        _primary_term = 3
                    }
                });
            }

            return new { took = 1, errors = false, items };
        }

        public Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
            where TResponse : TransportResponse, new()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Request<TResponse>(endpoint, boundConfiguration, postData));
        }

        public void Dispose() => ((IDisposable)_owner).Dispose();
    }
}
