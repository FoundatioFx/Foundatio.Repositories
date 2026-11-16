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
using Foundatio.Repositories.Exceptions;
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
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task CachedSave_WithRepeatedDocumentLocation_ReturnsTheFinalPersistedSource(bool customCacheKey, bool retryFirstWrite)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        configuration.Invoker.TransientIndexFailures = retryFirstWrite ? 1 : 0;
        TestDocument[] documents =
        [
            new() { Id = "one", Index = "first", Routing = "route", Value = "first source" },
            new() { Id = "one", Index = "first", Routing = "route", Value = "second source" }
        ];
        var options = new CommandOptions().SkipVersionCheck().Cache().Retry(1);
        if (customCacheKey)
            options.CacheKey("selected-documents");

        await repository.SaveAsync(documents, options);

        Assert.Equal(2, configuration.Invoker.Writes.Count);
        var persisted = await repository.GetByIdAsync(new Id("one", "route"), new CommandOptions().Cache(false));
        Assert.NotNull(persisted);
        var cached = await repository.GetByIdAsync(new Id("one", "route"), options);
        Assert.NotNull(cached);
        Assert.Equal(persisted.Value, cached.Value);
        Assert.Equal(persisted.Version, cached.Version);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CachedSave_WhenLaterDuplicateAppliesButReturnsUnknownOutcome_DoesNotCacheEarlierSuccess(bool customCacheKey)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        configuration.Invoker.AmbiguousBulkWriteNumber = 2;
        TestDocument[] documents =
        [
            new() { Id = "one", Index = "first", Routing = "route", Value = "confirmed earlier source" },
            new() { Id = "one", Index = "first", Routing = "route", Value = "applied later source" }
        ];
        var options = new CommandOptions().SkipVersionCheck().Cache().Retry(0);
        if (customCacheKey)
            options.CacheKey("selected-documents");

        await Assert.ThrowsAsync<DocumentException>(() => repository.SaveAsync(documents, options));

        Assert.Equal(2, configuration.Invoker.Writes.Count);
        var persisted = await repository.GetByIdAsync(new Id("one", "route"), new CommandOptions().Cache(false));
        Assert.NotNull(persisted);
        Assert.Equal("applied later source", persisted.Value);
        var cached = await repository.GetByIdAsync(new Id("one", "route"), options);
        Assert.NotNull(cached);
        Assert.Equal(persisted.Value, cached.Value);
        Assert.Equal(persisted.Version, cached.Version);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CachedSave_WhenDifferentRoutesShareOneShard_ReturnsTheFinalPersistedSource(bool customCacheKey)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        configuration.Invoker.SingleShard = true;
        TestDocument[] documents =
        [
            new() { Id = "one", Index = "first", Routing = "first-route", Value = "first source" },
            new() { Id = "one", Index = "first", Routing = "second-route", Value = "second source" }
        ];
        var options = new CommandOptions().SkipVersionCheck().Cache().Retry(0);
        if (customCacheKey)
            options.CacheKey("selected-documents");

        await repository.SaveAsync(documents, options);

        Assert.Equal(2, configuration.Invoker.Writes.Count);
        foreach (var document in documents)
        {
            var id = new Id(document.Id, document.Routing);
            var persisted = await repository.GetByIdAsync(id, new CommandOptions().Cache(false));
            Assert.NotNull(persisted);
            Assert.Equal("second source", persisted.Value);
            Assert.Equal("second-route", persisted.Routing);
            var cached = await repository.GetByIdAsync(id, options);
            Assert.NotNull(cached);
            Assert.Equal(persisted.Value, cached.Value);
            Assert.Equal(persisted.Routing, cached.Routing);
            Assert.Equal(persisted.Version, cached.Version);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CachedSave_WhenAnotherAliasRetryAppliesButReturnsUnknownOutcome_DoesNotCacheEarlierSuccess(bool customCacheKey)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        configuration.Invoker.IndexAliases.Add("first", "shared-v1");
        configuration.Invoker.IndexAliases.Add("second", "shared-v1");
        configuration.Invoker.TransientBulkOperationNumber = 2;
        configuration.Invoker.AmbiguousIndexWrite = true;
        TestDocument[] documents =
        [
            new() { Id = "one", Index = "first", Routing = "route", Value = "confirmed earlier source" },
            new() { Id = "one", Index = "second", Routing = "route", Value = "applied retry source" }
        ];
        var options = new CommandOptions().SkipVersionCheck().Cache().Retry(1);
        if (customCacheKey)
            options.CacheKey("selected-documents");

        await Assert.ThrowsAsync<DocumentException>(() => repository.SaveAsync(documents, options));

        Assert.Equal(2, configuration.Invoker.Requests);
        Assert.Equal(1, configuration.Invoker.IndexRequests);
        Assert.Equal(new[] { ("first", "one", "route"), ("second", "one", "route") }, configuration.Invoker.Writes);
        var persisted = await repository.GetByIdAsync(new Id("one", "route"), new CommandOptions().Cache(false));
        Assert.NotNull(persisted);
        Assert.Equal("applied retry source", persisted.Value);
        Assert.Equal("second", persisted.Index);
        var cached = await repository.GetByIdAsync(new Id("one", "route"), options);
        Assert.NotNull(cached);
        Assert.Equal(persisted.Value, cached.Value);
        Assert.Equal(persisted.Index, cached.Index);
        Assert.Equal(persisted.Version, cached.Version);
    }

    [Theory]
    [InlineData("Add", "OperationHandler")]
    [InlineData("Save", "OperationHandler")]
    [InlineData("Add", "DocumentsChanged")]
    [InlineData("Save", "DocumentsChanged")]
    [InlineData("Add", "Notifications")]
    [InlineData("Save", "Notifications")]
    public async Task CachedWrite_WithHarmlessPostWriteCallbacks_PopulatesCustomAndIdCache(string operation, string callback)
    {
        var document = new TestDocument { Id = "one", Index = "first", Routing = "route", Value = "persisted source", Version = operation is "Save" ? "1:1" : null };
        using var configuration = new TestConfiguration(operation is "Save" ? [document] : []);
        using var repository = new TestRepository(configuration, notificationsEnabled: callback is "Notifications");
        var options = new CommandOptions().Cache().CacheKey("selected-document").Retry(0);
        var expectedChange = operation is "Add" ? ChangeType.Added : ChangeType.Saved;
        int callbackCount = 0;
        void RecordCallback(TestDocument current)
        {
            callbackCount++;
            Assert.Same(document, current);
            Assert.Equal("3:10", current.Version);
        }
        if (callback is "OperationHandler")
        {
            if (operation is "Add")
                repository.DocumentsAdded.AddSyncHandler((_, args) => RecordCallback(Assert.Single(args.Documents)));
            else
                repository.DocumentsSaved.AddSyncHandler((_, args) => RecordCallback(Assert.Single(args.Documents).Value));
        }
        else if (callback is "DocumentsChanged")
        {
            repository.DocumentsChanged.AddSyncHandler((_, args) =>
            {
                Assert.Equal(expectedChange, args.ChangeType);
                RecordCallback(Assert.Single(args.Documents).Value);
            });
        }

        if (operation is "Add")
            await repository.AddAsync(document, options);
        else
            await repository.SaveAsync(document, options);

        Assert.Equal(callback is "Notifications" ? 0 : 1, callbackCount);
        if (callback is "Notifications")
            Assert.Equal((expectedChange, (string?)document.Id), Assert.Single(repository.Notifications));
        else
            Assert.Empty(repository.Notifications);
        var cache = Assert.IsType<InMemoryCacheClient>(configuration.Cache);
        Assert.Equal(2, cache.Count);
        Assert.Equal(0, cache.Hits);
        Assert.Equal(0, cache.Misses);
        foreach (string key in new[] { document.Id, "selected-document" })
        {
            var cached = Assert.Single(await repository.GetCachedHitsAsync(key));
            Assert.Equal(document.Id, cached.Id);
            Assert.Equal("first-v1", cached.GetIndex());
            Assert.Equal("route", cached.Routing);
            Assert.Equal("3:10", cached.Version);
            Assert.NotNull(cached.Document);
            Assert.Equal("persisted source", cached.Document.Value);
            Assert.Equal("3:10", cached.Document.Version);
        }

        var found = await repository.FindOneAsync(new RepositoryQuery(), options);
        Assert.NotNull(found.Document);
        Assert.Equal("persisted source", found.Document.Value);
        Assert.Equal("3:10", found.Document.Version);
        var byId = await repository.GetByIdAsync(new Id(document.Id, document.Routing), options);
        Assert.NotNull(byId);
        Assert.Equal("persisted source", byId.Value);
        Assert.Equal("3:10", byId.Version);
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Equal(1, configuration.Invoker.IndexRequests);
        Assert.Empty(configuration.Invoker.Reads);
        Assert.Equal(0, configuration.Invoker.Searches);
        Assert.Equal(4, cache.Hits);
        Assert.Equal(0, cache.Misses);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task CachedWrite_WhenFreshRepositorySavesInsideCallback_InvalidatesCustomKeyImmediately(bool cacheNestedWrite, bool bypassBaseInvalidation)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        using var nestedRepository = new TestRepository(configuration, bypassBaseInvalidation: bypassBaseInvalidation);
        var document = new TestDocument { Id = "one", Index = "first", Routing = "route", Value = "outer write" };
        var options = new CommandOptions().Cache().CacheKey("selected-document").Retry(0);
        int callbacks = 0;
        repository.DocumentsAdded.AddHandler(async (_, args) =>
        {
            callbacks++;
            Assert.Same(document, Assert.Single(args.Documents));
            var cached = Assert.Single(await repository.GetCachedHitsAsync("selected-document"));
            Assert.Equal("outer write", cached.Document!.Value);
            Assert.Equal("3:10", cached.Version);

            await Task.Yield();
            var reloaded = await nestedRepository.GetByIdAsync(new Id(document.Id, document.Routing), new CommandOptions().Cache(false));
            Assert.NotNull(reloaded);
            Assert.NotSame(document, reloaded);
            reloaded.Value = "nested save";
            await nestedRepository.SaveAsync(reloaded, new CommandOptions().Cache(cacheNestedWrite).Retry(0));

            var immediate = await repository.GetByIdAsync(new Id(document.Id, document.Routing), options);
            Assert.NotNull(immediate);
            Assert.Equal("nested save", immediate.Value);
            Assert.Equal("3:20", immediate.Version);
            var refreshed = Assert.Single(await repository.GetCachedHitsAsync("selected-document"));
            Assert.Equal("nested save", refreshed.Document!.Value);
            Assert.Equal("first-v1", refreshed.GetIndex());
            Assert.Equal("route", refreshed.Routing);
            Assert.Equal("3:20", refreshed.Version);
        });

        await repository.AddAsync(document, options);

        Assert.Equal(1, callbacks);
        Assert.Equal(1, nestedRepository.DocumentInvalidations);
        Assert.Equal(2, configuration.Invoker.IndexRequests);
        var current = await repository.GetByIdAsync(new Id(document.Id, document.Routing), options);
        Assert.NotNull(current);
        Assert.Equal("nested save", current.Value);
        Assert.Equal("3:20", current.Version);
        Assert.Equal("outer write", document.Value);
        Assert.Equal("3:10", document.Version);
    }

    [Theory]
    [InlineData("OtherId")]
    [InlineData("DifferentRoot")]
    [InlineData("DifferentScope")]
    public async Task CachedWrite_WhenNestedWriteDoesNotShareItsCacheIdentity_PreservesCustomKey(string isolation)
    {
        using var rootCache = new InMemoryCacheClient(new InMemoryCacheClientOptions { CloneValues = true });
        using var otherRootCache = new InMemoryCacheClient(new InMemoryCacheClientOptions { CloneValues = true });
        using var outerCache = new ScopedCacheClient(rootCache, "outer");
        using var nestedCache = new ScopedCacheClient(isolation is "DifferentRoot" ? otherRootCache : rootCache,
            isolation is "DifferentScope" ? "nested" : "outer");
        var document = new TestDocument { Id = "one", Index = "first", Routing = "route", Value = "outer write" };
        var nestedDocument = new TestDocument { Id = isolation is "OtherId" ? "other" : "one", Index = "first", Routing = "route", Value = "nested save", Version = "1:1" };
        using var configuration = new TestConfiguration(cache: outerCache);
        using var nestedConfiguration = new TestConfiguration([nestedDocument], cache: nestedCache);
        using var repository = new TestRepository(configuration);
        using var nestedRepository = new TestRepository(nestedConfiguration);
        var options = new CommandOptions().Cache().CacheKey("selected-document").Retry(0);
        int callbacks = 0;
        repository.DocumentsAdded.AddHandler(async (_, _) =>
        {
            callbacks++;
            var before = Assert.Single(await repository.GetCachedHitsAsync("selected-document"));
            Assert.Equal("outer write", before.Document!.Value);
            Assert.Equal("3:10", before.Version);

            await Task.Yield();
            var hitsBeforeWrite = rootCache.Hits;
            var missesBeforeWrite = rootCache.Misses;
            await nestedRepository.SaveAsync(nestedDocument, new CommandOptions().Cache(false).Retry(0));
            Assert.Equal(hitsBeforeWrite, rootCache.Hits);
            Assert.Equal(missesBeforeWrite, rootCache.Misses);
            Assert.Equal(0, otherRootCache.Hits);
            Assert.Equal(0, otherRootCache.Misses);

            var after = Assert.Single(await repository.GetCachedHitsAsync("selected-document"));
            Assert.Equal("outer write", after.Document!.Value);
            Assert.Equal("3:10", after.Version);
            var immediate = await repository.GetByIdAsync(new Id(document.Id, document.Routing), options);
            Assert.NotNull(immediate);
            Assert.Equal("outer write", immediate.Value);
            Assert.Equal("3:10", immediate.Version);
        });

        await repository.AddAsync(document, options);

        Assert.Equal(1, callbacks);
        var retained = Assert.Single(await repository.GetCachedHitsAsync("selected-document"));
        Assert.Equal("outer write", retained.Document!.Value);
        Assert.Equal("first-v1", retained.GetIndex());
        Assert.Equal("route", retained.Routing);
        Assert.Equal("3:10", retained.Version);
        var current = await repository.GetByIdAsync(new Id(document.Id, document.Routing), options);
        Assert.NotNull(current);
        Assert.Equal("outer write", current.Value);
        Assert.Equal("3:10", current.Version);
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Empty(configuration.Invoker.Reads);
        Assert.Equal(1, nestedConfiguration.Invoker.IndexRequests);
    }

    [Fact]
    public async Task CachedWrite_AfterNestedWriteCallbackThrows_AllowsSubsequentCustomKeyWrite()
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        using var subsequentRepository = new TestRepository(configuration);
        var document = new TestDocument { Id = "one", Index = "first", Routing = "route", Value = "outer write" };
        var options = new CommandOptions().Cache().CacheKey("selected-document").Retry(0);
        repository.DocumentsAdded.AddHandler(async (_, _) =>
        {
            await Task.Yield();
            document.Value = "nested save";
            await subsequentRepository.SaveAsync(document, new CommandOptions().Cache(false).Retry(0));
            throw new InvalidOperationException("Callback failed after the nested save.");
        });

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () => await repository.AddAsync(document, options));

        Assert.Equal("Callback failed after the nested save.", exception.Message);
        Assert.Equal("3:20", document.Version);
        Assert.False(await repository.CacheContainsAsync("selected-document"));
        document.Value = "subsequent save";
        await subsequentRepository.SaveAsync(document, options);

        var cached = Assert.Single(await subsequentRepository.GetCachedHitsAsync("selected-document"));
        Assert.Equal("subsequent save", cached.Document!.Value);
        Assert.Equal("first-v1", cached.GetIndex());
        Assert.Equal("route", cached.Routing);
        Assert.Equal("3:30", cached.Version);
        var current = await subsequentRepository.GetByIdAsync(new Id(document.Id, document.Routing), options);
        Assert.NotNull(current);
        Assert.Equal("subsequent save", current.Value);
        Assert.Equal("3:30", current.Version);
        Assert.Equal(2, Assert.IsType<InMemoryCacheClient>(configuration.Cache).Count);
        Assert.Equal(3, configuration.Invoker.Requests);
        Assert.Equal(3, configuration.Invoker.IndexRequests);
        Assert.Empty(configuration.Invoker.Reads);
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

        bool savedFromHandler = false;
        async Task OnWrittenAsync()
        {
            if (savedFromHandler)
                return;

            savedFromHandler = true;
            document.Value = "nested save";
            await repository.SaveAsync(document, new CommandOptions().Cache(false).Retry(0));
            cache.FailRemoval = true;
            if (handlerThrows)
                throw new InvalidOperationException("Callback failed.");
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
        Assert.True(savedFromHandler);
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
    [InlineData("Add", "Operation")]
    [InlineData("Add", "Changed")]
    [InlineData("Save", "Operation")]
    [InlineData("Save", "Changed")]
    [InlineData("Remove", "Operation")]
    [InlineData("Remove", "Changed")]
    public async Task WriteCompletion_WhenPostWriteHandlerChangesId_NotifiesPersistedLocations(string operation, string callback)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration, notificationsEnabled: true);
        TestDocument[] documents =
        [
            new() { Id = "one", Index = "first", Routing = "route", Value = "first source" },
            new() { Id = "two", Index = "first", Routing = "route", Value = "second source" }
        ];
        if (operation is not "Add")
            await repository.AddAsync(documents, new CommandOptions().Cache().Retry(0));
        repository.Notifications.Clear();
        repository.DocumentNotifications.Clear();
        repository.NotificationGroups.Clear();

        void ChangeId(TestDocument document) => document.Id = "unsaved-callback-id";
        if (callback is "Changed")
            repository.DocumentsChanged.AddSyncHandler((_, args) => ChangeId(args.Documents.First().Value));
        else if (operation is "Add")
            repository.DocumentsAdded.AddSyncHandler((_, args) => ChangeId(args.Documents.First()));
        else if (operation is "Save")
            repository.DocumentsSaved.AddSyncHandler((_, args) => ChangeId(args.Documents.First().Value));
        else
            repository.DocumentsRemoved.AddSyncHandler((_, args) => ChangeId(args.Documents.First()));

        IReadOnlyCollection<ModifiedDocument<TestDocument>>? changedDocuments = null;
        repository.DocumentsChanged.AddSyncHandler((_, args) => changedDocuments = args.Documents);

        if (operation is "Add")
            await repository.AddAsync(documents, new CommandOptions().Cache().Retry(0));
        else if (operation is "Save")
            await repository.SaveAsync(documents, new CommandOptions().Cache().Retry(0));
        else
            await repository.RemoveAsync(documents, new CommandOptions().Retry(0));

        Assert.Equal("unsaved-callback-id", documents[0].Id);
        Assert.Equal(new[] { "one", "two" }, repository.Notifications.Select(notification => notification.Id).OrderBy(id => id, StringComparer.Ordinal));
        var expectedChange = operation is "Add" ? ChangeType.Added : operation is "Save" ? ChangeType.Saved : ChangeType.Removed;
        Assert.All(repository.Notifications, notification => Assert.Equal(expectedChange, notification.ChangeType));
        Assert.Contains(documents[1], repository.DocumentNotifications);
        var notificationGroup = Assert.Single(repository.NotificationGroups);
        Assert.Equal(2, notificationGroup.Count);
        Assert.Same(changedDocuments, notificationGroup);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task SaveAsync_WhenSavedHandlerReplacesSoftDeletedValue_PreservesNotificationGroupAndPersistedId(bool batchNotifications, bool allDeleted)
    {
        using var configuration = new TestConfiguration();
        using var repository = new SoftDeleteTestRepository(configuration) { BatchNotifications = batchNotifications };
        SoftDeleteDocument[] originals =
        [
            new() { Id = "one", Index = "first", Routing = "route", Value = "first original" },
            new() { Id = "two", Index = "first", Routing = "route", Value = "second original" }
        ];
        SoftDeleteDocument[] documents =
        [
            new() { Id = "one", Index = "first", Routing = "route", Value = "first saved", IsDeleted = true },
            new() { Id = "two", Index = "first", Routing = "route", Value = "second saved", IsDeleted = allDeleted }
        ];
        var replacement = new SoftDeleteDocument { Id = "unsaved-callback-id", Index = "first", Routing = "route", Value = "callback replacement", IsDeleted = true };
        IReadOnlyCollection<ModifiedDocument<SoftDeleteDocument>>? savedDocuments = null;
        IReadOnlyCollection<ModifiedDocument<SoftDeleteDocument>>? changedDocuments = null;
        repository.DocumentsSaved.AddSyncHandler((_, args) =>
        {
            savedDocuments = args.Documents;
            var first = Assert.Single(args.Documents, document => document.Value.Id is "one");
            Assert.Same(originals[0], first.Original);
            first.Value = replacement;
        });
        repository.DocumentsChanged.AddSyncHandler((_, args) => changedDocuments = args.Documents);

        await repository.SaveAsync(documents, new CommandOptions().Originals().AddOriginals(originals[0]).AddOriginals(originals[1]).Retry(0));

        var notificationGroup = Assert.Single(repository.NotificationGroups);
        Assert.Same(savedDocuments, notificationGroup);
        Assert.Same(changedDocuments, notificationGroup);
        Assert.Equal(2, notificationGroup.Count);
        Assert.Contains(notificationGroup, document => ReferenceEquals(replacement, document.Value));
        Assert.Equal(new[] { ("first", "one", "route"), ("first", "two", "route") }, configuration.Invoker.Writes);
        Assert.Equal(2, repository.Notifications.Count);
        Assert.Contains((!batchNotifications || allDeleted ? ChangeType.Removed : ChangeType.Saved, "one"), repository.Notifications);
        Assert.Contains((allDeleted ? ChangeType.Removed : ChangeType.Saved, "two"), repository.Notifications);
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
    public async Task GetByIdsAsync_AfterWritingSameIdToDifferentRoutes_ReadsPersistedDocumentsWhenLocationsAreUnproven(bool customCacheKey)
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

        Assert.False(await repository.CacheContainsAsync("same"));
        if (customCacheKey)
            Assert.False(await repository.CacheContainsAsync("selected-documents"));
        var results = await repository.GetByIdsAsync(new Ids(new Id("same", "first"), new Id("same", "second")), options);

        Assert.Equal(documents.Select(document => document.Value), results.Select(document => document.Value));
        Assert.Equal(2, configuration.Invoker.Requests);
        Assert.Equal(new[] { new Id("same", "first"), new Id("same", "second") }, configuration.Invoker.Reads);
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
        private readonly bool _bypassBaseInvalidation;

        public TestRepository(TestConfiguration configuration, string? pipeline = null, bool notificationsEnabled = false, bool bypassBaseInvalidation = false) : base(new TestIndex(configuration))
        {
            HasParent = true;
            GetParentIdFunc = document => document.Routing;
            NotificationsEnabled = notificationsEnabled;
            DefaultPipeline = pipeline;
            _bypassBaseInvalidation = bypassBaseInvalidation;
        }

        public List<(ChangeType ChangeType, string? Id)> Notifications { get; } = [];
        public List<TestDocument?> DocumentNotifications { get; } = [];
        public List<IReadOnlyCollection<ModifiedDocument<TestDocument>>> NotificationGroups { get; } = [];
        public int DocumentInvalidations { get; private set; }
        public Task<bool> CacheContainsAsync(string id) => Cache.ExistsAsync(id);

        protected override Task InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<TestDocument>> documents, ChangeType? changeType = null)
        {
            DocumentInvalidations++;
            return _bypassBaseInvalidation
                ? Cache.RemoveAllAsync(documents.Select(document => document.Value.Id))
                : base.InvalidateCacheAsync(documents, changeType);
        }

        protected override Task PublishMessageAsync(EntityChanged message, TimeSpan? delay = null)
        {
            Notifications.Add((message.ChangeType, message.Id));
            return Task.CompletedTask;
        }

        protected override Task PublishChangeTypeMessageAsync(ChangeType changeType, TestDocument? document, IDictionary<string, object?>? data = null, TimeSpan? delay = null)
        {
            DocumentNotifications.Add(document);
            return base.PublishChangeTypeMessageAsync(changeType, document, data, delay);
        }

        protected override Task SendNotificationsAsync(ChangeType changeType, IReadOnlyCollection<ModifiedDocument<TestDocument>> documents, ICommandOptions options)
        {
            NotificationGroups.Add(documents);
            return base.SendNotificationsAsync(changeType, documents, options);
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

    public sealed class SoftDeleteDocument : IIdentity, IVersioned, ISupportSoftDeletes
    {
        public string Id { get; set; } = null!;
        public string Index { get; set; } = null!;
        public string Routing { get; set; } = null!;
        public string Value { get; set; } = null!;
        public string? Version { get; set; }
        public bool IsDeleted { get; set; }
    }

    private sealed class SoftDeleteTestRepository : ElasticRepositoryBase<SoftDeleteDocument>
    {
        public SoftDeleteTestRepository(TestConfiguration configuration) : base(new SoftDeleteTestIndex(configuration))
        {
            HasParent = true;
            GetParentIdFunc = document => document.Routing;
            NotificationsEnabled = true;
        }

        public List<(ChangeType ChangeType, string? Id)> Notifications { get; } = [];
        public List<IReadOnlyCollection<ModifiedDocument<SoftDeleteDocument>>> NotificationGroups { get; } = [];

        protected override Task SendNotificationsAsync(ChangeType changeType, IReadOnlyCollection<ModifiedDocument<SoftDeleteDocument>> documents, ICommandOptions options)
        {
            NotificationGroups.Add(documents);
            return base.SendNotificationsAsync(changeType, documents, options);
        }

        protected override Task PublishMessageAsync(EntityChanged message, TimeSpan? delay = null)
        {
            Notifications.Add((message.ChangeType, message.Id));
            return Task.CompletedTask;
        }
    }

    private sealed class SoftDeleteTestIndex(TestConfiguration configuration) : Index<SoftDeleteDocument>(configuration, "write-cache-tests")
    {
        public override string GetIndex(object target) => target is SoftDeleteDocument document ? document.Index : "first";
        public override Task EnsureIndexAsync(object? target) => Task.CompletedTask;
    }

    private sealed class FailingRemovalCache : InMemoryCacheClient, ICacheClient
    {
        public FailingRemovalCache() : base(new InMemoryCacheClientOptions { CloneValues = true })
        {
        }

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
        private int _bulkIndexOperations;
        public ResponseFactory ResponseFactory => _owner.ResponseFactory;
        public int Requests { get; private set; }
        public int IndexRequests { get; private set; }
        public int Searches { get; private set; }
        public int PipelineWrites { get; private set; }
        public int TransientIndexFailures { get; set; }
        public int? TransientBulkOperationNumber { get; set; }
        public int? AmbiguousBulkWriteNumber { get; set; }
        public bool AmbiguousIndexWrite { get; set; }
        public bool SingleShard { get; set; }
        public Dictionary<string, string> IndexAliases { get; } = new(StringComparer.Ordinal);
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
                var key = GetStorageKey(index, id, routing);
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
                    Writes.Add((index, id, routing));
                    _documents[key] = new TestDocument { Id = id, Index = index, Routing = routing, Value = value, Version = $"3:{Writes.Count * 10}" };
                    if (AmbiguousIndexWrite)
                    {
                        statusCode = 504;
                        response = new { status = 504, error = new { type = "timeout_exception", reason = "The response was lost after the write applied." } };
                    }
                    else
                        response = new { _index = GetPhysicalIndex(index), _id = id, result = parameters.GetValueOrDefault("op_type") is "create" ? "created" : "updated", _seq_no = Writes.Count * 10, _primary_term = 3, _version = Writes.Count, _shards = new { total = 1, successful = 1, failed = 0 } };
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
            else if (endpoint.PathAndQuery.Contains("_mget", StringComparison.Ordinal))
            {
                Assert.NotNull(postData);
                using var requestStream = new MemoryStream();
                postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                using var request = JsonDocument.Parse(requestStream.ToArray());
                var docs = request.RootElement.GetProperty("docs").EnumerateArray().Select(item =>
                    CreateGetResponse(item.GetProperty("_index").GetString()!, item.GetProperty("_id").GetString()!, item.GetProperty("routing").GetString(), out _)).ToArray();
                response = new { docs };
            }
            else
            {
                Assert.Contains("/_doc/", endpoint.PathAndQuery, StringComparison.Ordinal);
                var uri = new Uri(new Uri("http://localhost/"), endpoint.PathAndQuery);
                string index = Uri.UnescapeDataString(uri.Segments[1].TrimEnd('/'));
                string id = Uri.UnescapeDataString(uri.Segments[^1]);
                string? routing = uri.Query.TrimStart('?').Split('&')
                    .FirstOrDefault(value => value.StartsWith("routing=", StringComparison.Ordinal))?["routing=".Length..];
                routing = routing is null ? null : Uri.UnescapeDataString(routing);
                response = CreateGetResponse(index, id, routing, out bool found);
                if (!found)
                    statusCode = 404;
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
            bool errors = false;
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

                string? documentJson = reader.ReadLine();
                Assert.NotNull(documentJson);
                using var source = JsonDocument.Parse(documentJson);
                _bulkIndexOperations++;
                if (TransientIndexFailures > 0 || _bulkIndexOperations == TransientBulkOperationNumber)
                {
                    if (TransientIndexFailures > 0)
                        TransientIndexFailures--;
                    errors = true;
                    items.Add(new Dictionary<string, object>
                    {
                        [operation.Name] = new { _index = $"{index}-v1", _id = id, status = 429, error = new { type = "es_rejected_execution_exception", reason = "Injected retryable failure." } }
                    });
                    continue;
                }

                Writes.Add((index, id, routing));
                string value = source.RootElement.GetProperty("value").GetString()!;
                if (operation.Value.TryGetProperty("pipeline", out var pipeline) && pipeline.GetString() is "lowercase-value")
                {
                    value = value.ToLowerInvariant();
                    PipelineWrites++;
                }
                _documents[GetStorageKey(index, id, routing)] = new TestDocument { Id = id, Index = index, Routing = routing, Value = value, Version = $"3:{Writes.Count * 10}" };
                if (Writes.Count == AmbiguousBulkWriteNumber)
                {
                    errors = true;
                    items.Add(new Dictionary<string, object>
                    {
                        [operation.Name] = new { _index = $"{index}-v1", _id = id, status = 504, error = new { type = "timeout_exception", reason = "Injected ambiguous failure after the write applied." } }
                    });
                    continue;
                }
                items.Add(new Dictionary<string, object>
                {
                    [operation.Name] = new
                    {
                        _index = GetPhysicalIndex(index),
                        _id = id,
                        status = operation.Name is "create" ? 201 : 200,
                        result = operation.Name is "create" ? "created" : "updated",
                        _seq_no = Writes.Count * 10,
                        _primary_term = 3
                    }
                });
            }

            return new { took = 1, errors, items };
        }

        private (string Index, string Id, string Routing) GetStorageKey(string index, string id, string routing) =>
            (IndexAliases.GetValueOrDefault(index, index), id, SingleShard ? String.Empty : routing);

        private string GetPhysicalIndex(string index) => IndexAliases.GetValueOrDefault(index, $"{index}-v1");

        private object CreateGetResponse(string index, string id, string? routing, out bool found)
        {
            Reads.Add(new Id(id, routing));
            var document = SingleShard || IndexAliases.Count > 0
                ? _documents.GetValueOrDefault(GetStorageKey(index, id, routing ?? String.Empty))
                : _documents.Values.SingleOrDefault(document => document.Id == id && document.Routing == routing);
            found = document is not null;
            if (document is null)
                return new { _index = GetPhysicalIndex(index), _id = id, found = false };

            var version = ElasticDocumentVersion.Parse(document.Version ?? "3:10");
            return new
            {
                _index = GetPhysicalIndex(document.Index),
                _id = id,
                _routing = document.Routing,
                found = true,
                _seq_no = version.SequenceNumber,
                _primary_term = version.PrimaryTerm,
                _source = new { id, index = document.Index, routing = document.Routing, value = document.Value }
            };
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
