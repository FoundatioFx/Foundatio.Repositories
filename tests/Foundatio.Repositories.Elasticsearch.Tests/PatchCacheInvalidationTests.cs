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

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class PatchCacheInvalidationTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchAsync_WithClientSideSuccess_InvalidatesEachDocumentOnce(bool multipleIds)
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new TestRepository(configuration);
        var patch = new PartialPatch(new { value = "patched" });

        if (multipleIds)
            Assert.Equal(2, await repository.PatchAsync(new Ids("one", "two"), patch, o => o.Retry(0)));
        else
            Assert.True(await repository.PatchAsync(new Id("one"), patch, o => o.Retry(0)));

        Assert.Equal(1, repository.DocumentInvalidations);
        var keys = Assert.Single(cache.Removals);
        Assert.Equal(multipleIds ? 2 : 1, keys.Length);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchAsync_WhenSourceIdChanges_InvalidatesAndNotifiesOriginalWriteLocations(bool multipleIds)
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new TestRepository(configuration);
        await repository.SeedAsync("one");
        if (multipleIds)
            await repository.SeedAsync("two");
        await repository.SeedAsync("changed-source-id");
        var patch = new PartialPatch(new { id = "changed-source-id", value = "patched" });

        if (multipleIds)
            Assert.Equal(2, await repository.PatchAsync(new Ids("one", "two"), patch, options => options.Retry(0)));
        else
            Assert.True(await repository.PatchAsync(new Id("one"), patch, options => options.Retry(0)));

        Assert.False(await repository.CacheContainsAsync("one"));
        if (multipleIds)
            Assert.False(await repository.CacheContainsAsync("two"));
        Assert.False(await repository.CacheContainsAsync("changed-source-id"));
        Assert.Equal(1, repository.DocumentInvalidations);
        Assert.Equal(multipleIds ? new[] { "one", "two" } : ["one"], repository.PublishedIds);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RemoveByIdAsync_WhenPatchedSourceIdNamesAnotherDocument_DeletesRequestedMetadataIdentity(bool multipleIds)
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new TestRepository(configuration);
        configuration.Invoker.AddStoredDocument(new TestDocument { Id = "three", Value = "third source" });
        Assert.True(await repository.PatchAsync(new Id("one"), new PartialPatch(new { id = "two", value = "patched source" }), options => options.Retry(0)));
        var patched = await repository.GetByIdAsync(new Id("one"), options => options.Cache());
        Assert.NotNull(patched);
        Assert.Equal("two", patched.Id);
        Assert.Equal("patched source", patched.Value);
        repository.PublishedIds.Clear();
        var removingSourceIds = new List<string>();
        var removedSourceIds = new List<string>();
        repository.DocumentsRemoving.AddSyncHandler((_, args) => removingSourceIds.AddRange(args.Documents.Select(document => document.Id)));
        repository.DocumentsRemoved.AddSyncHandler((_, args) => removedSourceIds.AddRange(args.Documents.Select(document => document.Id)));

        if (multipleIds)
            await repository.RemoveAsync(new Ids("one", "three"), options => options.Retry(0));
        else
            await repository.RemoveAsync(new Id("one"), options => options.Retry(0));

        string[] expectedIds = multipleIds ? ["one", "three"] : ["one"];
        string[] expectedSourceIds = multipleIds ? ["two", "three"] : ["two"];
        Assert.Equal(expectedIds, configuration.Invoker.Deletes);
        Assert.Equal(expectedIds, repository.PublishedIds);
        Assert.Equal(expectedSourceIds, removingSourceIds);
        Assert.Equal(expectedSourceIds, removedSourceIds);
        Assert.False(await repository.CacheContainsAsync("one"));
        Assert.Null(await repository.GetByIdAsync(new Id("one"), options => options.Cache(false)));
        var unrelated = await repository.GetByIdAsync(new Id("two"), options => options.Cache(false));
        Assert.NotNull(unrelated);
        Assert.Equal("before", unrelated.Value);
        if (multipleIds)
            Assert.Null(await repository.GetByIdAsync(new Id("three"), options => options.Cache(false)));
    }

    [Fact]
    public async Task RemoveByIdAsync_WhenRequestedRoutingDiffersFromStoredRouting_DeletesReturnedMetadataLocation()
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new TestRepository(configuration);
        Assert.True(await repository.PatchAsync(new Id("one"), new PartialPatch(new { id = "two", value = "selected source" }), options => options.Retry(0)));
        configuration.Invoker.StoredRoutings["one"] = "stored-route";
        repository.PublishedIds.Clear();
        var removedSourceIds = new List<string>();
        repository.DocumentsRemoved.AddSyncHandler((_, args) => removedSourceIds.AddRange(args.Documents.Select(document => document.Id)));

        await repository.RemoveAsync(new Id("one", "requested-route"), options => options.Cache(false).ReadCache(false).Retry(0));

        Assert.Equal(new[] { "one" }, configuration.Invoker.Deletes);
        Assert.Contains("routing=stored-route", Assert.Single(configuration.Invoker.DeletePaths), StringComparison.Ordinal);
        Assert.Equal(new[] { "one" }, repository.PublishedIds);
        Assert.Equal(new[] { "two" }, removedSourceIds);
        Assert.Null(await repository.GetByIdAsync(new Id("one", "requested-route"), options => options.Cache(false)));
        var unrelated = await repository.GetByIdAsync(new Id("two"), options => options.Cache(false));
        Assert.NotNull(unrelated);
        Assert.Equal("before", unrelated.Value);
    }

    [Fact]
    public async Task RemoveByIdAsync_WhenReadOverrideProbesAnotherRouting_DoesNotCaptureThePrerequisiteRead()
    {
        using var cache = new RecordingCache(cloneValues: false);
        using var configuration = new TestConfiguration(cache);
        using var repository = new ForwardingOptionsRepository(configuration);
        Assert.True(await repository.PatchAsync(new Id("one"), new PartialPatch(new { id = "two" }), options => options.Retry(0)));
        var shared = await repository.GetByIdAsync(new Id("one"), options => options.Cache(false));
        Assert.NotNull(shared);
        await repository.SeedHitAsync("one", shared, "other-route");
        repository.PublishedIds.Clear();
        repository.BeforeRead = async options =>
        {
            repository.BeforeRead = null;
            var prerequisite = await repository.GetByIdsAsync(new Ids(new Id("one", "other-route")), options);
            Assert.Same(shared, Assert.Single(prerequisite));
            await repository.SeedHitAsync("one", shared, "requested-route");
        };

        await repository.RemoveAsync(new Id("one", "requested-route"), options => options.Retry(0));

        Assert.Equal(2, repository.ReadCalls);
        Assert.Equal(new[] { "one" }, configuration.Invoker.Deletes);
        Assert.Contains("routing=requested-route", Assert.Single(configuration.Invoker.DeletePaths), StringComparison.Ordinal);
        Assert.Equal(new[] { "one" }, repository.PublishedIds);
        Assert.Equal("two", shared.Id);
        Assert.Null(await repository.GetByIdAsync(new Id("one"), options => options.Cache(false)));
        Assert.NotNull(await repository.GetByIdAsync(new Id("two"), options => options.Cache(false)));
    }

    [Theory]
    [InlineData(false, "Removing")]
    [InlineData(false, "Changing")]
    [InlineData(true, "Removing")]
    [InlineData(true, "Changing")]
    public async Task RemoveByIdAsync_WhenBeforeHandlerChangesSourceId_PreservesRequestedDeleteTarget(bool multipleIds, string eventName)
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new TestRepository(configuration);
        configuration.Invoker.AddStoredDocument(new TestDocument { Id = "three", Value = "third source" });
        if (eventName is "Removing")
            repository.DocumentsRemoving.AddSyncHandler((_, args) => Assert.Single(args.Documents, document => document.Id is "one").Id = "two");
        else
            repository.DocumentsChanging.AddSyncHandler((_, args) => Assert.Single(args.Documents, document => document.Value.Id is "one").Value.Id = "two");
        var removedSourceIds = new List<string>();
        repository.DocumentsRemoved.AddSyncHandler((_, args) => removedSourceIds.AddRange(args.Documents.Select(document => document.Id)));

        if (multipleIds)
            await repository.RemoveAsync(new Ids("one", "three"), options => options.ReadCache(false).Retry(0));
        else
            await repository.RemoveAsync(new Id("one"), options => options.ReadCache(false).Retry(0));

        string[] expectedIds = multipleIds ? ["one", "three"] : ["one"];
        Assert.Equal(expectedIds, configuration.Invoker.Deletes);
        Assert.Equal(expectedIds, repository.PublishedIds);
        Assert.Equal(multipleIds ? new[] { "two", "three" } : ["two"], removedSourceIds);
        Assert.Null(await repository.GetByIdAsync(new Id("one"), options => options.Cache(false)));
        var unrelated = await repository.GetByIdAsync(new Id("two"), options => options.Cache(false));
        Assert.NotNull(unrelated);
        Assert.Equal("before", unrelated.Value);
        if (multipleIds)
            Assert.Null(await repository.GetByIdAsync(new Id("three"), options => options.Cache(false)));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RemoveByIdAsync_WhenHandlerForwardsOptionsToModelRemoval_KeepsEachRequestedTarget(bool beforeRemove)
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new TestRepository(configuration);
        Assert.True(await repository.PatchAsync(new Id("one"), new PartialPatch(new { id = "two" }), options => options.Retry(0)));
        repository.PublishedIds.Clear();
        bool nestedRemoval = false;
        async Task RemoveSourceTargetAsync(DocumentsEventArgs<TestDocument> args)
        {
            var document = Assert.Single(args.Documents);
            Assert.Equal("two", document.Id);
            if (nestedRemoval)
                return;

            nestedRemoval = true;
            await repository.RemoveAsync(document, args.Options);
        }
        if (beforeRemove)
            repository.DocumentsRemoving.AddHandler((_, args) => RemoveSourceTargetAsync(args));
        else
            repository.DocumentsRemoved.AddHandler((_, args) => RemoveSourceTargetAsync(args));

        await repository.RemoveAsync(new Id("one"), options => options.Retry(0));

        Assert.True(nestedRemoval);
        Assert.Equal(beforeRemove ? new[] { "two", "one" } : ["one", "two"], configuration.Invoker.Deletes);
        Assert.Equal(new[] { "one", "two" }, repository.PublishedIds.OrderBy(id => id, StringComparer.Ordinal));
        Assert.Null(await repository.GetByIdAsync(new Id("one"), options => options.Cache(false)));
        Assert.Null(await repository.GetByIdAsync(new Id("two"), options => options.Cache(false)));
    }

    [Fact]
    public async Task RemoveByIdAsync_WhenRemoveOverrideClonesThenForwardsOptions_DoesNotReuseOuterLocation()
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new ForwardingOptionsRepository(configuration);
        Assert.True(await repository.PatchAsync(new Id("one"), new PartialPatch(new { id = "two" }), options => options.Retry(0)));
        repository.PublishedIds.Clear();
        repository.AfterRemove = async (documents, options) =>
        {
            repository.AfterRemove = null;
            var document = Assert.Single(documents);
            Assert.Equal("two", document.Id);
            await repository.RemoveAsync(document, options);
        };

        await repository.RemoveAsync(new Id("one"), options => options.Retry(0));

        Assert.Equal(1, repository.ReadCalls);
        Assert.Equal(2, repository.RemoveCalls);
        Assert.Equal(new[] { "one", "two" }, configuration.Invoker.Deletes);
        Assert.Equal(new[] { "one", "two" }, repository.PublishedIds);
        Assert.Null(await repository.GetByIdAsync(new Id("one"), options => options.Cache(false)));
        Assert.Null(await repository.GetByIdAsync(new Id("two"), options => options.Cache(false)));
    }

    [Fact]
    public async Task RemoveByIdAsync_WhenReadOverrideClonesThenForwardsOptions_DoesNotCaptureNestedRead()
    {
        using var cache = new RecordingCache(cloneValues: false);
        using var configuration = new TestConfiguration(cache);
        using var repository = new ForwardingOptionsRepository(configuration);
        Assert.True(await repository.PatchAsync(new Id("one"), new PartialPatch(new { id = "two" }), options => options.Retry(0)));
        var shared = await repository.GetByIdAsync(new Id("one"), options => options.Cache(false));
        Assert.NotNull(shared);
        await repository.SeedHitAsync("one", shared);
        await repository.SeedHitAsync("two", shared);
        repository.PublishedIds.Clear();
        repository.AfterRead = async (documents, options) =>
        {
            repository.AfterRead = null;
            var nested = await repository.GetByIdsAsync(new Ids("two"), options);
            Assert.Same(Assert.Single(documents), Assert.Single(nested));
        };
        var removedSources = new List<string>();
        repository.DocumentsRemoved.AddSyncHandler((_, args) => removedSources.AddRange(args.Documents.Select(document => document.Id)));

        await repository.RemoveAsync(new Id("one"), options => options.Retry(0));

        Assert.Equal(2, repository.ReadCalls);
        Assert.Equal(1, repository.RemoveCalls);
        Assert.Equal(new[] { "one" }, configuration.Invoker.Deletes);
        Assert.Equal(new[] { "one" }, repository.PublishedIds);
        Assert.Equal(new[] { "two" }, removedSources);
        Assert.Equal("two", shared.Id);
        Assert.Null(await repository.GetByIdAsync(new Id("one"), options => options.Cache(false)));
        Assert.NotNull(await repository.GetByIdAsync(new Id("two"), options => options.Cache(false)));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task RemoveByIdAsync_WhenReadOverridePerformsPrerequisiteRead_PreservesRequestedLocation(bool emptyPrerequisite, bool cloneOptions)
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new ForwardingOptionsRepository(configuration) { CloneBaseOptions = cloneOptions };
        Assert.True(await repository.PatchAsync(new Id("one"), new PartialPatch(new { id = "two" }), options => options.Retry(0)));
        repository.PublishedIds.Clear();
        repository.BeforeRead = async options =>
        {
            repository.BeforeRead = null;
            var ids = emptyPrerequisite ? new Ids(Array.Empty<string>()) : new Ids("two");
            var prerequisite = await repository.GetByIdsAsync(ids, options);
            if (emptyPrerequisite)
                Assert.Empty(prerequisite);
            else
                Assert.Equal("two", Assert.Single(prerequisite).Id);
        };
        var removedSources = new List<string>();
        repository.DocumentsRemoved.AddSyncHandler((_, args) => removedSources.AddRange(args.Documents.Select(document => document.Id)));

        await repository.RemoveAsync(new Id("one"), options => options.ReadCache(false).Retry(0));

        Assert.Equal(2, repository.ReadCalls);
        Assert.Equal(1, repository.RemoveCalls);
        Assert.Equal(new[] { "one" }, configuration.Invoker.Deletes);
        Assert.Equal(new[] { "one" }, repository.PublishedIds);
        Assert.Equal(new[] { "two" }, removedSources);
        Assert.Null(await repository.GetByIdAsync(new Id("one"), options => options.Cache(false)));
        var unrelated = await repository.GetByIdAsync(new Id("two"), options => options.Cache(false));
        Assert.NotNull(unrelated);
        Assert.Equal("before", unrelated.Value);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task RemoveByIdAsync_WhenRemoveOverridePerformsPrerequisiteRemoval_PreservesRequestedLocation(bool emptyPrerequisite, bool cloneOptions)
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new ForwardingOptionsRepository(configuration) { CloneBaseOptions = cloneOptions };
        configuration.Invoker.AddStoredDocument(new TestDocument { Id = "three", Value = "prerequisite source" });
        Assert.True(await repository.PatchAsync(new Id("one"), new PartialPatch(new { id = "two" }), options => options.Retry(0)));
        repository.PublishedIds.Clear();
        repository.BeforeRemove = async (documents, options) =>
        {
            repository.BeforeRemove = null;
            Assert.Equal("two", Assert.Single(documents).Id);
            TestDocument[] prerequisite = emptyPrerequisite ? [] : [new TestDocument { Id = "three", Value = "prerequisite source" }];
            await repository.RemoveAsync(prerequisite, options);
        };
        var removedSources = new List<string>();
        repository.DocumentsRemoved.AddSyncHandler((_, args) => removedSources.AddRange(args.Documents.Select(document => document.Id)));

        await repository.RemoveAsync(new Id("one"), options => options.ReadCache(false).Retry(0));

        Assert.Equal(1, repository.ReadCalls);
        Assert.Equal(2, repository.RemoveCalls);
        string[] expectedTargets = emptyPrerequisite ? ["one"] : ["three", "one"];
        Assert.Equal(expectedTargets, configuration.Invoker.Deletes);
        Assert.Equal(expectedTargets, repository.PublishedIds);
        Assert.Equal(emptyPrerequisite ? new[] { "two" } : ["three", "two"], removedSources);
        Assert.Null(await repository.GetByIdAsync(new Id("one"), options => options.Cache(false)));
        var unrelated = await repository.GetByIdAsync(new Id("two"), options => options.Cache(false));
        Assert.NotNull(unrelated);
        Assert.Equal("before", unrelated.Value);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RemoveByIdsAsync_WhenRemoveOverridePartitionsDocuments_PreservesEachRequestedLocation(bool cloneOptions)
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new ForwardingOptionsRepository(configuration) { PartitionRemovals = true, CloneBaseOptions = cloneOptions };
        configuration.Invoker.AddStoredDocument(new TestDocument { Id = "three", Value = "requested source" });
        configuration.Invoker.AddStoredDocument(new TestDocument { Id = "four", Value = "unrelated source" });
        Assert.True(await repository.PatchAsync(new Id("one"), new PartialPatch(new { id = "two" }), options => options.Retry(0)));
        Assert.True(await repository.PatchAsync(new Id("three"), new PartialPatch(new { id = "four" }), options => options.Retry(0)));
        repository.PublishedIds.Clear();
        var removedSources = new List<string>();
        repository.DocumentsRemoved.AddSyncHandler((_, args) => removedSources.AddRange(args.Documents.Select(document => document.Id)));

        await repository.RemoveAsync(new Ids("one", "three"), options => options.ReadCache(false).Retry(0));

        Assert.Equal(1, repository.ReadCalls);
        Assert.Equal(1, repository.RemoveCalls);
        Assert.Equal(new[] { "one", "three" }, configuration.Invoker.Deletes);
        Assert.Equal(new[] { "one", "three" }, repository.PublishedIds);
        Assert.Equal(new[] { "two", "four" }, removedSources);
        Assert.Null(await repository.GetByIdAsync(new Id("one"), options => options.Cache(false)));
        Assert.Null(await repository.GetByIdAsync(new Id("three"), options => options.Cache(false)));
        var unrelatedTwo = await repository.GetByIdAsync(new Id("two"), options => options.Cache(false));
        Assert.NotNull(unrelatedTwo);
        Assert.Equal("before", unrelatedTwo.Value);
        var unrelatedFour = await repository.GetByIdAsync(new Id("four"), options => options.Cache(false));
        Assert.NotNull(unrelatedFour);
        Assert.Equal("unrelated source", unrelatedFour.Value);
    }

    [Fact]
    public async Task RemoveByIdsAsync_WhenOneModelReferenceRepresentsTwoLocations_FailsBeforeDeleting()
    {
        using var cache = new RecordingCache(cloneValues: false);
        using var configuration = new TestConfiguration(cache);
        using var repository = new TestRepository(configuration);
        var shared = new TestDocument { Id = "source-id", Value = "shared source" };
        await repository.SeedHitAsync("one", shared);
        await repository.SeedHitAsync("two", shared);

        await Assert.ThrowsAsync<DocumentException>(() => repository.RemoveAsync(new Ids("one", "two"), options => options.Retry(0)));

        Assert.Empty(configuration.Invoker.Deletes);
        Assert.Empty(repository.PublishedIds);
        Assert.Equal("source-id", shared.Id);
    }

    [Fact]
    public async Task RemoveAllAsync_WhenSelectedSourceIdNamesAnotherDocument_DeletesSelectedMetadataIdentity()
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new TestRepository(configuration);
        Assert.True(await repository.PatchAsync(new Id("one"), new PartialPatch(new { id = "two" }), options => options.Retry(0)));
        repository.PublishedIds.Clear();
        var removedSources = new List<string>();
        repository.DocumentsRemoved.AddSyncHandler((_, args) => removedSources.AddRange(args.Documents.Select(document => document.Id)));

        long removed = await repository.RemoveAllAsync(new RepositoryQuery<TestDocument>().Id("one"), new CommandOptions().Cache(false).Retry(0).PageLimit(10));

        Assert.Equal(1, removed);
        Assert.Equal(new[] { "one" }, configuration.Invoker.SearchedIds);
        Assert.Equal(new[] { "one" }, configuration.Invoker.Deletes);
        Assert.Equal(new[] { "one" }, repository.PublishedIds);
        Assert.Equal(new[] { "two" }, removedSources);
        Assert.Null(await repository.GetByIdAsync(new Id("one"), options => options.Cache(false)));
        var unrelated = await repository.GetByIdAsync(new Id("two"), options => options.Cache(false));
        Assert.NotNull(unrelated);
        Assert.Equal("before", unrelated.Value);
    }

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, false, false)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    [InlineData(true, true, true)]
    public async Task PatchAsync_WhenChangeHandlerReadsOrThrows_InvalidatesWriteLocationsBeforeTheHandler(bool sourceIdChanges, bool multipleIds, bool handlerThrows)
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new TestRepository(configuration, usePipeline: sourceIdChanges);
        string[] ids = multipleIds ? ["one", "two"] : ["one"];
        foreach (string id in ids)
        {
            var cached = await repository.GetByIdAsync(id, options => options.Cache());
            Assert.NotNull(cached);
            Assert.Equal("before", cached.Value);
        }

        var observedValues = new List<string>();
        repository.DocumentsChanged.AddHandler(async (_, _) =>
        {
            foreach (string id in ids)
            {
                var current = await repository.GetByIdAsync(id, options => options.Cache());
                Assert.NotNull(current);
                observedValues.Add(current.Value);
            }

            if (handlerThrows)
                throw new InvalidOperationException("Post-patch handler failed.");
        });
        var patch = sourceIdChanges
            ? new PartialPatch(new { id = "changed-source-id", value = "patched" })
            : new PartialPatch(new { value = "patched" });

        var exception = await Record.ExceptionAsync(async () =>
        {
            if (multipleIds)
                Assert.Equal(ids.Length, await repository.PatchAsync(new Ids(ids), patch, options => options.Retry(0)));
            else
                Assert.True(await repository.PatchAsync(ids[0], patch, options => options.Retry(0)));
        });

        if (handlerThrows)
        {
            Assert.IsType<InvalidOperationException>(exception);
            Assert.Equal("Post-patch handler failed.", exception.Message);
        }
        else
        {
            Assert.Null(exception);
            Assert.Equal(ids, repository.PublishedIds);
        }
        Assert.Equal(ids.Length, observedValues.Count);
        Assert.All(observedValues, value => Assert.Equal("patched", value));
        foreach (string id in ids)
        {
            var current = await repository.GetByIdAsync(id, options => options.Cache());
            Assert.NotNull(current);
            Assert.Equal("patched", current.Value);
        }
        Assert.Equal(sourceIdChanges ? 1 : 0, repository.DocumentInvalidations);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task PatchAsync_WhenUpdatedIdsCallbackReadsOrThrows_PreservesCacheAndCallbackOrder(bool sourceIdChanges, bool callbackThrows)
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new TestRepository(configuration, usePipeline: sourceIdChanges);
        string[] ids = ["one", "two"];
        foreach (string id in ids)
            Assert.NotNull(await repository.GetByIdAsync(id, options => options.Cache()));

        int changeEvents = 0;
        repository.DocumentsChanged.AddSyncHandler((_, _) => changeEvents++);
        int changeEventsAtCallback = 0;
        var notificationsAtCallback = new List<string?>();
        var observedReads = new List<Task<TestDocument?>>();
        var updatedIds = new List<string>();
        var patch = sourceIdChanges
            ? new PartialPatch(new { id = "changed-source-id", value = "patched" })
            : new PartialPatch(new { value = "patched" });

        long modified = await repository.PatchAsync(new Ids(ids), patch, options => options.Retry(0).UpdateCallback(changed =>
        {
            changeEventsAtCallback = changeEvents;
            notificationsAtCallback.AddRange(repository.PublishedIds);
            updatedIds.AddRange(changed);
            foreach (string id in changed)
                observedReads.Add(repository.GetByIdAsync(id, readOptions => readOptions.Cache()));
            if (callbackThrows)
                throw new InvalidOperationException("Updated IDs callback failed.");
        }));

        Assert.Equal(ids.Length, modified);
        Assert.Equal(1, changeEventsAtCallback);
        Assert.Equal(ids, notificationsAtCallback);
        Assert.Equal(ids, updatedIds);
        var observed = await Task.WhenAll(observedReads);
        Assert.Equal(ids.Length, observed.Length);
        Assert.All(observed, document =>
        {
            Assert.NotNull(document);
            Assert.Equal("patched", document.Value);
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchAsync_WhenChangeHandlerMutatesSourceIds_NotifiesOriginalWriteLocations(bool multipleIds)
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new TestRepository(configuration);
        repository.DocumentsChanged.AddSyncHandler((_, args) =>
        {
            foreach (var document in args.Documents)
                document.Value.Id = "unsaved-handler-id";
        });
        var patch = new PartialPatch(new { value = "patched" });

        if (multipleIds)
            Assert.Equal(2, await repository.PatchAsync(new Ids("one", "two"), patch, options => options.Retry(0)));
        else
            Assert.True(await repository.PatchAsync("one", patch, options => options.Retry(0)));

        Assert.Equal(multipleIds ? new[] { "one", "two" } : ["one"], repository.PublishedIds);
        Assert.Equal(1, repository.DocumentInvalidations);
    }

    public sealed class TestDocument : IIdentity
    {
        public string Id { get; set; } = null!;
        public string Value { get; set; } = null!;
    }

    private class TestRepository : ElasticRepositoryBase<TestDocument>
    {
        public TestRepository(TestConfiguration configuration, bool usePipeline = true) : base(new TestIndex(configuration))
        {
            DefaultPipeline = usePipeline ? "patch-cache-pipeline" : null;
        }

        public int DocumentInvalidations { get; private set; }
        public List<string?> PublishedIds { get; } = [];

        public Task SeedAsync(string id) => Cache.SetAsync(id, "cached");
        public Task<bool> CacheContainsAsync(string id) => Cache.ExistsAsync(id);
        public Task SeedHitAsync(string id, TestDocument document, string? routing = null) => Cache.SetAsync<ICollection<FindHit<TestDocument>>>(id,
            [new FindHit<TestDocument>(id, document, 0, routing: routing, data: new Dictionary<string, object?> { [ElasticDataKeys.Index] = "patch-cache-tests" })]);

        protected override Task InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<TestDocument>> documents, ChangeType? changeType = null)
        {
            if (documents.Count > 0)
                DocumentInvalidations++;

            return base.InvalidateCacheAsync(documents, changeType);
        }

        protected override Task PublishMessageAsync(EntityChanged message, TimeSpan? delay = null)
        {
            PublishedIds.Add(message.Id);
            return Task.CompletedTask;
        }
    }

    private sealed class ForwardingOptionsRepository(TestConfiguration configuration) : TestRepository(configuration)
    {
        public int ReadCalls { get; private set; }
        public int RemoveCalls { get; private set; }
        public Func<ICommandOptions, Task>? BeforeRead { get; set; }
        public Func<IReadOnlyCollection<TestDocument>, ICommandOptions, Task>? BeforeRemove { get; set; }
        public Func<IReadOnlyCollection<TestDocument>, ICommandOptions, Task>? AfterRead { get; set; }
        public Func<IReadOnlyCollection<TestDocument>, ICommandOptions, Task>? AfterRemove { get; set; }
        public bool PartitionRemovals { get; init; }
        public bool CloneBaseOptions { get; init; } = true;

        public override async Task<IReadOnlyCollection<TestDocument>> GetByIdsAsync(Ids ids, ICommandOptions? options = null)
        {
            ReadCalls++;
            options ??= new CommandOptions();
            if (BeforeRead is { } before)
                await before(options);
            var documents = await base.GetByIdsAsync(ids, CloneBaseOptions ? options.Clone() : options);
            if (AfterRead is { } callback)
                await callback(documents, options);
            return documents;
        }

        public override async Task RemoveAsync(IEnumerable<TestDocument> documents, ICommandOptions? options = null)
        {
            RemoveCalls++;
            options ??= new CommandOptions();
            var docs = documents.ToList();
            if (BeforeRemove is { } before)
                await before(docs, options);
            if (PartitionRemovals)
            {
                foreach (var document in docs)
                    await base.RemoveAsync([document], CloneBaseOptions ? options.Clone() : options);
            }
            else
                await base.RemoveAsync(docs, CloneBaseOptions ? options.Clone() : options);
            if (AfterRemove is { } callback)
                await callback(docs, options);
        }
    }

    private sealed class TestIndex(TestConfiguration configuration) : Index<TestDocument>(configuration, "patch-cache-tests")
    {
        public override Task EnsureIndexAsync(object? target) => Task.CompletedTask;
    }

    private sealed class RecordingCache : InMemoryCacheClient, ICacheClient
    {
        public RecordingCache()
        {
        }

        public RecordingCache(bool cloneValues) : base(new InMemoryCacheClientOptions { CloneValues = cloneValues })
        {
        }

        public List<string[]> Removals { get; } = [];

        Task<int> ICacheClient.RemoveAllAsync(IEnumerable<string>? keys)
        {
            var removedKeys = keys?.ToArray();
            if (removedKeys is { Length: > 0 })
                Removals.Add(removedKeys);

            return base.RemoveAllAsync(removedKeys);
        }
    }

    private sealed class TestConfiguration(RecordingCache cache) : ElasticConfiguration(cacheClient: cache)
    {
        public PatchResponseInvoker Invoker { get; } = new();
        protected override IRequestInvoker CreateRequestInvoker() => Invoker;
    }

    private sealed class PatchResponseInvoker : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _owner = new();
        private readonly Dictionary<string, TestDocument> _documents = new(StringComparer.Ordinal)
        {
            ["one"] = new() { Id = "one", Value = "before" },
            ["two"] = new() { Id = "two", Value = "before" }
        };
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };
        public ResponseFactory ResponseFactory => _owner.ResponseFactory;
        public List<string> Deletes { get; } = [];
        public List<string> DeletePaths { get; } = [];
        public List<string> SearchedIds { get; } = [];
        public Dictionary<string, string> StoredRoutings { get; } = new(StringComparer.Ordinal);

        public void AddStoredDocument(TestDocument document) => _documents.Add(document.Id, document);

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            using var requestStream = new MemoryStream();
            postData?.Write(requestStream, boundConfiguration.ConnectionSettings, false);
            requestStream.Position = 0;
            int statusCode = 200;
            object response;
            if (endpoint.PathAndQuery.Contains("_mapping", StringComparison.Ordinal))
            {
                response = new Dictionary<string, object>
                {
                    ["patch-cache-tests"] = new { mappings = new { properties = new { id = new { type = "keyword" }, value = new { type = "keyword" } } } }
                };
            }
            else if (endpoint.PathAndQuery.Contains("_search", StringComparison.Ordinal))
            {
                using var request = JsonDocument.Parse(requestStream);
                string[] ids = GetSearchIds(request.RootElement);
                SearchedIds.AddRange(ids);
                var hits = _documents.Where(document => ids.Contains(document.Key)).Select(document => new
                {
                    _index = "patch-cache-tests",
                    _id = document.Key,
                    _seq_no = 0,
                    _primary_term = 1,
                    sort = new[] { document.Value.Id },
                    _source = new { id = document.Value.Id, value = document.Value.Value }
                }).ToArray();
                response = new { took = 1, timed_out = false, hits = new { total = new { value = hits.Length, relation = "eq" }, hits } };
            }
            else if (endpoint.PathAndQuery.Contains("_mget", StringComparison.Ordinal))
            {
                using var request = JsonDocument.Parse(requestStream);
                response = new { docs = request.RootElement.GetProperty("docs").EnumerateArray().Select(document => GetDocumentResponse(document.GetProperty("_id").GetString()!)).ToArray() };
            }
            else if (endpoint.PathAndQuery.Contains("_bulk", StringComparison.Ordinal))
            {
                using var reader = new StreamReader(requestStream);
                var items = new List<object>();
                while (reader.ReadLine() is { } line)
                {
                    using var metadata = JsonDocument.Parse(line);
                    var operation = Assert.Single(metadata.RootElement.EnumerateObject());
                    string id = operation.Value.GetProperty("_id").GetString()!;
                    if (operation.Name is "delete")
                    {
                        Deletes.Add(id);
                        bool removed = _documents.Remove(id);
                        items.Add(new { delete = new { _index = "patch-cache-tests", _id = id, _seq_no = 2, _primary_term = 1, status = removed ? 200 : 404, result = removed ? "deleted" : "not_found" } });
                        continue;
                    }
                    string? documentJson = reader.ReadLine();
                    Assert.NotNull(documentJson);
                    using var source = JsonDocument.Parse(documentJson);
                    ApplyDocument(id, operation.Name is "update" ? source.RootElement.GetProperty("doc") : source.RootElement);
                    items.Add(new Dictionary<string, object>
                    {
                        [operation.Name] = new { _index = "patch-cache-tests", _id = id, _seq_no = 1, _primary_term = 1, status = 200, result = "updated" }
                    });
                }

                response = new { took = 1, errors = false, items };
            }
            else
            {
                var uri = new Uri(new Uri("http://localhost/"), endpoint.PathAndQuery);
                string id = Uri.UnescapeDataString(uri.Segments[^1]);
                if (endpoint.Method is Elastic.Transport.HttpMethod.DELETE)
                {
                    Deletes.Add(id);
                    DeletePaths.Add(endpoint.PathAndQuery);
                    bool removed = _documents.Remove(id);
                    statusCode = removed ? 200 : 404;
                    response = new { _index = "patch-cache-tests", _id = id, _seq_no = 2, _primary_term = 1, result = removed ? "deleted" : "not_found", _shards = new { total = 1, successful = 1, failed = 0 } };
                }
                else if (endpoint.PathAndQuery.Contains("/_update/", StringComparison.Ordinal))
                {
                    using var request = JsonDocument.Parse(requestStream);
                    ApplyDocument(id, request.RootElement.GetProperty("doc"));
                    response = new { _index = "patch-cache-tests", _id = id, _seq_no = 1, _primary_term = 1, result = "updated", _shards = new { total = 1, successful = 1, failed = 0 } };
                }
                else
                {
                    Assert.Contains("/_doc/", endpoint.PathAndQuery, StringComparison.Ordinal);
                    response = GetDocumentResponse(id);
                    statusCode = _documents.ContainsKey(id) ? 200 : 404;
                }
            }

            byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(response);
            using var responseStream = new MemoryStream(bytes);
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, statusCode, _headers, responseStream, "application/json", bytes.Length, null, null);
        }

        private object GetDocumentResponse(string id)
        {
            if (!_documents.TryGetValue(id, out var document))
                return new { _index = "patch-cache-tests", _id = id, found = false };
            return new { _index = "patch-cache-tests", _id = id, _routing = StoredRoutings.GetValueOrDefault(id), _seq_no = 0, _primary_term = 1, found = true, _source = new { id = document.Id, value = document.Value } };
        }

        private static string[] GetSearchIds(JsonElement element)
        {
            if (element.ValueKind is JsonValueKind.Object)
            {
                if (element.TryGetProperty("ids", out var ids))
                    return ids.GetProperty("values").EnumerateArray().Select(id => id.GetString()!).ToArray();

                foreach (var property in element.EnumerateObject())
                {
                    string[] found = GetSearchIds(property.Value);
                    if (found.Length > 0)
                        return found;
                }
            }
            else if (element.ValueKind is JsonValueKind.Array)
            {
                foreach (var item in element.EnumerateArray())
                {
                    string[] found = GetSearchIds(item);
                    if (found.Length > 0)
                        return found;
                }
            }

            return [];
        }

        private void ApplyDocument(string id, JsonElement source)
        {
            var document = _documents[id];
            if (source.TryGetProperty("id", out var sourceId))
                document.Id = sourceId.GetString()!;
            if (source.TryGetProperty("value", out var value))
                document.Value = value.GetString()!;
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
