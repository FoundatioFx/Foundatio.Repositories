using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Xunit;
using HttpMethod = Elastic.Transport.HttpMethod;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ProjectionCacheTests
{
    [Theory]
    [InlineData("Include", false)]
    [InlineData("Include", true)]
    [InlineData("Exclude", false)]
    [InlineData("Exclude", true)]
    [InlineData("IncludeMask", false)]
    [InlineData("IncludeMask", true)]
    [InlineData("ExcludeMask", false)]
    [InlineData("ExcludeMask", true)]
    public async Task GetByIdAsync_WithCallerSourceFiltering_DoesNotReplaceUnfilteredIdCache(string restriction, bool customCacheKey)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        var options = new CommandOptions().Cache();
        if (customCacheKey)
            options.CacheKey("projected-document");
        switch (restriction)
        {
            case "Include":
                options.Include("name");
                break;
            case "Exclude":
                options.Exclude("detail");
                break;
            case "IncludeMask":
                options.IncludeMask("name");
                break;
            case "ExcludeMask":
                options.ExcludeMask("detail");
                break;
        }

        var projected = await repository.GetByIdAsync("one", options);
        Assert.NotNull(projected);
        Assert.Equal("full name", projected.Name);
        Assert.Null(projected.Detail);
        Assert.Equal("3:10", projected.Version);
        if (customCacheKey)
        {
            var cachedProjection = await repository.GetByIdAsync("one", options);
            Assert.NotNull(cachedProjection);
            Assert.Null(cachedProjection.Detail);
            Assert.Equal(1, configuration.Invoker.Reads);
        }

        var full = await repository.GetByIdAsync("one", new CommandOptions().Cache());

        Assert.NotNull(full);
        Assert.Equal("full name", full.Name);
        Assert.Equal("full detail", full.Detail);
        Assert.Equal("3:10", full.Version);
        Assert.Equal(2, configuration.Invoker.Reads);
        var cachedFull = await repository.GetByIdAsync("one", new CommandOptions().Cache());
        Assert.NotNull(cachedFull);
        Assert.Equal("full detail", cachedFull.Detail);
        Assert.Equal(2, configuration.Invoker.Reads);
    }

    [Theory]
    [InlineData("Include", false, false)]
    [InlineData("Include", false, true)]
    [InlineData("Include", true, false)]
    [InlineData("Include", true, true)]
    [InlineData("Exclude", false, false)]
    [InlineData("Exclude", false, true)]
    [InlineData("Exclude", true, false)]
    [InlineData("Exclude", true, true)]
    [InlineData("IncludeMask", false, false)]
    [InlineData("IncludeMask", false, true)]
    [InlineData("IncludeMask", true, false)]
    [InlineData("IncludeMask", true, true)]
    [InlineData("ExcludeMask", false, false)]
    [InlineData("ExcludeMask", false, true)]
    [InlineData("ExcludeMask", true, false)]
    [InlineData("ExcludeMask", true, true)]
    public async Task IdRead_WithCallerSourceFilteringAndWarmIdCache_ReturnsTheProjection(string restriction, bool multipleIds, bool customCacheKey)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        var full = await repository.GetByIdAsync("one", new CommandOptions().Cache());
        Assert.NotNull(full);
        Assert.Equal("full detail", full.Detail);

        var options = new CommandOptions().Cache();
        if (customCacheKey)
            options.CacheKey("projected-after-full");
        options = restriction switch
        {
            "Include" => options.Include("name"),
            "Exclude" => options.Exclude("detail"),
            "IncludeMask" => options.IncludeMask("name"),
            "ExcludeMask" => options.ExcludeMask("detail"),
            _ => throw new ArgumentException("Unknown source restriction.", nameof(restriction))
        };

        var projected = await ReadProjectionAsync();

        Assert.Equal(multipleIds ? 2 : 1, projected.Count);
        Assert.All(projected, document =>
        {
            Assert.Equal("full name", document.Name);
            Assert.Null(document.Detail);
            Assert.Equal("3:10", document.Version);
        });
        int requests = configuration.Invoker.Reads + configuration.Invoker.MultiGets;
        Assert.Equal(2, requests);
        if (customCacheKey)
        {
            var cachedProjection = await ReadProjectionAsync();
            Assert.Equal(projected.Count, cachedProjection.Count);
            Assert.All(cachedProjection, document => Assert.Null(document.Detail));
            Assert.Equal(requests, configuration.Invoker.Reads + configuration.Invoker.MultiGets);
        }

        var cachedFull = await repository.GetByIdAsync("one", new CommandOptions().Cache());
        Assert.NotNull(cachedFull);
        Assert.Equal("full detail", cachedFull.Detail);
        Assert.Equal(requests, configuration.Invoker.Reads + configuration.Invoker.MultiGets);

        async Task<IReadOnlyCollection<TestDocument>> ReadProjectionAsync()
        {
            if (multipleIds)
                return await repository.GetByIdsAsync(new Ids("one", "two"), options);

            return [Assert.IsType<TestDocument>(await repository.GetByIdAsync("one", options))];
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task GetByIdsAsync_WithEventualDiscovery_CachesOnlyRealtimeHitsById(bool mixedLocations, bool customCacheKey)
    {
        using var configuration = new TestConfiguration();
        configuration.Invoker.SearchRouting = "discovered-parent";
        configuration.Invoker.SearchName = "stale name";
        configuration.Invoker.RealtimeName = "current name";
        configuration.Invoker.RealtimeSequenceNumber = 11;
        using var repository = new TestRepository(configuration, hasParent: true);
        var ids = new Ids("one");
        if (mixedLocations)
            ids.Add(new Id("two", "known-parent"));
        var options = new CommandOptions().Cache();
        if (customCacheKey)
            options.CacheKey("discovery-result");

        var documents = await repository.GetByIdsAsync(ids, options);

        var searchDocument = Assert.Single(documents, document => document.Id is "one");
        Assert.Equal("stale name", searchDocument.Name);
        Assert.Equal("3:10", searchDocument.Version);
        Assert.Equal(mixedLocations ? 2 : 1, documents.Count);
        if (customCacheKey)
        {
            var cachedQueryDocument = await repository.GetByIdAsync(new Id("one", "discovered-parent"),
                new CommandOptions().ReadCache().CacheKey("discovery-result"));
            Assert.NotNull(cachedQueryDocument);
            Assert.Equal("stale name", cachedQueryDocument.Name);
            Assert.Equal("3:10", cachedQueryDocument.Version);
            Assert.Equal(0, configuration.Invoker.Reads);
        }
        if (mixedLocations)
        {
            var knownDocument = await repository.GetByIdAsync(new Id("two", "known-parent"), new CommandOptions().Cache());
            Assert.NotNull(knownDocument);
            Assert.Equal("current name", knownDocument.Name);
            Assert.Equal("3:11", knownDocument.Version);
            Assert.Equal(0, configuration.Invoker.Reads);
        }

        var current = await repository.GetByIdAsync(new Id("one", "discovered-parent"), new CommandOptions().Cache());

        Assert.NotNull(current);
        Assert.Equal("current name", current.Name);
        Assert.Equal("3:11", current.Version);
        Assert.Equal(1, configuration.Invoker.Reads);
        Assert.Equal(1, configuration.Invoker.Searches);
        Assert.Equal(mixedLocations ? 1 : 0, configuration.Invoker.MultiGets);
        var cachedCurrent = await repository.GetByIdAsync(new Id("one", "discovered-parent"), new CommandOptions().Cache());
        Assert.NotNull(cachedCurrent);
        Assert.Equal("current name", cachedCurrent.Name);
        Assert.Equal(1, configuration.Invoker.Reads);
    }

    [Fact]
    public async Task GetByIdsAsync_WithImmediateDiscovery_PopulatesTheSharedIdCache()
    {
        using var configuration = new TestConfiguration();
        configuration.Invoker.SearchRouting = "parent";
        using var repository = new TestRepository(configuration, hasParent: true);

        var found = Assert.Single(await repository.GetByIdsAsync(new Ids("one"), new CommandOptions().Cache().ImmediateConsistency()));
        var cached = await repository.GetByIdAsync(new Id("one", "parent"), new CommandOptions().Cache());

        Assert.NotNull(cached);
        Assert.Equal(found.Name, cached.Name);
        Assert.Equal(found.Version, cached.Version);
        Assert.Equal(1, configuration.Invoker.Searches);
        Assert.Equal(0, configuration.Invoker.Reads);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetByIdsAsync_WithEventualDiscoveryMiss_PreservesRealtimeCacheGroups(bool customCacheKey)
    {
        using var configuration = new TestConfiguration();
        configuration.Invoker.SearchReturnsNoHits = true;
        configuration.Invoker.RealtimeName = "current name";
        configuration.Invoker.RealtimeSequenceNumber = 11;
        using var repository = new TestRepository(configuration, hasParent: true);
        var current = await repository.GetByIdAsync(new Id("one", "known-parent"), new CommandOptions().Cache());
        Assert.NotNull(current);
        var ids = new Ids("one", "missing") { new Id("two", "known-parent") };
        var options = new CommandOptions().Cache().ReadCache(false);
        if (customCacheKey)
            options.CacheKey("discovery-misses");

        var documents = await repository.GetByIdsAsync(ids, options);

        Assert.Equal("two", Assert.Single(documents).Id);
        var retained = Assert.Single(await repository.GetCachedHitsAsync("one"));
        Assert.NotNull(retained.Document);
        Assert.Equal("current name", retained.Document.Name);
        Assert.Equal("3:11", retained.Version);
        Assert.Equal("known-parent", retained.Routing);
        Assert.False(await repository.CacheContainsAsync("missing"));
        var known = Assert.Single(await repository.GetCachedHitsAsync("two"));
        Assert.NotNull(known.Document);
        Assert.Equal("current name", known.Document.Name);
        Assert.Equal("3:11", known.Version);
        if (customCacheKey)
        {
            var queryHits = await repository.GetCachedHitsAsync("discovery-misses");
            Assert.Equal(3, queryHits.Count);
            Assert.Equal("two", Assert.Single(queryHits, hit => hit.Document is not null).Id);
            Assert.Equal(new[] { "missing", "one" }, queryHits.Where(hit => hit.Document is null).Select(hit => hit.Id).OrderBy(id => id));
        }

        var cached = await repository.GetByIdAsync(new Id("one", "known-parent"), new CommandOptions().Cache());
        Assert.NotNull(cached);
        Assert.Equal("current name", cached.Name);
        Assert.Equal(1, configuration.Invoker.Reads);
        Assert.Equal(1, configuration.Invoker.MultiGets);
        Assert.Equal(1, configuration.Invoker.Searches);
    }

    [Fact]
    public async Task GetByIdsAsync_WithRealtimeAndDiscoveredRoutesForOneId_DoesNotCacheAnUnsafeIdGroup()
    {
        using var configuration = new TestConfiguration();
        configuration.Invoker.SearchRouting = "discovered-parent";
        configuration.Invoker.SearchName = "stale name";
        configuration.Invoker.RealtimeName = "current name";
        configuration.Invoker.RealtimeSequenceNumber = 11;
        using var repository = new TestRepository(configuration, hasParent: true);
        var ids = new Ids(new Id("one", "known-parent"), new Id("one"), new Id("two", "known-parent"));

        var documents = await repository.GetByIdsAsync(ids, new CommandOptions().Cache("mixed-routes"));

        Assert.Equal(3, documents.Count);
        Assert.Equal(2, documents.Count(document => document.Id is "one"));
        Assert.False(await repository.CacheContainsAsync("one"));
        var known = Assert.Single(await repository.GetCachedHitsAsync("two"));
        Assert.NotNull(known.Document);
        Assert.Equal("current name", known.Document.Name);
        Assert.Equal("3:11", known.Version);
        var queryHits = await repository.GetCachedHitsAsync("mixed-routes");
        Assert.Equal(3, queryHits.Count);
        var discovered = Assert.Single(queryHits, hit => hit.Routing is "discovered-parent");
        Assert.NotNull(discovered.Document);
        Assert.Equal("stale name", discovered.Document.Name);
        Assert.Equal("3:10", discovered.Version);

        var current = await repository.GetByIdAsync(new Id("one", "discovered-parent"), new CommandOptions().Cache());
        Assert.NotNull(current);
        Assert.Equal("current name", current.Name);
        Assert.Equal("3:11", current.Version);
        Assert.Equal(1, configuration.Invoker.Reads);
        Assert.Equal(1, configuration.Invoker.MultiGets);
        Assert.Equal(1, configuration.Invoker.Searches);
    }

    [Theory]
    [InlineData("Include", false, SoftDeleteQueryMode.ActiveOnly, false)]
    [InlineData("Include", false, SoftDeleteQueryMode.DeletedOnly, false)]
    [InlineData("Include", true, SoftDeleteQueryMode.ActiveOnly, false)]
    [InlineData("Include", true, SoftDeleteQueryMode.DeletedOnly, false)]
    [InlineData("Exclude", false, SoftDeleteQueryMode.ActiveOnly, false)]
    [InlineData("Exclude", false, SoftDeleteQueryMode.DeletedOnly, false)]
    [InlineData("Exclude", true, SoftDeleteQueryMode.ActiveOnly, false)]
    [InlineData("Exclude", true, SoftDeleteQueryMode.DeletedOnly, false)]
    [InlineData("Include", false, SoftDeleteQueryMode.ActiveOnly, true)]
    [InlineData("Include", false, SoftDeleteQueryMode.DeletedOnly, true)]
    [InlineData("Include", true, SoftDeleteQueryMode.ActiveOnly, true)]
    [InlineData("Include", true, SoftDeleteQueryMode.DeletedOnly, true)]
    [InlineData("Exclude", false, SoftDeleteQueryMode.ActiveOnly, true)]
    [InlineData("Exclude", false, SoftDeleteQueryMode.DeletedOnly, true)]
    [InlineData("Exclude", true, SoftDeleteQueryMode.ActiveOnly, true)]
    [InlineData("Exclude", true, SoftDeleteQueryMode.DeletedOnly, true)]
    public async Task IdRead_WithProjectedSoftDeleteFlag_PreservesTheRequestedMode(string restriction, bool multipleIds, SoftDeleteQueryMode mode, bool readOnlyRepository)
    {
        using var configuration = new TestConfiguration();
        configuration.Invoker.DeletedIds.Add("one");
        using ElasticReadOnlyRepositoryBase<SoftDeleteDocument> repository = readOnlyRepository
            ? new SoftDeleteReadOnlyRepository(configuration)
            : new SoftDeleteRepository(configuration);
        var options = new CommandOptions().Cache(false).SoftDeleteMode(mode);
        if (restriction is "Include")
            options.Include("id").Include("name");
        else
            options.Exclude("is_deleted");

        IReadOnlyCollection<SoftDeleteDocument> found;
        if (multipleIds)
        {
            found = await repository.GetByIdsAsync(new Ids("one", "two"), options);
        }
        else
        {
            var document = await repository.GetByIdAsync("one", options);
            found = document is null ? [] : [document];
        }

        if (mode is SoftDeleteQueryMode.ActiveOnly && !multipleIds)
        {
            Assert.Empty(found);
        }
        else
        {
            var document = Assert.Single(found);
            Assert.Equal(mode is SoftDeleteQueryMode.DeletedOnly ? "one" : "two", document.Id);
            Assert.Equal(mode is SoftDeleteQueryMode.DeletedOnly, document.IsDeleted);
            Assert.Equal("full name", document.Name);
        }
        Assert.Equal(multipleIds ? 1 : 0, configuration.Invoker.MultiGets);
        Assert.Equal(multipleIds ? 0 : 1, configuration.Invoker.Reads);
        Assert.Equal(0, configuration.Invoker.Searches);
    }

    [Theory]
    [InlineData("is_*", false, false)]
    [InlineData("is_*", false, true)]
    [InlineData("is_*", true, false)]
    [InlineData("is_*", true, true)]
    [InlineData("*", false, false)]
    [InlineData("*", false, true)]
    [InlineData("*", true, false)]
    [InlineData("*", true, true)]
    public async Task IdReads_WithWildcardExcludeCoveringSoftDeleteField_RejectBeforeReading(string exclusion, bool multipleIds, bool mask)
    {
        using var configuration = new TestConfiguration();
        configuration.Invoker.DeletedIds.Add("one");
        using var repository = new SoftDeleteReadOnlyRepository(configuration);
        var options = new CommandOptions().Cache(false).SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly).Include(["id", "name"]);
        if (mask)
            options.ExcludeMask(exclusion);
        else
            options.Exclude(exclusion);

        var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            if (multipleIds)
                await repository.GetByIdsAsync(new Ids("one", "two"), options);
            else
                await repository.GetByIdAsync("one", options);
        });

        Assert.Contains("is_deleted", exception.Message, StringComparison.Ordinal);
        Assert.Equal(0, configuration.Invoker.Reads);
        Assert.Equal(0, configuration.Invoker.MultiGets);
        Assert.Equal(0, configuration.Invoker.Searches);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IdReads_WithUnrelatedWildcardExclude_PreserveTheRequestedSourceFilter(bool multipleIds)
    {
        using var configuration = new TestConfiguration();
        configuration.Invoker.DeletedIds.Add("one");
        using var repository = new SoftDeleteRepository(configuration);
        var options = new CommandOptions().Cache(false).SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly).Exclude("det*");

        var document = multipleIds
            ? Assert.Single(await repository.GetByIdsAsync(new Ids("one", "two"), options))
            : await repository.GetByIdAsync("one", options);

        Assert.NotNull(document);
        Assert.Equal("one", document.Id);
        Assert.True(document.IsDeleted);
        Assert.Equal("full name", document.Name);
        Assert.Null(document.Detail);
        Assert.Equal(new[] { "det*" }, configuration.Invoker.LastSourceExcludes);
        Assert.Equal(multipleIds ? 0 : 1, configuration.Invoker.Reads);
        Assert.Equal(multipleIds ? 1 : 0, configuration.Invoker.MultiGets);
    }

    [Theory]
    [InlineData("Include", false)]
    [InlineData("Include", true)]
    [InlineData("Exclude", false)]
    [InlineData("Exclude", true)]
    public async Task IdReads_WithReusedAndClonedOptions_KeepRequiredFieldsBounded(string restriction, bool readOnlyRepository)
    {
        using var configuration = new TestConfiguration();
        configuration.Invoker.DeletedIds.Add("one");
        using ElasticReadOnlyRepositoryBase<SoftDeleteDocument> repository = readOnlyRepository
            ? new SoftDeleteReadOnlyRepository(configuration)
            : new SoftDeleteRepository(configuration);
        var options = new CommandOptions().Cache(false).RequiredFields(["id", "name"]);
        if (restriction is "Include")
            options.Include("id");
        else
            options.Exclude(["name", "detail", "is_deleted"]);
        var clone = options.Clone();

        for (int iteration = 0; iteration < 4; iteration++)
        {
            var currentOptions = iteration % 2 is 0 ? options : clone;
            currentOptions.SoftDeleteMode(SoftDeleteQueryMode.ActiveOnly);
            Assert.Null(await repository.GetByIdAsync("one", currentOptions));
            var active = Assert.Single(await repository.GetByIdsAsync(new Ids("one", "two"), currentOptions));
            Assert.Equal("two", active.Id);
            Assert.False(active.IsDeleted);
            Assert.Equal("full name", active.Name);
            Assert.Null(active.Detail);

            currentOptions.SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly);
            var deleted = await repository.GetByIdAsync("one", currentOptions);
            Assert.NotNull(deleted);
            Assert.True(deleted.IsDeleted);
            Assert.Equal("full name", deleted.Name);
            Assert.Null(deleted.Detail);
            var deletedBatch = Assert.Single(await repository.GetByIdsAsync(new Ids("one", "two"), currentOptions));
            Assert.Equal("one", deletedBatch.Id);
            Assert.True(deletedBatch.IsDeleted);
            Assert.Equal("full name", deletedBatch.Name);
            Assert.Null(deletedBatch.Detail);

            Assert.Equal(new[] { "id", "is_deleted", "name" }, currentOptions.GetRequiredFields()
                .Select(configuration.Client.Infer.Field).OrderBy(name => name, StringComparer.Ordinal));
        }

        Assert.Equal(3, options.GetRequiredFields().Count);
        Assert.Equal(3, clone.GetRequiredFields().Count);
        Assert.Equal(0, configuration.Invoker.Searches);
    }

    [Fact]
    public async Task GetByIdAsync_WithRepositoryDefaultExclude_StillCachesTheDefaultDocument()
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration, defaultExclude: true);

        var first = await repository.GetByIdAsync("one", new CommandOptions().Cache());
        var second = await repository.GetByIdAsync("one", new CommandOptions().Cache());

        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.Equal("full name", second.Name);
        Assert.Null(first.Detail);
        Assert.Null(second.Detail);
        Assert.Equal(1, configuration.Invoker.Reads);
    }

    [Theory]
    [InlineData("Include")]
    [InlineData("Exclude")]
    [InlineData("IncludeMask")]
    [InlineData("ExcludeMask")]
    public async Task FindOneAsync_WithQuerySourceFiltering_DoesNotReplaceUnfilteredIdCache(string restriction)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        var query = new RepositoryQuery<TestDocument>().Id("one");
        switch (restriction)
        {
            case "Include":
                query.Include("name");
                break;
            case "Exclude":
                query.Exclude("detail");
                break;
            case "IncludeMask":
                query.IncludeMask("name");
                break;
            case "ExcludeMask":
                query.ExcludeMask("detail");
                break;
        }
        var options = new CommandOptions().Cache("projected-query").ImmediateConsistency();

        var projected = await repository.FindOneAsync(query, options);
        Assert.NotNull(projected.Document);
        Assert.Equal("full name", projected.Document.Name);
        Assert.Null(projected.Document.Detail);
        Assert.Equal("3:10", projected.Document.Version);
        var cachedProjection = await repository.FindOneAsync(query, options);
        Assert.NotNull(cachedProjection.Document);
        Assert.Null(cachedProjection.Document.Detail);
        Assert.Equal(1, configuration.Invoker.Searches);

        var full = await repository.GetByIdAsync("one", new CommandOptions().Cache());

        Assert.NotNull(full);
        Assert.Equal("full name", full.Name);
        Assert.Equal("full detail", full.Detail);
        Assert.Equal("3:10", full.Version);
        Assert.Equal(1, configuration.Invoker.Reads);
    }

    [Theory]
    [InlineData("Include")]
    [InlineData("Exclude")]
    public async Task FindOneAsync_WhenConfigureQueryReturnsAProjectedQuery_DoesNotReplaceUnfilteredIdCache(string restriction)
    {
        using var configuration = new TestConfiguration();
        var configuredQuery = new RepositoryQuery<TestDocument>().Id("one");
        if (restriction is "Include")
            configuredQuery.Include("name");
        else
            configuredQuery.Exclude("detail");
        using var repository = new TestRepository(configuration, configuredQuery: configuredQuery);
        var query = new RepositoryQuery<TestDocument>().Id("one");
        var options = new CommandOptions().Cache("configured-projection").ImmediateConsistency();

        var projected = await repository.FindOneAsync(query, options);

        Assert.NotNull(projected.Document);
        Assert.Equal("full name", projected.Document.Name);
        Assert.Null(projected.Document.Detail);
        Assert.Equal("3:10", projected.Document.Version);
        Assert.Equal(1, repository.ConfigureQueryCalls);
        Assert.Equal(1, configuration.Invoker.Searches);
        Assert.Empty(query.GetIncludes());
        Assert.Empty(query.GetExcludes());

        var full = await repository.GetByIdAsync("one", new CommandOptions().Cache());

        Assert.NotNull(full);
        Assert.Equal("full name", full.Name);
        Assert.Equal("full detail", full.Detail);
        Assert.Equal("3:10", full.Version);
        Assert.Equal(1, configuration.Invoker.Reads);
        Assert.Equal(1, repository.ConfigureQueryCalls);
    }

    [Theory]
    [InlineData(false, "Include")]
    [InlineData(false, "Exclude")]
    [InlineData(false, "IncludeMask")]
    [InlineData(false, "ExcludeMask")]
    [InlineData(true, "Include")]
    [InlineData(true, "Exclude")]
    [InlineData(true, "IncludeMask")]
    [InlineData(true, "ExcludeMask")]
    public async Task GetByIdsAsync_WithProjectedDiscovery_DoesNotPopulateUnfilteredIdCache(bool replacementQuery, string restriction)
    {
        using var configuration = new TestConfiguration();
        configuration.Invoker.SearchRouting = "parent";
        var configuredQuery = new RepositoryQuery<TestDocument>().Id("one");
        ApplyProjection(configuredQuery);
        using var repository = new TestRepository(configuration, configuredQuery: replacementQuery ? configuredQuery : null, hasParent: true);
        if (!replacementQuery)
        {
            repository.BeforeQuery.AddHandler((_, args) =>
            {
                ApplyProjection(args.Query);
                return Task.CompletedTask;
            });
        }

        var projected = Assert.Single(await repository.GetByIdsAsync(new Ids("one"), new CommandOptions().Cache()));

        Assert.Equal("full name", projected.Name);
        Assert.Null(projected.Detail);
        var full = await repository.GetByIdAsync(new Id("one", "parent"), new CommandOptions().Cache());
        Assert.NotNull(full);
        Assert.Equal("full detail", full.Detail);
        Assert.Equal(1, configuration.Invoker.Searches);
        Assert.Equal(1, configuration.Invoker.Reads);

        void ApplyProjection(IRepositoryQuery query)
        {
            switch (restriction)
            {
                case "Include":
                    query.Include("name");
                    break;
                case "Exclude":
                    query.Exclude("detail");
                    break;
                case "IncludeMask":
                    query.IncludeMask("name");
                    break;
                case "ExcludeMask":
                    query.ExcludeMask("detail");
                    break;
            }
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetByIdsAsync_WithClonedImmediateDiscoveryOptions_KeepsDescriptorProjectionsOutOfIdCache(bool projectLaterPage)
    {
        using var configuration = new TestConfiguration();
        configuration.Invoker.SearchRouting = "parent";
        configuration.Invoker.SearchIds = projectLaterPage ? ["one", "two"] : ["one"];
        using var repository = new TestRepository(configuration, hasParent: true)
        {
            CloneOptions = true,
            PageLimitOverride = 1,
            NativeSourceProjection = "Exclude",
            ProjectionStartingPage = projectLaterPage ? 2 : 1
        };
        var options = new CommandOptions().Cache("projected-discovery").ImmediateConsistency();

        var documents = await repository.GetByIdsAsync(new Ids(configuration.Invoker.SearchIds), options);

        string projectedId = projectLaterPage ? "two" : "one";
        Assert.Equal(projectLaterPage ? 2 : 1, documents.Count);
        var projected = Assert.Single(documents, document => document.Id == projectedId);
        Assert.Equal("full name", projected.Name);
        Assert.Null(projected.Detail);
        if (projectLaterPage)
            Assert.Equal("full detail", Assert.Single(documents, document => document.Id is "one").Detail);
        Assert.False(await repository.CacheContainsAsync(projectedId));
        var explicitlyCached = Assert.Single(await repository.GetCachedHitsAsync("projected-discovery"), hit => hit.Id == projectedId);
        Assert.NotNull(explicitlyCached.Document);
        Assert.Null(explicitlyCached.Document.Detail);

        var full = await repository.GetByIdAsync(new Id(projectedId, "parent"), new CommandOptions().Cache());

        Assert.NotNull(full);
        Assert.Equal("full detail", full.Detail);
        Assert.Equal(projectLaterPage ? 2 : 1, configuration.Invoker.Searches);
        Assert.Equal(1, configuration.Invoker.Reads);
        Assert.Equal(0, configuration.Invoker.MultiGets);
    }

    [Theory]
    [InlineData("Include")]
    [InlineData("Exclude")]
    public async Task ClientPatch_WithNativeSourceQueryParameters_PreservesUnpatchedFields(string restriction)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration) { NativeSourceProjection = restriction };
        var patch = new ActionPatch<TestDocument>(document => document.Name = "patched");

        long modified = await repository.PatchAllAsync(new RepositoryQuery<TestDocument>().Id("one"), patch,
            new CommandOptions().Cache(false).Retry(0));

        Assert.Equal(1, modified);
        var source = Assert.Single(configuration.Invoker.WrittenSources);
        Assert.Equal("patched", source.GetProperty("name").GetString());
        Assert.Equal("full detail", source.GetProperty("detail").GetString());
        Assert.NotNull(configuration.Invoker.LastSearchPath);
        Assert.DoesNotContain("_source_includes", configuration.Invoker.LastSearchPath, StringComparison.Ordinal);
        Assert.DoesNotContain("_source_excludes", configuration.Invoker.LastSearchPath, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FindOneAsync_WithRepositoryDefaultExclude_StillPopulatesTheDefaultIdCache()
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration, defaultExclude: true);
        var query = new RepositoryQuery<TestDocument>().Id("one");
        var options = new CommandOptions().Cache("default-query").ImmediateConsistency();

        var first = await repository.FindOneAsync(query, options);
        var cachedById = await repository.GetByIdAsync("one", new CommandOptions().Cache());

        Assert.NotNull(first.Document);
        Assert.NotNull(cachedById);
        Assert.Equal("full name", cachedById.Name);
        Assert.Null(first.Document.Detail);
        Assert.Null(cachedById.Detail);
        Assert.Equal(1, configuration.Invoker.Searches);
        Assert.Equal(0, configuration.Invoker.Reads);

        await repository.InvalidateCacheAsync("one");
        var repeated = await repository.FindOneAsync(query,
            new CommandOptions().Cache("default-query-repeat").ImmediateConsistency());
        var cachedAfterRepeat = await repository.GetByIdAsync("one", new CommandOptions().Cache());

        Assert.NotNull(repeated.Document);
        Assert.NotNull(cachedAfterRepeat);
        Assert.Equal("full name", cachedAfterRepeat.Name);
        Assert.Null(repeated.Document.Detail);
        Assert.Null(cachedAfterRepeat.Detail);
        Assert.Equal(2, configuration.Invoker.Searches);
        Assert.Equal(0, configuration.Invoker.Reads);
    }

    public sealed class TestDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Name { get; set; }
        public string? Detail { get; set; }
        public string? Version { get; set; }
    }

    public sealed class SoftDeleteDocument : IIdentity, ISupportSoftDeletes
    {
        public string Id { get; set; } = null!;
        public string? Name { get; set; }
        public string? Detail { get; set; }
        [JsonPropertyName("is_deleted")]
        public bool IsDeleted { get; set; }
    }

    private sealed class SoftDeleteRepository(TestConfiguration configuration)
        : ElasticRepositoryBase<SoftDeleteDocument>(new Index<SoftDeleteDocument>(configuration, "projection-cache-tests"))
    { }

    private sealed class SoftDeleteReadOnlyRepository(TestConfiguration configuration)
        : ElasticReadOnlyRepositoryBase<SoftDeleteDocument>(new Index<SoftDeleteDocument>(configuration, "projection-cache-tests"))
    { }

    private sealed class TestRepository : ElasticRepositoryBase<TestDocument>
    {
        private readonly IRepositoryQuery<TestDocument>? _configuredQuery;

        public TestRepository(TestConfiguration configuration, bool defaultExclude = false, IRepositoryQuery<TestDocument>? configuredQuery = null, bool hasParent = false)
            : base(new TestIndex(configuration))
        {
            _configuredQuery = configuredQuery;
            HasParent = hasParent;
            if (defaultExclude)
                AddDefaultExclude(document => document.Detail);
        }

        public int ConfigureQueryCalls { get; private set; }
        public bool CloneOptions { get; set; }
        public int? PageLimitOverride { get; set; }
        public string? NativeSourceProjection { get; set; }
        public int ProjectionStartingPage { get; set; } = 1;

        public Task<bool> CacheContainsAsync(string key) => Cache.ExistsAsync(key);

        public async Task<ICollection<FindHit<TestDocument>>> GetCachedHitsAsync(string key)
        {
            var value = await Cache.GetAsync<ICollection<FindHit<TestDocument>>>(key);
            return value.HasValue && !value.IsNull ? value.Value : [];
        }

        protected override IRepositoryQuery ConfigureQuery(IRepositoryQuery<TestDocument> query)
        {
            ConfigureQueryCalls++;
            return base.ConfigureQuery(_configuredQuery ?? query);
        }

        protected override ICommandOptions<TestDocument> ConfigureOptions(ICommandOptions<TestDocument>? options)
        {
            var configured = base.ConfigureOptions(CloneOptions ? options?.Clone() : options);
            if (PageLimitOverride is { } limit)
                configured.PageLimit(limit);
            return configured;
        }

        protected override async Task<SearchRequestDescriptor<TestDocument>> ConfigureSearchDescriptorAsync(SearchRequestDescriptor<TestDocument> search, IRepositoryQuery query, ICommandOptions options)
        {
            search = await base.ConfigureSearchDescriptorAsync(search, query, options);
            if (options.GetPage() < ProjectionStartingPage)
                return search;

            return NativeSourceProjection switch
            {
                "Include" => search.SourceIncludes(document => document.Id, document => document.Name!),
                "Exclude" => search.SourceExcludes(document => document.Detail!),
                _ => search
            };
        }
    }

    private sealed class TestIndex(TestConfiguration configuration) : Index<TestDocument>(configuration, "projection-cache-tests")
    {
        public override Task EnsureIndexAsync(object? target) => Task.CompletedTask;
    }

    private sealed class TestConfiguration : ElasticConfiguration
    {
        public SourceFilteringInvoker Invoker { get; } = new();
        protected override IRequestInvoker CreateRequestInvoker() => Invoker;
    }

    private sealed class SourceFilteringInvoker : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _owner = new();
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };
        public ResponseFactory ResponseFactory => _owner.ResponseFactory;
        public int Reads { get; private set; }
        public int MultiGets { get; private set; }
        public int Searches { get; private set; }
        public bool SearchReturnsNoHits { get; set; }
        public string[] SearchIds { get; set; } = ["one"];
        public string? SearchRouting { get; set; }
        public string SearchName { get; set; } = "full name";
        public string RealtimeName { get; set; } = "full name";
        public long RealtimeSequenceNumber { get; set; } = 10;
        public HashSet<string> DeletedIds { get; } = [];
        public string[] LastSourceExcludes { get; private set; } = [];
        public string? LastSearchPath { get; private set; }
        public List<JsonElement> WrittenSources { get; } = [];

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            object response;
            var parameters = new Uri($"http://localhost{endpoint.PathAndQuery}").Query.TrimStart('?')
                .Split('&', StringSplitOptions.RemoveEmptyEntries)
                .Select(parameter => parameter.Split('=', 2))
                .ToDictionary(pair => Uri.UnescapeDataString(pair[0]), pair => pair.Length > 1 ? Uri.UnescapeDataString(pair[1]) : String.Empty);
            if (endpoint.PathAndQuery.Contains("_mapping", StringComparison.Ordinal))
            {
                response = new Dictionary<string, object>
                {
                    ["projection-cache-tests"] = new
                    {
                        mappings = new { properties = new { id = new { type = "keyword" }, name = new { type = "keyword" }, detail = new { type = "keyword" }, is_deleted = new { type = "boolean" } } }
                    }
                };
            }
            else if (endpoint.PathAndQuery.Contains("_refresh", StringComparison.Ordinal))
            {
                response = new { _shards = new { total = 1, successful = 1, failed = 0 } };
            }
            else if (endpoint.PathAndQuery.Contains("_pit", StringComparison.Ordinal))
            {
                response = endpoint.Method is HttpMethod.DELETE
                    ? (object)new { succeeded = true, num_freed = 1 }
                    : new { id = "projection-point-in-time" };
            }
            else if (endpoint.PathAndQuery.Contains("_bulk", StringComparison.Ordinal))
            {
                Assert.NotNull(postData);
                using var requestStream = new MemoryStream();
                postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                requestStream.Position = 0;
                using var reader = new StreamReader(requestStream);
                string[] lines = reader.ReadToEnd().Split('\n', StringSplitOptions.RemoveEmptyEntries);
                var items = new List<object>();
                for (int i = 0; i < lines.Length; i += 2)
                {
                    using var header = JsonDocument.Parse(lines[i]);
                    using var source = JsonDocument.Parse(lines[i + 1]);
                    WrittenSources.Add(source.RootElement.Clone());
                    var operation = header.RootElement.GetProperty("index");
                    items.Add(new
                    {
                        index = new
                        {
                            _index = operation.GetProperty("_index").GetString(),
                            _id = operation.GetProperty("_id").GetString(),
                            status = 200,
                            result = "updated",
                            _seq_no = 11,
                            _primary_term = 3
                        }
                    });
                }
                response = new { took = 1, errors = false, items };
            }
            else if (endpoint.PathAndQuery.Contains("_mget", StringComparison.Ordinal))
            {
                MultiGets++;
                Assert.NotNull(postData);
                using var requestStream = new MemoryStream();
                postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                using var request = JsonDocument.Parse(requestStream.ToArray());
                var includes = parameters.TryGetValue("_source_includes", out string? include) ? include.Split(',').ToHashSet() : [];
                var excludes = parameters.TryGetValue("_source_excludes", out string? exclude) ? exclude.Split(',').ToHashSet() : [];
                var docs = request.RootElement.GetProperty("docs").EnumerateArray().Select(document =>
                {
                    string id = document.GetProperty("_id").GetString()!;
                    string? routing = document.TryGetProperty("routing", out var route) ? route.GetString() : null;
                    return new { _index = "projection-cache-tests", _id = id, _routing = routing, found = true, _seq_no = RealtimeSequenceNumber, _primary_term = 3, _source = FilterSource(includes, excludes, id, RealtimeName) };
                }).ToArray();
                response = new { docs };
            }
            else if (endpoint.PathAndQuery.Contains("_search", StringComparison.Ordinal))
            {
                Searches++;
                LastSearchPath = endpoint.PathAndQuery;
                Assert.NotNull(postData);
                using var requestStream = new MemoryStream();
                postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                using var request = JsonDocument.Parse(requestStream.ToArray());
                request.RootElement.TryGetProperty("_source", out var sourceFilter);
                var includes = parameters.TryGetValue("_source_includes", out string? include) ? include.Split(',').ToHashSet() : ReadFields(sourceFilter, "includes");
                var excludes = parameters.TryGetValue("_source_excludes", out string? exclude) ? exclude.Split(',').ToHashSet() : ReadFields(sourceFilter, "excludes");
                int from = request.RootElement.TryGetProperty("from", out var offset) ? offset.GetInt32() : 0;
                int size = request.RootElement.TryGetProperty("size", out var limit) ? limit.GetInt32() : SearchIds.Length;
                string? pointInTimeId = request.RootElement.TryGetProperty("pit", out _) ? "projection-point-in-time" : null;
                object[] hits = SearchReturnsNoHits ? [] : SearchIds.Skip(from).Take(size).Select(id =>
                    (object)new { _index = "projection-cache-tests", _id = id, _routing = SearchRouting, _seq_no = 10, _primary_term = 3, _source = FilterSource(includes, excludes, id, SearchName) }).ToArray();
                response = new { took = 1, timed_out = false, pit_id = pointInTimeId, hits = new { total = new { value = SearchReturnsNoHits ? 0 : SearchIds.Length, relation = "eq" }, hits } };
            }
            else
            {
                string path = endpoint.PathAndQuery.Split('?', 2)[0];
                const string documentMarker = "/_doc/";
                int documentOffset = path.IndexOf(documentMarker, StringComparison.Ordinal);
                Assert.True(documentOffset >= 0);
                string id = Uri.UnescapeDataString(path[(documentOffset + documentMarker.Length)..]);
                Reads++;
                var includes = parameters.TryGetValue("_source_includes", out string? include) ? include.Split(',').ToHashSet() : [];
                var excludes = parameters.TryGetValue("_source_excludes", out string? exclude) ? exclude.Split(',').ToHashSet() : [];
                response = new { _index = "projection-cache-tests", _id = id, _routing = parameters.GetValueOrDefault("routing"), found = true, _seq_no = RealtimeSequenceNumber, _primary_term = 3, _source = FilterSource(includes, excludes, id, RealtimeName) };
            }

            byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(response);
            using var stream = new MemoryStream(bytes);
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, 200, _headers, stream, "application/json", bytes.Length, null, null);
        }

        private Dictionary<string, object> FilterSource(HashSet<string> includes, HashSet<string> excludes, string id, string name)
        {
            LastSourceExcludes = excludes.ToArray();
            var source = new Dictionary<string, object> { ["id"] = id, ["name"] = name, ["detail"] = "full detail" };
            if (DeletedIds.Count > 0)
                source["is_deleted"] = DeletedIds.Contains(id);

            return source
                .Where(field => (includes.Count is 0 || includes.Any(pattern => MatchesField(pattern, field.Key)))
                    && !excludes.Any(pattern => MatchesField(pattern, field.Key)))
                .ToDictionary(field => field.Key, field => field.Value);
        }

        private static bool MatchesField(string pattern, string field) =>
            Regex.IsMatch(field, "\\A" + Regex.Escape(pattern).Replace("\\*", ".*", StringComparison.Ordinal) + "\\z", RegexOptions.CultureInvariant | RegexOptions.Singleline);

        private static HashSet<string> ReadFields(JsonElement sourceFilter, string name)
        {
            if (sourceFilter.ValueKind is not JsonValueKind.Object || !sourceFilter.TryGetProperty(name, out var value))
                return [];

            return value.ValueKind is JsonValueKind.String
                ? [value.GetString()!]
                : value.EnumerateArray().Select(field => field.GetString()!).ToHashSet();
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
