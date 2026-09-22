using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class MultiGetErrorTests : ElasticRepositoryTestBase
{
    private readonly ILogEventRepository _repository;

    public MultiGetErrorTests(ITestOutputHelper output) : base(output)
    {
        _repository = new DailyLogEventRepository(_configuration);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    [Theory]
    [InlineData(null, false)]
    [InlineData("strict-mget", false)]
    [InlineData(null, true)]
    [InlineData("strict-mget", true)]
    public async Task UnresolvedItemErrors_DoNotReadOrWriteFallbackCache(string? cacheKey, bool readOnly)
    {
        var existing = await _repository.AddAsync(LogEventGenerator.Default, o => o.ImmediateConsistency());
        string recoveredId = ObjectId.GenerateNewId(new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc)).ToString();
        await _repository.AddAsync(LogEventGenerator.Generate(recoveredId, createdUtc: DateTime.UtcNow), o => o.ImmediateConsistency());
        string missingId1 = ObjectId.GenerateNewId(new DateTime(2001, 1, 1, 0, 0, 0, DateTimeKind.Utc)).ToString();
        string missingId2 = ObjectId.GenerateNewId(new DateTime(2002, 1, 1, 0, 0, 0, DateTimeKind.Utc)).ToString();

        var options = new CommandOptions().Cache(cacheKey).ImmediateConsistency().ThrowOnMultiGetErrors();
        if (readOnly)
            options.Cache(false).ReadCache();

        ICommandOptions? fallbackOptions = null;
        _repository.BeforeQuery.AddHandler((_, args) =>
        {
            fallbackOptions = args.Options;
            return Task.CompletedTask;
        });

        var exception = await Assert.ThrowsAsync<DocumentException>(() =>
            _repository.GetByIdsAsync([existing.Id, recoveredId, missingId1, missingId2], options));

        Assert.Contains(missingId1, exception.Message);
        Assert.Contains(missingId2, exception.Message);
        Assert.Contains("index_not_found_exception", exception.Message);
        Assert.DoesNotContain(recoveredId, exception.Message);
        Assert.DoesNotContain(existing.Id, exception.Message);
        Assert.NotNull(fallbackOptions);
        Assert.NotSame(options, fallbackOptions);
        Assert.False(fallbackOptions.ShouldUseCache());
        Assert.False(fallbackOptions.ShouldReadCache());
        Assert.Equal(Consistency.Immediate, fallbackOptions.GetConsistency());
        Assert.Equal(!readOnly, options.ShouldUseCache());
        Assert.True(options.ShouldReadCache());
        Assert.Equal(cacheKey, options.GetCacheKey());
        Assert.Empty(_cache.Keys.Where(key => key.StartsWith("LogEvent:", StringComparison.Ordinal)));
    }

    [Theory]
    [InlineData(null, false, false)]
    [InlineData("strict-mget", false, false)]
    [InlineData(null, true, false)]
    [InlineData("strict-mget", true, false)]
    [InlineData(null, false, true)]
    [InlineData("strict-mget", false, true)]
    [InlineData(null, true, true)]
    [InlineData("strict-mget", true, true)]
    public async Task RecoveredItem_PreservesCallerCachingAndConsistency(string? cacheKey, bool readOnly, bool strict)
    {
        string id = ObjectId.GenerateNewId(new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc)).ToString();
        var document = await _repository.AddAsync(LogEventGenerator.Generate(id, createdUtc: DateTime.UtcNow), o => o.ImmediateConsistency());
        var options = new CommandOptions().Cache(cacheKey).ImmediateConsistency().ThrowOnMultiGetErrors(strict);
        if (readOnly)
            options.Cache(false).ReadCache();

        int fallbackCount = 0;
        _repository.BeforeQuery.AddHandler((_, args) =>
        {
            fallbackCount++;
            Assert.False(args.Options.ShouldUseCache());
            Assert.False(args.Options.ShouldReadCache());
            Assert.Equal(Consistency.Immediate, args.Options.GetConsistency());
            return Task.CompletedTask;
        });

        Assert.Equal(document, Assert.Single(await _repository.GetByIdsAsync([id], options)));
        Assert.Equal(!readOnly, options.ShouldUseCache());
        Assert.True(options.ShouldReadCache());
        Assert.Equal(cacheKey, options.GetCacheKey());

        var cacheKeys = _cache.Keys.Where(key => key.StartsWith("LogEvent:", StringComparison.Ordinal)).ToList();
        if (readOnly)
        {
            Assert.Empty(cacheKeys);
        }
        else
        {
            Assert.Equal(cacheKey is null ? 1 : 2, cacheKeys.Count);
            foreach (string key in cacheKeys)
            {
                var cached = await _cache.GetAsync<ICollection<FindHit<LogEvent>>>(key);
                Assert.True(cached.HasValue);
                Assert.Equal(document, Assert.Single(cached.Value).Document);
            }
        }

        Assert.Equal(document, Assert.Single(await _repository.GetByIdsAsync([id], options)));
        Assert.Equal(readOnly ? 2 : 1, fallbackCount);
    }
}
