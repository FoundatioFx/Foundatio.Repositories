using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using TimeZoneConverter;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ReadOnlyRepositoryTests : ElasticRepositoryTestBase
{
    private readonly IIdentityRepository _identityRepository;
    private readonly ILogEventRepository _dailyRepository;
    private readonly IEmployeeRepository _employeeRepository;

    public ReadOnlyRepositoryTests(ITestOutputHelper output) : base(output)
    {
        _identityRepository = new IdentityRepository(_configuration);
        _dailyRepository = new DailyLogEventRepository(_configuration);
        _employeeRepository = new EmployeeRepository(_configuration);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    [Fact]
    public async Task CanCacheFindResultAsync()
    {
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

        var employees = await _employeeRepository.GetAllByAgeAsync(20);
        Assert.Single(employees.Documents);

        string json = JsonConvert.SerializeObject(employees);
        var results = JsonConvert.DeserializeObject<FindResults<Employee>>(json);
        Assert.NotNull(results);
        Assert.Single(results.Documents);
    }

    [Fact]
    public async Task CanCacheSaveByKeyAsync()
    {
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency().Cache("test"));
        Assert.Equal(2, _cache.Count);

        var employeeResult1 = await _employeeRepository.FindOneAsync(new RepositoryQuery().Age(20), new CommandOptions().Cache("test"));
        Assert.NotNull(employeeResult1);
        Assert.Equal(2, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        var employeeResult2 = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache("test"));
        Assert.NotNull(employeeResult2);
        Assert.Equal(2, _cache.Count);
        Assert.Equal(2, _cache.Hits);
        Assert.Equal(0, _cache.Misses);
    }

    [Fact]
    public async Task CanCacheFindOneAsync()
    {
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

        var employeeResult = await _employeeRepository.FindOneAsync(new RepositoryQuery().Company("123"), new CommandOptions().Cache(true).CacheKey("test"));
        Assert.Null(employeeResult);
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        // doc exists, but we already cached the result with this cache key
        employeeResult = await _employeeRepository.FindOneAsync(new RepositoryQuery(), new CommandOptions().Cache(true).CacheKey("test"));
        Assert.Null(employeeResult);
        Assert.Equal(1, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        await _cache.RemoveAsync("Employee:test");
        Assert.Equal(0, _cache.Count);

        employeeResult = await _employeeRepository.FindOneAsync(new RepositoryQuery(), new CommandOptions().Cache(true).CacheKey("test"));
        Assert.NotNull(employeeResult.Document);
        Assert.Equal(1, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(4, _cache.Misses);

        employeeResult = await _employeeRepository.FindOneAsync(new RepositoryQuery(), new CommandOptions().Cache(true).CacheKey("test"));
        Assert.NotNull(employeeResult.Document);
        Assert.Equal(1, _cache.Count);
        Assert.Equal(2, _cache.Hits);
        Assert.Equal(4, _cache.Misses);
    }

    [Fact]
    public async Task InvalidateCacheAsync()
    {
        var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.Cache());
        Assert.NotNull(identity);
        Assert.NotNull(identity.Id);
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        await _identityRepository.InvalidateCacheAsync(identity);
        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        await _identityRepository.SaveAsync(identity, o => o.Cache());
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        await _identityRepository.InvalidateCacheAsync(new List<Identity> {
            identity
        });
        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        await _identityRepository.SaveAsync(new List<Identity> {
            identity
        }, o => o.Cache());
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        await _identityRepository.InvalidateCacheAsync(new List<Identity> {
            identity
        });
        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        await _identityRepository.SaveAsync(new List<Identity> {
            identity
        }, o => o.Cache());
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        await _identityRepository.InvalidateCacheAsync(identity);
        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        await _identityRepository.InvalidateCacheAsync(new List<Identity> {
            identity
        });
        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);
    }

    [Fact]
    public async Task InvalidateCacheWithInvalidArgumentsAsync()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await _identityRepository.InvalidateCacheAsync((Identity)null));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await _identityRepository.InvalidateCacheAsync((IReadOnlyCollection<Identity>)null));
        await _identityRepository.InvalidateCacheAsync(new List<Identity>());
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await _identityRepository.InvalidateCacheAsync(new List<Identity> {
            null
        }));
    }

    [Fact]
    public async Task CountAsync()
    {
        Assert.Equal(0, await _identityRepository.CountAsync());

        var identity = IdentityGenerator.Default;
        var result = await _identityRepository.AddAsync(identity, o => o.ImmediateConsistency());
        Assert.Equal(identity, result);

        Assert.Equal(1, await _identityRepository.CountAsync());
    }

    [Fact]
    public async Task CountWithTimeSeriesAsync()
    {
        Assert.Equal(0, await _dailyRepository.CountAsync());

        var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(createdUtc: DateTime.UtcNow.AddDays(-1)), o => o.ImmediateConsistency());
        Assert.NotNull(yesterdayLog);
        Assert.NotNull(yesterdayLog.Id);

        var nowLog = LogEventGenerator.Default;
        var result = await _dailyRepository.AddAsync(nowLog, o => o.ImmediateConsistency());
        Assert.Equal(nowLog, result);

        Assert.Equal(2, await _dailyRepository.CountAsync());
    }

    [Fact]
    public async Task GetByIdAsync()
    {
        var identity = await _identityRepository.AddAsync(IdentityGenerator.Default);
        Assert.NotNull(identity);
        Assert.NotNull(identity.Id);

        Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id));
    }

    [Fact]
    public async Task GetByIdWithCacheAsync()
    {
        var identity = await _identityRepository.AddAsync(IdentityGenerator.Default);
        Assert.NotNull(identity);
        Assert.NotNull(identity.Id);

        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache()));
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(1, _cache.Misses);

        Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache()));
        Assert.Equal(1, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(1, _cache.Misses);

        Assert.Null(await _identityRepository.GetByIdAsync("not-yet", o => o.Cache()));
        Assert.Equal(2, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        Assert.Null(await _identityRepository.GetByIdAsync("not-yet", o => o.Cache()));
        Assert.Equal(2, _cache.Count);
        Assert.Equal(2, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        var newIdentity = await _identityRepository.AddAsync(IdentityGenerator.Generate("not-yet"), o => o.Cache());
        Assert.NotNull(newIdentity);
        Assert.NotNull(newIdentity.Id);
        Assert.Equal(2, _cache.Count);
        Assert.Equal(2, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        Assert.Equal(newIdentity, await _identityRepository.GetByIdAsync("not-yet", o => o.Cache()));
        Assert.Equal(2, _cache.Count);
        Assert.Equal(3, _cache.Hits);
        Assert.Equal(2, _cache.Misses);
    }

    [Fact]
    public async Task GetByIdWithNullCacheKeyAsync()
    {
        var identity = await _identityRepository.AddAsync(IdentityGenerator.Default);
        Assert.NotNull(identity);
        Assert.NotNull(identity.Id);

        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache(null)));
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(1, _cache.Misses);

        Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache(null)));
        Assert.Equal(1, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(1, _cache.Misses);

        Assert.Null(await _identityRepository.GetByIdAsync("not-yet", o => o.Cache(null)));
        Assert.Equal(2, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        Assert.Null(await _identityRepository.GetByIdAsync("not-yet", o => o.Cache(null)));
        Assert.Equal(2, _cache.Count);
        Assert.Equal(2, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        var newIdentity = await _identityRepository.AddAsync(IdentityGenerator.Generate("not-yet"), o => o.Cache(null));
        Assert.NotNull(newIdentity);
        Assert.NotNull(newIdentity.Id);
        Assert.Equal(2, _cache.Count);
        Assert.Equal(2, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        Assert.Equal(newIdentity, await _identityRepository.GetByIdAsync("not-yet", o => o.Cache(null)));
        Assert.Equal(2, _cache.Count);
        Assert.Equal(3, _cache.Hits);
        Assert.Equal(2, _cache.Misses);
    }

    [Fact]
    public async Task GetByIdAnyIdsWithCacheAsync()
    {
        var identity = await _identityRepository.AddAsync(IdentityGenerator.Default);
        Assert.NotNull(identity);
        Assert.NotNull(identity.Id);

        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache()));
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(1, _cache.Misses);

        string cacheKey = _cache.Keys.Single();
        var cacheValue = await _cache.GetAsync<IEnumerable<FindHit<Identity>>>(cacheKey);
        Assert.True(cacheValue.HasValue);
        Assert.Equal(identity, cacheValue.Value.Single().Document);

        var results = await _identityRepository.GetByIdsAsync(new Ids(identity.Id), o => o.Cache());
        Assert.Single(results);
        Assert.Equal(identity, results.First());
        Assert.Equal(1, _cache.Count);
        Assert.Equal(2, _cache.Hits);
        Assert.Equal(1, _cache.Misses);
        cacheValue = await _cache.GetAsync<IEnumerable<FindHit<Identity>>>(cacheKey);
        Assert.True(cacheValue.HasValue);
        Assert.Equal(identity, cacheValue.Value.Single().Document);

        await _identityRepository.InvalidateCacheAsync(identity);
        Assert.Equal(0, _cache.Count);
        Assert.Equal(3, _cache.Hits);
        Assert.Equal(1, _cache.Misses);

        results = await _identityRepository.GetByIdsAsync(new Ids(identity.Id), o => o.Cache());
        Assert.Single(results);
        Assert.Equal(identity, results.First());
        Assert.Equal(1, _cache.Count);
        Assert.Equal(3, _cache.Hits);
        Assert.Equal(2, _cache.Misses);
        cacheValue = await _cache.GetAsync<IEnumerable<FindHit<Identity>>>(cacheKey);
        Assert.True(cacheValue.HasValue);
        Assert.Equal(identity, cacheValue.Value.Single().Document);

        Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache()));
        Assert.Equal(1, _cache.Count);
        Assert.Equal(5, _cache.Hits);
        Assert.Equal(2, _cache.Misses);
        cacheValue = await _cache.GetAsync<IEnumerable<FindHit<Identity>>>(cacheKey);
        Assert.True(cacheValue.HasValue);
        Assert.Equal(identity, cacheValue.Value.Single().Document);
    }

    [Fact]
    public async Task GetByIdWithTimeSeriesAsync()
    {
        var utcNow = DateTime.UtcNow;
        var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(createdUtc: utcNow.AddDays(-1)));
        Assert.NotNull(yesterdayLog);
        Assert.NotNull(yesterdayLog.Id);

        var nowLog = await _dailyRepository.AddAsync(LogEventGenerator.Default);
        Assert.NotNull(nowLog);
        Assert.NotNull(nowLog.Id);

        Assert.Equal(yesterdayLog, await _dailyRepository.GetByIdAsync(yesterdayLog.Id));
        Assert.Equal(nowLog, await _dailyRepository.GetByIdAsync(nowLog.Id));
    }

    [Fact]
    public async Task GetByIdWithOutOfSyncIndexAsync()
    {
        var utcNow = DateTime.UtcNow;
        var yesterday = utcNow.AddDays(-1);
        var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId(yesterday).ToString(), createdUtc: yesterday));
        Assert.NotNull(yesterdayLog);
        Assert.NotNull(yesterdayLog.Id);

        Assert.Equal(yesterdayLog, await _dailyRepository.GetByIdAsync(yesterdayLog.Id));
    }

    [Fact]
    public async Task GetByIdsAsync()
    {
        var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default);
        Assert.NotNull(identity1);
        Assert.NotNull(identity1.Id);

        var identity2 = await _identityRepository.AddAsync(IdentityGenerator.Generate());
        Assert.NotNull(identity2);
        Assert.NotNull(identity2.Id);

        var results = await _identityRepository.GetByIdsAsync(new[] { identity1.Id, identity2.Id });
        Assert.NotNull(results);
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task GetByIdsWithInvalidIdAsync()
    {
        var identity = await _identityRepository.AddAsync(IdentityGenerator.Generate());
        Assert.NotNull(identity);
        Assert.NotNull(identity.Id);

        var result = await _identityRepository.GetByIdsAsync((Ids)null);
        Assert.Empty(result);

        result = await _identityRepository.GetByIdsAsync(new string[] { null });
        Assert.Empty(result);

        result = await _identityRepository.GetByIdsAsync(new[] { IdentityGenerator.Default.Id, identity.Id });
        Assert.Single(result);
    }

    [Fact]
    public async Task GetByIdsWithCachingAsync()
    {
        var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default);
        Assert.NotNull(identity1);
        Assert.NotNull(identity1.Id);

        var identity2 = await _identityRepository.AddAsync(IdentityGenerator.Generate());
        Assert.NotNull(identity2);
        Assert.NotNull(identity2.Id);

        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        var results = await _identityRepository.GetByIdsAsync(new Ids(identity1.Id), o => o.Cache());
        Assert.NotNull(results);
        Assert.Single(results);
        Assert.Equal(identity1, results.First());
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(1, _cache.Misses);

        results = await _identityRepository.GetByIdsAsync(new Ids(identity1.Id, identity2.Id), o => o.Cache());
        Assert.NotNull(results);
        Assert.Equal(2, results.Count);
        Assert.Equal(2, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        results = await _identityRepository.GetByIdsAsync(new Ids(identity1.Id, identity2.Id), o => o.Cache());
        Assert.NotNull(results);
        Assert.Equal(2, results.Count);
        Assert.Equal(2, _cache.Count);
        Assert.Equal(3, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        var identity = await _identityRepository.GetByIdAsync(identity1.Id, o => o.Cache());
        Assert.Equal(identity1, identity);
        Assert.Equal(2, _cache.Count);
        Assert.Equal(4, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        results = await _identityRepository.GetByIdsAsync(new Ids(identity1.Id, identity2.Id, "not-yet"), o => o.Cache());
        Assert.NotNull(results);
        Assert.Equal(3, _cache.Count);
        Assert.Equal(6, _cache.Hits);
        Assert.Equal(3, _cache.Misses);
        Assert.Equal(2, results.Count);

        results = await _identityRepository.GetByIdsAsync(new Ids(identity1.Id, identity2.Id, "not-yet"), o => o.Cache());
        Assert.NotNull(results);
        Assert.Equal(3, _cache.Count);
        Assert.Equal(9, _cache.Hits);
        Assert.Equal(3, _cache.Misses);
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task GetByIdsWithInvalidIdAndCachingAsync()
    {
        var identity = await _identityRepository.AddAsync(IdentityGenerator.Generate());
        Assert.NotNull(identity);
        Assert.NotNull(identity.Id);

        var result = await _identityRepository.GetByIdsAsync((Ids)null, o => o.Cache());
        Assert.Empty(result);
        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        result = await _identityRepository.GetByIdsAsync(new Ids((string)null), o => o.Cache());
        Assert.Empty(result);
        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        result = await _identityRepository.GetByIdsAsync(new Ids(IdentityGenerator.Default.Id, identity.Id, null), o => o.Cache());
        Assert.Single(result);
        Assert.Equal(2, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(2, _cache.Misses);
    }

    [Fact]
    public async Task GetByIdsWithTimeSeriesAsync()
    {
        var utcNow = DateTime.UtcNow;
        var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(createdUtc: utcNow.AddDays(-1)));
        Assert.NotNull(yesterdayLog);
        Assert.NotNull(yesterdayLog.Id);

        var nowLog = await _dailyRepository.AddAsync(LogEventGenerator.Default);
        Assert.NotNull(nowLog);
        Assert.NotNull(nowLog.Id);

        var results = await _dailyRepository.GetByIdsAsync(new[] { yesterdayLog.Id, nowLog.Id });
        Assert.NotNull(results);
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task GetByIdsWithOutOfSyncIndexAsync()
    {
        var utcNow = DateTime.UtcNow;
        var yesterday = utcNow.AddDays(-1);
        var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId(yesterday).ToString(), createdUtc: yesterday));
        Assert.NotNull(yesterdayLog);
        Assert.NotNull(yesterdayLog.Id);

        var results = await _dailyRepository.GetByIdsAsync(new[] { yesterdayLog.Id });
        Assert.NotNull(results);
        Assert.Single(results);
    }

    [Fact]
    public async Task GetAllAsync()
    {
        var identities = IdentityGenerator.GenerateIdentities(25);
        await _identityRepository.AddAsync(identities, o => o.ImmediateConsistency());

        var results = await _identityRepository.GetAllAsync(o => o.PageLimit(100));
        Assert.NotNull(results);
        Assert.Equal(25, results.Total);
        Assert.Equal(25, results.Documents.Count);
        Assert.Equal(identities.OrderBy(i => i.Id), results.Documents.OrderBy(i => i.Id));

        results = await _identityRepository.GetAllAsync();
        Assert.NotNull(results);
        Assert.Equal(25, results.Total);
        Assert.Equal(10, results.Documents.Count);

        Assert.True(await results.NextPageAsync());
        Assert.Equal(10, results.Documents.Count);
        Assert.Equal(2, results.Page);
        Assert.Equal(25, results.Total);
        Assert.True(results.HasMore);

        Assert.True(await results.NextPageAsync());
        Assert.Equal(5, results.Documents.Count);
        Assert.Equal(3, results.Page);
        Assert.Equal(25, results.Total);
        Assert.False(results.HasMore);
    }

    [Fact]
    public async Task GetAllWithPagingAsync()
    {
        var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(identity1);
        Assert.NotNull(identity1.Id);

        var identity2 = await _identityRepository.AddAsync(IdentityGenerator.Generate(), o => o.ImmediateConsistency());
        Assert.NotNull(identity2);
        Assert.NotNull(identity2.Id);

        var results = await _identityRepository.GetAllAsync(o => o.PageLimit(1));
        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.True(results.HasMore);
        Assert.Equal(2, results.Total);

        Assert.True(await results.NextPageAsync());
        Assert.Single(results.Documents);
        Assert.Equal(2, results.Page);
        Assert.Equal(2, results.Total);
        Assert.False(results.HasMore);
        var secondDoc = results.Documents.First();

        Assert.False(await results.NextPageAsync());
        Assert.Empty(results.Documents);
        Assert.Equal(2, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(2, results.Total);

        var secondPageResults = await _identityRepository.GetAllAsync(o => o.PageNumber(2).PageLimit(1));
        Assert.Equal(secondDoc, secondPageResults.Documents.First());
    }

    [Fact]
    public async Task GetAllWithSnapshotPagingAsync()
    {
        var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(identity1);

        var identity2 = await _identityRepository.AddAsync(IdentityGenerator.Generate(), o => o.ImmediateConsistency());
        Assert.NotNull(identity2);

        var allIds = new HashSet<string> { identity1.Id, identity2.Id };

        await _client.ClearScrollAsync(cancellationToken: TestCancellationToken);
        long baselineScrollCount = await GetCurrentScrollCountAsync();

        var results = await _identityRepository.GetAllAsync(o => o.PageLimit(1).SnapshotPagingLifetime(TimeSpan.FromMinutes(10)));
        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.True(results.HasMore);
        Assert.Contains(results.Documents.First().Id, allIds);
        var firstPageId = results.Documents.First().Id;
        Assert.Equal(2, results.Total);
        long currentScrollCount = await GetCurrentScrollCountAsync();
        Assert.Equal(baselineScrollCount + 1, currentScrollCount);

        Assert.True(await results.NextPageAsync());
        Assert.Single(results.Documents);
        Assert.Equal(2, results.Page);
        Assert.Equal(2, results.Total);
        Assert.Contains(results.Documents.First().Id, allIds);
        Assert.NotEqual(firstPageId, results.Documents.First().Id); // Ensure we got a different document
        // returns true even though there are no more results because we don't know if there are more or not for scrolls until we try to get the next page
        Assert.True(results.HasMore);
        var secondDoc = results.Documents.First();
        currentScrollCount = await GetCurrentScrollCountAsync();
        Assert.Equal(baselineScrollCount + 1, currentScrollCount);

        Assert.False(await results.NextPageAsync());
        Assert.Empty(results.Documents);
        Assert.Equal(2, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(2, results.Total);
        currentScrollCount = await GetCurrentScrollCountAsync();
        Assert.Equal(baselineScrollCount, currentScrollCount);

        var secondPageResults = await _identityRepository.GetAllAsync(o => o.PageNumber(2).PageLimit(1));
        Assert.Equal(secondDoc, secondPageResults.Documents.First());

        // make sure a scroll that only has a single page will clear the scroll immediately
        results = await _identityRepository.GetAllAsync(o => o.PageLimit(5).SnapshotPagingLifetime(TimeSpan.FromMinutes(10)));
        Assert.NotNull(results);
        Assert.Equal(2, results.Documents.Count);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(identity1.Id, results.Documents.First().Id);
        Assert.Equal(identity2.Id, results.Documents.Last().Id);
        Assert.Equal(2, results.Total);
        currentScrollCount = await GetCurrentScrollCountAsync();
        Assert.Equal(baselineScrollCount, currentScrollCount);

        // use while loop and verify scroll is cleared
        var findResults = await _identityRepository.GetAllAsync(o => o.PageLimit(1).SnapshotPagingLifetime(TimeSpan.FromMinutes(10)));
        do
        {
            Assert.Single(findResults.Documents);
            Assert.Equal(2, findResults.Total);

            currentScrollCount = await GetCurrentScrollCountAsync();
            Assert.Equal(baselineScrollCount + 1, currentScrollCount);
        } while (await findResults.NextPageAsync());

        currentScrollCount = await GetCurrentScrollCountAsync();
        Assert.Equal(baselineScrollCount, currentScrollCount);

        findResults = await _identityRepository.GetAllAsync(o => o.PageLimit(5).SnapshotPagingLifetime(TimeSpan.FromMinutes(10)));
        do
        {
            Assert.Equal(2, findResults.Documents.Count);
            Assert.Equal(1, findResults.Page);
            Assert.Equal(2, findResults.Total);

            currentScrollCount = await GetCurrentScrollCountAsync();
            Assert.Equal(baselineScrollCount, currentScrollCount);
        } while (await findResults.NextPageAsync());

        currentScrollCount = await GetCurrentScrollCountAsync();
        Assert.Equal(baselineScrollCount, currentScrollCount);
    }

    private async Task<long> GetCurrentScrollCountAsync()
    {
        var stats = await _client.Nodes.StatsAsync();
        var nodeStats = stats.Nodes.First().Value;
        return nodeStats.Indices.Search.ScrollCurrent;
    }

    [Fact]
    public async Task GetAllWithAsyncQueryAsync()
    {
        var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(identity1);
        Assert.NotNull(identity1.Id);

        var identity2 = await _identityRepository.AddAsync(IdentityGenerator.Generate(), o => o.ImmediateConsistency());
        Assert.NotNull(identity2);
        Assert.NotNull(identity2.Id);

        var results = await _identityRepository.GetAllAsync(o => o.AsyncQuery(TimeSpan.FromMinutes(1)));
        Assert.NotNull(results);
        Assert.Equal(2, results.Documents.Count);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(identity1.Id, results.Documents.First().Id);
        Assert.Equal(2, results.Total);

        string asyncQueryId = results.GetAsyncQueryId();
        Assert.Null(asyncQueryId);
        Assert.False(results.IsAsyncQueryPartial());
        Assert.False(results.IsAsyncQueryRunning());

        results = await _identityRepository.GetAllAsync(o => o.AsyncQueryId(asyncQueryId, TimeSpan.FromMinutes(1)));
        Assert.NotNull(results);
        Assert.Equal(2, results.Documents.Count);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(identity1.Id, results.Documents.First().Id);
        Assert.Equal(2, results.Total);

        Assert.Equal(asyncQueryId, results.GetAsyncQueryId());
        Assert.False(results.IsAsyncQueryPartial());
        Assert.False(results.IsAsyncQueryRunning());

        results = await _identityRepository.GetAllAsync(o => o.AsyncQueryId(asyncQueryId, TimeSpan.FromMinutes(1), autoDelete: true));
        Assert.NotNull(results);
        Assert.Equal(2, results.Documents.Count);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(identity1.Id, results.Documents.First().Id);
        Assert.Equal(2, results.Total);

        Assert.Null(results.GetAsyncQueryId());
        Assert.False(results.IsAsyncQueryPartial());
        Assert.False(results.IsAsyncQueryRunning());

        await _identityRepository.AddAsync(IdentityGenerator.GenerateIdentities(1000), o => o.ImmediateConsistency());

        results = await _identityRepository.GetAllAsync(o => o.AsyncQuery(TimeSpan.Zero));
        Assert.NotNull(results);
        Assert.Empty(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Empty(results.Documents);
        Assert.Equal(0, results.Total);

        asyncQueryId = results.GetAsyncQueryId();
        Assert.NotNull(asyncQueryId);
        Assert.True(results.IsAsyncQueryPartial());
        Assert.True(results.IsAsyncQueryRunning());

        await _identityRepository.RemoveQueryAsync(asyncQueryId);

        // getting query that doesn't exist returns empty (don't love it, but other things are doing similar)
        await Assert.ThrowsAsync<AsyncQueryNotFoundException>(() => _identityRepository.GetAllAsync(o => o.AsyncQueryId(asyncQueryId)));

        // removing query that does not exist to make sure it doesn't throw
        await _identityRepository.RemoveQueryAsync(asyncQueryId);

        // setting to null is ignored
        await _identityRepository.GetAllAsync(o => o.AsyncQueryId(null));
    }

    [Fact]
    public async Task CountWithAsyncQueryAsync()
    {
        var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(identity1);
        Assert.NotNull(identity1.Id);

        var identity2 = await _identityRepository.AddAsync(IdentityGenerator.Generate(), o => o.ImmediateConsistency());
        Assert.NotNull(identity2);
        Assert.NotNull(identity2.Id);

        var results = await _identityRepository.CountAsync(o => o.AsyncQuery(TimeSpan.FromMinutes(1)));
        Assert.NotNull(results);
        Assert.Equal(2, results);

        string asyncQueryId = results.GetAsyncQueryId();
        Assert.Null(asyncQueryId);
        Assert.False(results.IsAsyncQueryPartial());
        Assert.False(results.IsAsyncQueryRunning());

        results = await _identityRepository.CountAsync(o => o.AsyncQueryId(asyncQueryId, TimeSpan.FromMinutes(1)));
        Assert.NotNull(results);
        Assert.Equal(2, results);
        Assert.Equal(2, results.Total);

        Assert.Equal(asyncQueryId, results.GetAsyncQueryId());
        Assert.False(results.IsAsyncQueryPartial());
        Assert.False(results.IsAsyncQueryRunning());

        results = await _identityRepository.CountAsync(o => o.AsyncQueryId(asyncQueryId, TimeSpan.FromMinutes(1), autoDelete: true));
        Assert.NotNull(results);
        Assert.Equal(2, results);
        Assert.Equal(2, results.Total);

        Assert.Null(results.GetAsyncQueryId());
        Assert.False(results.IsAsyncQueryPartial());
        Assert.False(results.IsAsyncQueryRunning());

        await _identityRepository.AddAsync(IdentityGenerator.GenerateIdentities(1000), o => o.ImmediateConsistency());

        results = await _identityRepository.CountAsync(o => o.AsyncQuery(TimeSpan.Zero));
        Assert.NotNull(results);
        Assert.Equal(0, results);
        Assert.Equal(0, results.Total);

        asyncQueryId = results.GetAsyncQueryId();
        Assert.NotNull(asyncQueryId);
        Assert.True(results.IsAsyncQueryPartial());
        Assert.True(results.IsAsyncQueryRunning());

        await _identityRepository.RemoveQueryAsync(asyncQueryId);

        // getting query that doesn't exist returns empty (don't love it, but other things are doing similar)
        results = await _identityRepository.CountAsync(o => o.AsyncQueryId(asyncQueryId));

        // removing query that does not exist to make sure it doesn't throw
        await _identityRepository.RemoveQueryAsync(asyncQueryId);

        // setting to null is ignored
        await _identityRepository.GetAllAsync(o => o.AsyncQueryId(null));
    }

    [Fact]
    public async Task FindWithRuntimeFieldsAsync()
    {
        var employee1 = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee1);
        Assert.NotNull(employee1.Id);

        var employee2 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(name: "Blake", age: 3), o => o.ImmediateConsistency());
        Assert.NotNull(employee2);
        Assert.NotNull(employee2.Id);

        var results = await _employeeRepository.FindAsync(q => q.FilterExpression("unmappedage:>20").RuntimeField("unmappedAge", ElasticRuntimeFieldType.Long));
        Assert.NotNull(results);
        Assert.Single(results.Documents);
    }

    [Fact]
    public async Task FindWithResolvedRuntimeFieldsAsync()
    {
        var employee1 = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee1);
        Assert.NotNull(employee1.Id);

        var employee2 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(name: "Blake", age: 3), o => o.ImmediateConsistency());
        Assert.NotNull(employee2);
        Assert.NotNull(employee2.Id);

        var results = await _employeeRepository.FindAsync(q => q.FilterExpression($"unmappedcompanyname:{employee1.CompanyName}"), o => o.RuntimeFieldResolver(f => String.Equals(f, "unmappedCompanyName", StringComparison.OrdinalIgnoreCase) ? Task.FromResult(new ElasticRuntimeField { Name = "unmappedCompanyName", FieldType = ElasticRuntimeFieldType.Keyword }) : Task.FromResult<ElasticRuntimeField>(null)));
        Assert.NotNull(results);
        Assert.Single(results.Documents);
    }

    [Fact]
    public async Task CanUseOptInRuntimeFieldResolving()
    {
        var employee1 = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee1);
        Assert.NotNull(employee1.Id);

        var employee2 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(name: "Blake", age: 3), o => o.ImmediateConsistency());
        Assert.NotNull(employee2);
        Assert.NotNull(employee2.Id);

        var results = await _employeeRepository.FindAsync(q => q.FilterExpression($"unmappedemailaddress:{employee1.UnmappedEmailAddress}"));
        Assert.NotNull(results);
        Assert.Empty(results.Documents);

        results = await _employeeRepository.FindAsync(q => q.FilterExpression($"unmappedemailaddress:{employee1.UnmappedEmailAddress}"), o => o.EnableRuntimeFieldResolver());
        Assert.NotNull(results);
        Assert.Single(results.Documents);

        results = await _employeeRepository.FindAsync(q => q.FilterExpression($"unmappedemailaddress:{employee1.UnmappedEmailAddress}"), o => o.EnableRuntimeFieldResolver(false));
        Assert.NotNull(results);
        Assert.Empty(results.Documents);
    }

    [Fact]
    public async Task FindWithSearchAfterPagingAsync()
    {
        var employee1 = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee1);
        Assert.NotNull(employee1.Id);

        var employee2 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(name: "Blake"), o => o.ImmediateConsistency());
        Assert.NotNull(employee2);
        Assert.NotNull(employee2.Id);

        var results = await _employeeRepository.FindAsync(q => q.SortDescending(d => d.Name), o => o.PageLimit(1).SearchAfterPaging());
        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.True(results.HasMore);
        Assert.Equal(employee1.Id, results.Documents.First().Id);
        Assert.Equal(2, results.Total);
        Assert.Null(results.GetSearchBeforeToken());
        Assert.NotEmpty(results.GetSearchAfterToken());

        Assert.True(await results.NextPageAsync());
        Assert.Single(results.Documents);
        Assert.Equal(2, results.Page);
        Assert.Equal(2, results.Total);
        Assert.Equal(employee2.Id, results.Documents.First().Id);
        Assert.False(results.HasMore);
        string searchBeforeToken = results.GetSearchBeforeToken();
        Assert.NotEmpty(searchBeforeToken);
        Assert.Null(results.GetSearchAfterToken());

        Assert.False(await results.NextPageAsync());
        Assert.Empty(results.Documents);
        Assert.Equal(2, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(2, results.Total);

        // Search after with no sort.
        results = await _employeeRepository.FindAsync(q => q, o => o.PageLimit(1).SearchAfterPaging());
        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.True(results.HasMore);
        Assert.Equal(employee1.Id, results.Documents.First().Id);
        Assert.Equal(2, results.Total);
        Assert.Null(results.GetSearchBeforeToken());
        Assert.NotEmpty(results.GetSearchAfterToken());

        // try search before
        results = await _employeeRepository.FindAsync(q => q.SortDescending(d => d.Name), o => o.PageLimit(1).SearchBeforeToken(searchBeforeToken));
        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(employee1.Id, results.Documents.First().Id);
        Assert.Equal(2, results.Total);
        Assert.Null(results.GetSearchBeforeToken());
        Assert.NotEmpty(results.GetSearchAfterToken());

        results = await _employeeRepository.FindAsync(q => q.SortDescending(d => d.Name), o => o.PageLimit(5).SearchAfterPaging());
        Assert.NotNull(results);
        Assert.Equal(2, results.Documents.Count);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(employee1.Id, results.Documents.First().Id);
        Assert.Equal(employee2.Id, results.Documents.Last().Id);
        Assert.Equal(2, results.Total);

        // use while loop
        var findResults = await _employeeRepository.FindAsync(q => q.SortDescending(d => d.Name), o => o.PageLimit(1).SearchAfterPaging());
        do
        {
            Assert.Single(findResults.Documents);
            Assert.Equal(2, findResults.Total);
        } while (await findResults.NextPageAsync());

        findResults = await _employeeRepository.FindAsync(q => q.SortDescending(d => d.Name), o => o.PageLimit(5).SearchAfterPaging());
        do
        {
            Assert.Equal(2, findResults.Documents.Count);
            Assert.Equal(1, findResults.Page);
            Assert.Equal(2, findResults.Total);
        } while (await findResults.NextPageAsync());
    }

    [Fact]
    public async Task GetAllWithSearchAfterPagingWithCustomSortAsync()
    {
        var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(identity1);
        Assert.NotNull(identity1.Id);

        var identity2 = await _identityRepository.AddAsync(IdentityGenerator.Generate(), o => o.ImmediateConsistency());
        Assert.NotNull(identity2);
        Assert.NotNull(identity2.Id);

        var results = await _identityRepository.FindAsync(q => q.SortDescending(d => d.Id), o => o.PageLimit(1).SearchAfterPaging());
        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.True(results.HasMore);
        Assert.Equal(identity2.Id, results.Documents.First().Id);
        Assert.Equal(2, results.Total);

        Assert.True(await results.NextPageAsync());
        Assert.Single(results.Documents);
        Assert.Equal(2, results.Page);
        Assert.Equal(2, results.Total);
        Assert.Equal(identity1.Id, results.Documents.First().Id);
        Assert.False(results.HasMore);
        var secondDoc = results.Documents.First();

        Assert.False(await results.NextPageAsync());
        Assert.Empty(results.Documents);
        Assert.Equal(2, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(2, results.Total);
    }

    [Fact]
    public async Task GetAllAsync_WithNoSortAndPaging_ReturnsAllDocumentsWithoutDuplicates()
    {
        var identities = IdentityGenerator.GenerateIdentities(100);
        await _identityRepository.AddAsync(identities, o => o.ImmediateConsistency());

        var results = await _identityRepository.GetAllAsync(o => o.PageLimit(10));
        var viewedIds = new HashSet<string>();
        int pagedRecords = 0;
        do
        {
            viewedIds.AddRange(results.Hits.Select(h => h.Id));
            pagedRecords += results.Documents.Count;
        } while (await results.NextPageAsync());

        Assert.Equal(100, pagedRecords);
        Assert.Equal(100, viewedIds.Count);
        Assert.True(identities.All(e => viewedIds.Contains(e.Id)));
    }

    [Fact]
    public async Task GetAllAsync_WithNoSort_ReturnsDocumentsSortedByIdAscending()
    {
        var identities = IdentityGenerator.GenerateIdentities(100);
        await _identityRepository.AddAsync(identities, o => o.ImmediateConsistency());

        var results = await _identityRepository.GetAllAsync(o => o.PageLimit(100));
        var ids = results.Documents.Select(d => d.Id).ToList();

        Assert.Equal(100, ids.Count);
        Assert.Equal(ids.OrderBy(id => id).ToList(), ids);
    }

    [Fact]
    public async Task FindAsync_WithNoSortAndPaging_ReturnsAllDocumentsWithoutDuplicates()
    {
        var identities = IdentityGenerator.GenerateIdentities(100);
        await _identityRepository.AddAsync(identities, o => o.ImmediateConsistency());

        var results = await _identityRepository.FindAsync(q => q, o => o.PageLimit(10));
        var viewedIds = new HashSet<string>();
        int pagedRecords = 0;
        do
        {
            viewedIds.AddRange(results.Hits.Select(h => h.Id));
            pagedRecords += results.Documents.Count;
        } while (await results.NextPageAsync());

        Assert.Equal(100, pagedRecords);
        Assert.Equal(100, viewedIds.Count);
        Assert.True(identities.All(e => viewedIds.Contains(e.Id)));
    }

    [Fact]
    public async Task GetAllWithSearchAfterAsync()
    {
        var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(identity1);
        Assert.NotNull(identity1.Id);

        var identity2 = await _identityRepository.AddAsync(IdentityGenerator.Generate(), o => o.ImmediateConsistency());
        Assert.NotNull(identity2);
        Assert.NotNull(identity2.Id);

        var results = await _identityRepository.FindAsync(q => q.SortDescending(d => d.Id), o => o.PageLimit(1));
        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.True(results.HasMore);
        Assert.Equal(identity2.Id, results.Documents.First().Id);
        Assert.Equal(2, results.Total);

        results = await _identityRepository.FindAsync(q => q.SortDescending(d => d.Id), o => o.PageLimit(1).SearchAfter(results.Hits.First().GetSorts()));
        Assert.Single(results.Documents);
        Assert.Equal(2, results.Total);
        Assert.Equal(identity1.Id, results.Documents.First().Id);
        Assert.False(results.HasMore);

        results = await _identityRepository.FindAsync(q => q.SortDescending(d => d.Id), o => o.PageLimit(1).SearchAfter(results.Hits.First().GetSorts()));
        Assert.Empty(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(2, results.Total);
    }

    [Fact]
    public async Task GetAllWithAliasedDateRangeAsync()
    {
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(nextReview: DateTimeOffset.Now), o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        var results = await _employeeRepository.FindAsync(o => o.DateRange(DateTime.UtcNow.SubtractHours(1), DateTime.UtcNow, "next").AggregationsExpression("date:next"));
        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(1, results.Total);

        Assert.Single(results.Aggregations);
        Assert.True(results.Aggregations.ContainsKey("date_next"));
        var aggregation = results.Aggregations["date_next"] as BucketAggregate;
        Assert.NotNull(aggregation);
        Assert.InRange(aggregation.Items.Count, 120, 121);
        Assert.Equal(0, aggregation.Total);
    }

    [Fact]
    public async Task GetWithDateRangeFilterExpressionHonoringTimeZoneAsync()
    {
        var chicagoTimeZone = TZConvert.GetTimeZoneInfo("America/Chicago");
        var asiaTimeZone = TZConvert.GetTimeZoneInfo("Asia/Shanghai");
        var utcNow = DateTime.UtcNow;
        var chicagoNow = TimeZoneInfo.ConvertTimeFromUtc(utcNow, chicagoTimeZone);
        var asiaNow = TimeZoneInfo.ConvertTimeFromUtc(utcNow, asiaTimeZone);
        _logger.LogInformation($"UTC: {utcNow:o} Chicago: {chicagoNow:o} Asia: {asiaNow:o}");

        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(nextReview: utcNow), o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);
        var filter = $"next:[\"{utcNow.SubtractHours(1):o}\" TO \"{utcNow:o}\"]";
        var results = await _employeeRepository.FindAsync(o => o.FilterExpression(filter));
        _logger.LogInformation($"Count: {results.Total} - UTC range");
        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(1, results.Total);

        filter = $"next:[\"{chicagoNow.SubtractHours(1):o}\" TO \"{chicagoNow:o}\"]^\"America/Chicago\"";
        results = await _employeeRepository.FindAsync(o => o.FilterExpression(filter));
        _logger.LogInformation($"Count: {results.Total} - Chicago range");
        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(1, results.Total);

        filter = $"next:[\"{asiaNow.SubtractHours(1):o}\" TO \"{asiaNow:o}\"]^\"Asia/Shanghai\"";
        results = await _employeeRepository.FindAsync(o => o.FilterExpression(filter));
        _logger.LogInformation($"Count: {results.Total} - Asia range");
        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(1, results.Total);
    }

    [Fact]
    public async Task GetWithDateRangeHonoringTimeZoneAsync()
    {
        var chicagoTimeZone = TZConvert.GetTimeZoneInfo("America/Chicago");
        var asiaTimeZone = TZConvert.GetTimeZoneInfo("Asia/Shanghai");

        var utcNow = DateTime.UtcNow;
        var chicagoNow = TimeZoneInfo.ConvertTimeFromUtc(utcNow, chicagoTimeZone);
        var asiaNow = TimeZoneInfo.ConvertTimeFromUtc(utcNow, asiaTimeZone);

        _logger.LogInformation("UTC: {UtcNow} Chicago: {ChicagoNow} Asia: {AsiaNow}", utcNow, chicagoNow, asiaNow);

        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(nextReview: utcNow), o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);

        var results = await _employeeRepository.FindAsync(o => o.DateRange(utcNow.SubtractHours(1), utcNow, "next"));
        _logger.LogInformation("Count: {Total} - UTC range", results.Total);

        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(1, results.Total);

        results = await _employeeRepository.FindAsync(o => o.DateRange(chicagoNow.SubtractHours(1), chicagoNow, "next", "America/Chicago"));
        _logger.LogInformation("Count: {Total} - Chicago range", results.Total);

        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(1, results.Total);

        results = await _employeeRepository.FindAsync(o => o.DateRange(asiaNow.SubtractHours(1), asiaNow, "next", "Asia/Shanghai"));
        _logger.LogInformation($"Count: {results.Total} - Asia range");
        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(1, results.Total);

        results = await _employeeRepository.FindAsync(o => o.DateRange(null, asiaNow, "next", "Asia/Shanghai").AggregationsExpression("date:next"));
        _logger.LogInformation($"Count: {results.Total} - Asia LTE");
        Assert.Single(results.Documents);

        results = await _employeeRepository.FindAsync(o => o.DateRange(null, DateTime.MaxValue, "next", "Asia/Shanghai").AggregationsExpression("date:next"));
        _logger.LogInformation($"Count: {results.Total} - Max date LTE with asia time zone");
        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(1, results.Total);

        results = await _employeeRepository.FindAsync(o => o.DateRange(asiaNow.SubtractHours(1), null, "next", "Asia/Shanghai").AggregationsExpression("date:next"));
        _logger.LogInformation($"Count: {results.Total} - Chicago (-1) GTE with asia time zone");
        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(1, results.Total);

        results = await _employeeRepository.FindAsync(o => o.DateRange(DateTime.MinValue, DateTime.MaxValue, "next", "Asia/Shanghai").AggregationsExpression("date:next"));
        _logger.LogInformation($"Count: {results.Total} - Min to max date range with asia time zone");
        Assert.NotNull(results);
        Assert.Single(results.Documents);
        Assert.Equal(1, results.Page);
        Assert.False(results.HasMore);
        Assert.Equal(1, results.Total);

        // No matching documents will be found.
        results = await _employeeRepository.FindAsync(o => o.DateRange(DateTime.MinValue, DateTime.MinValue, "next", "Asia/Shanghai").AggregationsExpression("date:next"));
        _logger.LogInformation($"Count: {results.Total} - Min to min date range with asia time zone");
        Assert.Empty(results.Documents);

        // start date won't be used but max value will.
        results = await _employeeRepository.FindAsync(o => o.DateRange(DateTime.MaxValue, DateTime.MaxValue, "next", "Asia/Shanghai").AggregationsExpression("date:next"));
        _logger.LogInformation($"Count: {results.Total} - Max to max date range with asia time zone");
        Assert.Empty(results.Documents);

        await Assert.ThrowsAsync<ArgumentNullException>(() => _employeeRepository.FindAsync(o => o.DateRange(null, null, "next", "Asia/Shanghai").AggregationsExpression("date:next")));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _employeeRepository.FindAsync(o => o.DateRange(DateTime.MaxValue, DateTime.MinValue, "next", "Asia/Shanghai").AggregationsExpression("date:next")));
    }

    [Fact]
    public async Task ExistsAsync()
    {
        Assert.False(await _identityRepository.ExistsAsync(Id.Null));

        var identity = IdentityGenerator.Default;
        Assert.False(await _identityRepository.ExistsAsync(identity.Id));

        var result = await _identityRepository.AddAsync(identity, o => o.ImmediateConsistency());
        Assert.Equal(identity, result);

        Assert.True(await _identityRepository.ExistsAsync(identity.Id));
    }

    [Fact]
    public async Task ExistsWithTimeSeriesAsync()
    {
        Assert.False(await _dailyRepository.ExistsAsync(Id.Null));

        var utcNow = DateTime.UtcNow;
        var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(createdUtc: utcNow.AddDays(-1)), o => o.ImmediateConsistency());
        Assert.NotNull(yesterdayLog);
        Assert.NotNull(yesterdayLog.Id);

        var nowLog = await _dailyRepository.AddAsync(LogEventGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(nowLog);
        Assert.NotNull(nowLog.Id);

        Assert.True(await _dailyRepository.ExistsAsync(yesterdayLog.Id));
        Assert.True(await _dailyRepository.ExistsAsync(nowLog.Id));
    }

    [Fact]
    public async Task ShouldNotIncludeWhenDeletedAsync()
    {
        var deletedEmployee = EmployeeGenerator.Generate(age: 20, name: "Deleted");
        deletedEmployee.IsDeleted = true;
        await _employeeRepository.AddAsync(deletedEmployee, o => o.ImmediateConsistency());

        var employee2 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

        var employees = await _employeeRepository.GetAllByAgeAsync(20);
        Assert.Single(employees.Documents);
    }

    [Theory]
    [InlineData("age")]
    [InlineData("decimalAge")]
    public async Task CanSearchAfterAndBeforeWithMultipleSorts(string secondarySort)
    {
        await _employeeRepository.AddAsync(EmployeeGenerator.GenerateEmployees(count: 100), o => o.ImmediateConsistency());
        int pageSize = 10;

        var employeePages = new List<FindResults<Employee>>();
        string searchAfterToken = null;
        string searchBeforeToken = null;
        int page = 0;
        do
        {
            page++;
            var employees = await _employeeRepository.FindAsync(q => q.Sort(e => e.Name).Sort(e => e.CompanyName).SortDescending(secondarySort), o => o.SearchAfterToken(searchAfterToken).PageLimit(pageSize).QueryLogLevel(LogLevel.Information));
            searchBeforeToken = employees.GetSearchBeforeToken();
            searchAfterToken = employees.GetSearchAfterToken();
            if (page == 1)
            {
                Assert.Null(searchBeforeToken);
                Assert.NotNull(searchAfterToken);
            }
            else if (page == 10)
            {
                Assert.NotNull(searchBeforeToken);
                Assert.Null(searchAfterToken);
            }
            else
            {
                Assert.NotNull(searchBeforeToken);
                Assert.NotNull(searchAfterToken);
            }

            foreach (var employeePage in employeePages)
            {
                foreach (var employee in employees.Documents)
                {
                    bool documentExists = employeePage.Documents.Any(d => d.Id == employee.Id);
                    if (documentExists)
                        Assert.False(documentExists);
                }
            }

            employeePages.Add(employees);
            if (!employees.HasMore)
                break;
        } while (page < 20);

        Assert.Equal(10, page);
        Assert.Equal(10, employeePages.Count);

        do
        {
            page--;
            var employees = await _employeeRepository.FindAsync(q => q.Sort(e => e.Name).Sort(e => e.CompanyName).SortDescending(e => e.Age), o => o.SearchBeforeToken(searchBeforeToken).PageLimit(pageSize).QueryLogLevel(LogLevel.Information));
            searchBeforeToken = employees.GetSearchBeforeToken();
            searchAfterToken = employees.GetSearchAfterToken();
            if (page == 1)
            {
                Assert.Null(searchBeforeToken);
                Assert.NotNull(searchAfterToken);
            }
            else if (page == 10)
            {
                Assert.NotNull(searchBeforeToken);
                Assert.Null(searchAfterToken);
            }
            else
            {
                Assert.NotNull(searchBeforeToken);
                Assert.NotNull(searchAfterToken);
            }

            var matchingPage = employeePages[page - 1];
            for (int i = 0; i < pageSize; i++)
                Assert.Equal(matchingPage.Documents.ToArray()[i].Id, employees.Documents.ToArray()[i].Id);
        } while (page > 1);
    }

    [Fact]
    public async Task CanSearchAfterAndBeforeWithSortExpression()
    {
        await _employeeRepository.AddAsync(EmployeeGenerator.GenerateEmployees(count: 100), o => o.ImmediateConsistency());
        int pageSize = 10;

        var employeePages = new List<FindResults<Employee>>();
        string searchAfterToken = null;
        string searchBeforeToken = null;
        int page = 0;
        do
        {
            page++;
            var employees = await _employeeRepository.FindAsync(q => q.SortExpression("name companyname -age"), o => o.SearchAfterToken(searchAfterToken).PageLimit(pageSize).QueryLogLevel(LogLevel.Information));
            searchBeforeToken = employees.GetSearchBeforeToken();
            searchAfterToken = employees.GetSearchAfterToken();
            if (page == 1)
            {
                Assert.Null(searchBeforeToken);
                Assert.NotNull(searchAfterToken);
            }
            else if (page == 10)
            {
                Assert.NotNull(searchBeforeToken);
                Assert.Null(searchAfterToken);
            }
            else
            {
                Assert.NotNull(searchBeforeToken);
                Assert.NotNull(searchAfterToken);
            }

            foreach (var employeePage in employeePages)
            {
                foreach (var employee in employees.Documents)
                {
                    bool documentExists = employeePage.Documents.Any(d => d.Id == employee.Id);
                    if (documentExists)
                        Assert.False(documentExists);
                }
            }

            employeePages.Add(employees);
            if (!employees.HasMore)
                break;
        } while (page < 20);

        Assert.Equal(10, page);
        Assert.Equal(10, employeePages.Count);

        do
        {
            page--;
            var employees = await _employeeRepository.FindAsync(q => q.SortExpression("name companyname -age"), o => o.SearchBeforeToken(searchBeforeToken).PageLimit(pageSize).QueryLogLevel(LogLevel.Information));
            searchBeforeToken = employees.GetSearchBeforeToken();
            searchAfterToken = employees.GetSearchAfterToken();
            if (page == 1)
            {
                Assert.Null(searchBeforeToken);
                Assert.NotNull(searchAfterToken);
            }
            else if (page == 10)
            {
                Assert.NotNull(searchBeforeToken);
                Assert.Null(searchAfterToken);
            }
            else
            {
                Assert.NotNull(searchBeforeToken);
                Assert.NotNull(searchAfterToken);
            }

            var matchingPage = employeePages[page - 1];
            for (int i = 0; i < pageSize; i++)
                Assert.Equal(matchingPage.Documents.ToArray()[i].Id, employees.Documents.ToArray()[i].Id);
        } while (page > 1);
    }

    [Fact]
    public async Task SearchShouldNotReturnDeletedDocumentsAsync()
    {
        var employee = EmployeeGenerator.Generate(age: 20, name: "Deleted");
        employee = await _employeeRepository.AddAsync(employee, o => o.ImmediateConsistency());

        var employees = await _employeeRepository.GetAllByAgeAsync(20);
        Assert.Equal(1, employees.Total);

        employee.IsDeleted = true;
        await _employeeRepository.SaveAsync(employee, o => o.Consistency(Consistency.Eventual));
        employees = await _employeeRepository.GetAllByAgeAsync(20);
        Assert.Equal(0, employees.Total);

        Assert.Null(await _employeeRepository.GetByIdAsync(employee.Id));
        var employeeById = await _employeeRepository.GetByIdAsync(employee.Id, o => o.IncludeSoftDeletes());
        Assert.NotNull(employeeById);
        Assert.True(employeeById.IsDeleted);

        Assert.Empty(await _employeeRepository.GetByIdsAsync(new[] { employee.Id }));
        Assert.Single(await _employeeRepository.GetByIdsAsync(new[] { employee.Id }, o => o.IncludeSoftDeletes()));

        Assert.False(await _employeeRepository.ExistsAsync(employee.Id));
        Assert.True(await _employeeRepository.ExistsAsync(employee.Id, o => o.IncludeSoftDeletes()));
    }

    [Fact]
    public async Task OnlyIdsShouldNotReturnDocuments()
    {
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 20), o => o.ImmediateConsistency());

        var results = await _employeeRepository.FindAsAsync<Identity>(o => o.OnlyIds());
        Assert.Empty(results.Documents);
        Assert.Null(results.Hits.First().Document);
        Assert.NotNull(results.Hits.First().Id);
    }
}
