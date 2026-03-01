using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.AsyncEx;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Xunit;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class RepositoryTests : ElasticRepositoryTestBase
{
    private readonly IEmployeeRepository _employeeRepository;
    private readonly ILogEventRepository _dailyRepository;
    private readonly ILogEventRepository _dailyRepositoryWithNoCaching;
    private readonly IIdentityRepository _identityRepository;
    private readonly IIdentityRepository _identityRepositoryWithNoCaching;

    public RepositoryTests(ITestOutputHelper output) : base(output)
    {
        _dailyRepository = new DailyLogEventRepository(_configuration);
        _dailyRepositoryWithNoCaching = new DailyLogEventWithNoCachingRepository(_configuration);
        _employeeRepository = new EmployeeRepository(_configuration);
        _identityRepository = new IdentityRepository(_configuration);
        _identityRepositoryWithNoCaching = new IdentityWithNoCachingRepository(_configuration);
    }

    public override async ValueTask InitializeAsync()
    {
        _logger.LogInformation("Starting init");
        await base.InitializeAsync();
        await RemoveDataAsync();
        _logger.LogInformation("Done removing data");
    }

    [Fact]
    public async Task AddAsync()
    {
        var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Generate());
        Assert.NotNull(identity1);
        Assert.NotNull(identity1.Id);

        var disposables = new List<IDisposable>(2);
        var countdownEvent = new AsyncCountdownEvent(2);

        try
        {
            var identity2 = IdentityGenerator.Default;
            disposables.Add(_identityRepository.DocumentsAdding.AddSyncHandler((o, args) =>
            {
                Assert.Equal(identity2, args.Documents.First());
                countdownEvent.Signal();
            }));

            disposables.Add(_identityRepository.DocumentsAdded.AddSyncHandler((o, args) =>
            {
                Assert.Equal(identity2, args.Documents.First());
                countdownEvent.Signal();
            }));

            var result = await _identityRepository.AddAsync(identity2);
            Assert.Equal(IdentityGenerator.Default.Id, result.Id);

            await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
            Assert.Equal(0, countdownEvent.CurrentCount);
        }
        finally
        {
            foreach (var disposable in disposables)
                disposable.Dispose();

            disposables.Clear();
        }
    }

    [Fact]
    public async Task CanQueryByDeleted()
    {
        var employee1 = EmployeeGenerator.Default;
        employee1.IsDeleted = true;
        employee1 = await _employeeRepository.AddAsync(employee1, o => o.ImmediateConsistency());
        Assert.NotNull(employee1);
        Assert.NotNull(employee1.Id);

        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());

        var allEmployees = await _employeeRepository.FindAsync(null, o => o.IncludeSoftDeletes());
        Assert.Equal(2, allEmployees.Total);

        var onlyDeleted = await _employeeRepository.FindAsync(null, o => o.SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly));
        Assert.Equal(1, onlyDeleted.Total);
        Assert.Equal(employee1.Id, onlyDeleted.Documents.First().Id);

        var nonDeletedEmployees = await _employeeRepository.FindAsync(null, o => o.SoftDeleteMode(SoftDeleteQueryMode.ActiveOnly));
        Assert.Equal(1, nonDeletedEmployees.Total);
        Assert.NotEqual(employee1.Id, nonDeletedEmployees.Documents.First().Id);
    }

    [Fact]
    public async Task CanQueryByDeletedSearch()
    {
        var employee1 = EmployeeGenerator.Default;
        employee1.IsDeleted = true;
        employee1 = await _employeeRepository.AddAsync(employee1, o => o.ImmediateConsistency());
        Assert.NotNull(employee1);
        Assert.NotNull(employee1.Id);

        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());

        var allEmployees = await _employeeRepository.FindAsync(null, o => o.IncludeSoftDeletes());
        Assert.Equal(2, allEmployees.Total);

        var onlyDeleted = await _employeeRepository.FindAsync(q => q.FilterExpression("isDeleted:true"), o => o.IncludeSoftDeletes());
        Assert.Equal(1, onlyDeleted.Total);
        Assert.Equal(employee1.Id, onlyDeleted.Documents.First().Id);

        var nonDeletedEmployees = await _employeeRepository.GetAllAsync();
        Assert.Equal(1, nonDeletedEmployees.Total);
        Assert.NotEqual(employee1.Id, nonDeletedEmployees.Documents.First().Id);
    }

    [Fact]
    public async Task QueryByIdsWorksWithSoftDeletedModes()
    {
        var employee1 = EmployeeGenerator.Default;
        employee1.IsDeleted = true;
        employee1 = await _employeeRepository.AddAsync(employee1, o => o.ImmediateConsistency());
        Assert.NotNull(employee1);
        Assert.NotNull(employee1.Id);

        var employee2 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());

        var employees = await _employeeRepository.FindAsync(q => q.Id(employee1.Id, employee2.Id));
        Assert.Equal(1, employees.Total);
        Assert.Equal(employee2.Id, employees.Documents.Single().Id);

        employees = await _employeeRepository.FindAsync(q => q.Id(employee1.Id, employee2.Id), o => o.IncludeSoftDeletes());
        Assert.Equal(2, employees.Total);

        employees = await _employeeRepository.FindAsync(q => q.Id(employee1.Id, employee2.Id), o => o.SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly));
        Assert.Equal(1, employees.Total);
        Assert.Equal(employee1.Id, employees.Documents.Single().Id);
    }

    [Fact]
    public async Task AddDuplicateAsync()
    {
        var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(identity1);
        Assert.NotNull(identity1.Id);

        await Assert.ThrowsAsync<DuplicateDocumentException>(async () => await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency()));
        Assert.Equal(1, await _identityRepository.CountAsync());
    }

    [Fact]
    public async Task AddDuplicateCollectionAsync()
    {
        var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(identity1);
        Assert.NotNull(identity1.Id);

        var identities = new List<Identity> {
            IdentityGenerator.Default,
            IdentityGenerator.Generate()
        };

        await Assert.ThrowsAsync<DuplicateDocumentException>(async () => await _identityRepository.AddAsync(identities, o => o.ImmediateConsistency()));
        Assert.Equal(2, await _identityRepository.CountAsync());
    }

    [Fact]
    public async Task AddWithCachingAsync()
    {
        var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.Cache());
        Assert.NotNull(identity);
        Assert.NotNull(identity.Id);
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache()));
        Assert.Equal(1, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(0, _cache.Misses);
    }

    [Fact]
    public async Task AddWithTimeSeriesAsync()
    {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate());
        Assert.NotNull(log);
        Assert.NotNull(log.Id);

        Assert.Equal(log, await _dailyRepository.GetByIdAsync(log.Id));
    }

    [Fact]
    public async Task AddCollectionAsync()
    {
        var identity = IdentityGenerator.Generate();
        await _identityRepository.AddAsync(new List<Identity> { identity });
        Assert.NotNull(identity.Id);

        Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id));
    }

    [Fact]
    public async Task AddCollectionWithTimeSeriesAsync()
    {
        var utcNow = DateTime.UtcNow;
        var yesterdayLog = LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1));
        var nowLog = LogEventGenerator.Default;

        var logs = new List<LogEvent> { yesterdayLog, nowLog };
        await _dailyRepository.AddAsync(logs, o => o.ImmediateConsistency());

        var results = await _dailyRepository.GetByIdsAsync(new List<string> { yesterdayLog.Id, nowLog.Id });
        Assert.Equal(logs, results.OrderBy(d => d.CreatedUtc).ToList());

        var getAllResults = await _dailyRepository.GetAllAsync();
        Assert.Equal(logs, getAllResults.Documents.OrderBy(d => d.CreatedUtc).ToList());
    }

    [Fact]
    public async Task AddCollectionWithCachingAsync()
    {
        var identity = IdentityGenerator.Generate();
        await _identityRepository.AddAsync(new List<Identity> { identity, IdentityGenerator.Generate() }, o => o.Cache());
        Assert.NotNull(identity);
        Assert.NotNull(identity.Id);
        Assert.Equal(2, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache()));
        Assert.Equal(2, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(0, _cache.Misses);
    }

    [Fact]
    public async Task AddCollectionWithCacheKeyAsync()
    {
        var identity = IdentityGenerator.Generate();
        var identity2 = IdentityGenerator.Generate();
        await _identityRepository.AddAsync(new List<Identity> { identity, identity2 }, o => o.Cache());
        Assert.NotNull(identity);
        Assert.NotNull(identity.Id);
        Assert.Equal(2, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache()));
        Assert.Equal(2, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        var idsResult = await _identityRepository.GetByIdsAsync(new[] { identity.Id }, o => o.Cache());
        Assert.Equal(identity, idsResult.Single());
        Assert.Equal(2, _cache.Count);
        Assert.Equal(2, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        await _identityRepository.InvalidateCacheAsync(new List<Identity> { identity, identity2 });
        Assert.Equal(0, _cache.Count);
        Assert.Equal(2, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache()));
        Assert.Equal(1, _cache.Count);
        Assert.Equal(2, _cache.Hits);
        Assert.Equal(1, _cache.Misses);

        idsResult = await _identityRepository.GetByIdsAsync(new[] { identity.Id, identity2.Id }, o => o.Cache());
        Assert.Equal(2, idsResult.Count);
        Assert.Equal(2, _cache.Count);
        Assert.Equal(3, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        idsResult = await _identityRepository.GetByIdsAsync(new[] { identity.Id, identity2.Id }, o => o.Cache());
        Assert.Equal(2, idsResult.Count);
        Assert.Equal(2, _cache.Count);
        Assert.Equal(5, _cache.Hits);
        Assert.Equal(2, _cache.Misses);
    }

    [Fact]
    public async Task SaveAsync()
    {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Default, o => o.Notifications(false));
        Assert.NotNull(log);
        Assert.NotNull(log.Id);

        var disposables = new List<IDisposable>();
        var countdownEvent = new AsyncCountdownEvent(5);

        try
        {
            disposables.Add(_dailyRepository.DocumentsChanging.AddSyncHandler((o, args) =>
            {
                Assert.Equal(log, args.Documents.First().Value);
                countdownEvent.Signal();
            }));
            disposables.Add(_dailyRepository.DocumentsChanged.AddSyncHandler((o, args) =>
            {
                Assert.Equal(log, args.Documents.First().Value);
                countdownEvent.Signal();
            }));
            disposables.Add(_dailyRepository.DocumentsSaving.AddSyncHandler((o, args) =>
            {
                Assert.Equal(log, args.Documents.First().Value);
                countdownEvent.Signal();
            }));
            disposables.Add(_dailyRepository.DocumentsSaved.AddSyncHandler((o, args) =>
            {
                Assert.Equal(log, args.Documents.First().Value);
                countdownEvent.Signal();
            }));
            await _messageBus.SubscribeAsync<EntityChanged>((msg, ct) =>
            {
                Assert.Equal(nameof(LogEvent), msg.Type);
                Assert.Equal(log.Id, msg.Id);
                Assert.Equal(ChangeType.Saved, msg.ChangeType);
                countdownEvent.Signal();
                return Task.CompletedTask;
            }, TestCancellationToken);

            log.CompanyId = ObjectId.GenerateNewId().ToString();
            var result = await _dailyRepository.SaveAsync(log);
            Assert.Equal(log.CompanyId, result.CompanyId);

            await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromSeconds(2)).Token);
            Assert.Equal(0, countdownEvent.CurrentCount);
        }
        finally
        {
            foreach (var disposable in disposables)
                disposable.Dispose();

            disposables.Clear();
        }
    }

    [Fact]
    public async Task AddAndSaveAsync()
    {
        var logEntry = await _dailyRepository.AddAsync(LogEventGenerator.Default, o => o.Notifications(false));
        Assert.NotNull(logEntry);
        Assert.NotNull(logEntry.Id);

        var disposables = new List<IDisposable>(4);
        var saveCountdownEvent = new AsyncCountdownEvent(2);
        var notificationCountdownEvent = new AsyncCountdownEvent(2);
        // Save requires an id to be set.
        var addedLog = LogEventGenerator.Generate(id: ObjectId.GenerateNewId().ToString());
        try
        {
            disposables.Add(_dailyRepository.DocumentsSaving.AddSyncHandler((o, args) =>
            {
                Assert.Equal(logEntry, args.Documents.First().Value);
                saveCountdownEvent.Signal();
            }));
            disposables.Add(_dailyRepository.DocumentsSaved.AddSyncHandler((o, args) =>
            {
                Assert.Equal(logEntry, args.Documents.First().Value);
                saveCountdownEvent.Signal();
            }));
            await _messageBus.SubscribeAsync<EntityChanged>((msg, ct) =>
            {
                Assert.Equal(nameof(LogEvent), msg.Type);
                Assert.True(msg.Id == logEntry.Id || msg.Id == addedLog.Id);
                Assert.Equal(ChangeType.Saved, msg.ChangeType);
                notificationCountdownEvent.Signal();
                return Task.CompletedTask;
            }, TestCancellationToken);

            logEntry.CompanyId = ObjectId.GenerateNewId().ToString();
            await _dailyRepository.SaveAsync(new List<LogEvent> { logEntry, addedLog });

            await notificationCountdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromSeconds(2)).Token);
            Assert.Equal(0, notificationCountdownEvent.CurrentCount);
            Assert.Equal(0, saveCountdownEvent.CurrentCount);
        }
        finally
        {
            foreach (var disposable in disposables)
                disposable.Dispose();

            disposables.Clear();
        }
    }

    [Fact]
    public async Task SaveWithOriginalFromOptions()
    {
        var original = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.Notifications(false));
        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);
        Assert.Equal(0, _messageBus.MessagesSent);
        Assert.Equal(0, _employeeRepository.QueryCount);

        var copy = original.DeepClone();
        copy.Age = 30;
        await _employeeRepository.SaveAsync(copy, o => o.AddOriginals(original));

        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);
        Assert.Equal(1, _messageBus.MessagesSent);
        Assert.Equal(0, _employeeRepository.QueryCount);
    }

    [Fact]
    public async Task AddAndSaveWithCacheAsync()
    {
        var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.Cache());
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        string cacheKey = _cache.Keys.Single();
        var cacheValue = await _cache.GetAsync<IEnumerable<FindHit<Identity>>>(cacheKey);
        Assert.True(cacheValue.HasValue);
        Assert.Equal(identity, cacheValue.Value.Single().Document);

        identity = await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache());
        Assert.NotNull(identity);
        Assert.Equal(2, _cache.Hits);

        cacheValue = await _cache.GetAsync<IEnumerable<FindHit<Identity>>>(cacheKey);
        Assert.True(cacheValue.HasValue);
        Assert.Equal(identity, cacheValue.Value.Single().Document);

        await _identityRepository.InvalidateCacheAsync(identity);
        Assert.Equal(0, _cache.Count);
        Assert.Equal(3, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        var result = await _identityRepository.SaveAsync(identity, o => o.Cache());
        Assert.NotNull(result);
        Assert.Equal(1, _cache.Count);
        Assert.Equal(3, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        cacheValue = await _cache.GetAsync<IEnumerable<FindHit<Identity>>>(cacheKey);
        Assert.True(cacheValue.HasValue);
        Assert.Equal(identity, cacheValue.Value.Single().Document);
    }

    [Fact]
    public async Task AddAndSaveWithCacheKeyAsync()
    {
        var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.Cache());
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        var cache = new ScopedCacheClient(_cache, nameof(Identity));
        var cacheValue = await cache.GetAsync<IEnumerable<FindHit<Identity>>>(identity.Id);
        Assert.Equal(1, _cache.Hits);
        Assert.True(cacheValue.HasValue);
        Assert.Equal(identity, cacheValue.Value.Single().Document);

        identity = await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache());
        Assert.NotNull(identity);
        Assert.Equal(2, _cache.Hits);

        await _identityRepository.InvalidateCacheAsync(identity);
        Assert.Equal(0, _cache.Count);
        Assert.Equal(2, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        var result = await _identityRepository.SaveAsync(identity, o => o.Cache());
        Assert.NotNull(result);
        Assert.Equal(1, _cache.Count);
        Assert.Equal(2, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        cacheValue = await cache.GetAsync<IEnumerable<FindHit<Identity>>>(identity.Id);
        Assert.True(cacheValue.HasValue);
        Assert.Equal(identity, cacheValue.Value.Single().Document);
    }

    [Fact]
    public Task SaveWithNoIdentityAsync()
    {
        var identity = IdentityGenerator.Generate();
        return Assert.ThrowsAsync<ArgumentException>(async () => await _identityRepository.SaveAsync(new List<Identity> { identity }, o => o.Cache()));
    }

    [Fact]
    public async Task SaveWithOutOfSyncIndexAsync()
    {
        var utcNow = DateTime.UtcNow;
        var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)), o => o.ImmediateConsistency());
        Assert.NotNull(yesterdayLog);
        Assert.NotNull(yesterdayLog.Id);

        Assert.Equal(1, await _dailyRepository.CountAsync());

        yesterdayLog.Message = "updated";
        await _dailyRepository.SaveAsync(yesterdayLog, o => o.ImmediateConsistency());

        Assert.Equal(1, await _dailyRepository.CountAsync());
    }

    [Fact]
    public async Task CanGetAggregationsAsync()
    {
        var utcNow = DateTime.UtcNow;
        var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)), o => o.ImmediateConsistency());
        Assert.NotNull(yesterdayLog);
        Assert.NotNull(yesterdayLog.Id);

        var result = await _dailyRepository.CountAsync(q => q.AggregationsExpression("cardinality:companyId max:createdUtc"));
        Assert.Equal(2, result.Aggregations.Count);
        var cardinalityAgg = result.Aggregations.Cardinality("cardinality_companyId");
        Assert.NotNull(cardinalityAgg);
        Assert.Equal(1, cardinalityAgg.Value.GetValueOrDefault());

        var maxAgg = result.Aggregations.Max<DateTime>("max_createdUtc");
        Assert.NotNull(maxAgg);
        Assert.True(yesterdayLog.CreatedUtc.Subtract(maxAgg.Value).TotalSeconds < 1);
    }

    [Fact]
    public async Task CanGetDateAggregationAsync()
    {
        var utcNow = DateTime.UtcNow;
        var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)), o => o.ImmediateConsistency());
        Assert.NotNull(yesterdayLog);
        Assert.NotNull(yesterdayLog.Id);

        var result = await _dailyRepository.CountAsync(q => q.AggregationsExpression("date:(createdUtc min:createdUtc)"));
        Assert.Single(result.Aggregations);
        var dateAgg = result.Aggregations.DateHistogram("date_createdUtc");
        Assert.NotNull(dateAgg);
        Assert.Single(dateAgg.Buckets);
        Assert.Equal(utcNow.AddDays(-1).Date, dateAgg.Buckets.First().Date);
        Assert.Equal(utcNow.AddDays(-1).Floor(TimeSpan.FromMilliseconds(1)), dateAgg.Buckets.First().Aggregations.Min<DateTime>("min_createdUtc").Value.Floor(TimeSpan.FromMilliseconds(1)));

        result = await _dailyRepository.CountAsync(q => q.AggregationsExpression("date:(createdUtc~1h^-3h min:createdUtc)"));
        Assert.Single(result.Aggregations);
        dateAgg = result.Aggregations.DateHistogram("date_createdUtc");
        Assert.NotNull(dateAgg);
        Assert.Single(dateAgg.Buckets);
    }

    [Fact]
    public async Task CanGetGeoGridAggregationAsync()
    {
        var utcNow = DateTime.UtcNow;
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)), o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        Assert.NotNull(employee.Id);
        await _employeeRepository.AddAsync(EmployeeGenerator.GenerateEmployees(), o => o.ImmediateConsistency());

        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression("geogrid:(location~6 max:age)"));
        Assert.Single(result.Aggregations);
        var geoAgg = result.Aggregations.GeoHash("geogrid_location");
        Assert.NotNull(geoAgg);
        Assert.InRange(geoAgg.Buckets.Count, 1, 11);
    }

    [Fact]
    public async Task SaveWithCachingAsync()
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

        identity = await _identityRepository.SaveAsync(identity, o => o.Cache());
        Assert.NotNull(identity);
        Assert.NotNull(identity.Id);
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache()));
        Assert.Equal(1, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(0, _cache.Misses);
    }

    [Fact]
    public async Task SaveCollectionAsync()
    {
        var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate(ObjectId.GenerateNewId().ToString()) };
        await _identityRepository.SaveAsync(identities);

        var results = await _identityRepository.GetByIdsAsync(identities.Select(i => i.Id).ToList());
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task SaveCollectionWithTimeSeriesAsync()
    {
        var utcNow = DateTime.UtcNow;
        var yesterdayLog = LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1));
        var nowLog = LogEventGenerator.Default;

        var logs = new List<LogEvent> { yesterdayLog, nowLog };
        await _dailyRepository.AddAsync(logs);

        foreach (var logEvent in logs)
            logEvent.Message = "updated";

        await _dailyRepository.SaveAsync(logs);

        var results = await _dailyRepository.GetByIdsAsync(new List<string> { yesterdayLog.Id, nowLog.Id });
        Assert.Equal(logs, results.OrderBy(d => d.CreatedUtc).ToList());
    }

    [Fact]
    public async Task SaveCollectionWithCachingAsync()
    {
        var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
        await _identityRepository.AddAsync(identities, o => o.Cache());
        Assert.Equal(2, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        await _identityRepository.InvalidateCacheAsync(identities);
        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        await _identityRepository.SaveAsync(identities, o => o.Cache());
        Assert.Equal(2, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        var results = await _identityRepository.GetByIdsAsync(new Ids(identities.Select(i => i.Id)), o => o.Cache());
        Assert.Equal(2, results.Count);
        Assert.Equal(2, _cache.Count);
        Assert.Equal(2, _cache.Hits);
        Assert.Equal(0, _cache.Misses);
    }

    [Fact]
    public async Task AddAsync_WithMockedTimeProvider_ShouldSetExactTimes()
    {
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMilliseconds(100)));
        _configuration.TimeProvider = timeProvider;

        var expectedTime = timeProvider.GetUtcNow().UtcDateTime;
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);

        Assert.Equal(expectedTime, employee.CreatedUtc);
        Assert.Equal(expectedTime, employee.UpdatedUtc);

        var createdUtc = employee.CreatedUtc;
        var updatedUtc = employee.UpdatedUtc;
        employee.Name = Guid.NewGuid().ToString();
        timeProvider.Advance(TimeSpan.FromMilliseconds(100));
        var updatedExpectedTime = timeProvider.GetUtcNow().UtcDateTime;

        employee = await _employeeRepository.SaveAsync(employee);

        Assert.Equal(createdUtc, employee.CreatedUtc);
        Assert.Equal(updatedExpectedTime, employee.UpdatedUtc);
        Assert.True(updatedUtc < employee.UpdatedUtc, $"Previous UpdatedUtc: {updatedUtc} Current UpdatedUtc: {employee.UpdatedUtc}");
    }

    [Fact]
    public async Task AddAsync_WithFutureMockedTimeProvider_ShouldRespectMockedTime()
    {
        // Arrange
        var futureTime = new DateTimeOffset(2030, 1, 15, 12, 0, 0, TimeSpan.Zero);
        var timeProvider = new FakeTimeProvider(futureTime);
        _configuration.TimeProvider = timeProvider;

        // Act
        var expectedTime = timeProvider.GetUtcNow().UtcDateTime;
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);

        // Assert
        // These assertions will fail if SetDates/SetCreatedDates doesn't receive the TimeProvider
        // because the extension methods default to TimeProvider.System and will see the future
        // date as "in the future" and overwrite it with the real system time
        Assert.Equal(expectedTime, employee.CreatedUtc);
        Assert.Equal(expectedTime, employee.UpdatedUtc);
    }

    [Fact]
    public async Task SaveAsync_WithFutureMockedTimeProvider_ShouldRespectMockedTime()
    {
        // Arrange
        var futureTime = new DateTimeOffset(2030, 1, 15, 12, 0, 0, TimeSpan.Zero);
        var timeProvider = new FakeTimeProvider(futureTime);
        _configuration.TimeProvider = timeProvider;

        var expectedTime = timeProvider.GetUtcNow().UtcDateTime;
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);

        Assert.Equal(expectedTime, employee.CreatedUtc);
        Assert.Equal(expectedTime, employee.UpdatedUtc);

        var createdUtc = employee.CreatedUtc;
        employee.Name = Guid.NewGuid().ToString();
        timeProvider.Advance(TimeSpan.FromHours(1));
        var updatedExpectedTime = timeProvider.GetUtcNow().UtcDateTime;

        // Act
        employee = await _employeeRepository.SaveAsync(employee);

        // Assert
        // CreatedUtc should remain unchanged, UpdatedUtc should reflect the advanced time
        Assert.Equal(createdUtc, employee.CreatedUtc);
        Assert.Equal(updatedExpectedTime, employee.UpdatedUtc);
    }

    [Fact]
    public async Task AddAsync_IHaveCreatedDateWithFutureMockedTimeProvider_ShouldRespectMockedTime()
    {
        // Arrange
        var futureTime = new DateTimeOffset(2030, 1, 15, 12, 0, 0, TimeSpan.Zero);
        var timeProvider = new FakeTimeProvider(futureTime);
        _configuration.TimeProvider = timeProvider;

        var expectedTime = timeProvider.GetUtcNow().UtcDateTime;
        var logEvent = new LogEvent { Message = "test", CompanyId = LogEventGenerator.DefaultCompanyId, Date = futureTime };

        // Act
        logEvent = await _dailyRepository.AddAsync(logEvent);

        // Assert
        // This will fail if SetCreatedDates doesn't receive the TimeProvider, because it defaults
        // to TimeProvider.System which sees the future date as "in the future" and overwrites it
        Assert.Equal(expectedTime, logEvent.CreatedUtc);
    }

    [Fact]
    public async Task CannotSetFutureCreatedAndModifiedTimesAsync()
    {
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(createdUtc: DateTime.MaxValue, updatedUtc: DateTime.MaxValue));
        Assert.True(employee.CreatedUtc != DateTime.MaxValue);
        Assert.True(employee.UpdatedUtc != DateTime.MaxValue);

        employee.CreatedUtc = DateTime.MaxValue;
        employee.UpdatedUtc = DateTime.MaxValue;

        employee = await _employeeRepository.SaveAsync(employee);
        Assert.True(employee.CreatedUtc != DateTime.MaxValue);
        Assert.True(employee.UpdatedUtc != DateTime.MaxValue);
    }

    [Fact]
    public async Task JsonPatchAsync()
    {
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
        Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
        var patch = new PatchDocument(new ReplaceOperation { Path = "name", Value = "Patched" });
        await _employeeRepository.PatchAsync(employee.Id, new JsonPatch(patch));

        employee = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
        Assert.Equal("Patched", employee.Name);
        Assert.Equal("1:1", employee.Version);
    }

    [Fact]
    public async Task JsonPatchAllAsync()
    {
        await _employeeRepository.AddAsync(EmployeeGenerator.GenerateEmployees(100), o => o.ImmediateConsistency());
        var patch = new PatchDocument(new ReplaceOperation { Path = "name", Value = "Patched" });
        await _employeeRepository.PatchAllAsync(q => q, new JsonPatch(patch));

        var employees = await _employeeRepository.GetAllAsync(o => o.PageLimit(1000).ImmediateConsistency());
        Assert.Equal(100, employees.Documents.Count);
        Assert.True(employees.Documents.All(e => e.Name == "Patched"));
    }

    [Fact]
    public async Task ConcurrentJsonPatchAsync()
    {
        var resetEvent = new AutoResetEvent(false);
        var cts = new CancellationTokenSource();

        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.Cache(false));
        string employeeId = employee.Id;
        _ = Parallel.ForEachAsync(Enumerable.Range(1, 100), new ParallelOptions { MaxDegreeOfParallelism = 2, CancellationToken = cts.Token }, async (i, ct) =>
        {
            var e = await _employeeRepository.GetByIdAsync(employeeId, o => o.Cache(false));
            resetEvent.Set();
            e.CompanyName = "Company " + i;
            try
            {
                await _employeeRepository.SaveAsync(e, o => o.Cache(false));
                _logger.LogInformation("Set company {Iteration}", i);
            }
            catch (VersionConflictDocumentException)
            {
                _logger.LogInformation("Got version conflict {Iteration}", i);
            }
        });

        _logger.LogInformation("Saving name");
        resetEvent.WaitOne();
        employee = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        resetEvent.WaitOne();
        employee.Name = "Saved";
        try
        {
            await _employeeRepository.SaveAsync(employee, o => o.Cache(false));
            _logger.LogInformation("Saved name");
        }
        catch (VersionConflictDocumentException)
        {
            _logger.LogInformation("Conflict attempting to save name");
        }

        resetEvent.WaitOne();
        _logger.LogInformation("Patching name");
        var patch = new PatchDocument(new ReplaceOperation { Path = "name", Value = "Patched" });
        await _employeeRepository.PatchAsync(employee.Id, new JsonPatch(patch));
        _logger.LogInformation("Done patching name");

        await cts.CancelAsync();
        await Task.Delay(100, TestCancellationToken);

        employee = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
        Assert.Equal("Patched", employee.Name);
        _logger.LogInformation("Got employee with company {CompanyName}", employee.CompanyName);
        Assert.NotEqual(EmployeeGenerator.Default.CompanyName, employee.CompanyName);
    }

    [Fact]
    public async Task ConcurrentJsonPatchAllAsync()
    {
        var resetEvent = new AutoResetEvent(false);
        var cts = new CancellationTokenSource();

        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.Cache(false));
        string employeeId = employee.Id;
        _ = Parallel.ForEachAsync(Enumerable.Range(1, 100), new ParallelOptions { MaxDegreeOfParallelism = 2, CancellationToken = cts.Token }, async (i, ct) =>
        {
            var e = await _employeeRepository.GetByIdAsync(employeeId, o => o.Cache(false));
            resetEvent.Set();
            e.CompanyName = "Company " + i;
            try
            {
                await _employeeRepository.SaveAsync(e, o => o.Cache(false));
                _logger.LogInformation("Set company {Iteration}", i);
            }
            catch (VersionConflictDocumentException)
            {
                _logger.LogInformation("Got version conflict {Iteration}", i);
            }
        });

        _logger.LogInformation("Saving name");
        resetEvent.WaitOne();
        employee = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        resetEvent.WaitOne();
        employee.Name = "Saved";
        try
        {
            await _employeeRepository.SaveAsync(employee, o => o.Cache(false));
            _logger.LogInformation("Saved name");
        }
        catch (VersionConflictDocumentException)
        {
            _logger.LogInformation("Conflict attempting to save name");
        }

        resetEvent.WaitOne();
        _logger.LogInformation("Patching name");
        var patch = new PatchDocument(new ReplaceOperation { Path = "name", Value = "Patched" });
        await _employeeRepository.PatchAllAsync(q => q.Age(EmployeeGenerator.Default.Age), new JsonPatch(patch), o => o.ImmediateConsistency());
        _logger.LogInformation("Done patching name");

        await cts.CancelAsync();
        await Task.Delay(100, TestCancellationToken);

        employee = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
        Assert.Equal("Patched", employee.Name);
        _logger.LogInformation("Got employee with company {CompanyName}", employee.CompanyName);
        Assert.NotEqual(EmployeeGenerator.Default.CompanyName, employee.CompanyName);
    }

    [Fact]
    public async Task PartialPatchAsync()
    {
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
        await _employeeRepository.PatchAsync(employee.Id, new PartialPatch(new { name = "Patched" }));

        employee = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
        Assert.Equal("Patched", employee.Name);
        Assert.Equal("1:1", employee.Version);
    }

    [Fact]
    public async Task ScriptPatchAsync()
    {
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
        await _employeeRepository.PatchAsync(employee.Id, new ScriptPatch("ctx._source.name = 'Patched';"));

        employee = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
        Assert.Equal("Patched", employee.Name);
        Assert.Equal("1:1", employee.Version);
    }

    [Fact]
    public async Task ConcurrentScriptPatchAsync()
    {
        var resetEvent = new AutoResetEvent(false);
        var cts = new CancellationTokenSource();

        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.Cache(false));
        string employeeId = employee.Id;
        _ = Parallel.ForEachAsync(Enumerable.Range(1, 100), new ParallelOptions { MaxDegreeOfParallelism = 2, CancellationToken = cts.Token }, async (i, ct) =>
        {
            var e = await _employeeRepository.GetByIdAsync(employeeId, o => o.Cache(false));
            resetEvent.Set();
            e.CompanyName = "Company " + i;
            try
            {
                await _employeeRepository.SaveAsync(e, o => o.Cache(false));
                _logger.LogInformation("Set company {Iteration}", i);
            }
            catch (VersionConflictDocumentException)
            {
                _logger.LogInformation("Got version conflict {Iteration}", i);
            }
        });

        _logger.LogInformation("Saving name");
        resetEvent.WaitOne();
        employee = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        resetEvent.WaitOne();
        employee.Name = "Saved";
        try
        {
            await _employeeRepository.SaveAsync(employee, o => o.Cache(false));
            _logger.LogInformation("Saved name");
        }
        catch (VersionConflictDocumentException)
        {
            _logger.LogInformation("Conflict attempting to save name");
        }

        resetEvent.WaitOne();
        _logger.LogInformation("Patching name");
        await _employeeRepository.PatchAsync(employee.Id, new ScriptPatch("ctx._source.name = 'Patched';"));
        _logger.LogInformation("Done patching name");

        await cts.CancelAsync();
        await Task.Delay(100, TestCancellationToken);

        employee = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
        Assert.Equal("Patched", employee.Name);
        _logger.LogInformation("Got employee with company {CompanyName}", employee.CompanyName);
        Assert.NotEqual(EmployeeGenerator.Default.CompanyName, employee.CompanyName);
    }

    [Fact]
    public async Task ActionPatchAsync()
    {
        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
        await _employeeRepository.PatchAsync(employee.Id, new ActionPatch<Employee>(e => e.Name = "Patched"));

        employee = await _employeeRepository.GetByIdAsync(employee.Id);
        Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
        Assert.Equal("Patched", employee.Name);
        Assert.Equal("1:1", employee.Version);
    }

    [Fact]
    public async Task ActionPatchAllAsync()
    {
        await _employeeRepository.AddAsync(EmployeeGenerator.GenerateEmployees(100), o => o.ImmediateConsistency());
        await _employeeRepository.PatchAllAsync(q => q, new ActionPatch<Employee>(e => e.Name = "Patched"));

        var employees = await _employeeRepository.GetAllAsync(o => o.PageLimit(1000).ImmediateConsistency());
        Assert.Equal(100, employees.Documents.Count);
        Assert.True(employees.Documents.All(e => e.Name == "Patched"));
    }

    [Fact]
    public async Task ConcurrentActionPatchAsync()
    {
        var resetEvent = new AutoResetEvent(false);
        var cts = new CancellationTokenSource();

        var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default, o => o.Cache(false));
        string employeeId = employee.Id;
        _ = Parallel.ForEachAsync(Enumerable.Range(1, 100), new ParallelOptions { MaxDegreeOfParallelism = 2, CancellationToken = cts.Token }, async (i, ct) =>
        {
            var e = await _employeeRepository.GetByIdAsync(employeeId, o => o.Cache(false));
            resetEvent.Set();
            e.CompanyName = "Company " + i;
            try
            {
                await _employeeRepository.SaveAsync(e, o => o.Cache(false));
                _logger.LogInformation("Set company {Iteration}", i);
            }
            catch (VersionConflictDocumentException)
            {
                _logger.LogInformation("Got version conflict {Iteration}", i);
            }
        });

        _logger.LogInformation("Saving name");
        resetEvent.WaitOne();
        employee = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        resetEvent.WaitOne();
        employee.Name = "Saved";
        try
        {
            await _employeeRepository.SaveAsync(employee, o => o.Cache(false));
            _logger.LogInformation("Saved name");
        }
        catch (VersionConflictDocumentException)
        {
            _logger.LogInformation("Conflict attempting to save name");
        }

        resetEvent.WaitOne();
        _logger.LogInformation("Patching name");
        await _employeeRepository.PatchAsync(employee.Id, new ActionPatch<Employee>(e => e.Name = "Patched"));
        _logger.LogInformation("Done patching name");

        await cts.CancelAsync();
        await Task.Delay(100, TestCancellationToken);

        employee = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
        Assert.Equal("Patched", employee.Name);
        _logger.LogInformation("Got employee with company {CompanyName}", employee.CompanyName);
        Assert.NotEqual(EmployeeGenerator.Default.CompanyName, employee.CompanyName);
    }

    [Fact]
    public async Task ScriptPatchAllAsync()
    {
        var utcNow = DateTime.UtcNow;
        var logs = new List<LogEvent> {
            LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1), companyId: "1"),
            LogEventGenerator.Generate(createdUtc: utcNow, companyId: "1"),
            LogEventGenerator.Generate(createdUtc: utcNow, companyId: "2"),
        };

        await _dailyRepository.AddAsync(logs, o => o.Cache().ImmediateConsistency());
        Assert.Equal(5, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        Assert.Equal(3, await _dailyRepository.IncrementValueAsync(logs.Select(l => l.Id).ToArray()));
        Assert.Equal(2, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(2, _cache.Misses);

        var results = await _dailyRepository.GetAllByCompanyAsync("1");
        Assert.Equal(2, results.Documents.Count);
        foreach (var document in results.Documents)
        {
            Assert.Equal("1", document.CompanyId);
            Assert.Equal(1, document.Value);
        }

        await _dailyRepository.SaveAsync(logs, o => o.Cache().ImmediateConsistency());

        results = await _dailyRepository.GetAllByCompanyAsync("1");
        Assert.Equal(2, results.Documents.Count);
        foreach (var document in results.Documents)
        {
            Assert.Equal("1", document.CompanyId);
            Assert.Equal(0, document.Value);
        }
    }

    [Fact]
    public async Task ScriptPatchAllWithNoCacheAsync()
    {
        var utcNow = DateTime.UtcNow;
        var logs = new List<LogEvent> {
            LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1), companyId: "1"),
            LogEventGenerator.Generate(createdUtc: utcNow, companyId: "1"),
            LogEventGenerator.Generate(createdUtc: utcNow, companyId: "2"),
        };

        await _dailyRepositoryWithNoCaching.AddAsync(logs, o => o.ImmediateConsistency());
        Assert.Equal(3, await _dailyRepositoryWithNoCaching.IncrementValueAsync(q => q.Id(logs.Select(l => l.Id).ToArray())));

        var results = await _dailyRepositoryWithNoCaching.GetAllByCompanyAsync("1");
        Assert.Equal(2, results.Documents.Count);
        foreach (var document in results.Documents)
        {
            Assert.Equal("1", document.CompanyId);
            Assert.Equal(1, document.Value);
        }

        await _dailyRepositoryWithNoCaching.SaveAsync(logs, o => o.ImmediateConsistency());

        results = await _dailyRepositoryWithNoCaching.GetAllByCompanyAsync("1");
        Assert.Equal(2, results.Documents.Count);
        foreach (var document in results.Documents)
        {
            Assert.Equal("1", document.CompanyId);
            Assert.Equal(0, document.Value);
        }
    }

    [Fact]
    public async Task PatchAllBulkAsync()
    {
        Log.SetLogLevel<DailyLogEventRepository>(LogLevel.Warning);
        const int COUNT = 1000;
        const int BATCH_SIZE = 250;
        int added = 0;
        do
        {
            await _dailyRepository.AddAsync(LogEventGenerator.GenerateLogs(BATCH_SIZE));
            added += BATCH_SIZE;
        } while (added < COUNT);
        await _client.Indices.RefreshAsync(_configuration.DailyLogEvents.Name, ct: TestCancellationToken);
        Log.SetLogLevel<DailyLogEventRepository>(LogLevel.Trace);

        Assert.Equal(COUNT, await _dailyRepository.IncrementValueAsync(Array.Empty<string>()));
    }

    [Fact]
    public async Task PatchAllBulkConcurrentlyAsync()
    {
        Log.SetLogLevel<DailyLogEventRepository>(LogLevel.Warning);
        const int COUNT = 1000;
        const int BATCH_SIZE = 250;
        int added = 0;
        do
        {
            await _dailyRepository.AddAsync(LogEventGenerator.GenerateLogs(BATCH_SIZE));
            added += BATCH_SIZE;
        } while (added < COUNT);
        await _client.Indices.RefreshAsync(_configuration.DailyLogEvents.Name, ct: TestCancellationToken);
        Log.SetLogLevel<DailyLogEventRepository>(LogLevel.Trace);

        var tasks = Enumerable.Range(1, 6).Select(async i =>
        {
            Assert.Equal(COUNT, await _dailyRepository.IncrementValueAsync(Array.Empty<string>(), i));
        });

        await Task.WhenAll(tasks);
        var events = await _dailyRepository.GetAllAsync();
        foreach (var ev in events.Documents)
            Assert.Equal(21, ev.Value);
    }

    [Fact]
    public async Task RemoveAsync()
    {
        var log = await _dailyRepository.AddAsync(LogEventGenerator.Default);
        Assert.NotNull(log);
        Assert.NotNull(log.Id);

        var disposables = new List<IDisposable>(2);
        var countdownEvent = new AsyncCountdownEvent(2);

        try
        {
            disposables.Add(_dailyRepository.DocumentsRemoving.AddSyncHandler((o, args) =>
            {
                Assert.Equal(log, args.Documents.First());
                countdownEvent.Signal();
            }));
            disposables.Add(_dailyRepository.DocumentsRemoved.AddSyncHandler((o, args) =>
            {
                Assert.Equal(log, args.Documents.First());
                countdownEvent.Signal();
            }));

            await _dailyRepository.RemoveAsync(log);

            await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
            Assert.Equal(0, countdownEvent.CurrentCount);
        }
        finally
        {
            foreach (var disposable in disposables)
                disposable.Dispose();

            disposables.Clear();
        }
    }

    [Fact]
    public async Task RemoveWithTimeSeriesAsync()
    {
        var log = LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString());
        await _dailyRepository.AddAsync(log, o => o.ImmediateConsistency());

        Assert.Equal(1, await _dailyRepository.CountAsync());

        await _dailyRepository.RemoveAsync(log, o => o.ImmediateConsistency());
        Assert.Equal(0, await _dailyRepository.CountAsync());
    }

    [Fact(Skip = "We need to look into how we want to handle this.")]
    public async Task RemoveWithOutOfSyncIndexAsync()
    {
        var utcNow = DateTime.UtcNow;
        var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)), o => o.ImmediateConsistency());
        Assert.NotNull(yesterdayLog);
        Assert.NotNull(yesterdayLog.Id);

        Assert.Equal(1, await _dailyRepository.CountAsync());

        await _dailyRepository.RemoveAsync(yesterdayLog, o => o.ImmediateConsistency());

        Assert.Equal(1, await _dailyRepository.CountAsync());
    }

    [Fact]
    public Task RemoveUnsavedDocumentAsync()
    {
        return _dailyRepository.RemoveAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: DateTime.UtcNow));
    }

    [Fact]
    public Task RemoveUnsavedDocumentsAsync()
    {
        return _dailyRepository.RemoveAsync(new List<LogEvent> {
            LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: DateTime.UtcNow),
            LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: DateTime.UtcNow)
        });
    }

    [Fact]
    public async Task RemoveByIdsWithCachingAsync()
    {
        var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency());
        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        await _identityRepository.RemoveAsync(identity.Id, o => o.ImmediateConsistency());
        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(1, _cache.Misses);

        identity = await _identityRepository.AddAsync(IdentityGenerator.Generate(), o => o.Cache().ImmediateConsistency());
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(1, _cache.Misses);

        await _identityRepository.RemoveAsync(identity.Id, o => o.ImmediateConsistency());
        Assert.Equal(0, _cache.Count);
        Assert.Equal(1, _cache.Hits);
        Assert.Equal(1, _cache.Misses);

        Assert.Equal(0, await _identityRepository.CountAsync());
    }

    [Fact]
    public async Task RemoveWithCachingAsync()
    {
        var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
        await _identityRepository.AddAsync(identities, o => o.Cache().ImmediateConsistency());
        Assert.Equal(2, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        await _identityRepository.RemoveAsync(identities.First(), o => o.ImmediateConsistency());
        Assert.Equal(1, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        await _identityRepository.RemoveAsync(identities, o => o.ImmediateConsistency());
        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        Assert.Equal(0, await _identityRepository.CountAsync());
    }

    [Fact]
    public async Task RemoveCollectionAsync()
    {
        var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
        await _identityRepository.AddAsync(identities, o => o.ImmediateConsistency());
        await _identityRepository.RemoveAsync(identities, o => o.ImmediateConsistency());

        Assert.Equal(0, await _identityRepository.CountAsync());
    }

    [Fact]
    public async Task RemoveCollectionWithTimeSeriesAsync()
    {
        var utcNow = DateTime.UtcNow;
        var yesterdayLog = LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1));
        var nowLog = LogEventGenerator.Default;

        var logs = new List<LogEvent> { yesterdayLog, nowLog };
        await _dailyRepository.AddAsync(logs, o => o.ImmediateConsistency());
        Assert.Equal(2, await _dailyRepository.CountAsync());

        await _dailyRepository.RemoveAsync(logs, o => o.ImmediateConsistency());
        Assert.Equal(0, await _dailyRepository.CountAsync());
    }

    [Fact]
    public async Task RemoveCollectionWithCachingAsync()
    {
        var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
        await _identityRepository.AddAsync(identities, o => o.Cache().ImmediateConsistency());
        Assert.Equal(2, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        await _identityRepository.RemoveAsync(identities, o => o.ImmediateConsistency());
        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        Assert.Equal(0, await _identityRepository.CountAsync());
    }

    [Fact(Skip = "We need to look into how we want to handle this.")]
    public async Task RemoveCollectionWithOutOfSyncIndexAsync()
    {
        var utcNow = DateTime.UtcNow;
        var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)), o => o.ImmediateConsistency());
        Assert.NotNull(yesterdayLog);
        Assert.NotNull(yesterdayLog.Id);

        Assert.Equal(1, await _dailyRepository.CountAsync());

        await _dailyRepository.RemoveAsync(new List<LogEvent> { yesterdayLog }, o => o.ImmediateConsistency());

        Assert.Equal(1, await _dailyRepository.CountAsync());
    }

    [Fact]
    public async Task RemoveAllAsync()
    {
        await _identityRepository.RemoveAllAsync();

        var identities = new List<Identity> { IdentityGenerator.Default };
        await _identityRepository.AddAsync(identities, o => o.ImmediateConsistency());

        var disposables = new List<IDisposable>(2);
        var countdownEvent = new AsyncCountdownEvent(2);

        try
        {
            disposables.Add(_identityRepository.DocumentsRemoving.AddSyncHandler((o, args) =>
            {
                countdownEvent.Signal();
            }));
            disposables.Add(_identityRepository.DocumentsRemoved.AddSyncHandler((o, args) =>
            {
                countdownEvent.Signal();
            }));

            await _identityRepository.RemoveAllAsync(o => o.ImmediateConsistency());
            await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
            Assert.Equal(0, countdownEvent.CurrentCount);
        }
        finally
        {
            foreach (var disposable in disposables)
                disposable.Dispose();

            disposables.Clear();
        }

        Assert.Equal(0, await _identityRepository.CountAsync());
    }

    [Fact]
    public async Task RemoveAllWithBatchingAsync()
    {
        const int COUNT = 1000;
        Log.SetLogLevel<IdentityRepository>(LogLevel.Information);
        await _identityRepository.AddAsync(IdentityGenerator.GenerateIdentities(COUNT), o => o.ImmediateConsistency());
        Log.SetLogLevel<IdentityRepository>(LogLevel.Trace);

        var disposables = new List<IDisposable>(2);
        var countdownEvent = new AsyncCountdownEvent(COUNT * 2);

        try
        {
            disposables.Add(_identityRepository.DocumentsRemoving.AddSyncHandler((o, args) =>
            {
                countdownEvent.Signal(args.Documents.Count);
            }));
            disposables.Add(_identityRepository.DocumentsRemoved.AddSyncHandler((o, args) =>
            {
                countdownEvent.Signal(args.Documents.Count);
            }));

            var sw = Stopwatch.StartNew();
            Assert.Equal(COUNT, await _identityRepository.RemoveAllAsync(o => o.ImmediateConsistency()));
            sw.Stop();
            _logger.LogInformation($"Deleted {COUNT} documents in {sw.ElapsedMilliseconds}ms");

            await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
            Assert.Equal(0, countdownEvent.CurrentCount);
            Assert.Equal(0, await _identityRepository.CountAsync());
        }
        finally
        {
            foreach (var disposable in disposables)
                disposable.Dispose();

            disposables.Clear();
        }
    }

    [Fact]
    public async Task RemoveAllWithDeleteByQueryAsync()
    {
        const int COUNT = 10000;
        Log.SetLogLevel<IdentityWithNoCachingRepository>(LogLevel.Information);
        await _identityRepositoryWithNoCaching.AddAsync(IdentityGenerator.GenerateIdentities(COUNT), o => o.ImmediateConsistency());
        Log.SetLogLevel<IdentityWithNoCachingRepository>(LogLevel.Trace);

        var disposables = new List<IDisposable>(2);
        var countdownEvent = new AsyncCountdownEvent(20);

        try
        {
            disposables.Add(_identityRepositoryWithNoCaching.DocumentsRemoving.AddSyncHandler((o, args) =>
            {
                countdownEvent.Signal();
            }));
            disposables.Add(_identityRepositoryWithNoCaching.DocumentsRemoved.AddSyncHandler((o, args) =>
            {
                countdownEvent.Signal();
            }));

            var sw = Stopwatch.StartNew();
            Assert.Equal(COUNT, await _identityRepositoryWithNoCaching.RemoveAllAsync(o => o.ImmediateConsistency(true)));
            sw.Stop();
            _logger.LogInformation($"Deleted {COUNT} documents in {sw.ElapsedMilliseconds}ms");

            await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);
            Assert.Equal(0, countdownEvent.CurrentCount);

            Assert.Equal(0, await _identityRepositoryWithNoCaching.CountAsync());
        }
        finally
        {
            foreach (var disposable in disposables)
                disposable.Dispose();

            disposables.Clear();
        }
    }

    [Fact]
    public async Task RemoveAllWithCachingAsync()
    {
        var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
        await _identityRepository.AddAsync(identities, o => o.Cache().ImmediateConsistency());
        Assert.Equal(2, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        await _identityRepository.RemoveAllAsync(o => o.ImmediateConsistency());
        Assert.Equal(0, _cache.Count);
        Assert.Equal(0, _cache.Hits);
        Assert.Equal(0, _cache.Misses);

        Assert.Equal(0, await _identityRepository.CountAsync());
    }

    [Fact]
    public async Task RemoveAllWithTimeSeriesAsync()
    {
        var utcNow = DateTime.UtcNow;
        var yesterdayLog = LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1));
        var nowLog = LogEventGenerator.Default;

        var logs = new List<LogEvent> { yesterdayLog, nowLog };
        await _dailyRepository.AddAsync(logs, o => o.ImmediateConsistency());
        Assert.Equal(2, await _dailyRepository.CountAsync());

        await _dailyRepository.RemoveAllAsync(o => o.ImmediateConsistency());
        Assert.Equal(0, await _dailyRepository.CountAsync());
    }
}
