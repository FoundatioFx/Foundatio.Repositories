using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.JsonPatch;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Nest;
using Foundatio.AsyncEx;
using Xunit;
using Xunit.Abstractions;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class RepositoryTests : ElasticRepositoryTestBase {
        private readonly EmployeeRepository _employeeRepository;
        private readonly DailyLogEventRepository _dailyRepository;
        private readonly DailyLogEventRepository _dailyRepositoryWithNoCaching;
        private readonly IdentityRepository _identityRepository;
        private readonly IdentityRepository _identityRepositoryWithNoCaching;

        public RepositoryTests(ITestOutputHelper output) : base(output) {
            _dailyRepository = new DailyLogEventRepository(_configuration);
            _dailyRepositoryWithNoCaching = new DailyLogEventWithNoCachingRepository(_configuration);
            _employeeRepository = new EmployeeRepository(_configuration);
            _identityRepository = new IdentityRepository(_configuration);
            _identityRepositoryWithNoCaching = new IdentityWithNoCachingRepository(_configuration);

            RemoveDataAsync().GetAwaiter().GetResult();
        }

        [Fact]
        public async Task AddAsync() {
            var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Generate());
            Assert.NotNull(identity1?.Id);

            var disposables = new List<IDisposable>(2);
            var countdownEvent = new AsyncCountdownEvent(2);

            try {
                var identity2 = IdentityGenerator.Default;
                disposables.Add(_identityRepository.DocumentsAdding.AddSyncHandler((o, args) => {
                    Assert.Equal(identity2, args.Documents.First());
                    countdownEvent.Signal();
                }));

                disposables.Add(_identityRepository.DocumentsAdded.AddSyncHandler((o, args) => {
                    Assert.Equal(identity2, args.Documents.First());
                    countdownEvent.Signal();
                }));

                var result = await _identityRepository.AddAsync(identity2);
                Assert.Equal(IdentityGenerator.Default.Id, result.Id);

                await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
                Assert.Equal(0, countdownEvent.CurrentCount);
            } finally {
                foreach (var disposable in disposables)
                    disposable.Dispose();

                disposables.Clear();
            }
        }

        [Fact]
        public async Task CanQueryByDeleted() {
            var employee1 = EmployeeGenerator.Default;
            employee1.IsDeleted = true;
            employee1 = await _employeeRepository.AddAsync(employee1, o => o.ImmediateConsistency());
            Assert.NotNull(employee1?.Id);

            await _employeeRepository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());

            var allEmployees = await _employeeRepository.GetByQueryAsync(q => q.SoftDeleteMode(SoftDeleteQueryMode.All));
            Assert.Equal(2, allEmployees.Total);

            var onlyDeleted = await _employeeRepository.GetByQueryAsync(q => q.SoftDeleteMode(SoftDeleteQueryMode.DeletedOnly));
            Assert.Equal(1, onlyDeleted.Total);
            Assert.Equal(employee1.Id, onlyDeleted.Documents.First().Id);

            var nonDeletedEmployees = await _employeeRepository.GetByQueryAsync(q => q.SoftDeleteMode(SoftDeleteQueryMode.ActiveOnly));
            Assert.Equal(1, nonDeletedEmployees.Total);
            Assert.NotEqual(employee1.Id, nonDeletedEmployees.Documents.First().Id);
        }

        [Fact]
        public async Task CanQueryByDeletedSearch() {
            var employee1 = EmployeeGenerator.Default;
            employee1.IsDeleted = true;
            employee1 = await _employeeRepository.AddAsync(employee1, o => o.ImmediateConsistency());
            Assert.NotNull(employee1?.Id);

            await _employeeRepository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());

            var allEmployees = await _employeeRepository.SearchAsync(new RepositoryQuery().SoftDeleteMode(SoftDeleteQueryMode.All));
            Assert.Equal(2, allEmployees.Total);

            var onlyDeleted = await _employeeRepository.SearchAsync(new RepositoryQuery().SoftDeleteMode(SoftDeleteQueryMode.All), "isDeleted:true");
            Assert.Equal(1, onlyDeleted.Total);
            Assert.Equal(employee1.Id, onlyDeleted.Documents.First().Id);

            var nonDeletedEmployees = await _employeeRepository.SearchAsync(null);
            Assert.Equal(1, nonDeletedEmployees.Total);
            Assert.NotEqual(employee1.Id, nonDeletedEmployees.Documents.First().Id);
        }

        [Fact]
        public async Task AddDuplicateAsync() {
            var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency());
            Assert.NotNull(identity1?.Id);

            await Assert.ThrowsAsync<DuplicateDocumentException>(async () => await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency()));
            Assert.Equal(1, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task AddDuplicateCollectionAsync() {
            var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.ImmediateConsistency());
            Assert.NotNull(identity1?.Id);

            var identities = new List<Identity> {
                IdentityGenerator.Default,
                IdentityGenerator.Generate()
            };

            await Assert.ThrowsAsync<DuplicateDocumentException>(async () => await _identityRepository.AddAsync(identities, o => o.ImmediateConsistency()));
            Assert.Equal(2, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task AddWithCachingAsync() {
            var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.Cache());
            Assert.NotNull(identity?.Id);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache()));
            Assert.Equal(1, _cache.Count);
            Assert.Equal(1, _cache.Hits);
            Assert.Equal(0, _cache.Misses);
        }

        [Fact]
        public async Task AddWithTimeSeriesAsync() {
            var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate());
            Assert.NotNull(log?.Id);

            Assert.Equal(log, await _dailyRepository.GetByIdAsync(log.Id));
        }

        [Fact]
        public async Task AddCollectionAsync() {
            var identity = IdentityGenerator.Generate();
            await _identityRepository.AddAsync(new List<Identity> { identity });
            Assert.NotNull(identity.Id);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id));
        }

        [Fact]
        public async Task AddCollectionWithTimeSeriesAsync() {
            var utcNow = SystemClock.UtcNow;
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
        public async Task AddCollectionWithCachingAsync() {
            var identity = IdentityGenerator.Generate();
            await _identityRepository.AddAsync(new List<Identity> { identity, IdentityGenerator.Generate() }, o => o.Cache());
            Assert.NotNull(identity?.Id);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache()));
            Assert.Equal(2, _cache.Count);
            Assert.Equal(1, _cache.Hits);
            Assert.Equal(0, _cache.Misses);
        }

        [Fact]
        public async Task SaveAsync() {
            var log = await _dailyRepository.AddAsync(LogEventGenerator.Default, o => o.Notifications(false));
            Assert.NotNull(log?.Id);

            var disposables = new List<IDisposable>();
            var countdownEvent = new AsyncCountdownEvent(5);

            try {
                disposables.Add(_dailyRepository.DocumentsChanging.AddSyncHandler((o, args) => {
                    Assert.Equal(log, args.Documents.First().Value);
                    countdownEvent.Signal();
                }));
                disposables.Add(_dailyRepository.DocumentsChanged.AddSyncHandler((o, args) => {
                    Assert.Equal(log, args.Documents.First().Value);
                    countdownEvent.Signal();
                }));
                disposables.Add(_dailyRepository.DocumentsSaving.AddSyncHandler((o, args) => {
                    Assert.Equal(log, args.Documents.First().Value);
                    countdownEvent.Signal();
                }));
                disposables.Add(_dailyRepository.DocumentsSaved.AddSyncHandler((o, args) => {
                    Assert.Equal(log, args.Documents.First().Value);
                    countdownEvent.Signal();
                }));
                await _messageBus.SubscribeAsync<EntityChanged>((msg, ct) => {
                    Assert.Equal(nameof(LogEvent), msg.Type);
                    Assert.Equal(log.Id, msg.Id);
                    Assert.Equal(ChangeType.Saved, msg.ChangeType);
                    countdownEvent.Signal();
                    return Task.CompletedTask;
                });

                log.CompanyId = ObjectId.GenerateNewId().ToString();
                var result = await _dailyRepository.SaveAsync(log);
                Assert.Equal(log.CompanyId, result.CompanyId);

                await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromSeconds(2)).Token);
                Assert.Equal(0, countdownEvent.CurrentCount);
            } finally {
                foreach (var disposable in disposables)
                    disposable.Dispose();

                disposables.Clear();
            }
        }

        [Fact]
        public async Task AddAndSaveAsync() {
            var logEntry = await _dailyRepository.AddAsync(LogEventGenerator.Default, o => o.Notifications(false));
            Assert.NotNull(logEntry?.Id);

            var disposables = new List<IDisposable>(4);
            var saveCountdownEvent = new AsyncCountdownEvent(2);
            var notificationCountdownEvent = new AsyncCountdownEvent(2);
            // Save requires an id to be set.
            var addedLog = LogEventGenerator.Generate(id: ObjectId.GenerateNewId().ToString());
            try {
                disposables.Add(_dailyRepository.DocumentsSaving.AddSyncHandler((o, args) => {
                    Assert.Equal(logEntry, args.Documents.First().Value);
                    saveCountdownEvent.Signal();
                }));
                disposables.Add(_dailyRepository.DocumentsSaved.AddSyncHandler((o, args) => {
                    Assert.Equal(logEntry, args.Documents.First().Value);
                    saveCountdownEvent.Signal();
                }));
                await _messageBus.SubscribeAsync<EntityChanged>((msg, ct) => {
                    Assert.Equal(nameof(LogEvent), msg.Type);
                    Assert.True(msg.Id == logEntry.Id || msg.Id == addedLog.Id);
                    Assert.Equal(ChangeType.Saved, msg.ChangeType);
                    notificationCountdownEvent.Signal();
                    return Task.CompletedTask;
                });

                logEntry.CompanyId = ObjectId.GenerateNewId().ToString();
                await _dailyRepository.SaveAsync(new List<LogEvent> { logEntry, addedLog });

                await notificationCountdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromSeconds(2)).Token);
                Assert.Equal(0, notificationCountdownEvent.CurrentCount);
                Assert.Equal(0, saveCountdownEvent.CurrentCount);
            } finally {
                foreach (var disposable in disposables)
                    disposable.Dispose();

                disposables.Clear();
            }
        }

        [Fact]
        public async Task SaveWithOriginalFromOptions() {
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
        public async Task AddAndSaveWithCacheAsync() {
            var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.Cache());
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            string cacheKey = _cache.Keys.Single();
            var cacheValue = await _cache.GetAsync<Identity>(cacheKey);
            Assert.True(cacheValue.HasValue);
            Assert.Equal(identity, cacheValue.Value);

            identity = await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache());
            Assert.NotNull(identity);
            Assert.Equal(2, _cache.Hits);

            cacheValue = await _cache.GetAsync<Identity>(cacheKey);
            Assert.True(cacheValue.HasValue);
            Assert.Equal(identity, cacheValue.Value);

            await _identityRepository.InvalidateCacheAsync(identity);
            Assert.Equal(0, _cache.Count);
            Assert.Equal(3, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            var result = await _identityRepository.SaveAsync(identity, o => o.Cache());
            Assert.NotNull(result);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(3, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            cacheValue = await _cache.GetAsync<Identity>(cacheKey);
            Assert.True(cacheValue.HasValue);
            Assert.Equal(identity, cacheValue.Value);
        }

        [Fact]
        public async Task SaveWithNoIdentityAsync() {
            var identity = IdentityGenerator.Generate();
            await Assert.ThrowsAsync<ApplicationException>(async () => await _identityRepository.SaveAsync(new List<Identity> { identity }, o => o.Cache()));
        }

        [Fact]
        public async Task SaveWithOutOfSyncIndexAsync() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)), o => o.ImmediateConsistency());
            Assert.NotNull(yesterdayLog?.Id);

            Assert.Equal(1, await _dailyRepository.CountAsync());

            yesterdayLog.Message = "updated";
            await _dailyRepository.SaveAsync(yesterdayLog, o => o.ImmediateConsistency());

            Assert.Equal(1, await _dailyRepository.CountAsync());
        }

        [Fact]
        public async Task CanGetAggregationsAsync() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)), o => o.ImmediateConsistency());
            Assert.NotNull(yesterdayLog?.Id);

            var result = await _dailyRepository.CountBySearchAsync(null, aggregations: "cardinality:companyId max:createdUtc");
            Assert.Equal(2, result.Aggregations.Count);
            var cardinalityAgg = result.Aggregations.Cardinality("cardinality_companyId");
            Assert.NotNull(cardinalityAgg);
            Assert.Equal(1, cardinalityAgg.Value.GetValueOrDefault());

            var maxAgg = result.Aggregations.Max<DateTime>("max_createdUtc");
            Assert.NotNull(maxAgg);
            Assert.True(yesterdayLog.CreatedUtc.Subtract(maxAgg.Value).TotalSeconds < 1);
        }

        [Fact]
        public async Task CanGetDateAggregationAsync() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)), o => o.ImmediateConsistency());
            Assert.NotNull(yesterdayLog?.Id);

            var result = await _dailyRepository.CountBySearchAsync(null, aggregations: "date:(createdUtc min:createdUtc)");
            Assert.Equal(1, result.Aggregations.Count);
            var dateAgg = result.Aggregations.DateHistogram("date_createdUtc");
            Assert.NotNull(dateAgg);
            Assert.Equal(1, dateAgg.Buckets.Count);
            Assert.Equal(utcNow.AddDays(-1).Date, dateAgg.Buckets.First().Date);
            Assert.Equal(utcNow.AddDays(-1).Floor(TimeSpan.FromMilliseconds(1)), dateAgg.Buckets.First().Aggregations.Min<DateTime>("min_createdUtc").Value.Floor(TimeSpan.FromMilliseconds(1)));

            result = await _dailyRepository.CountBySearchAsync(null, aggregations: "date:(createdUtc~1h^-3h min:createdUtc)");
            Assert.Equal(1, result.Aggregations.Count);
            dateAgg = result.Aggregations.DateHistogram("date_createdUtc");
            Assert.NotNull(dateAgg);
            Assert.Equal(1, dateAgg.Buckets.Count);
        }

        [Fact]
        public async Task CanGetGeoGridAggregationAsync() {
            var utcNow = SystemClock.UtcNow;
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)), o => o.ImmediateConsistency());
            Assert.NotNull(employee?.Id);
            await _employeeRepository.AddAsync(EmployeeGenerator.GenerateEmployees(), o => o.ImmediateConsistency());

            var result = await _employeeRepository.CountBySearchAsync(null, aggregations: "geogrid:(location~6 max:age)");
            Assert.Equal(1, result.Aggregations.Count);
            var geoAgg = result.Aggregations.GeoHash("geogrid_location");
            Assert.NotNull(geoAgg);
            Assert.InRange(geoAgg.Buckets.Count, 1, 11);
        }

        [Fact]
        public async Task SaveWithCachingAsync() {
            var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, o => o.Cache());
            Assert.NotNull(identity?.Id);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _identityRepository.InvalidateCacheAsync(identity);
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            identity = await _identityRepository.SaveAsync(identity, o => o.Cache());
            Assert.NotNull(identity?.Id);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, o => o.Cache()));
            Assert.Equal(1, _cache.Count);
            Assert.Equal(1, _cache.Hits);
            Assert.Equal(0, _cache.Misses);
        }

        [Fact]
        public async Task SaveCollectionAsync() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate(ObjectId.GenerateNewId().ToString()) };
            await _identityRepository.SaveAsync(identities);

            var results = await _identityRepository.GetByIdsAsync(identities.Select(i => i.Id).ToList());
            Assert.Equal(2, results.Count);
        }

        [Fact]
        public async Task SaveCollectionWithTimeSeriesAsync() {
            var utcNow = SystemClock.UtcNow;
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
        public async Task SaveCollectionWithCachingAsync() {
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
        public async Task SetCreatedAndModifiedTimesAsync() {
            using (TestSystemClock.Install()) {
                SystemClock.Test.SubtractTime(TimeSpan.FromMilliseconds(100));
                var nowUtc = SystemClock.UtcNow;
                var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
                Assert.True(employee.CreatedUtc >= nowUtc);
                Assert.True(employee.UpdatedUtc >= nowUtc);

                var createdUtc = employee.CreatedUtc;
                var updatedUtc = employee.UpdatedUtc;

                employee.Name = Guid.NewGuid().ToString();
                SystemClock.Test.AddTime(TimeSpan.FromMilliseconds(100));
                employee = await _employeeRepository.SaveAsync(employee);
                Assert.Equal(createdUtc, employee.CreatedUtc);
                Assert.True(updatedUtc < employee.UpdatedUtc, $"Previous UpdatedUtc: {updatedUtc} Current UpdatedUtc: {employee.UpdatedUtc}");
            }
        }

        [Fact]
        public async Task CannotSetFutureCreatedAndModifiedTimesAsync() {
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
        public async Task JsonPatchAsync() {
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
            var patch = new PatchDocument(new ReplaceOperation { Path = "name", Value = "Patched" });
            await _employeeRepository.PatchAsync(employee.Id, new Models.JsonPatch(patch));

            employee = await _employeeRepository.GetByIdAsync(employee.Id);
            Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
            Assert.Equal("Patched", employee.Name);
            Assert.Equal(2, employee.Version);
        }

        [Fact]
        public async Task PartialPatchAsync() {
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
            await _employeeRepository.PatchAsync(employee.Id, new PartialPatch(new { name = "Patched" }));

            employee = await _employeeRepository.GetByIdAsync(employee.Id);
            Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
            Assert.Equal("Patched", employee.Name);
            Assert.Equal(2, employee.Version);
        }

        [Fact]
        public async Task ScriptPatchAsync() {
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
            await _employeeRepository.PatchAsync(employee.Id, new ScriptPatch("ctx._source.name = 'Patched';"));

            employee = await _employeeRepository.GetByIdAsync(employee.Id);
            Assert.Equal(EmployeeGenerator.Default.Age, employee.Age);
            Assert.Equal("Patched", employee.Name);
            Assert.Equal(2, employee.Version);
        }

        [Fact]
        public async Task ScriptPatchAllAsync() {
            var utcNow = SystemClock.UtcNow;
            var logs = new List<LogEvent> {
                LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1), companyId: "1"),
                LogEventGenerator.Generate(createdUtc: utcNow, companyId: "1"),
                LogEventGenerator.Generate(createdUtc: utcNow, companyId: "2"),
            };

            await _dailyRepository.AddAsync(logs, o => o.Cache().ImmediateConsistency());
            Assert.Equal(5, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            Assert.Equal(3, await _dailyRepository.IncrementValueAsync(logs.Select(l => l.Id).ToArray()));
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            var results = await _dailyRepository.GetAllByCompanyAsync("1");
            Assert.Equal(2, results.Documents.Count);
            foreach (var document in results.Documents) {
                Assert.Equal("1", document.CompanyId);
                Assert.Equal(1, document.Value);
            }

            await _dailyRepository.SaveAsync(logs, o => o.Cache().ImmediateConsistency());

            results = await _dailyRepository.GetAllByCompanyAsync("1");
            Assert.Equal(2, results.Documents.Count);
            foreach (var document in results.Documents) {
                Assert.Equal("1", document.CompanyId);
                Assert.Equal(0, document.Value);
            }
        }

        [Fact]
        public async Task ScriptPatchAllWithNoCacheAsync() {
            var utcNow = SystemClock.UtcNow;
            var logs = new List<LogEvent> {
                LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1), companyId: "1"),
                LogEventGenerator.Generate(createdUtc: utcNow, companyId: "1"),
                LogEventGenerator.Generate(createdUtc: utcNow, companyId: "2"),
            };

            await _dailyRepositoryWithNoCaching.AddAsync(logs, o => o.ImmediateConsistency());
            Assert.Equal(3, await _dailyRepositoryWithNoCaching.IncrementValueAsync(q => q.Id(logs.Select(l => l.Id).ToArray())));

            var results = await _dailyRepositoryWithNoCaching.GetAllByCompanyAsync("1");
            Assert.Equal(2, results.Documents.Count);
            foreach (var document in results.Documents) {
                Assert.Equal("1", document.CompanyId);
                Assert.Equal(1, document.Value);
            }

            await _dailyRepositoryWithNoCaching.SaveAsync(logs, o => o.ImmediateConsistency());

            results = await _dailyRepositoryWithNoCaching.GetAllByCompanyAsync("1");
            Assert.Equal(2, results.Documents.Count);
            foreach (var document in results.Documents) {
                Assert.Equal("1", document.CompanyId);
                Assert.Equal(0, document.Value);
            }
        }

        [Fact]
        public async Task PatchAllBulkAsync() {
            Log.SetLogLevel<DailyLogEventRepository>(LogLevel.Warning);
            const int COUNT = 1000 * 10;
            int added = 0;
            do {
                await _dailyRepository.AddAsync(LogEventGenerator.GenerateLogs(1000), o => o.ImmediateConsistency(true));
                added += 1000;
            } while (added < COUNT);
            Log.SetLogLevel<DailyLogEventRepository>(LogLevel.Trace);

            Assert.Equal(COUNT, await _dailyRepository.IncrementValueAsync(new string[0]));
        }

        [Fact]
        public async Task PatchAllBulkConcurrentlyAsync() {
            Log.SetLogLevel<DailyLogEventRepository>(LogLevel.Warning);
            const int COUNT = 1000 * 10;
            int added = 0;
            do {
                await _dailyRepository.AddAsync(LogEventGenerator.GenerateLogs(1000));
                added += 1000;
            } while (added < COUNT);
            await _client.RefreshAsync(Indices.All);
            Log.SetLogLevel<DailyLogEventRepository>(LogLevel.Trace);

            var tasks = Enumerable.Range(1, 6).Select(async i => {
                Assert.Equal(COUNT, await _dailyRepository.IncrementValueAsync(new string[0], i));
            });

            await Task.WhenAll(tasks);
            var events = await _dailyRepository.GetAllAsync();
            foreach (var ev in events.Documents)
                Assert.Equal(21, ev.Value);
        }

        [Fact]
        public async Task RemoveAsync() {
            var log = await _dailyRepository.AddAsync(LogEventGenerator.Default);
            Assert.NotNull(log?.Id);

            var disposables = new List<IDisposable>(2);
            var countdownEvent = new AsyncCountdownEvent(2);

            try {
                disposables.Add(_dailyRepository.DocumentsRemoving.AddSyncHandler((o, args) => {
                    Assert.Equal(log, args.Documents.First());
                    countdownEvent.Signal();
                }));
                disposables.Add(_dailyRepository.DocumentsRemoved.AddSyncHandler((o, args) => {
                    Assert.Equal(log, args.Documents.First());
                    countdownEvent.Signal();
                }));

                await _dailyRepository.RemoveAsync(log);

                await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
                Assert.Equal(0, countdownEvent.CurrentCount);

            } finally {
                foreach (var disposable in disposables)
                    disposable.Dispose();

                disposables.Clear();
            }
        }

        [Fact]
        public async Task RemoveWithTimeSeriesAsync() {
            var log = LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString());
            await _dailyRepository.AddAsync(log, o => o.ImmediateConsistency());

            Assert.Equal(1, await _dailyRepository.CountAsync());

            await _dailyRepository.RemoveAsync(log, o => o.ImmediateConsistency());
            Assert.Equal(0, await _dailyRepository.CountAsync());
        }

        [Fact(Skip = "We need to look into how we want to handle this.")]
        public async Task RemoveWithOutOfSyncIndexAsync() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)), o => o.ImmediateConsistency());
            Assert.NotNull(yesterdayLog?.Id);

            Assert.Equal(1, await _dailyRepository.CountAsync());

            await _dailyRepository.RemoveAsync(yesterdayLog, o => o.ImmediateConsistency());

            Assert.Equal(1, await _dailyRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveUnsavedDocumentAsync() {
            await _dailyRepository.RemoveAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: SystemClock.UtcNow));
        }

        [Fact]
        public async Task RemoveUnsavedDocumentsAsync() {
            await _dailyRepository.RemoveAsync(new List<LogEvent> {
                LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: SystemClock.UtcNow),
                LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: SystemClock.UtcNow)
            });
        }

        [Fact]
        public async Task RemoveByIdsWithCachingAsync() {
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
        public async Task RemoveWithCachingAsync() {
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
        public async Task RemoveCollectionAsync() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
            await _identityRepository.AddAsync(identities, o => o.ImmediateConsistency());
            await _identityRepository.RemoveAsync(identities, o => o.ImmediateConsistency());

            Assert.Equal(0, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveCollectionWithTimeSeriesAsync() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1));
            var nowLog = LogEventGenerator.Default;

            var logs = new List<LogEvent> { yesterdayLog, nowLog };
            await _dailyRepository.AddAsync(logs, o => o.ImmediateConsistency());
            Assert.Equal(2, await _dailyRepository.CountAsync());

            await _dailyRepository.RemoveAsync(logs, o => o.ImmediateConsistency());
            Assert.Equal(0, await _dailyRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveCollectionWithCachingAsync() {
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
        public async Task RemoveCollectionWithOutOfSyncIndexAsync() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)), o => o.ImmediateConsistency());
            Assert.NotNull(yesterdayLog?.Id);

            Assert.Equal(1, await _dailyRepository.CountAsync());

            await _dailyRepository.RemoveAsync(new List<LogEvent> { yesterdayLog }, o => o.ImmediateConsistency());

            Assert.Equal(1, await _dailyRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveAllAsync() {
            await _identityRepository.RemoveAllAsync();

            var identities = new List<Identity> { IdentityGenerator.Default };
            await _identityRepository.AddAsync(identities, o => o.ImmediateConsistency());

            var disposables = new List<IDisposable>(2);
            var countdownEvent = new AsyncCountdownEvent(2);

            try {
                disposables.Add(_identityRepository.DocumentsRemoving.AddSyncHandler((o, args) => {
                    countdownEvent.Signal();
                }));
                disposables.Add(_identityRepository.DocumentsRemoved.AddSyncHandler((o, args) => {
                    countdownEvent.Signal();
                }));

                await _identityRepository.RemoveAllAsync(o => o.ImmediateConsistency());
                await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
                Assert.Equal(0, countdownEvent.CurrentCount);

            } finally {
                foreach (var disposable in disposables)
                    disposable.Dispose();

                disposables.Clear();
            }

            Assert.Equal(0, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveAllWithBatchingAsync() {
            const int COUNT = 1000;
            Log.SetLogLevel<IdentityRepository>(LogLevel.Information);
            await _identityRepository.AddAsync(IdentityGenerator.GenerateIdentities(COUNT), o => o.ImmediateConsistency());
            Log.SetLogLevel<IdentityRepository>(LogLevel.Trace);

            var disposables = new List<IDisposable>(2);
            var countdownEvent = new AsyncCountdownEvent(COUNT * 2);

            try {
                disposables.Add(_identityRepository.DocumentsRemoving.AddSyncHandler((o, args) => {
                    countdownEvent.Signal(args.Documents.Count);
                }));
                disposables.Add(_identityRepository.DocumentsRemoved.AddSyncHandler((o, args) => {
                    countdownEvent.Signal(args.Documents.Count);
                }));

                var sw = Stopwatch.StartNew();
                Assert.Equal(COUNT, await _identityRepository.RemoveAllAsync(o => o.ImmediateConsistency()));
                sw.Stop();
                _logger.LogInformation($"Deleted {COUNT} documents in {sw.ElapsedMilliseconds}ms");

                await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
                Assert.Equal(0, countdownEvent.CurrentCount);
                Assert.Equal(0, await _identityRepository.CountAsync());
            } finally {
                foreach (var disposable in disposables)
                    disposable.Dispose();

                disposables.Clear();
            }
        }

        [Fact]
        public async Task RemoveAllWithDeleteByQueryAsync() {
            const int COUNT = 10000;
            Log.SetLogLevel<IdentityWithNoCachingRepository>(LogLevel.Information);
            await _identityRepositoryWithNoCaching.AddAsync(IdentityGenerator.GenerateIdentities(COUNT), o => o.ImmediateConsistency());
            Log.SetLogLevel<IdentityWithNoCachingRepository>(LogLevel.Trace);

            var disposables = new List<IDisposable>(2);
            var countdownEvent = new AsyncCountdownEvent(1);

            try {
                disposables.Add(_identityRepositoryWithNoCaching.DocumentsRemoving.AddSyncHandler((o, args) => {
                    countdownEvent.Signal();
                }));
                disposables.Add(_identityRepositoryWithNoCaching.DocumentsRemoved.AddSyncHandler((o, args) => {
                    countdownEvent.Signal();
                }));

                var sw = Stopwatch.StartNew();
                Assert.Equal(COUNT, await _identityRepositoryWithNoCaching.RemoveAllAsync(o => o.ImmediateConsistency(true)));
                sw.Stop();
                _logger.LogInformation($"Deleted {COUNT} documents in {sw.ElapsedMilliseconds}ms");

                await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
                Assert.Equal(0, countdownEvent.CurrentCount);

                Assert.Equal(0, await _identityRepositoryWithNoCaching.CountAsync());
            } finally {
                foreach (var disposable in disposables)
                    disposable.Dispose();

                disposables.Clear();
            }
        }

        [Fact]
        public async Task RemoveAllWithCachingAsync() {
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
        public async Task RemoveAllWithTimeSeriesAsync() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1));
            var nowLog = LogEventGenerator.Default;

            var logs = new List<LogEvent> { yesterdayLog, nowLog };
            await _dailyRepository.AddAsync(logs, o => o.ImmediateConsistency());
            Assert.Equal(2, await _dailyRepository.CountAsync());

            await _dailyRepository.RemoveAllAsync(o => o.ImmediateConsistency());
            Assert.Equal(0, await _dailyRepository.CountAsync());
        }
    }
}