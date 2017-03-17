using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentValidation;
using Nito.AsyncEx;
using Xunit;
using Xunit.Abstractions;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Validators;
using Foundatio.Repositories.JsonPatch;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class RepositoryTests : ElasticRepositoryTestBase {
        private readonly EmployeeRepository _employeeRepository;
        private readonly DailyLogEventRepository _dailyRepository;
        private readonly IdentityRepository _identityRepository;
        private readonly IdentityRepository _identityRepositoryWithNoCaching;

        public RepositoryTests(ITestOutputHelper output) : base(output) {
            _dailyRepository = new DailyLogEventRepository(_configuration);
            _employeeRepository = new EmployeeRepository(_configuration);
            _identityRepository = new IdentityRepository(_configuration);
            _identityRepositoryWithNoCaching = new IdentityWithNoCachingRepository(_configuration);

            RemoveDataAsync().GetAwaiter().GetResult();
        }

        [Fact]
        public async Task Add() {
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
            employee1 = await _employeeRepository.AddAsync(employee1);
            Assert.NotNull(employee1?.Id);

            await _employeeRepository.AddAsync(EmployeeGenerator.Generate());

            await _client.RefreshAsync();
            var allEmployees = await _employeeRepository.QueryAsync(new MyAppQuery().IncludeDeleted());
            Assert.Equal(2, allEmployees.Total);

            var onlyDeleted = await _employeeRepository.QueryAsync(new MyAppQuery().IncludeOnlyDeleted());
            Assert.Equal(1, onlyDeleted.Total);
            Assert.Equal(employee1.Id, onlyDeleted.Documents.First().Id);

            var nonDeletedEmployees = await _employeeRepository.QueryAsync(new MyAppQuery().IncludeDeleted(false));
            Assert.Equal(1, nonDeletedEmployees.Total);
            Assert.NotEqual(employee1.Id, nonDeletedEmployees.Documents.First().Id);
        }

        [Fact]
        public async Task CanQueryByDeletedSearch() {
            var employee1 = EmployeeGenerator.Default;
            employee1.IsDeleted = true;
            employee1 = await _employeeRepository.AddAsync(employee1);
            Assert.NotNull(employee1?.Id);

            await _employeeRepository.AddAsync(EmployeeGenerator.Generate());

            await _client.RefreshAsync();
            var allEmployees = await _employeeRepository.SearchAsync(new MyAppQuery().IncludeDeleted());
            Assert.Equal(2, allEmployees.Total);

            var onlyDeleted = await _employeeRepository.SearchAsync(new MyAppQuery().IncludeDeleted(), "deleted:true");
            Assert.Equal(1, onlyDeleted.Total);
            Assert.Equal(employee1.Id, onlyDeleted.Documents.First().Id);

            var nonDeletedEmployees = await _employeeRepository.SearchAsync(new MyAppQuery());
            Assert.Equal(1, nonDeletedEmployees.Total);
            Assert.NotEqual(employee1.Id, nonDeletedEmployees.Documents.First().Id);
        }

        [Fact]
        public async Task AddDuplicate() {
            var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default);
            Assert.NotNull(identity1?.Id);

            var identity2 = await _identityRepository.AddAsync(IdentityGenerator.Default);
            Assert.NotNull(identity2?.Id);

            Assert.Equal(identity1, identity2);

            await _client.RefreshAsync();
            Assert.Equal(1, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task AddWithCaching() {
            var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, addToCache: true);
            Assert.NotNull(identity?.Id);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, useCache: true));
            Assert.Equal(1, _cache.Count);
            Assert.Equal(1, _cache.Hits);
            Assert.Equal(0, _cache.Misses);
        }

        [Fact]
        public async Task AddWithTimeSeries() {
            var log = await _dailyRepository.AddAsync(LogEventGenerator.Generate());
            Assert.NotNull(log?.Id);

            Assert.Equal(log, await _dailyRepository.GetByIdAsync(log.Id));
        }

        [Fact]
        public async Task AddCollection() {
            var identity = IdentityGenerator.Generate();
            await _identityRepository.AddAsync(new List<Identity> { identity });
            Assert.NotNull(identity.Id);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id));
        }

        [Fact]
        public async Task AddCollectionWithTimeSeries() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1));
            var nowLog = LogEventGenerator.Default;

            var logs = new List<LogEvent> { yesterdayLog, nowLog };
            await _dailyRepository.AddAsync(logs);

            var results = await _dailyRepository.GetByIdsAsync(new List<string> { yesterdayLog.Id, nowLog.Id });
            Assert.Equal(logs, results.OrderBy(d => d.CreatedUtc).ToList());

            await _client.RefreshAsync();
            var getAllResults = await _dailyRepository.GetAllAsync();
            Assert.Equal(logs, getAllResults.Documents.OrderBy(d => d.CreatedUtc).ToList());
        }

        [Fact]
        public async Task AddCollectionWithCaching() {
            var identity = IdentityGenerator.Generate();
            await _identityRepository.AddAsync(new List<Identity> { identity, IdentityGenerator.Generate() }, addToCache: true);
            Assert.NotNull(identity?.Id);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, useCache: true));
            Assert.Equal(2, _cache.Count);
            Assert.Equal(1, _cache.Hits);
            Assert.Equal(0, _cache.Misses);
        }

        [Fact]
        public async Task Save() {
            var log = await _dailyRepository.AddAsync(LogEventGenerator.Default, sendNotification: false);
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
                await _messgeBus.SubscribeAsync<EntityChanged>((msg, ct) => {
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
        public async Task AddAndSave() {
            var log = await _dailyRepository.AddAsync(LogEventGenerator.Default, sendNotification: false);
            Assert.NotNull(log?.Id);

            var disposables = new List<IDisposable>(4);
            var countdownEvent = new AsyncCountdownEvent(5);
            // Save requires an id to be set.
            var addedLog = LogEventGenerator.Generate(id: ObjectId.GenerateNewId().ToString());
            try {
                disposables.Add(_dailyRepository.DocumentsAdding.AddSyncHandler((o, args) => {
                    Assert.Equal(addedLog, args.Documents.First());
                    countdownEvent.Signal();
                }));
                disposables.Add(_dailyRepository.DocumentsAdded.AddSyncHandler((o, args) => {
                    Assert.Equal(addedLog, args.Documents.First());
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
                await _messgeBus.SubscribeAsync<EntityChanged>((msg, ct) => {
                    Assert.Equal(nameof(LogEvent), msg.Type);
                    Assert.Equal(log.Id, msg.Id);
                    Assert.Equal(ChangeType.Saved, msg.ChangeType);
                    countdownEvent.Signal();
                    return Task.CompletedTask;
                });

                log.CompanyId = ObjectId.GenerateNewId().ToString();
                await _dailyRepository.SaveAsync(new List<LogEvent> { log, addedLog });

                await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromSeconds(2)).Token);
                Assert.Equal(0, countdownEvent.CurrentCount);
            } finally {
                foreach (var disposable in disposables)
                    disposable.Dispose();

                disposables.Clear();
            }
        }

        [Fact]
        public async Task AddAndSaveWithCache() {
            var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, addToCache: true);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            string cacheKey = _cache.Keys.Single();
            var cacheValue = await _cache.GetAsync<Identity>(cacheKey);
            Assert.True(cacheValue.HasValue);
            Assert.Equal(identity, cacheValue.Value);

            identity = await _identityRepository.GetByIdAsync(identity.Id, useCache: true);
            Assert.NotNull(identity);
            Assert.Equal(2, _cache.Hits);

            cacheValue = await _cache.GetAsync<Identity>(cacheKey);
            Assert.True(cacheValue.HasValue);
            Assert.Equal(identity, cacheValue.Value);

            await _identityRepository.InvalidateCacheAsync(identity);
            Assert.Equal(0, _cache.Count);
            Assert.Equal(3, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            var result = await _identityRepository.SaveAsync(identity, addToCache: true);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(3, _cache.Hits);
            Assert.Equal(1, _cache.Misses);
            cacheValue = await _cache.GetAsync<Identity>(cacheKey);
            Assert.True(cacheValue.HasValue);
            Assert.Equal(identity, cacheValue.Value);
        }

        [Fact]
        public async Task SaveWithNoIdentity() {
            var identity = IdentityGenerator.Generate();
            await Assert.ThrowsAsync<ApplicationException>(async () => await _identityRepository.SaveAsync(new List<Identity> { identity }, addToCache: true));
        }

        [Fact]
        public async Task SaveWithOutOfSyncIndex() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)));
            Assert.NotNull(yesterdayLog?.Id);

            await _client.RefreshAsync();
            Assert.Equal(1, await _dailyRepository.CountAsync());

            yesterdayLog.Message = "updated";
            await _dailyRepository.SaveAsync(yesterdayLog);

            await _client.RefreshAsync();
            Assert.Equal(1, await _dailyRepository.CountAsync());
        }

        [Fact]
        public async Task CanGetAggregations() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)));
            Assert.NotNull(yesterdayLog?.Id);

            await _client.RefreshAsync();
            var result = await _dailyRepository.CountBySearchAsync(null, aggregations: "cardinality:companyId max:createdUtc");
            Assert.Equal(2, result.Aggregations.Count);
            var cardinalityAgg = result.Aggregations.FirstOrDefault(a => a.Key == "cardinality_companyId");
            Assert.NotNull(cardinalityAgg);
            Assert.Equal(1, cardinalityAgg.Value.Value);

            var maxAgg = result.Aggregations.FirstOrDefault(a => a.Key == "max_createdUtc");
            Assert.NotNull(maxAgg);
            Assert.True(maxAgg.Value.Value.HasValue);
            Assert.True(yesterdayLog.CreatedUtc.Subtract(__unixEpoch.AddMilliseconds(maxAgg.Value.Value.Value)).TotalSeconds < 1);
        }

        private static DateTime __unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        [Fact]
        public async Task CanGetDateAggregation() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)));
            Assert.NotNull(yesterdayLog?.Id);

            await _client.RefreshAsync();
            var result = await _dailyRepository.CountBySearchAsync(null, aggregations: "date:(createdUtc min:createdUtc)");
            Assert.Equal(1, result.Aggregations.Count);
            var dateAgg = result.Aggregations.FirstOrDefault(a => a.Key == "date_createdUtc");
            Assert.NotNull(dateAgg);
            Assert.Equal(1, dateAgg.Value.Buckets.Count);

            result = await _dailyRepository.CountBySearchAsync(null, aggregations: "date:(createdUtc~1h^-3h min:createdUtc)");
            Assert.Equal(1, result.Aggregations.Count);
            dateAgg = result.Aggregations.FirstOrDefault(a => a.Key == "date_createdUtc");
            Assert.NotNull(dateAgg);
            Assert.Equal(1, dateAgg.Value.Buckets.Count);
        }

        [Fact]
        public async Task CanGetGeoGridAggregation() {
            var utcNow = SystemClock.UtcNow;
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)));
            Assert.NotNull(employee?.Id);
            await _employeeRepository.AddAsync(EmployeeGenerator.GenerateEmployees());

            await _client.RefreshAsync();
            var result = await _employeeRepository.CountBySearchAsync(null, aggregations: "geogrid:(location~6 max:age)");
            Assert.Equal(1, result.Aggregations.Count);
            var geoAgg = result.Aggregations.FirstOrDefault(a => a.Key == "geogrid_location");
            Assert.NotNull(geoAgg);
            Assert.InRange(geoAgg.Value.Buckets.Count, 1, 11);
        }

        [Fact]
        public async Task SaveWithCaching() {
            var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, addToCache: true);
            Assert.NotNull(identity?.Id);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _identityRepository.InvalidateCacheAsync(identity);
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            identity = await _identityRepository.SaveAsync(identity, addToCache: true);
            Assert.NotNull(identity?.Id);
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(1, _cache.Misses); // Save will attempt to lookup the original document using the cache.

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, useCache: true));
            Assert.Equal(1, _cache.Count);
            Assert.Equal(1, _cache.Hits);
            Assert.Equal(1, _cache.Misses);
        }

        [Fact]
        public async Task SaveCollection() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate(ObjectId.GenerateNewId().ToString()) };
            await _identityRepository.SaveAsync(identities);

            var results = await _identityRepository.GetByIdsAsync(identities.Select(i => i.Id).ToList());
            Assert.Equal(2, results.Count);
        }

        [Fact]
        public async Task SaveCollectionWithTimeSeries() {
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
        public async Task SaveCollectionWithCaching() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
            await _identityRepository.AddAsync(identities, addToCache: true);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _identityRepository.InvalidateCacheAsync(identities);
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _identityRepository.SaveAsync(identities, addToCache: true);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(2, _cache.Misses); // Save will attempt to lookup the original document using the cache.

            var results = await _identityRepository.GetByIdsAsync(identities.Select(i => i.Id).ToList(), useCache: true);
            Assert.Equal(2, results.Count);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(2, _cache.Hits);
            Assert.Equal(2, _cache.Misses);
        }

        [Fact]
        public async Task SetCreatedAndModifiedTimes() {
            using (TestSystemClock.Install()) {
                SystemClock.Test.SubtractTime(TimeSpan.FromMilliseconds(100));
                DateTime nowUtc = SystemClock.UtcNow;
                var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
                Assert.True(employee.CreatedUtc >= nowUtc);
                Assert.True(employee.UpdatedUtc >= nowUtc);

                DateTime createdUtc = employee.CreatedUtc;
                DateTime updatedUtc = employee.UpdatedUtc;

                employee.Name = Guid.NewGuid().ToString();
                SystemClock.Test.AddTime(TimeSpan.FromMilliseconds(100));
                employee = await _employeeRepository.SaveAsync(employee);
                Assert.Equal(createdUtc, employee.CreatedUtc);
                Assert.True(updatedUtc < employee.UpdatedUtc, $"Previous UpdatedUtc: {updatedUtc} Current UpdatedUtc: {employee.UpdatedUtc}");
            }
        }

        [Fact]
        public async Task CannotSetFutureCreatedAndModifiedTimes() {
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
        public async Task JsonPatch() {
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);

            var patch = new PatchDocument(new ReplaceOperation { Path = "name", Value = "Patched" });
            await _employeeRepository.PatchAsync(employee.Id, patch);

            employee = await _employeeRepository.GetByIdAsync(employee.Id);
            Assert.Equal("Patched", employee.Name);
            Assert.Equal(2, employee.Version);
        }

        [Fact]
        public async Task PartialPatch() {
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);

            await _employeeRepository.PatchAsync(employee.Id, new { name = "Patched" });

            employee = await _employeeRepository.GetByIdAsync(employee.Id);
            Assert.Equal("Patched", employee.Name);
            Assert.Equal(2, employee.Version);
        }

        [Fact]
        public async Task ScriptPatch() {
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);

            await _employeeRepository.PatchAsync(employee.Id, "ctx._source.name = 'Patched';");

            employee = await _employeeRepository.GetByIdAsync(employee.Id);
            Assert.Equal("Patched", employee.Name);
            Assert.Equal(2, employee.Version);
        }

        [Fact]
        public async Task ScriptPatchAll() {
            var utcNow = SystemClock.UtcNow;
            var logs = new List<LogEvent> {
                LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1), companyId: "1"),
                LogEventGenerator.Generate(createdUtc: utcNow, companyId: "1"),
                LogEventGenerator.Generate(createdUtc: utcNow, companyId: "2"),
            };

            await _dailyRepository.AddAsync(logs, addToCache: true);
            Assert.Equal(5, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _client.RefreshAsync();
            Assert.Equal(3, await _dailyRepository.IncrementValueAsync(logs.Select(l => l.Id).ToArray()));
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _client.RefreshAsync();
            var results = await _dailyRepository.GetAllByCompanyAsync("1");
            Assert.Equal(2, results.Documents.Count);
            foreach (var document in results.Documents) {
                Assert.Equal("1", document.CompanyId);
                Assert.Equal(1, document.Value);
            }

            await _dailyRepository.SaveAsync(logs, addToCache: true);
            await _client.RefreshAsync();

            results = await _dailyRepository.GetAllByCompanyAsync("1");
            Assert.Equal(2, results.Documents.Count);
            foreach (var document in results.Documents) {
                Assert.Equal("1", document.CompanyId);
                Assert.Equal(0, document.Value);
            }
        }

        [Fact(Skip = "TODO: Investigate why this fails.")]
        public async Task PatchAllBulk() {
            Log.SetLogLevel<DailyLogEventRepository>(LogLevel.Warning);
            const int COUNT = 1000 * 10;
            int added = 0;
            do {
                await _dailyRepository.AddAsync(LogEventGenerator.GenerateLogs(1000));
                added += 1000;
            } while (added < COUNT);
            Log.SetLogLevel<DailyLogEventRepository>(LogLevel.Trace);

            await _client.RefreshAsync();
            Assert.Equal(COUNT, await _dailyRepository.IncrementValueAsync(new string[0]));
        }

        [Fact(Skip = "TODO: Investigate why this fails.")]
        public async Task PatchAllBulkConcurrently() {
            Log.SetLogLevel<DailyLogEventRepository>(LogLevel.Warning);
            const int COUNT = 1000 * 10;
            int added = 0;
            do {
                await _dailyRepository.AddAsync(LogEventGenerator.GenerateLogs(1000));
                added += 1000;
            } while (added < COUNT);
            Log.SetLogLevel<DailyLogEventRepository>(LogLevel.Trace);

            await _client.RefreshAsync();
            var tasks = Enumerable.Range(1, 6).Select(async i => {
                Assert.Equal(COUNT, await _dailyRepository.IncrementValueAsync(new string[0], i));
            });

            await Task.WhenAll(tasks);
        }

        [Fact]
        public async Task Remove() {
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
        public async Task RemoveWithTimeSeries() {
            var log = LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString());
            await _dailyRepository.AddAsync(log);

            await _client.RefreshAsync();
            Assert.Equal(1, await _dailyRepository.CountAsync());

            await _dailyRepository.RemoveAsync(log);

            await _client.RefreshAsync();
            Assert.Equal(0, await _dailyRepository.CountAsync());
        }

        [Fact(Skip = "We need to look into how we want to handle this.")]
        public async Task RemoveWithOutOfSyncIndex() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)));
            Assert.NotNull(yesterdayLog?.Id);

            await _client.RefreshAsync();
            Assert.Equal(1, await _dailyRepository.CountAsync());

            await _dailyRepository.RemoveAsync(yesterdayLog);

            await _client.RefreshAsync();
            Assert.Equal(1, await _dailyRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveUnsavedDocument() {
            await _dailyRepository.RemoveAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: SystemClock.UtcNow));
        }

        [Fact]
        public async Task RemoveWithCaching() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
            await _identityRepository.AddAsync(identities, addToCache: true);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _identityRepository.RemoveAsync(identities.First());
            Assert.Equal(1, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _identityRepository.RemoveAsync(identities);
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _client.RefreshAsync();
            Assert.Equal(0, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveCollection() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
            await _identityRepository.AddAsync(identities);
            await _identityRepository.RemoveAsync(identities);

            await _client.RefreshAsync();
            Assert.Equal(0, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveCollectionWithTimeSeries() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1));
            var nowLog = LogEventGenerator.Default;

            var logs = new List<LogEvent> { yesterdayLog, nowLog };
            await _dailyRepository.AddAsync(logs);

            await _client.RefreshAsync();
            Assert.Equal(2, await _dailyRepository.CountAsync());

            await _dailyRepository.RemoveAsync(logs);

            await _client.RefreshAsync();
            Assert.Equal(0, await _dailyRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveCollectionWithCaching() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
            await _identityRepository.AddAsync(identities, addToCache: true);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _identityRepository.RemoveAsync(identities);
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _client.RefreshAsync();
            Assert.Equal(0, await _identityRepository.CountAsync());
        }

        [Fact(Skip = "We need to look into how we want to handle this.")]
        public async Task RemoveCollectionWithOutOfSyncIndex() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId().ToString(), createdUtc: utcNow.AddDays(-1)));
            Assert.NotNull(yesterdayLog?.Id);

            await _client.RefreshAsync();
            Assert.Equal(1, await _dailyRepository.CountAsync());

            await _dailyRepository.RemoveAsync(new List<LogEvent> { yesterdayLog });

            await _client.RefreshAsync();
            Assert.Equal(1, await _dailyRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveAll() {
            await _identityRepository.RemoveAllAsync();

            var identities = new List<Identity> { IdentityGenerator.Default };
            await _identityRepository.AddAsync(identities);
            await _client.RefreshAsync();

            var disposables = new List<IDisposable>(2);
            var countdownEvent = new AsyncCountdownEvent(2);

            try {
                disposables.Add(_identityRepository.DocumentsRemoving.AddSyncHandler((o, args) => {
                    countdownEvent.Signal();
                }));
                disposables.Add(_identityRepository.DocumentsRemoved.AddSyncHandler((o, args) => {
                    countdownEvent.Signal();
                }));

                await _identityRepository.RemoveAllAsync();
                await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
                Assert.Equal(0, countdownEvent.CurrentCount);

            } finally {
                foreach (var disposable in disposables)
                    disposable.Dispose();

                disposables.Clear();
            }

            await _client.RefreshAsync();
            Assert.Equal(0, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveAllWithBatching() {
            const int COUNT = 10000;
            Log.SetLogLevel<IdentityRepository>(LogLevel.Information);
            await _identityRepository.AddAsync(IdentityGenerator.GenerateIdentities(COUNT));
            await _client.RefreshAsync();
            Log.SetLogLevel<IdentityRepository>(LogLevel.Trace);

            var sw = Stopwatch.StartNew();
            Assert.Equal(COUNT, await _identityRepository.RemoveAllAsync());
            sw.Stop();

            _logger.Info($"Deleted {COUNT} documents in {sw.ElapsedMilliseconds}ms");

            await _client.RefreshAsync();
            Assert.Equal(0, await _identityRepository.CountAsync());

            // TODO: Ensure only one removed notification is sent out.
        }

        [Fact]
        public async Task RemoveAllWithDeleteByQuery() {
            const int COUNT = 10000;
            Log.SetLogLevel<IdentityWithNoCachingRepository>(LogLevel.Information);
            await _identityRepositoryWithNoCaching.AddAsync(IdentityGenerator.GenerateIdentities(COUNT));
            await _client.RefreshAsync();
            Log.SetLogLevel<IdentityWithNoCachingRepository>(LogLevel.Trace);

            var sw = Stopwatch.StartNew();
            Assert.Equal(COUNT, await _identityRepositoryWithNoCaching.RemoveAllAsync());
            sw.Stop();

            _logger.Info($"Deleted {COUNT} documents in {sw.ElapsedMilliseconds}ms");

            await _client.RefreshAsync();
            Assert.Equal(0, await _identityRepositoryWithNoCaching.CountAsync());

            // TODO: Ensure only one removed notification is sent out.
        }

        [Fact]
        public async Task RemoveAllWithCaching() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
            await _identityRepository.AddAsync(identities, addToCache: true);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _client.RefreshAsync();
            await _identityRepository.RemoveAllAsync();
            Assert.Equal(0, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

            await _client.RefreshAsync();
            Assert.Equal(0, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveAllWithTimeSeries() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1));
            var nowLog = LogEventGenerator.Default;

            var logs = new List<LogEvent> { yesterdayLog, nowLog };
            await _dailyRepository.AddAsync(logs);

            await _client.RefreshAsync();
            Assert.Equal(2, await _dailyRepository.CountAsync());

            await _dailyRepository.RemoveAllAsync();

            await _client.RefreshAsync();
            Assert.Equal(0, await _dailyRepository.CountAsync());
        }

        [Fact]
        public async Task ValidatorThrowsOnAdd() {
            var employeeRepositoryWithValidator = new EmployeeRepository(_configuration, new EmployeeValidatorWithValidateException());

            await Assert.ThrowsAsync<ValidationException>(async () => await employeeRepositoryWithValidator.AddAsync(EmployeeGenerator.Default));

            employeeRepositoryWithValidator = new EmployeeRepository(_configuration, new EmployeeValidatorWithRequiredFields());
            
            await Assert.ThrowsAsync<ValidationException>(async () => await employeeRepositoryWithValidator.AddAsync(EmployeeGenerator.Generate(age: 50, yearsEmployed: 10, companyId: EmployeeGenerator.DefaultCompanyId)));
        }

        [Fact]
        public async Task ValidatorThrowsOnSave() {
            var employee1 = await _employeeRepository.AddAsync(EmployeeGenerator.Generate());
            Assert.NotNull(employee1?.Id);
            await _client.RefreshAsync();

            var employeeRepositoryWithValidator = new EmployeeRepository(_configuration, new EmployeeValidatorWithValidateException());
            await Assert.ThrowsAsync<ValidationException>(async () => await employeeRepositoryWithValidator.SaveAsync(employee1));

            employeeRepositoryWithValidator = new EmployeeRepository(_configuration, new EmployeeValidatorWithRequiredFields());
            employee1.Name = null;
            employee1.CompanyName = null;
            await Assert.ThrowsAsync<ValidationException>(async () => await employeeRepositoryWithValidator.SaveAsync(employee1));
        }
    }
}
