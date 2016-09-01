using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Tests.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Repositories.JsonPatch;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Nito.AsyncEx;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class RepositoryTests : ElasticRepositoryTestBase {
        private readonly EmployeeRepository _employeeRepository;
        private readonly DailyLogEventRepository _dailyRepository;
        private readonly IdentityRepository _identityRepository;

        public RepositoryTests(ITestOutputHelper output) : base(output) {
            _dailyRepository = new DailyLogEventRepository(_configuration, _cache, Log.CreateLogger<DailyLogEventRepository>());
            _employeeRepository = new EmployeeRepository(_configuration, _cache, Log.CreateLogger<EmployeeRepository>());
            _identityRepository = new IdentityRepository(_configuration, _cache, Log.CreateLogger<IdentityRepository>());
            
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
            Assert.Equal(logs, results.Documents.OrderBy(d => d.CreatedUtc).ToList());

            await _client.RefreshAsync();
            results = await _dailyRepository.GetAllAsync();
            Assert.Equal(logs, results.Documents.OrderBy(d => d.CreatedUtc).ToList());
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
            var log = await _dailyRepository.AddAsync(LogEventGenerator.Default);
            Assert.NotNull(log?.Id);
            
            var disposables = new List<IDisposable>(4);
            var countdownEvent = new AsyncCountdownEvent(4);

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

                log.CompanyId = ObjectId.GenerateNewId().ToString();
                var result = await _dailyRepository.SaveAsync(log);
                Assert.Equal(log.CompanyId, result.CompanyId);

                await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
                Assert.Equal(0, countdownEvent.CurrentCount);
            } finally {
                foreach (var disposable in disposables)
                    disposable.Dispose();

                disposables.Clear();
            }
        }


        [Fact]
        public async Task AddAndSave() {
            var log = await _dailyRepository.AddAsync(LogEventGenerator.Default);
            Assert.NotNull(log?.Id);
            
            var disposables = new List<IDisposable>(4);
            var countdownEvent = new AsyncCountdownEvent(4);

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

                log.CompanyId = ObjectId.GenerateNewId().ToString();
                await _dailyRepository.SaveAsync(new List<LogEvent> { log, addedLog });

                await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
                Assert.Equal(0, countdownEvent.CurrentCount);
            } finally {
                foreach (var disposable in disposables)
                    disposable.Dispose();

                disposables.Clear();
            }
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
            var result = await _dailyRepository.CountBySearchAsync(null, aggregations: "companyId");
            Assert.Equal(1, result.Aggregations.Count);
            var agg = result.Aggregations.FirstOrDefault(a => a.Field == "companyId");
            Assert.NotNull(agg);
            Assert.Equal(1, agg.Terms.Count);
            Assert.Equal(1, agg.Terms.First().Value.Total);

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
            Assert.Equal(2, results.Documents.Count);
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
            Assert.Equal(logs, results.Documents.OrderBy(d => d.CreatedUtc).ToList());
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
            Assert.Equal(2, results.Documents.Count);
            Assert.Equal(2, _cache.Count);
            Assert.Equal(2, _cache.Hits);
            Assert.Equal(2, _cache.Misses);
        }
        
        [Fact]
        public async Task SetCreatedAndModifiedTimes() {
            DateTime nowUtc = SystemClock.UtcNow;
            var employee = await _employeeRepository.AddAsync(EmployeeGenerator.Default);
            Assert.True(employee.CreatedUtc >= nowUtc);
            Assert.True(employee.UpdatedUtc >= nowUtc);

            DateTime createdUtc = employee.CreatedUtc;
            DateTime updatedUtc = employee.UpdatedUtc;

            employee.Name = Guid.NewGuid().ToString();
            SystemClock.UtcNowFunc = () => DateTime.UtcNow.AddMilliseconds(100);
            employee = await _employeeRepository.SaveAsync(employee);
            Assert.Equal(createdUtc, employee.CreatedUtc);
            Assert.True(updatedUtc < employee.UpdatedUtc, $"Previous UpdatedUtc: {updatedUtc} Current UpdatedUtc: {employee.UpdatedUtc}");
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
        public async Task PatchAll() {
            var utcNow = SystemClock.UtcNow;
            var logs = new List<LogEvent> {
                LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1), companyId: "1"),
                LogEventGenerator.Generate(createdUtc: utcNow, companyId: "1"),
                LogEventGenerator.Generate(createdUtc: utcNow, companyId: "2"),
            };

            await _dailyRepository.AddAsync(logs, addToCache: true);
            await _client.RefreshAsync();
            Assert.Equal(5, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);
            
            Assert.Equal(3, await _dailyRepository.IncrementValue(logs.Select(l => l.Id).ToArray()));
            await _client.RefreshAsync();
            Assert.Equal(2, _cache.Count);
            Assert.Equal(0, _cache.Hits);
            Assert.Equal(0, _cache.Misses);

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
    }
}