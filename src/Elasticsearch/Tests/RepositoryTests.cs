using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Jobs;
using Foundatio.Logging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
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
        private readonly IQueue<WorkItemData> _workItemQueue = new InMemoryQueue<WorkItemData>();

        public RepositoryTests(ITestOutputHelper output) : base(output) {
            _dailyRepository = new DailyLogEventRepository(MyAppConfiguration, _cache, Log.CreateLogger<DailyLogEventRepository>());
            _employeeRepository = new EmployeeRepository(MyAppConfiguration, _cache, Log.CreateLogger<EmployeeRepository>());
            _identityRepository = new IdentityRepository(MyAppConfiguration, _cache, Log.CreateLogger<IdentityRepository>());
            
            RemoveDataAsync().GetAwaiter().GetResult();
        }

        protected override ElasticConfiguration GetElasticConfiguration() {
            return new MyAppElasticConfiguration(_workItemQueue, _cache, Log);
        }

        private MyAppElasticConfiguration MyAppConfiguration => _configuration as MyAppElasticConfiguration;

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
            Assert.Equal(_cache.Count, 1);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, useCache: true));
            Assert.Equal(_cache.Count, 1);
            Assert.Equal(_cache.Hits, 1);
            Assert.Equal(_cache.Misses, 0);
        }

        [Fact]
        public async Task AddCollection() {
            var identity = IdentityGenerator.Generate();
            await _identityRepository.AddAsync(new List<Identity> { identity }, addToCache: true);
            Assert.NotNull(identity.Id);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, useCache: true));
        }

        [Fact]
        public async Task AddCollectionWithCaching() {
            var identity = IdentityGenerator.Generate();
            await _identityRepository.AddAsync(new List<Identity> { identity }, addToCache: true);
            Assert.NotNull(identity?.Id);
            Assert.Equal(_cache.Count, 1);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, useCache: true));
            Assert.Equal(_cache.Count, 1);
            Assert.Equal(_cache.Hits, 1);
            Assert.Equal(_cache.Misses, 0);
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
        public async Task SaveWithNoIdentity() {
            var identity = IdentityGenerator.Generate();
            await Assert.ThrowsAsync<ApplicationException>(async () => await _identityRepository.SaveAsync(new List<Identity> { identity }, addToCache: true));
        }

        [Fact]
        public async Task SaveWithCaching() {
            var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, addToCache: true);
            Assert.NotNull(identity?.Id);
            Assert.Equal(_cache.Count, 1);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            await _identityRepository.InvalidateCacheAsync(identity);
            Assert.Equal(_cache.Count, 0);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            identity = await _identityRepository.SaveAsync(identity, addToCache: true);
            Assert.NotNull(identity?.Id);
            Assert.Equal(_cache.Count, 1);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);
            
            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, useCache: true));
            Assert.Equal(_cache.Count, 1);
            Assert.Equal(_cache.Hits, 1);
            Assert.Equal(_cache.Misses, 0);
        }

        [Fact]
        public async Task SaveCollection() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate(ObjectId.GenerateNewId().ToString()) };
            await _identityRepository.SaveAsync(identities);

            var results = await _identityRepository.GetByIdsAsync(identities.Select(i => i.Id).ToList());
            Assert.Equal(2, results.Documents.Count);
        }

        [Fact]
        public async Task SaveCollectionWithCaching() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
            await _identityRepository.AddAsync(identities, addToCache: true);
            Assert.Equal(_cache.Count, 2);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            await _identityRepository.InvalidateCacheAsync(identities);
            Assert.Equal(_cache.Count, 0);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            await _identityRepository.SaveAsync(identities, addToCache: true);
            Assert.Equal(_cache.Count, 2);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            var results = await _identityRepository.GetByIdsAsync(identities.Select(i => i.Id).ToList(), useCache: true);
            Assert.Equal(2, results.Documents.Count);
            Assert.Equal(_cache.Count, 2);
            Assert.Equal(_cache.Hits, 2);
            Assert.Equal(_cache.Misses, 0);
        }
        
        [Fact]
        public async Task SetCreatedAndModifiedTimesAsync() {
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
        public async Task RemoveWithCaching() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
            await _identityRepository.AddAsync(identities, addToCache: true);
            Assert.Equal(_cache.Count, 2);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            await _identityRepository.RemoveAsync(identities.First());
            Assert.Equal(_cache.Count, 1);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);
            
            await _identityRepository.RemoveAsync(identities);
            Assert.Equal(_cache.Count, 0);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

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
        public async Task RemoveCollectionWithCaching() {
            var identities = new List<Identity> { IdentityGenerator.Default, IdentityGenerator.Generate() };
            await _identityRepository.AddAsync(identities, addToCache: true);
            Assert.Equal(_cache.Count, 2);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);
            
            await _identityRepository.RemoveAsync(identities);
            Assert.Equal(_cache.Count, 0);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            await _client.RefreshAsync();
            Assert.Equal(0, await _identityRepository.CountAsync());
        }

        [Fact]
        public async Task RemoveAll() {
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
            Assert.Equal(_cache.Count, 2);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            await _client.RefreshAsync();
            await _identityRepository.RemoveAllAsync();
            Assert.Equal(_cache.Count, 0);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            await _client.RefreshAsync();
            Assert.Equal(0, await _identityRepository.CountAsync());
        }
    }
}