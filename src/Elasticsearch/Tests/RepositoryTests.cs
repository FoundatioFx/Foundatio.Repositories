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
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Nito.AsyncEx;
using Xunit;
using Xunit.Abstractions;
using Foundatio.Repositories.Elasticsearch.Tests.Extensions;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class RepositoryTests : ElasticRepositoryTestBase {
        private readonly IdentityRepository _identityRepository;
        private readonly DailyLogEventRepository _dailyRepository;
        private readonly IQueue<WorkItemData> _workItemQueue = new InMemoryQueue<WorkItemData>();

        public RepositoryTests(ITestOutputHelper output) : base(output) {
            _identityRepository = new IdentityRepository(_configuration as MyAppElasticConfiguration, _cache, Log.CreateLogger<IdentityRepository>());
            _dailyRepository = new DailyLogEventRepository(_configuration as MyAppElasticConfiguration, _cache, Log.CreateLogger<DailyLogEventRepository>());

            RemoveDataAsync().GetAwaiter().GetResult();
        }

        protected override ElasticConfiguration GetElasticConfiguration() {
            return new MyAppElasticConfiguration(_workItemQueue, _cache, Log);
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
            await _identityRepository.AddAsync(new List<Identity> { identity, null }, addToCache: true);
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
        }

        [Fact]
        public async Task SaveCollection() {
            throw new NotImplementedException();
        }

        [Fact]
        public async Task SaveCollectionWithCaching() {
            throw new NotImplementedException();
        }

        [Fact]
        public async Task Remove() {
            throw new NotImplementedException();
        }

        [Fact]
        public async Task RemoveWithCaching() {
            throw new NotImplementedException();
        }

        [Fact]
        public async Task RemoveCollection() {
            throw new NotImplementedException();
        }

        [Fact]
        public async Task RemoveCollectionWithCaching() {
            throw new NotImplementedException();
        }

        [Fact]
        public async Task RemoveAll() {
            throw new NotImplementedException();
        }

        [Fact]
        public async Task RemoveAllWithCaching() {
            throw new NotImplementedException();
        }
        
        // TODO: Timeseries tests
        // TODO: Verify handlers.
        //AsyncEvent<DocumentsEventArgs<T>> DocumentsAdding { get; }
        //AsyncEvent<DocumentsEventArgs<T>> DocumentsAdded { get; }
        //AsyncEvent<ModifiedDocumentsEventArgs<T>> DocumentsSaving { get; }
        //AsyncEvent<ModifiedDocumentsEventArgs<T>> DocumentsSaved { get; }
        //AsyncEvent<DocumentsEventArgs<T>> DocumentsRemoving { get; }
        //AsyncEvent<DocumentsEventArgs<T>> DocumentsRemoved { get; }
        //AsyncEvent<DocumentsChangeEventArgs<T>> DocumentsChanging { get; }
        //AsyncEvent<DocumentsChangeEventArgs<T>> DocumentsChanged { get; }
    }
}