using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Jobs;
using Foundatio.Logging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Queries;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;
using Foundatio.Utility;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class ReadOnlyRepositoryTests : ElasticRepositoryTestBase {
        private readonly IdentityRepository _identityRepository;
        private readonly DailyLogEventRepository _dailyRepository;
        private readonly IQueue<WorkItemData> _workItemQueue = new InMemoryQueue<WorkItemData>();

        public ReadOnlyRepositoryTests(ITestOutputHelper output) : base(output) {
            _identityRepository = new IdentityRepository(MyAppConfiguration, _cache, Log.CreateLogger<IdentityRepository>());
            _dailyRepository = new DailyLogEventRepository(MyAppConfiguration, _cache, Log.CreateLogger<DailyLogEventRepository>());

            RemoveDataAsync().GetAwaiter().GetResult();
        }

        protected override ElasticConfiguration GetElasticConfiguration() {
            return new MyAppElasticConfiguration(_workItemQueue, _cache, Log);
        }

        private MyAppElasticConfiguration MyAppConfiguration => _configuration as MyAppElasticConfiguration;

        [Fact]
        public async Task InvalidateCache() {
            var identity = await _identityRepository.AddAsync(IdentityGenerator.Default, addToCache: true);
            Assert.NotNull(identity?.Id);
            Assert.Equal(_cache.Count, 1);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            await _identityRepository.InvalidateCacheAsync(identity);
            Assert.Equal(_cache.Count, 0);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);
            
            await _identityRepository.SaveAsync(identity, addToCache: true);
            Assert.Equal(_cache.Count, 1);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            await _identityRepository.InvalidateCacheAsync(new List<Identity> { identity });
            Assert.Equal(_cache.Count, 0);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);
            
            await _identityRepository.SaveAsync(new List<Identity> { identity }, addToCache: true);
            Assert.Equal(_cache.Count, 1);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            await _identityRepository.InvalidateCacheAsync(new List<Identity> { identity });
            Assert.Equal(_cache.Count, 0);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);
            
            await _identityRepository.SaveAsync(new List<Identity> { identity }, addToCache: true);
            Assert.Equal(_cache.Count, 1);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            await _identityRepository.InvalidateCacheAsync(identity);
            Assert.Equal(_cache.Count, 0);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            await _identityRepository.InvalidateCacheAsync(new List<Identity> { identity });
            Assert.Equal(_cache.Count, 0);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);
        }
        
        [Fact]
        public async Task InvalidateCacheWithInvalidArguments() {
            await Assert.ThrowsAsync<ArgumentNullException>(async () => await _identityRepository.InvalidateCacheAsync((Identity)null));
            await Assert.ThrowsAsync<ArgumentNullException>(async () => await _identityRepository.InvalidateCacheAsync((ICollection<Identity>)null));
            await _identityRepository.InvalidateCacheAsync(new List<Identity>());
            await Assert.ThrowsAsync<ArgumentNullException>(async () => await _identityRepository.InvalidateCacheAsync(new List<Identity> { null }));
        }

        [Fact]
        public async Task Count() {
            Assert.Equal(0, await _identityRepository.CountAsync());

            var identity = IdentityGenerator.Default;
            var result = await _identityRepository.AddAsync(identity);
            Assert.Equal(identity, result);

            await _client.RefreshAsync();
            Assert.Equal(1, await _identityRepository.CountAsync());
        }
        
        [Fact]
        public async Task CountWithTimeSeries() {
            Assert.Equal(0, await _dailyRepository.CountAsync());
            
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(createdUtc: SystemClock.UtcNow.AddDays(-1)));
            Assert.NotNull(yesterdayLog?.Id);
            
            var nowLog = LogEventGenerator.Default;
            var result = await _dailyRepository.AddAsync(nowLog);
            Assert.Equal(nowLog, result);

            await _client.RefreshAsync();
            Assert.Equal(2, await _dailyRepository.CountAsync());
        }
        
        [Fact]
        public async Task GetById() {
            var identity = await _identityRepository.AddAsync(IdentityGenerator.Default);
            Assert.NotNull(identity?.Id);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id));
        }
        
        [Fact]
        public async Task GetByIdWithCache() {
            var identity = await _identityRepository.AddAsync(IdentityGenerator.Default);
            Assert.NotNull(identity?.Id);
            
            Assert.Equal(_cache.Count, 0);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, useCache: true));
            Assert.Equal(_cache.Count, 1);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 1);

            Assert.Equal(identity, await _identityRepository.GetByIdAsync(identity.Id, useCache: true));
            Assert.Equal(_cache.Count, 1);
            Assert.Equal(_cache.Hits, 1);
            Assert.Equal(_cache.Misses, 1);
        }

        [Fact]
        public async Task GetByIdWithTimeSeries() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(createdUtc: utcNow.AddDays(-1)));
            Assert.NotNull(yesterdayLog?.Id);

            var nowLog = await _dailyRepository.AddAsync(LogEventGenerator.Default);
            Assert.NotNull(nowLog?.Id);

            Assert.Equal(yesterdayLog, await _dailyRepository.GetByIdAsync(yesterdayLog.Id));
            Assert.Equal(nowLog, await _dailyRepository.GetByIdAsync(nowLog.Id));
        }
        
        [Fact]
        public async Task GetByIds() {
            var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default);
            Assert.NotNull(identity1?.Id);

            var identity2 = await _identityRepository.AddAsync(IdentityGenerator.Generate());
            Assert.NotNull(identity2?.Id);
            
            var results = await _identityRepository.GetByIdsAsync(new[] { identity1.Id, identity2.Id });
            Assert.NotNull(results);
            Assert.Equal(2, results.Total);
        }
        
        [Fact]
        public async Task GetByIdsWithInvalidId() {
            var identity = await _identityRepository.AddAsync(IdentityGenerator.Generate());
            Assert.NotNull(identity?.Id);

            var result = await _identityRepository.GetByIdsAsync(null);
            Assert.Equal(0, result.Total);

            result = await _identityRepository.GetByIdsAsync(new string[] { null });
            Assert.Equal(0, result.Total);

            result = await _identityRepository.GetByIdsAsync(new [] { IdentityGenerator.Default.Id, identity.Id });
            Assert.Equal(1, result.Total);
        }
        
        [Fact]
        public async Task GetByIdsWithCaching() {
            var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default);
            Assert.NotNull(identity1?.Id);
            
            var identity2 = await _identityRepository.AddAsync(IdentityGenerator.Generate());
            Assert.NotNull(identity2?.Id);

            Assert.Equal(_cache.Count, 0);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);
            
            var results = await _identityRepository.GetByIdsAsync(new[] { identity1.Id }, useCache: true);
            Assert.NotNull(results);
            Assert.Equal(1, results.Total);
            Assert.Equal(identity1, results.Documents.First());
            Assert.Equal(_cache.Count, 1);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 1);

            results = await _identityRepository.GetByIdsAsync(new[] { identity1.Id, identity2.Id }, useCache: true);
            Assert.NotNull(results);
            Assert.Equal(2, results.Total);
            Assert.Equal(_cache.Count, 2);
            Assert.Equal(_cache.Hits, 1);
            Assert.Equal(_cache.Misses, 2);
            
            results = await _identityRepository.GetByIdsAsync(new[] { identity1.Id, identity2.Id }, useCache: true);
            Assert.NotNull(results);
            Assert.Equal(2, results.Total);
            Assert.Equal(_cache.Count, 2);
            Assert.Equal(_cache.Hits, 3);
            Assert.Equal(_cache.Misses, 2);
            
            var identity = await _identityRepository.GetByIdAsync(identity1.Id, useCache: true);
            Assert.Equal(identity1, identity);
            Assert.Equal(_cache.Count, 2);
            Assert.Equal(_cache.Hits, 4);
            Assert.Equal(_cache.Misses, 2);
        }

        [Fact]
        public async Task GetByIdsWithInvalidIdAndCaching() {
            var identity = await _identityRepository.AddAsync(IdentityGenerator.Generate());
            Assert.NotNull(identity?.Id);

            var result = await _identityRepository.GetByIdsAsync(null, useCache: true);
            Assert.Equal(0, result.Total);
            Assert.Equal(_cache.Count, 0);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);
            
            result = await _identityRepository.GetByIdsAsync(new string[] { null }, useCache: true);
            Assert.Equal(0, result.Total);
            Assert.Equal(_cache.Count, 0);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 0);
            
            result = await _identityRepository.GetByIdsAsync(new[] { IdentityGenerator.Default.Id, identity.Id }, useCache: true);
            Assert.Equal(1, result.Total);
            Assert.Equal(_cache.Count, 1);
            Assert.Equal(_cache.Hits, 0);
            Assert.Equal(_cache.Misses, 2);
        }

        [Fact]
        public async Task GetByIdsWithTimeSeries() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(createdUtc: utcNow.AddDays(-1)));
            Assert.NotNull(yesterdayLog?.Id);

            var nowLog = await _dailyRepository.AddAsync(LogEventGenerator.Default);
            Assert.NotNull(nowLog?.Id);
            
            var results = await _dailyRepository.GetByIdsAsync(new[] { yesterdayLog.Id, nowLog.Id });
            Assert.NotNull(results);
            Assert.Equal(2, results.Total);
        }
        
        [Fact]
        public async Task GetAll() {
            var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default);
            Assert.NotNull(identity1?.Id);

            var identity2 = await _identityRepository.AddAsync(IdentityGenerator.Generate());
            Assert.NotNull(identity2?.Id);

            await _client.RefreshAsync();
            var results = await _identityRepository.GetAllAsync();
            Assert.NotNull(results);
            Assert.Equal(2, results.Total);
        }

        [Fact]
        public async Task GetAllWithPaging() {
            var identity1 = await _identityRepository.AddAsync(IdentityGenerator.Default);
            Assert.NotNull(identity1?.Id);

            var identity2 = await _identityRepository.AddAsync(IdentityGenerator.Generate());
            Assert.NotNull(identity2?.Id);

            await _client.RefreshAsync();
            var results = await _identityRepository.GetAllAsync(paging: new PagingOptions().WithLimit(1));
            Assert.NotNull(results);
            Assert.True(results.HasMore);
            Assert.Equal(1, results.Documents.Count);
            Assert.Equal(1, results.Page);
            Assert.Equal(2, results.Total);

            Assert.True(await results.NextPageAsync());
            Assert.False(results.HasMore);
            Assert.Equal(1, results.Documents.Count);
            Assert.Equal(2, results.Page);
            Assert.Equal(2, results.Total);
            
            Assert.False(await results.NextPageAsync());
            Assert.False(results.HasMore);
            Assert.Equal(1, results.Documents.Count);
            Assert.Equal(2, results.Page);
            Assert.Equal(2, results.Total);
            
            var secondPageResults = await _identityRepository.GetAllAsync(paging: new PagingOptions().WithPage(2).WithLimit(1));
            Assert.Equal(results.Documents.First(), secondPageResults.Documents.First());

            //TODO Ensure we are using snapshot paging...
        }

        [Fact]
        public async Task Exists() {
            Assert.False(await _identityRepository.ExistsAsync(null));

            var identity = IdentityGenerator.Default;
            Assert.False(await _identityRepository.ExistsAsync(identity.Id));

            var result = await _identityRepository.AddAsync(identity);
            Assert.Equal(identity, result);

            await _client.RefreshAsync();
            Assert.True(await _identityRepository.ExistsAsync(identity.Id));
        }

        [Fact]
        public async Task ExistsWithTimeSeries() {
            Assert.False(await _dailyRepository.ExistsAsync(null));

            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(createdUtc: utcNow.AddDays(-1)));
            Assert.NotNull(yesterdayLog?.Id);
            
            var nowLog = await _dailyRepository.AddAsync(LogEventGenerator.Default);
            Assert.NotNull(nowLog?.Id);

            await _client.RefreshAsync();
            Assert.True(await _dailyRepository.ExistsAsync(yesterdayLog.Id));
            Assert.True(await _dailyRepository.ExistsAsync(nowLog.Id));
        }
    }
}