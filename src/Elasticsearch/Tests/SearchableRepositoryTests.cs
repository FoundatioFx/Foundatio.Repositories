using Foundatio.Repositories.Elasticsearch.Queries;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Queries;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries;
using Foundatio.Repositories.Queries;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Nito.AsyncEx;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests
{
    public sealed class SearchableRepositoryTests : ElasticRepositoryTestBase {
        private readonly IdentityRepository _identityRepository;
        private readonly DailyLogEventRepository _dailyRepository;

        public SearchableRepositoryTests(ITestOutputHelper output) : base(output) {
            _identityRepository = new IdentityRepository(_configuration);
            _dailyRepository = new DailyLogEventRepository(_configuration);

            RemoveDataAsync().GetAwaiter().GetResult();
        }

        [Fact]
        public async Task CountByQuery() {
            Assert.Equal(0, await _identityRepository.CountAsync());

            var identity = IdentityGenerator.Default;
            var result = await _identityRepository.AddAsync(identity);
            Assert.Equal(identity, result);

            await _client.RefreshAsync();
            Assert.Equal(0, await _identityRepository.CountBySearchAsync(null, "id:test"));
            Assert.Equal(1, await _identityRepository.CountBySearchAsync(null, $"id:{identity.Id}"));
        }

        [Fact]
        public async Task CountByQueryWithTimeSeries() {
            Assert.Equal(0, await _dailyRepository.CountAsync());

            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1)));
            Assert.NotNull(yesterdayLog?.Id);

            var nowLog = await _dailyRepository.AddAsync(LogEventGenerator.Default);
            Assert.NotNull(nowLog?.Id);

            await _client.RefreshAsync();
            Assert.Equal(0, await _dailyRepository.CountBySearchAsync(null, "id:test"));
            Assert.Equal(1, await _dailyRepository.CountBySearchAsync(null, $"id:{nowLog.Id}"));
            Assert.Equal(1, await _dailyRepository.CountBySearchAsync(new ElasticQuery().WithDateRange(utcNow.AddHours(-1), utcNow.AddHours(1), "created"), $"id:{nowLog.Id}"));
            Assert.Equal(0, await _dailyRepository.CountBySearchAsync(new ElasticQuery().WithDateRange(utcNow.AddDays(-1), utcNow.AddHours(-12), "created"), $"id:{nowLog.Id}"));
            Assert.Equal(1, await _dailyRepository.CountBySearchAsync(new ElasticQuery().WithDateRange(utcNow.AddDays(-1), utcNow.AddHours(-12), "created")));
            Assert.Equal(1, await _dailyRepository.CountBySearchAsync(new ElasticQuery().WithDateRange(utcNow.AddHours(-1), utcNow.AddHours(1), "created")));
        }

        [Fact]
        public async Task SearchByQuery() {
            var identity = IdentityGenerator.Default;
            var result = await _identityRepository.AddAsync(identity);
            Assert.Equal(identity, result);

            await _client.RefreshAsync();
            var results = await _identityRepository.SearchAsync(null, "id:test");
            Assert.Equal(0, results.Documents.Count);

            var disposables = new List<IDisposable>(1);
            var countdownEvent = new AsyncCountdownEvent(1);

            try
            {
                var filter = $"id:{identity.Id}";
                disposables.Add(_identityRepository.BeforeQuery.AddSyncHandler((o, args) => {
                    Assert.Equal(filter, ((ElasticQuery)args.Query).Filter);
                    countdownEvent.Signal();
                }));

                results = await _identityRepository.SearchAsync(null, filter);
                Assert.Equal(1, results.Documents.Count);
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
        public async Task SearchByQueryWithTimeSeries() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1), companyId: "1234567890"));
            Assert.NotNull(yesterdayLog?.Id);

            var nowLog = await _dailyRepository.AddAsync(LogEventGenerator.Default);
            Assert.NotNull(nowLog?.Id);

            var results = await _dailyRepository.GetByIdsAsync(new[] { yesterdayLog.Id, nowLog.Id });
            Assert.NotNull(results);
            Assert.Equal(2, results.Total);

            await _client.RefreshAsync();
            results = await _dailyRepository.SearchAsync(new MyAppQuery().WithCompany("test"));
            Assert.Equal(0, results.Documents.Count);

            results = await _dailyRepository.SearchAsync(new MyAppQuery().WithCompany(yesterdayLog.CompanyId));
            Assert.Equal(1, results.Documents.Count);

            results = await _dailyRepository.SearchAsync(new MyAppQuery().WithCompany(yesterdayLog.CompanyId).WithDateRange(utcNow.Subtract(TimeSpan.FromHours(1)), utcNow, "created"));
            Assert.Equal(0, results.Documents.Count);

            results = await _dailyRepository.SearchAsync(new ElasticQuery().WithId(yesterdayLog.Id));
            Assert.Equal(1, results.Documents.Count);
        }

        //[Fact]
        //public async Task GetAggregations() {
        //    throw new NotImplementedException();
        //}
    }
}