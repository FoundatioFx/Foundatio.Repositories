using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Foundatio.AsyncEx;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Xunit;
using Xunit.Abstractions;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class QueryableRepositoryTests : ElasticRepositoryTestBase {
        private readonly IIdentityRepository _identityRepository;
        private readonly ILogEventRepository _dailyRepository;

        public QueryableRepositoryTests(ITestOutputHelper output) : base(output) {
            _identityRepository = new IdentityRepository(_configuration);
            _dailyRepository = new DailyLogEventRepository(_configuration);
        }

        public override async Task InitializeAsync() {
            await base.InitializeAsync();
            await RemoveDataAsync();
        }

        [Fact]
        public async Task CountByQueryAsync() {
            Assert.Equal(0, await _identityRepository.CountAsync());

            var identity = IdentityGenerator.Default;
            var result = await _identityRepository.AddAsync(identity, o => o.ImmediateConsistency());
            Assert.Equal(identity, result);
            
            Assert.Equal(0, await _identityRepository.CountByQueryAsync(q => q.FilterExpression("id:test")));
            Assert.Equal(1, await _identityRepository.CountByQueryAsync(q => q.FilterExpression($"id:{identity.Id}")));
        }

        [Fact]
        public async Task CountByQueryWithTimeSeriesAsync() {
            Assert.Equal(0, await _dailyRepository.CountAsync());

            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1)), o => o.ImmediateConsistency());
            Assert.NotNull(yesterdayLog?.Id);

            var nowLog = await _dailyRepository.AddAsync(LogEventGenerator.Default, o => o.ImmediateConsistency());
            Assert.NotNull(nowLog?.Id);

            Assert.Equal(0, await _dailyRepository.CountByQueryAsync(q => q.FilterExpression("id:test")));
            Assert.Equal(1, await _dailyRepository.CountByQueryAsync(q => q.FilterExpression($"id:{nowLog.Id}")));
            Assert.Equal(1, await _dailyRepository.CountByQueryAsync(q => q.DateRange(utcNow.AddHours(-1), utcNow.AddHours(1), "createdUtc").FilterExpression($"id:{nowLog.Id}")));
            Assert.Equal(0, await _dailyRepository.CountByQueryAsync(q => q.DateRange(utcNow.AddDays(-1), utcNow.AddHours(-12), (LogEvent l) => l.CreatedUtc).FilterExpression($"id:{nowLog.Id}")));
            Assert.Equal(1, await _dailyRepository.CountByQueryAsync(q => q.DateRange(utcNow.AddDays(-1), utcNow.AddHours(-12), "created")));
            Assert.Equal(1, await _dailyRepository.CountByQueryAsync(q => q.DateRange(utcNow.AddHours(-1), utcNow.AddHours(1), "createdUtc")));
        }

        [Fact]
        public async Task SearchByQueryAsync() {
            var identity = IdentityGenerator.Default;
            var result = await _identityRepository.AddAsync(identity, o => o.ImmediateConsistency());
            Assert.Equal(identity, result);

            var results = await _identityRepository.QueryAsync(q => q.FilterExpression("id:test"));
            Assert.Equal(0, results.Documents.Count);

            var disposables = new List<IDisposable>(1);
            var countdownEvent = new AsyncCountdownEvent(1);

            try {
                string filter = $"id:{identity.Id}";
                disposables.Add(_identityRepository.BeforeQuery.AddSyncHandler((o, args) => {
                    Assert.Equal(filter, args.Query.GetFilterExpression());
                    countdownEvent.Signal();
                }));

                results = await _identityRepository.QueryAsync(q => q.FilterExpression(filter));
                Assert.Equal(1, results.Documents.Count);
                await countdownEvent.WaitAsync(new CancellationTokenSource(TimeSpan.FromMilliseconds(250)).Token);
                Assert.Equal(0, countdownEvent.CurrentCount);
            } finally {
                foreach (var disposable in disposables)
                    disposable.Dispose();

                disposables.Clear();
            }
        }

        [Fact]
        public async Task SearchByQueryWithTimeSeriesAsync() {
            var utcNow = SystemClock.UtcNow;
            var yesterdayLog = await _dailyRepository.AddAsync(LogEventGenerator.Generate(ObjectId.GenerateNewId(utcNow.AddDays(-1)).ToString(), createdUtc: utcNow.AddDays(-1), companyId: "1234567890"), o => o.ImmediateConsistency());
            Assert.NotNull(yesterdayLog?.Id);

            var nowLog = await _dailyRepository.AddAsync(LogEventGenerator.Default, o => o.ImmediateConsistency());
            Assert.NotNull(nowLog?.Id);

            var results = await _dailyRepository.GetAsync(new[] { yesterdayLog.Id, nowLog.Id });
            Assert.NotNull(results);
            Assert.Equal(2, results.Count);

            var searchResults = await _dailyRepository.QueryAsync(q => q.Company("test"));
            Assert.Equal(0, searchResults.Total);

            searchResults = await _dailyRepository.QueryAsync(q => q.Company(yesterdayLog.CompanyId));
            Assert.Equal(1, searchResults.Total);

            searchResults = await _dailyRepository.QueryAsync(q => q.Company(yesterdayLog.CompanyId).DateRange(utcNow.Subtract(TimeSpan.FromHours(1)), utcNow, "created"));
            Assert.Equal(0, searchResults.Total);
            
            searchResults = await _dailyRepository.QueryAsync(q => q.Company(yesterdayLog.CompanyId).DateRange(utcNow.Subtract(TimeSpan.FromHours(1)), DateTime.MaxValue, (LogEvent e) => e.CreatedUtc));
            Assert.Equal(0, searchResults.Total);

            searchResults = await _dailyRepository.QueryAsync(q => q.Id(yesterdayLog.Id));
            Assert.Equal(1, searchResults.Total);
        }

        //[Fact]
        //public async Task GetAggregations() {
        //    throw new NotImplementedException();
        //}
    }
}