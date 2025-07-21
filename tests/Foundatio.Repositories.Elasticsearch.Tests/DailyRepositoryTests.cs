using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using Foundatio.Utility;
using Microsoft.Extensions.Time.Testing;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class DailyRepositoryTests : ElasticRepositoryTestBase
{
    private readonly IFileAccessHistoryRepository _fileAccessHistoryRepository;

    public DailyRepositoryTests(ITestOutputHelper output) : base(output)
    {
        _fileAccessHistoryRepository = new FileAccessHistoryRepository(_configuration.DailyFileAccessHistory);
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    [Fact]
    public async Task AddAsyncWithCustomDateIndex()
    {
        var utcNow = new DateTime(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var history = await _fileAccessHistoryRepository.AddAsync(new FileAccessHistory { Path = "path1", AccessedDateUtc = utcNow }, o => o.ImmediateConsistency());
        Assert.NotNull(history?.Id);

        var result = await _fileAccessHistoryRepository.FindOneAsync(f => f.Id(history.Id));
        Assert.Equal("file-access-history-daily-v1-2023.01.01", result.Data.GetString("index"));
    }

    [Fact]
    public async Task AddAsyncWithCurrentDateViaDocumentsAdding()
    {
        _configuration.TimeProvider = new FakeTimeProvider(new DateTimeOffset(2023, 02, 1, 0, 0, 0, TimeSpan.Zero));

        try
        {
            // NOTE: This has to be async handler as there is no way to remove a sync handler.
            _fileAccessHistoryRepository.DocumentsAdding.AddHandler(OnDocumentsAdding);

            var history = await _fileAccessHistoryRepository.AddAsync(new FileAccessHistory { Path = "path2" }, o => o.ImmediateConsistency());
            Assert.NotNull(history?.Id);

            var result = await _fileAccessHistoryRepository.FindOneAsync(f => f.Id(history.Id));
            Assert.Equal("file-access-history-daily-v1-2023.02.01", result.Data.GetString("index"));
        }
        finally
        {
            _fileAccessHistoryRepository.DocumentsAdding.RemoveHandler(OnDocumentsAdding);
        }
    }

    private Task OnDocumentsAdding(object sender, DocumentsEventArgs<FileAccessHistory> arg)
    {
        foreach (var document in arg.Documents)
        {
            if (document.AccessedDateUtc == DateTime.MinValue || document.AccessedDateUtc > _configuration.TimeProvider.GetUtcNow().UtcDateTime)
                document.AccessedDateUtc = _configuration.TimeProvider.GetUtcNow().UtcDateTime;
        }

        return Task.CompletedTask;
    }

    [Fact]
    public async Task CanAddAsync()
    {
        var history = await _fileAccessHistoryRepository.AddAsync(new FileAccessHistory { AccessedDateUtc = DateTime.UtcNow });
        Assert.NotNull(history?.Id);
    }

    [Fact]
    public async Task AddAsyncConcurrentUpdates()
    {
        // The outer loop divides the iterations into 5 batches, each representing a different day offset.
        // Within each batch, the inner Parallel.ForEachAsync loop performs 10 concurrent updates.
        // This structure ensures controlled concurrency while simulating updates across multiple days.
        for (int index = 0; index < 5; index++)
        {
            await Parallel.ForEachAsync(Enumerable.Range(0, 10), async (_, _) =>
            {
                var history = await _fileAccessHistoryRepository.AddAsync(new FileAccessHistory { AccessedDateUtc = DateTime.UtcNow.AddDays(index) });
                Assert.NotNull(history?.Id);
            });
        }
    }
}
