using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using Foundatio.Utility;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class MonthlyRepositoryTests : ElasticRepositoryTestBase
{
    private readonly IFileAccessHistoryRepository _fileAccessHistoryRepository;

    public MonthlyRepositoryTests(ITestOutputHelper output) : base(output)
    {
        _fileAccessHistoryRepository = new FileAccessHistoryRepository(_configuration.MonthlyFileAccessHistory);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    [Fact]
    public async Task AddAsyncWithCustomDateIndex()
    {
        var utcNow = new DateTime(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var history = await _fileAccessHistoryRepository.AddAsync(new FileAccessHistory { Path = "path1", AccessedDateUtc = utcNow }, o => o.ImmediateConsistency());
        Assert.NotNull(history);
        Assert.NotNull(history.Id);

        var result = await _fileAccessHistoryRepository.FindOneAsync(f => f.Id(history.Id));
        Assert.Equal("file-access-history-monthly-v1-2023.01", result.Data.GetString("index"));
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
            Assert.NotNull(history);
            Assert.NotNull(history.Id);

            var result = await _fileAccessHistoryRepository.FindOneAsync(f => f.Id(history.Id));
            Assert.Equal("file-access-history-monthly-v1-2023.02", result.Data.GetString("index"));
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
}
