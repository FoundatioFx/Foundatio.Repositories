using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ExternallyManagedIndexTests : ElasticRepositoryTestBase
{
    private readonly ExternallyManagedLogEventRepository _externallyManagedLogEventRepository;

    public ExternallyManagedIndexTests(ITestOutputHelper output) : base(output)
    {
        _externallyManagedLogEventRepository = new ExternallyManagedLogEventRepository(_configuration);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    private sealed class BroadFilterVersionedLogEventIndex : DailyIndex<LogEvent>
    {
        public BroadFilterVersionedLogEventIndex(IElasticConfiguration configuration)
            : base(configuration, "broad-filter-logevents", 1, doc => ((LogEvent)doc).Date.UtcDateTime)
        {
        }

        protected override string MappingIndexPattern => $"{Name}-*";
    }

    private async Task<string> IndexRawLogEventsAsync(DateTime utcDate, bool includeId, int count)
    {
        string index = $"{_configuration.ExternallyManagedLogEvents.Name}-{utcDate:yyyy.MM.dd}";
        for (int i = 0; i < count; i++)
        {
            var document = new Dictionary<string, object>
            {
                ["companyId"] = ObjectId.GenerateNewId().ToString(),
                ["message"] = $"message {i}",
                ["value"] = i,
                ["date"] = utcDate
            };
            if (includeId)
                document["id"] = ObjectId.GenerateNewId().ToString();

            var response = await _client.IndexAsync(document, d => d.Index(index).Refresh(Refresh.True), TestCancellationToken);
            Assert.True(response.IsValidResponse, response.DebugInformation);
        }

        return index;
    }

    [Fact]
    public async Task CountAsync_OnExternallyManagedIndexWithNoIdMapping_ReturnsTotal()
    {
        var utcDate = DateTime.UtcNow;
        string index = await IndexRawLogEventsAsync(utcDate, includeId: false, count: 3);

        try
        {
            var result = await _externallyManagedLogEventRepository.CountAsync(q => q.Index(utcDate, utcDate));

            Assert.Equal(3, result.Total);
        }
        finally
        {
            await _client.Indices.DeleteAsync(index, cancellationToken: TestCancellationToken);
        }
    }

    [Fact]
    public async Task FindAsync_OnExternallyManagedIndexThroughUmbrellaAlias_ReturnsDocuments()
    {
        var utcDate = DateTime.UtcNow;
        string index = await IndexRawLogEventsAsync(utcDate, includeId: false, count: 3);
        string alias = _configuration.ExternallyManagedLogEvents.Name;

        try
        {
            var aliasResponse = await _client.Indices.UpdateAliasesAsync(x => x.Actions(
                a => a.Add(ad => ad.Alias(alias).Index(index))), cancellationToken: TestCancellationToken);
            Assert.True(aliasResponse.IsValidResponse, aliasResponse.DebugInformation);

            var results = await _externallyManagedLogEventRepository.FindAsync(q => q.SortAscending("value"));

            Assert.Equal(3, results.Documents.Count);
        }
        finally
        {
            await _client.Indices.DeleteAsync(index, cancellationToken: TestCancellationToken);
        }
    }

    [Fact]
    public async Task FindAsync_OnExternallyManagedIndexWithNoIdMapping_ReturnsDocuments()
    {
        // Arrange
        // Regression test for https://github.com/FoundatioFx/Foundatio.Repositories/issues/305: mirrors a
        // Logstash-created daily index with no "id" field at all. Before the fix, DefaultSortQueryBuilder
        // unconditionally injected an id tiebreaker and every FindAsync call failed with "all shards failed".
        var utcDate = DateTime.UtcNow;
        string index = await IndexRawLogEventsAsync(utcDate, includeId: false, count: 3);

        try
        {
            // Act
            var results = await _externallyManagedLogEventRepository.FindAsync(q => q.Index(utcDate, utcDate));

            // Assert
            Assert.Equal(3, results.Documents.Count);
        }
        finally
        {
            await _client.Indices.DeleteAsync(index, cancellationToken: TestCancellationToken);
        }
    }

    [Fact]
    public async Task FindAsync_OnExternallyManagedIndexWithTextMappedId_ReturnsDocuments()
    {
        // Arrange
        // The raw id is dynamically mapped as text+keyword, so the index must opt out of the
        // code-declared id tiebreaker just as it does when id is entirely unmapped.
        var utcDate = DateTime.UtcNow;
        string index = await IndexRawLogEventsAsync(utcDate, includeId: true, count: 3);

        try
        {
            // Act
            var results = await _externallyManagedLogEventRepository.FindAsync(q => q.Index(utcDate, utcDate));

            // Assert
            Assert.Equal(3, results.Documents.Count);
        }
        finally
        {
            await _client.Indices.DeleteAsync(index, cancellationToken: TestCancellationToken);
        }
    }

    [Fact]
    public async Task FindAsync_OnExternallyManagedIndexWithUnresolvableServerMapping_LogsWarning()
    {
        // Arrange
        // An explicit sort forces the mapping resolver's first lookup while no matching index exists.
        int start = Log.LogEntries.Count;

        // Act
        var results = await _externallyManagedLogEventRepository.FindAsync(q => q.SortAscending("value"));

        // Assert
        Assert.Empty(results.Documents);
        Assert.Contains(Log.LogEntries.Skip(start), l => l.LogLevel == LogLevel.Warning && l.Message.Contains("field resolution will fall back to the code-declared mapping only"));
    }

    [Fact]
    public async Task FindAsync_WithLiveSearchAfterPagingOnExternallyManagedIndexWithoutSort_ThrowsQueryValidationException()
    {
        var utcDate = DateTime.UtcNow;
        string index = await IndexRawLogEventsAsync(utcDate, includeId: false, count: 3);

        try
        {
            var exception = await Assert.ThrowsAsync<QueryValidationException>(() =>
                _externallyManagedLogEventRepository.FindAsync(
                    q => q.Index(utcDate, utcDate),
                    o => o.PageLimit(1).SearchAfterPaging()));

            Assert.Contains("requires at least one sortable field", exception.Message);
        }
        finally
        {
            await _client.Indices.DeleteAsync(index, cancellationToken: TestCancellationToken);
        }
    }

    [Fact]
    public async Task FindAsync_WithPointInTimePagingOnExternallyManagedIndexWithoutSort_SupportsBackwardNavigation()
    {
        var utcDate = DateTime.UtcNow;
        string index = await IndexRawLogEventsAsync(utcDate, includeId: false, count: 5);
        var pointInTime = (ISupportPointInTime)_externallyManagedLogEventRepository;
        string? pointInTimeId = null;

        try
        {
            var firstPage = await _externallyManagedLogEventRepository.FindAsync(
                q => q.Index(utcDate, utcDate),
                o => o.PageLimit(2).SearchAfterPaging(SearchAfterPagingMode.PointInTime));
            pointInTimeId = firstPage.GetPointInTimeId();
            var firstPageValues = firstPage.Documents.Select(d => d.Value).ToArray();

            var secondPage = await _externallyManagedLogEventRepository.FindAsync(
                q => q.Index(utcDate, utcDate),
                o => o.PageLimit(2)
                    .SearchAfterPaging(SearchAfterPagingMode.PointInTime)
                    .PointInTimeId(pointInTimeId)
                    .SearchAfterToken(firstPage.GetSearchAfterToken(), _serializer));
            pointInTimeId = secondPage.GetPointInTimeId();

            var backToFirstPage = await _externallyManagedLogEventRepository.FindAsync(
                q => q.Index(utcDate, utcDate),
                o => o.PageLimit(2)
                    .SearchAfterPaging(SearchAfterPagingMode.PointInTime)
                    .PointInTimeId(pointInTimeId)
                    .SearchBeforeToken(secondPage.GetSearchBeforeToken(), _serializer));
            pointInTimeId = backToFirstPage.GetPointInTimeId();

            Assert.Equal(firstPageValues, backToFirstPage.Documents.Select(d => d.Value));
        }
        finally
        {
            if (!String.IsNullOrEmpty(pointInTimeId))
                await pointInTime.ClosePointInTimeAsync(pointInTimeId);

            await _client.Indices.DeleteAsync(index, cancellationToken: TestCancellationToken);
        }
    }

    [Fact]
    public async Task FindAsync_WithPointInTimeSearchAfterPagingOnExternallyManagedIndexWithoutSort_ReturnsAllDocuments()
    {
        var utcDate = DateTime.UtcNow;
        const int documentCount = 10;
        string index = await IndexRawLogEventsAsync(utcDate, includeId: false, count: documentCount);

        try
        {
            var results = await _externallyManagedLogEventRepository.FindAsync(
                q => q.Index(utcDate, utcDate),
                o => o.PageLimit(3).SearchAfterPaging(SearchAfterPagingMode.PointInTime));
            var viewedValues = new HashSet<int>();
            int pagedRecords = 0;
            do
            {
                viewedValues.AddRange(results.Documents.Select(d => d.Value));
                pagedRecords += results.Documents.Count;
            } while (await results.NextPageAsync());

            Assert.Equal(documentCount, pagedRecords);
            Assert.Equal(documentCount, viewedValues.Count);
        }
        finally
        {
            await _client.Indices.DeleteAsync(index, cancellationToken: TestCancellationToken);
        }
    }

    [Fact]
    public async Task FindAsync_WithSearchAfterPagingOnExternallyManagedIndexWithNoIdMapping_ReturnsAllDocumentsWithoutDuplicates()
    {
        // Arrange
        // With no id field, Live paging relies on the caller's stable, unique value sort.
        var utcDate = DateTime.UtcNow;
        const int documentCount = 25;
        string index = await IndexRawLogEventsAsync(utcDate, includeId: false, count: documentCount);

        try
        {
            // Act
            var results = await _externallyManagedLogEventRepository.FindAsync(
                q => q.Index(utcDate, utcDate).SortAscending("value"),
                o => o.PageLimit(7).SearchAfterPaging());
            var viewedValues = new HashSet<int>();
            int pagedRecords = 0;
            int pageCount = 0;
            do
            {
                pageCount++;
                viewedValues.AddRange(results.Documents.Select(d => d.Value));
                pagedRecords += results.Documents.Count;
            } while (await results.NextPageAsync());

            // Assert
            Assert.True(pageCount >= 4);
            Assert.Equal(documentCount, pagedRecords);
            Assert.Equal(documentCount, viewedValues.Count);
        }
        finally
        {
            await _client.Indices.DeleteAsync(index, cancellationToken: TestCancellationToken);
        }
    }

    [Fact]
    public async Task MappingResolver_WithMalformedExternalIndexName_UsesLatestValidDatedMapping()
    {
        string prefix = _configuration.ExternallyManagedLogEvents.Name;
        string olderValidIndex = $"{prefix}-{DateTime.UtcNow.AddDays(-1):yyyy.MM.dd}";
        string latestValidIndex = $"{prefix}-{DateTime.UtcNow:yyyy.MM.dd}";
        string malformedIndex = $"{prefix}-not-a-date";
        var indexes = Indices.Parse($"{prefix}-*");

        await using AsyncDisposableAction cleanup = new(async () =>
            await _client.Indices.DeleteAsync(indexes, d => d.IgnoreUnavailable(), TestCancellationToken));

        var olderValidResponse = await _client.Indices.CreateAsync(olderValidIndex, d => d
            .Mappings(m => m.Properties(p => p.Keyword("message"))), TestCancellationToken);
        var latestValidResponse = await _client.Indices.CreateAsync(latestValidIndex, d => d
            .Mappings(m => m.Properties(p => p.Text("message", t => t.Fields(f => f.Keyword("sort"))))), TestCancellationToken);
        var malformedResponse = await _client.Indices.CreateAsync(malformedIndex, d => d
            .Mappings(m => m.Properties(p => p.Keyword("message"))), TestCancellationToken);
        Assert.True(olderValidResponse.IsValidResponse, olderValidResponse.DebugInformation);
        Assert.True(latestValidResponse.IsValidResponse, latestValidResponse.DebugInformation);
        Assert.True(malformedResponse.IsValidResponse, malformedResponse.DebugInformation);

        _configuration.ExternallyManagedLogEvents.MappingResolver.RefreshMapping();

        Assert.Equal("message.sort", _configuration.ExternallyManagedLogEvents.MappingResolver.GetSortFieldName("message"));
    }

    [Fact]
    public async Task MappingResolver_WithSameDateAndMultipleVersions_PrefersHighestParsedVersion()
    {
        // Arrange: two indexes share the newest date and differ only by parsed version number.
        // "v10" sorts below "v9" in ordinal name order, so this pins numeric (not lexical)
        // tie-breaking -- and unlike a single-candidate test, it fails if selection ever falls
        // back to Elasticsearch's response order, which is unspecified.
        string prefix = "broad-filter-logevents";
        string today = DateTime.UtcNow.ToString("yyyy.MM.dd");
        string v9Index = $"{prefix}-v9-{today}";
        string v10Index = $"{prefix}-v10-{today}";

        await using AsyncDisposableAction cleanup = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{prefix}-v*"), d => d.IgnoreUnavailable(), TestCancellationToken));

        var v9Response = await _client.Indices.CreateAsync(v9Index, d => d
            .Mappings(m => m.Properties(p => p.Keyword("message"))), TestCancellationToken);
        var v10Response = await _client.Indices.CreateAsync(v10Index, d => d
            .Mappings(m => m.Properties(p => p.Text("message", t => t.Fields(f => f.Keyword("sort"))))), TestCancellationToken);
        Assert.True(v9Response.IsValidResponse, v9Response.DebugInformation);
        Assert.True(v10Response.IsValidResponse, v10Response.DebugInformation);

        // Act: default GetIndexDate parses "{name}-v{version}-{date}", so both candidates survive
        // malformed-name exclusion; the broad filter makes them compete on the same date.
        using var index = new BroadFilterVersionedLogEventIndex(_configuration);
        string? sortFieldName = index.MappingResolver.GetSortFieldName("message");

        // Assert
        Assert.Equal("message.sort", sortFieldName);
    }

    [Fact]
    public async Task ReadOperations_OnExternallyManagedIndexWithNoIdMapping_RemainScopedAndReturnDocuments()
    {
        // Arrange
        var utcDate = DateTime.UtcNow;
        string index = await IndexRawLogEventsAsync(utcDate, includeId: false, count: 3);
        string alias = _configuration.ExternallyManagedLogEvents.Name;

        try
        {
            var aliasResponse = await _client.Indices.UpdateAliasesAsync(x => x.Actions(
                a => a.Add(ad => ad.Alias(alias).Index(index))), cancellationToken: TestCancellationToken);
            Assert.True(aliasResponse.IsValidResponse, aliasResponse.DebugInformation);

            // Act
            var projected = await _externallyManagedLogEventRepository.FindAsAsync<LogEvent>(q => q.Index(utcDate, utcDate));
            var first = await _externallyManagedLogEventRepository.FindOneAsync(q => q.Index(utcDate, utcDate));
            var count = await _externallyManagedLogEventRepository.CountAsync(q => q.Index(utcDate, utcDate));
            bool exists = await _externallyManagedLogEventRepository.ExistsAsync(q => q.Index(utcDate, utcDate));
            var all = await _externallyManagedLogEventRepository.GetAllAsync();

            // Assert
            Assert.Equal(3, projected.Documents.Count);
            Assert.NotNull(first.Document);
            Assert.Equal(3, count.Total);
            Assert.True(exists);
            Assert.Equal(3, all.Documents.Count);
        }
        finally
        {
            await _client.Indices.DeleteAsync(index, cancellationToken: TestCancellationToken);
        }
    }
}
