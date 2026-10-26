using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class TimeSeriesIndexCultureTests
{
    [Theory]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    [InlineData("fr-FR")]
    [InlineData("th-TH")]
    [InlineData("ar-SA")]
    public async Task DeleteOldIndexesAsync_DailyNamesProducedInCurrentCulture_SelectsOnlyExpiredIndex(string culture)
    {
        // Arrange
        using var scope = new CultureScope(culture);
        var now = new DateTimeOffset(2026, 10, 6, 0, 0, 0, TimeSpan.Zero);
        using var configuration = new NoClientConfiguration(new FakeTimeProvider(now));
        using var index = new TestDailyIndex(configuration) { MaxIndexAge = TimeSpan.FromDays(30) };
        string expired = index.GetVersionedIndex(new DateTime(2026, 7, 5, 0, 0, 0, DateTimeKind.Utc));
        string recent = index.GetVersionedIndex(new DateTime(2026, 10, 5, 0, 0, 0, DateTimeKind.Utc));
        using var maintenanceScope = new CultureScope("en-US");

        // Act
        await index.DeleteExpiredAsync(expired, recent);

        // Assert
        Assert.Equal(new[] { expired }, index.DeletedIndexes);
    }

    [Theory]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    [InlineData("fr-FR")]
    [InlineData("th-TH")]
    [InlineData("ar-SA")]
    public async Task DeleteOldIndexesAsync_MonthlyNamesProducedInCurrentCulture_SelectsOnlyExpiredIndex(string culture)
    {
        // Arrange
        using var scope = new CultureScope(culture);
        var now = new DateTimeOffset(2026, 10, 6, 0, 0, 0, TimeSpan.Zero);
        using var configuration = new NoClientConfiguration(new FakeTimeProvider(now));
        using var index = new TestMonthlyIndex(configuration) { MaxIndexAge = TimeSpan.FromDays(30) };
        string expired = index.GetVersionedIndex(new DateTime(2026, 7, 5, 0, 0, 0, DateTimeKind.Utc));
        string recent = index.GetVersionedIndex(new DateTime(2026, 10, 5, 0, 0, 0, DateTimeKind.Utc));
        using var maintenanceScope = new CultureScope("en-US");

        // Act
        await index.DeleteExpiredAsync(expired, recent);

        // Assert
        Assert.Equal(new[] { expired }, index.DeletedIndexes);
    }

    [Theory]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    [InlineData("fr-FR")]
    [InlineData("th-TH")]
    [InlineData("ar-SA")]
    public void GetIndex_DailyIndexWithCurrentCulture_ReturnsGregorianAlias(string culture)
    {
        // Arrange
        using var scope = new CultureScope(culture);
        using var configuration = new NoClientConfiguration();
        using var index = new TestDailyIndex(configuration);
        var date = new DateTime(2026, 10, 5, 12, 34, 56, DateTimeKind.Utc);

        // Act
        string name = index.GetIndex(date);

        // Assert
        Assert.Equal("events-2026.10.05", name);
    }

    [Theory]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    [InlineData("fr-FR")]
    [InlineData("th-TH")]
    [InlineData("ar-SA")]
    public void GetIndex_MonthlyIndexWithCurrentCulture_ReturnsGregorianAlias(string culture)
    {
        // Arrange
        using var scope = new CultureScope(culture);
        using var configuration = new NoClientConfiguration();
        using var index = new TestMonthlyIndex(configuration);
        var date = new DateTime(2026, 10, 5, 12, 34, 56, DateTimeKind.Utc);

        // Act
        string name = index.GetIndex(date);

        // Assert
        Assert.Equal("events-2026.10", name);
    }

    [Theory]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    [InlineData("fr-FR")]
    [InlineData("th-TH")]
    [InlineData("ar-SA")]
    public void GetVersionedIndex_DailyIndexWithCurrentCulture_RoundTripsGregorianDate(string culture)
    {
        // Arrange
        using var scope = new CultureScope(culture);
        using var configuration = new NoClientConfiguration();
        using var index = new TestDailyIndex(configuration);
        var date = new DateTime(2026, 10, 5, 12, 34, 56, DateTimeKind.Utc);

        // Act
        string name = index.GetVersionedIndex(date);
        var parsed = index.ParseDate(name);
        string explicitVersionName = index.GetVersionedIndex(date, 3);
        var explicitVersionDate = index.ParseDate(explicitVersionName);

        // Assert
        Assert.Equal("events-v2-2026.10.05", name);
        Assert.Equal(date.Date, parsed);
        Assert.Equal(DateTimeKind.Utc, parsed.Kind);
        Assert.Equal("events-v3-2026.10.05", explicitVersionName);
        Assert.Equal(date.Date, explicitVersionDate);
    }

    [Theory]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    [InlineData("fr-FR")]
    [InlineData("th-TH")]
    [InlineData("ar-SA")]
    public void GetVersionedIndex_MonthlyIndexWithCurrentCulture_RoundTripsGregorianDate(string culture)
    {
        // Arrange
        using var scope = new CultureScope(culture);
        using var configuration = new NoClientConfiguration();
        using var index = new TestMonthlyIndex(configuration);
        var date = new DateTime(2026, 10, 5, 12, 34, 56, DateTimeKind.Utc);
        var expectedDate = new DateTime(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc);

        // Act
        string name = index.GetVersionedIndex(date);
        var parsed = index.ParseDate(name);
        string explicitVersionName = index.GetVersionedIndex(date, 3);
        var explicitVersionDate = index.ParseDate(explicitVersionName);

        // Assert
        Assert.Equal("events-v2-2026.10", name);
        Assert.Equal(expectedDate, parsed);
        Assert.Equal(DateTimeKind.Utc, parsed.Kind);
        Assert.Equal("events-v3-2026.10", explicitVersionName);
        Assert.Equal(expectedDate, explicitVersionDate);
    }

    private sealed class CultureScope : IDisposable
    {
        private readonly CultureInfo _originalCulture = CultureInfo.CurrentCulture;
        private readonly CultureInfo _originalUICulture = CultureInfo.CurrentUICulture;

        public CultureScope(string culture)
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo(culture);
        }

        public void Dispose()
        {
            CultureInfo.CurrentCulture = _originalCulture;
            CultureInfo.CurrentUICulture = _originalUICulture;
        }
    }

    private sealed class NoClientConfiguration : ElasticConfiguration
    {
        public NoClientConfiguration(TimeProvider? timeProvider = null) : base(timeProvider: timeProvider) { }

        protected override ElasticsearchClient CreateElasticClient()
        {
            throw new InvalidOperationException("Culture tests must not access Elasticsearch.");
        }
    }

    private sealed class TestDailyIndex : DailyIndex
    {
        public TestDailyIndex(IElasticConfiguration configuration) : base(configuration, "events", 2) { }

        public List<string> DeletedIndexes { get; } = new();

        public Task DeleteExpiredAsync(params string[] names)
        {
            var indexes = names.Select(name => new IndexInfo { Index = name, DateUtc = GetIndexDate(name) }).ToList();
            return DeleteOldIndexesAsync(indexes);
        }

        public DateTime ParseDate(string name) => GetIndexDate(name);

        protected override Task DeleteIndexAsync(string name)
        {
            DeletedIndexes.Add(name);
            return Task.CompletedTask;
        }
    }

    private sealed class TestMonthlyIndex : MonthlyIndex
    {
        public TestMonthlyIndex(IElasticConfiguration configuration) : base(configuration, "events", 2) { }

        public List<string> DeletedIndexes { get; } = new();

        public Task DeleteExpiredAsync(params string[] names)
        {
            var indexes = names.Select(name => new IndexInfo { Index = name, DateUtc = GetIndexDate(name) }).ToList();
            return DeleteOldIndexesAsync(indexes);
        }

        public DateTime ParseDate(string name) => GetIndexDate(name);

        protected override Task DeleteIndexAsync(string name)
        {
            DeletedIndexes.Add(name);
            return Task.CompletedTask;
        }
    }
}
