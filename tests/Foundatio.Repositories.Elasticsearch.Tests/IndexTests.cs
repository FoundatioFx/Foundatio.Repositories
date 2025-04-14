using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Utility;
using Microsoft.Extensions.Time.Testing;
using Xunit;
using Xunit.Abstractions;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class IndexTests : ElasticRepositoryTestBase
{
    public IndexTests(ITestOutputHelper output) : base(output)
    {
        Log.SetLogLevel<EmployeeRepository>(LogLevel.Warning);
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync(false);
    }

    [Theory]
    [MemberData(nameof(AliasesDatesToCheck))]
    public async Task CanCreateDailyAliasesAsync(DateTime utcNow)
    {
        _configuration.TimeProvider = new FakeTimeProvider(new DateTimeOffset(utcNow, TimeSpan.Zero));
        var index = new DailyEmployeeIndex(_configuration, 1);
        await index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        IEmployeeRepository repository = new EmployeeRepository(index);

        for (int i = 0; i < 35; i += 5)
        {
            var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractDays(i)));
            Assert.NotNull(employee?.Id);

            Assert.Equal(1, await index.GetCurrentVersionAsync());
            var existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
            _logger.LogRequest(existsResponse);
            Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
            Assert.True(existsResponse.Exists);

            var aliasesResponse = await _client.Indices.GetAliasAsync(index.GetIndex(employee.CreatedUtc));
            _logger.LogRequest(aliasesResponse);
            Assert.True(aliasesResponse.IsValidResponse);
            Assert.Single(aliasesResponse.Indices);

            var aliases = aliasesResponse.Indices.Values.Single().Aliases.Select(s => s.Key).ToList();
            aliases.Sort();

            Assert.Equal(GetExpectedEmployeeDailyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));
        }
    }

    [Theory]
    [MemberData(nameof(AliasesDatesToCheck))]
    public async Task CanCreateMonthlyAliasesAsync(DateTime utcNow)
    {
        _configuration.TimeProvider = new FakeTimeProvider(new DateTimeOffset(utcNow, TimeSpan.Zero));
        var index = new MonthlyEmployeeIndex(_configuration, 1);
        await index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        IEmployeeRepository repository = new EmployeeRepository(index);

        for (int i = 0; i < 4; i++)
        {
            var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractMonths(i)));
            Assert.NotNull(employee?.Id);

            Assert.Equal(1, await index.GetCurrentVersionAsync());
            var existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
            _logger.LogRequest(existsResponse);
            Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
            Assert.True(existsResponse.Exists);

            var aliasesResponse = await _client.Indices.GetAliasAsync(index.GetIndex(employee.CreatedUtc));
            _logger.LogRequest(aliasesResponse);
            Assert.True(aliasesResponse.IsValidResponse);
            Assert.Single(aliasesResponse.Indices);

            var aliases = aliasesResponse.Indices.Values.Single().Aliases.Select(s => s.Key).ToList();
            aliases.Sort();

            Assert.Equal(GetExpectedEmployeeMonthlyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));
        }
    }

    public static IEnumerable<object[]> AliasesDatesToCheck => new List<object[]> {
        new object[] { new DateTime(2016, 2, 29, 0, 0, 0, DateTimeKind.Utc) },
        new object[] { new DateTime(2016, 8, 31, 0, 0, 0, DateTimeKind.Utc) },
        new object[] { new DateTime(2016, 9, 1, 0, 0, 0, DateTimeKind.Utc) },
        new object[] { new DateTime(2017, 3, 1, 0, 0, 0, DateTimeKind.Utc) },
        new object[] { new DateTime(2017, 4, 10, 18, 43, 39, 0, DateTimeKind.Utc) },
        new object[] { new DateTime(2017, 12, 31, 11, 59, 59, DateTimeKind.Utc).EndOfDay() },
        new object[] { DateTime.UtcNow }
    }.ToArray();

    [Fact]
    public async Task GetByDateBasedIndexAsync()
    {
        Log.DefaultMinimumLevel = LogLevel.Trace;

        await _configuration.DailyLogEvents.ConfigureAsync();

        var indexes = await _client.GetIndicesPointingToAliasAsync(_configuration.DailyLogEvents.Name);
        Assert.Empty(indexes);

        var alias = await _client.Indices.GetAliasAsync(_configuration.DailyLogEvents.Name);
        _logger.LogRequest(alias);
        Assert.False(alias.IsValidResponse);

        var utcNow = DateTime.UtcNow;
        ILogEventRepository repository = new DailyLogEventRepository(_configuration);
        var logEvent = await repository.AddAsync(LogEventGenerator.Generate(createdUtc: utcNow));
        Assert.NotNull(logEvent?.Id);

        logEvent = await repository.AddAsync(LogEventGenerator.Generate(createdUtc: utcNow.SubtractDays(1)), o => o.ImmediateConsistency());
        Assert.NotNull(logEvent?.Id);

        alias = await _client.Indices.GetAliasAsync(_configuration.DailyLogEvents.Name);
        _logger.LogRequest(alias);
        Assert.True(alias.IsValidResponse);
        Assert.Equal(2, alias.Indices.Count);

        indexes = await _client.GetIndicesPointingToAliasAsync(_configuration.DailyLogEvents.Name);
        Assert.Equal(2, indexes.Count);

        await repository.RemoveAllAsync(o => o.ImmediateConsistency());

        Assert.Equal(0, await repository.CountAsync());
    }

    [Fact]
    public async Task MaintainWillCreateAliasOnVersionedIndexAsync()
    {
        var version1Index = new VersionedEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new VersionedEmployeeIndex(_configuration, 2);
        await version2Index.DeleteAsync();

        // Indexes don't exist yet so the current version will be the index version.
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName)).Exists);
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        Assert.True((await _client.Indices.ExistsAsync(version2Index.VersionedName)).Exists);
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        // delete all aliases
        await _configuration.Cache.RemoveAllAsync();
        await DeleteAliasesAsync(version1Index.VersionedName);
        await DeleteAliasesAsync(version2Index.VersionedName);

        await _client.Indices.RefreshAsync(Indices.All);
        var aliasesResponse = await _client.Indices.GetAliasAsync($"{version1Index.VersionedName},{version2Index.VersionedName}");
        Assert.Empty(aliasesResponse.Indices.Values.SelectMany(i => i.Aliases));

        // Indexes exist but no alias so the oldest index version will be used.
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        await version1Index.MaintainAsync();
        aliasesResponse = await _client.Indices.GetAliasAsync(version1Index.VersionedName);
        Assert.Single(aliasesResponse.Indices.Single().Value.Aliases);
        aliasesResponse = await _client.Indices.GetAliasAsync(version2Index.VersionedName);
        Assert.Empty(aliasesResponse.Indices.Single().Value.Aliases);

        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());
    }

    [Fact]
    public async Task MaintainWillCreateAliasesOnTimeSeriesIndexAsync()
    {
        var utcNow = DateTimeOffset.UtcNow;
        _configuration.TimeProvider = new FakeTimeProvider(utcNow);
        var version1Index = new DailyEmployeeIndex(_configuration, 1);
        await version1Index.DeleteAsync();

        var version2Index = new DailyEmployeeIndex(_configuration, 2);
        await version2Index.DeleteAsync();

        // Indexes don't exist yet so the current version will be the index version.
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        await using AsyncDisposableAction _ = new(() => version1Index.DeleteAsync());
        await version1Index.ConfigureAsync();
        await version1Index.EnsureIndexAsync(utcNow.UtcDateTime);
        Assert.True((await _client.Indices.ExistsAsync(version1Index.GetVersionedIndex(utcNow.UtcDateTime))).Exists);
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());

        // delete all aliases
        await _configuration.Cache.RemoveAllAsync();
        await DeleteAliasesAsync(version1Index.GetVersionedIndex(utcNow.UtcDateTime));

        await using AsyncDisposableAction version2Scope = new(() => version2Index.DeleteAsync());
        await version2Index.ConfigureAsync();
        await version2Index.EnsureIndexAsync(utcNow.UtcDateTime);
        Assert.True((await _client.Indices.ExistsAsync(version2Index.GetVersionedIndex(utcNow.UtcDateTime))).Exists);
        Assert.Equal(2, await version2Index.GetCurrentVersionAsync());

        // delete all aliases
        await _configuration.Cache.RemoveAllAsync();
        await DeleteAliasesAsync(version2Index.GetVersionedIndex(utcNow.UtcDateTime));

        await _client.Indices.RefreshAsync(Indices.All);
        var aliasesResponse = await _client.Indices.GetAliasAsync($"{version1Index.GetVersionedIndex(utcNow.UtcDateTime)},{version2Index.GetVersionedIndex(utcNow.UtcDateTime)}");
        Assert.Empty(aliasesResponse.Indices.Values.SelectMany(i => i.Aliases));

        // Indexes exist but no alias so the oldest index version will be used.
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        await version1Index.MaintainAsync();
        aliasesResponse = await _client.Indices.GetAliasAsync(version1Index.GetVersionedIndex(utcNow.UtcDateTime));
        Assert.Equal(version1Index.Aliases.Count + 1, aliasesResponse.Indices.Single().Value.Aliases.Count);
        aliasesResponse = await _client.Indices.GetAliasAsync(version2Index.GetVersionedIndex(utcNow.UtcDateTime));
        Assert.Empty(aliasesResponse.Indices.Single().Value.Aliases);

        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());
    }

    private async Task DeleteAliasesAsync(string index)
    {
        var aliasesResponse = await _client.Indices.GetAliasAsync(index);
        var aliases = aliasesResponse.Indices.Single(a => a.Key == index).Value.Aliases.Select(s => s.Key).ToList();
        foreach (string alias in aliases)
        {
            await _client.Indices.DeleteAliasAsync(new DeleteAliasRequest(index, alias));
        }
    }

    [Fact]
    public async Task MaintainDailyIndexesAsync()
    {
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow.Subtract(TimeSpan.FromDays(15)));
        _configuration.TimeProvider = timeProvider;
        var index = new DailyEmployeeIndex(_configuration, 1);
        await index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        IEmployeeRepository repository = new EmployeeRepository(index);

        var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: timeProvider.GetUtcNow().UtcDateTime), o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        await index.MaintainAsync();
        Assert.Equal(1, await index.GetCurrentVersionAsync());
        var existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        var aliasesResponse = await _client.Indices.GetAliasAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Single(aliasesResponse.Indices);
        var aliases = aliasesResponse.Indices.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeDailyAliases(index, timeProvider.GetUtcNow().UtcDateTime, employee.CreatedUtc), String.Join(", ", aliases));

        timeProvider.Advance(TimeSpan.FromDays(6));
        index.MaxIndexAge = TimeSpan.FromDays(10);
        await index.MaintainAsync();
        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        aliasesResponse = await _client.Indices.GetAliasAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Single(aliasesResponse.Indices);
        aliases = aliasesResponse.Indices.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeDailyAliases(index, timeProvider.GetUtcNow().UtcDateTime, employee.CreatedUtc), String.Join(", ", aliases));

        timeProvider.Advance(TimeSpan.FromDays(9));
        await index.MaintainAsync();
        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.False(existsResponse.Exists);

        aliasesResponse = await _client.Indices.GetAliasAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.False(aliasesResponse.IsValidResponse);
    }

    [Fact]
    public async Task MaintainMonthlyIndexesAsync()
    {
        var utcNow = new DateTimeOffset(2016, 8, 31, 0, 0, 0, TimeSpan.Zero);
        var timeProvider = new FakeTimeProvider(utcNow);
        _configuration.TimeProvider = timeProvider;
        var index = new MonthlyEmployeeIndex(_configuration, 1)
        {
            MaxIndexAge = timeProvider.GetUtcNow().UtcDateTime.EndOfMonth() - timeProvider.GetUtcNow().UtcDateTime.SubtractMonths(4).StartOfMonth()
        };
        await index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        IEmployeeRepository repository = new EmployeeRepository(index);

        for (int i = 0; i < 4; i++)
        {
            var created = utcNow.SubtractMonths(i);
            var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: created.UtcDateTime));
            Assert.NotNull(employee?.Id);

            Assert.Equal(1, await index.GetCurrentVersionAsync());
            var existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
            _logger.LogRequest(existsResponse);
            Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
            Assert.True(existsResponse.Exists);

            var aliasesResponse = await _client.Indices.GetAliasAsync(index.GetIndex(employee.CreatedUtc));
            _logger.LogRequest(aliasesResponse);
            Assert.True(aliasesResponse.IsValidResponse);
            Assert.Single(aliasesResponse.Indices);

            var aliases = aliasesResponse.Indices.Values.Single().Aliases.Select(s => s.Key).ToList();
            aliases.Sort();

            Assert.Equal(GetExpectedEmployeeMonthlyAliases(index, utcNow.UtcDateTime, employee.CreatedUtc), String.Join(", ", aliases));
        }

        await index.MaintainAsync();

        for (int i = 0; i < 4; i++)
        {
            var created = utcNow.SubtractMonths(i);
            var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: created.UtcDateTime));
            Assert.NotNull(employee?.Id);

            Assert.Equal(1, await index.GetCurrentVersionAsync());
            var existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
            _logger.LogRequest(existsResponse);
            Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
            Assert.True(existsResponse.Exists);

            var aliasesResponse = await _client.Indices.GetAliasAsync(index.GetIndex(employee.CreatedUtc));
            _logger.LogRequest(aliasesResponse);
            Assert.True(aliasesResponse.IsValidResponse);
            Assert.Single(aliasesResponse.Indices);

            var aliases = aliasesResponse.Indices.Values.Single().Aliases.Select(s => s.Key).ToList();
            aliases.Sort();

            Assert.Equal(GetExpectedEmployeeMonthlyAliases(index, utcNow.UtcDateTime, employee.CreatedUtc), String.Join(", ", aliases));
        }
    }

    [Fact]
    public async Task MaintainOnlyOldIndexesAsync()
    {
        _configuration.TimeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow.EndOfYear());

        var index = new MonthlyEmployeeIndex(_configuration, 1)
        {
            MaxIndexAge = _configuration.TimeProvider.GetUtcNow().UtcDateTime.EndOfMonth() - _configuration.TimeProvider.GetUtcNow().UtcDateTime.SubtractMonths(12).StartOfMonth()
        };

        await index.EnsureIndexAsync(_configuration.TimeProvider.GetUtcNow().UtcDateTime.SubtractMonths(12));
        var existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(_configuration.TimeProvider.GetUtcNow().UtcDateTime.SubtractMonths(12)));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        index.MaxIndexAge = _configuration.TimeProvider.GetUtcNow().UtcDateTime.EndOfMonth() - _configuration.TimeProvider.GetUtcNow().UtcDateTime.StartOfMonth();

        await index.MaintainAsync();
        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(_configuration.TimeProvider.GetUtcNow().UtcDateTime.SubtractMonths(12)));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.False(existsResponse.Exists);
    }

    [Fact]
    public async Task CanCreateAndDeleteIndex()
    {
        var index = new EmployeeIndex(_configuration);

        await index.ConfigureAsync();
        var existsResponse = await _client.Indices.ExistsAsync(index.Name);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        await index.DeleteAsync();
        existsResponse = await _client.Indices.ExistsAsync(index.Name);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.False(existsResponse.Exists);
    }

    [Fact]
    public async Task CanChangeIndexSettings()
    {
        var index1 = new VersionedEmployeeIndex(_configuration, 1, i => i
            .Settings(s => s
                .NumberOfReplicas(0)
                .Setting("index.mapping.total_fields.limit", 2000)
                .Analysis(a => a.Analyzers(a1 => a1.Custom("custom1", c => c.Filters("uppercase").Tokenizer("whitespace"))))
            ));
        await index1.DeleteAsync();

        await index1.ConfigureAsync();
        var settings = await _client.Indices.GetSettingsAsync(index1.VersionedName);
        Assert.Equal(0, settings.Indices[index1.VersionedName].Settings.NumberOfReplicas);
        Assert.NotNull(settings.Indices[index1.VersionedName].Settings.Analysis.Analyzers["custom1"]);

        var index2 = new VersionedEmployeeIndex(_configuration, 1, i => i.Settings(s => s
            .NumberOfReplicas(1)
            .Setting("index.mapping.total_fields.limit", 3000)
            .Analysis(a => a.Analyzers(a1 => a1.Custom("custom1", c => c.Filters("uppercase").Tokenizer("whitespace")).Custom("custom2", c => c.Filters("uppercase").Tokenizer("whitespace"))))
        ));

        await index2.ConfigureAsync();
        settings = await _client.Indices.GetSettingsAsync(index1.VersionedName);
        Assert.Equal(1, settings.Indices[index1.VersionedName].Settings.NumberOfReplicas);
        Assert.NotNull(settings.Indices[index1.VersionedName].Settings.Analysis.Analyzers["custom1"]);
    }

    [Fact]
    public async Task CanAddIndexMappings()
    {
        var index1 = new VersionedEmployeeIndex(_configuration, 1, null, m => m.Properties(p => p.Keyword(k => k.Name(n => n.EmailAddress))));
        await index1.DeleteAsync();

        await index1.ConfigureAsync();
        var fieldMapping = await _client.Indices.GetFieldMappingAsync<Employee>("emailAddress", d => d.Indices(index1.VersionedName));
        Assert.NotNull(fieldMapping.Indices[index1.VersionedName].Mappings["emailAddress"]);

        var index2 = new VersionedEmployeeIndex(_configuration, 1, null, m => m.Properties(p => p.Keyword(k => k.Name(n => n.EmailAddress)).Number(k => k.Name(n => n.Age))));

        await index2.ConfigureAsync();
        fieldMapping = await _client.Indices.GetFieldMappingAsync<Employee>("age", d => d.Indices(index2.VersionedName));
        Assert.NotNull(fieldMapping.Indices[index2.VersionedName].Mappings["age"]);
    }

    [Fact]
    public async Task WillWarnWhenAttemptingToChangeFieldMappingType()
    {
        var index1 = new VersionedEmployeeIndex(_configuration, 1, null, m => m.Properties(p => p.Keyword(k => k.Name(n => n.EmailAddress))));
        await index1.DeleteAsync();

        await index1.ConfigureAsync();
        var existsResponse = await _client.Indices.ExistsAsync(index1.VersionedName);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        var index2 = new VersionedEmployeeIndex(_configuration, 1, null, m => m.Properties(p => p.Number(k => k.Name(n => n.EmailAddress))));

        await index2.ConfigureAsync();
        Assert.Contains(Log.LogEntries, l => l.LogLevel == LogLevel.Error && l.Message.Contains("requires a new index version"));
    }

    [Fact]
    public async Task CanCreateAndDeleteVersionedIndex()
    {
        var index = new VersionedEmployeeIndex(_configuration, 1);
        await index.DeleteAsync();

        await index.ConfigureAsync();
        var existsResponse = await _client.Indices.ExistsAsync(index.VersionedName);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        await _client.AssertSingleIndexAlias(index.VersionedName, index.Name);

        await index.DeleteAsync();
        existsResponse = await _client.Indices.ExistsAsync(index.VersionedName);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.False(existsResponse.Exists);

        Assert.Equal(0, await _client.GetAliasIndexCount(index.Name));
    }

    [Fact]
    public async Task CanCreateAndDeleteDailyIndex()
    {
        var index = new DailyEmployeeIndex(_configuration, 1);
        await index.DeleteAsync();

        await index.ConfigureAsync();
        var todayDate = DateTime.Now;
        var yesterdayDate = DateTime.Now.SubtractDays(1);
        string todayIndex = index.GetIndex(todayDate);
        string yesterdayIndex = index.GetIndex(yesterdayDate);

        await index.EnsureIndexAsync(todayDate);
        await index.EnsureIndexAsync(yesterdayDate);

        var existsResponse = await _client.Indices.ExistsAsync(todayIndex);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        existsResponse = await _client.Indices.ExistsAsync(yesterdayIndex);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        await index.DeleteAsync();

        existsResponse = await _client.Indices.ExistsAsync(todayIndex);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.False(existsResponse.Exists);

        existsResponse = await _client.Indices.ExistsAsync(yesterdayIndex);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.False(existsResponse.Exists);
    }

    [Fact]
    public async Task MaintainOnlyOldIndexesWithNoExistingAliasesAsync()
    {
        _configuration.TimeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow.EndOfYear());

        var index = new MonthlyEmployeeIndex(_configuration, 1)
        {
            MaxIndexAge = _configuration.TimeProvider.GetUtcNow().UtcDateTime.EndOfMonth() - _configuration.TimeProvider.GetUtcNow().UtcDateTime.SubtractMonths(12).StartOfMonth()
        };

        await index.EnsureIndexAsync(_configuration.TimeProvider.GetUtcNow().UtcDateTime.SubtractMonths(12));
        var existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(_configuration.TimeProvider.GetUtcNow().UtcDateTime.SubtractMonths(12)));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        index.MaxIndexAge = _configuration.TimeProvider.GetUtcNow().UtcDateTime.EndOfMonth() - _configuration.TimeProvider.GetUtcNow().UtcDateTime.StartOfMonth();
        await DeleteAliasesAsync(index.GetVersionedIndex(_configuration.TimeProvider.GetUtcNow().UtcDateTime.SubtractMonths(12)));

        await index.MaintainAsync();
        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(_configuration.TimeProvider.GetUtcNow().UtcDateTime.SubtractMonths(12)));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.False(existsResponse.Exists);
    }

    [Fact]
    public async Task MaintainOnlyOldIndexesWithPartialAliasesAsync()
    {
        _configuration.TimeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow.EndOfYear());

        var index = new MonthlyEmployeeIndex(_configuration, 1)
        {
            MaxIndexAge = _configuration.TimeProvider.GetUtcNow().UtcDateTime.EndOfMonth() - _configuration.TimeProvider.GetUtcNow().UtcDateTime.SubtractMonths(12).StartOfMonth()
        };

        await index.EnsureIndexAsync(_configuration.TimeProvider.GetUtcNow().UtcDateTime.SubtractMonths(11));
        await index.EnsureIndexAsync(_configuration.TimeProvider.GetUtcNow().UtcDateTime.SubtractMonths(12));
        var existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(_configuration.TimeProvider.GetUtcNow().UtcDateTime.SubtractMonths(12)));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        index.MaxIndexAge = _configuration.TimeProvider.GetUtcNow().UtcDateTime.EndOfMonth() - _configuration.TimeProvider.GetUtcNow().UtcDateTime.StartOfMonth();
        await DeleteAliasesAsync(index.GetVersionedIndex(_configuration.TimeProvider.GetUtcNow().UtcDateTime.SubtractMonths(12)));

        await index.MaintainAsync();
        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(_configuration.TimeProvider.GetUtcNow().UtcDateTime.SubtractMonths(12)));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.False(existsResponse.Exists);
    }

    [Theory]
    [MemberData(nameof(AliasesDatesToCheck))]
    public async Task DailyAliasMaxAgeAsync(DateTime utcNow)
    {
        _configuration.TimeProvider = new FakeTimeProvider(new DateTimeOffset(utcNow, TimeSpan.Zero));

        var index = new DailyEmployeeIndex(_configuration, 1)
        {
            MaxIndexAge = TimeSpan.FromDays(45)
        };

        await index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        IEmployeeRepository version1Repository = new EmployeeRepository(index);

        var employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        var existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        var aliasesResponse = await _client.Indices.GetAliasAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Single(aliasesResponse.Indices);
        var aliases = aliasesResponse.Indices.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeDailyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

        employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractDays(2)), o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        aliasesResponse = await _client.Indices.GetAliasAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Single(aliasesResponse.Indices);
        aliases = aliasesResponse.Indices.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeDailyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

        employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractDays(35)), o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        aliasesResponse = await _client.Indices.GetAliasAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Single(aliasesResponse.Indices);
        aliases = aliasesResponse.Indices.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeDailyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));
    }

    [Theory]
    [MemberData(nameof(AliasesDatesToCheck))]
    public async Task MonthlyAliasMaxAgeAsync(DateTime utcNow)
    {
        _configuration.TimeProvider = new FakeTimeProvider(new DateTimeOffset(utcNow, TimeSpan.Zero));

        var index = new MonthlyEmployeeIndex(_configuration, 1)
        {
            MaxIndexAge = TimeSpan.FromDays(90)
        };
        await index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        IEmployeeRepository repository = new EmployeeRepository(index);

        var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        var existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        var aliasesResponse = await _client.Indices.GetAliasAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Single(aliasesResponse.Indices);
        var aliases = aliasesResponse.Indices.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeMonthlyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

        employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractDays(2)), o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        aliasesResponse = await _client.Indices.GetAliasAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Single(aliasesResponse.Indices);
        aliases = aliasesResponse.Indices.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeMonthlyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

        employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractDays(35)), o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        aliasesResponse = await _client.Indices.GetAliasAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Single(aliasesResponse.Indices);
        aliases = aliasesResponse.Indices.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeMonthlyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));
    }

    [Theory]
    [MemberData(nameof(AliasesDatesToCheck))]
    public async Task DailyIndexMaxAgeAsync(DateTime utcNow)
    {
        _configuration.TimeProvider = new FakeTimeProvider(new DateTimeOffset(utcNow, TimeSpan.Zero));

        var index = new DailyEmployeeIndex(_configuration, 1)
        {
            MaxIndexAge = TimeSpan.FromDays(1)
        };
        await index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();

        await index.EnsureIndexAsync(utcNow);
        var existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(utcNow));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        await index.EnsureIndexAsync(utcNow.SubtractDays(1));
        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(utcNow.SubtractDays(1)));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        await Assert.ThrowsAsync<ArgumentException>(async () => await index.EnsureIndexAsync(utcNow.SubtractDays(2)));
        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(utcNow.SubtractDays(2)));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.False(existsResponse.Exists);
    }

    [Theory]
    [MemberData(nameof(AliasesDatesToCheck))]
    public async Task MonthlyIndexMaxAgeAsync(DateTime utcNow)
    {
        _configuration.TimeProvider = new FakeTimeProvider(new DateTimeOffset(utcNow, TimeSpan.Zero));

        var index = new MonthlyEmployeeIndex(_configuration, 1)
        {
            MaxIndexAge = _configuration.TimeProvider.GetUtcNow().UtcDateTime.EndOfMonth() - _configuration.TimeProvider.GetUtcNow().UtcDateTime.StartOfMonth()
        };
        await index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();

        await index.EnsureIndexAsync(utcNow);
        var existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(utcNow));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        await index.EnsureIndexAsync(utcNow.Subtract(index.MaxIndexAge.GetValueOrDefault()));
        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(utcNow.Subtract(index.MaxIndexAge.GetValueOrDefault())));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        var endOfTwoMonthsAgo = utcNow.SubtractMonths(2).EndOfMonth();
        if (utcNow - endOfTwoMonthsAgo >= index.MaxIndexAge.GetValueOrDefault())
        {
            await Assert.ThrowsAsync<ArgumentException>(async () => await index.EnsureIndexAsync(endOfTwoMonthsAgo));
            existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(endOfTwoMonthsAgo));
            _logger.LogRequest(existsResponse);
            Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
            Assert.False(existsResponse.Exists);
        }
    }

    private static string GetExpectedEmployeeDailyAliases(IIndex index, DateTime utcNow, DateTime indexDateUtc)
    {
        double totalDays = utcNow.Date.Subtract(indexDateUtc.Date).TotalDays;
        var aliases = new List<string> { index.Name, index.GetIndex(indexDateUtc) };
        if (totalDays <= 30)
            aliases.Add($"{index.Name}-last30days");
        if (totalDays <= 7)
            aliases.Add($"{index.Name}-last7days");
        if (totalDays <= 1)
            aliases.Add($"{index.Name}-today");

        aliases.Sort();
        return String.Join(", ", aliases);
    }

    private static string GetExpectedEmployeeMonthlyAliases(IIndex index, DateTime utcNow, DateTime indexDateUtc)
    {
        var aliases = new List<string> { index.Name, index.GetIndex(indexDateUtc) };
        if (new DateTimeRange(utcNow.SubtractDays(1).StartOfMonth(), utcNow.EndOfMonth()).Contains(indexDateUtc))
            aliases.Add($"{index.Name}-today");

        if (new DateTimeRange(utcNow.SubtractDays(7).StartOfMonth(), utcNow.EndOfMonth()).Contains(indexDateUtc))
            aliases.Add($"{index.Name}-last7days");

        if (new DateTimeRange(utcNow.SubtractDays(30).StartOfMonth(), utcNow.EndOfMonth()).Contains(indexDateUtc))
            aliases.Add($"{index.Name}-last30days");

        if (new DateTimeRange(utcNow.SubtractDays(60).StartOfMonth(), utcNow.EndOfMonth()).Contains(indexDateUtc))
            aliases.Add($"{index.Name}-last60days");

        aliases.Sort();
        return String.Join(", ", aliases);
    }
}
