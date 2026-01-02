using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
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
        Log.SetLogLevel<EmployeeRepository>(LogLevel.Trace);
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

            var aliasesResponse = await _client.Indices.GetAliasAsync((Indices)index.GetIndex(employee.CreatedUtc));
            _logger.LogRequest(aliasesResponse);
            Assert.True(aliasesResponse.IsValidResponse);
            Assert.Single(aliasesResponse.Aliases);

            var aliases = aliasesResponse.Aliases.Values.Single().Aliases.Select(s => s.Key).ToList();
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

            var aliasesResponse = await _client.Indices.GetAliasAsync((Indices)index.GetIndex(employee.CreatedUtc));
            _logger.LogRequest(aliasesResponse);
            Assert.True(aliasesResponse.IsValidResponse);
            Assert.Single(aliasesResponse.Aliases);

            var aliases = aliasesResponse.Aliases.Values.Single().Aliases.Select(s => s.Key).ToList();
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
        await _configuration.DailyLogEvents.ConfigureAsync();

        var indexes = await _client.GetIndicesPointingToAliasAsync(_configuration.DailyLogEvents.Name);
        Assert.Empty(indexes);

        var alias = await _client.Indices.GetAliasAsync((Indices)_configuration.DailyLogEvents.Name);
        _logger.LogRequest(alias);
        Assert.False(alias.IsValidResponse);

        var utcNow = DateTime.UtcNow;
        ILogEventRepository repository = new DailyLogEventRepository(_configuration);
        var logEvent = await repository.AddAsync(LogEventGenerator.Generate(createdUtc: utcNow));
        Assert.NotNull(logEvent?.Id);

        logEvent = await repository.AddAsync(LogEventGenerator.Generate(createdUtc: utcNow.SubtractDays(1)), o => o.ImmediateConsistency());
        Assert.NotNull(logEvent?.Id);

        alias = await _client.Indices.GetAliasAsync((Indices)_configuration.DailyLogEvents.Name);
        _logger.LogRequest(alias);
        Assert.True(alias.IsValidResponse);
        Assert.Equal(2, alias.Aliases.Count);

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

        await _client.Indices.RefreshAsync(Elastic.Clients.Elasticsearch.Indices.All);
        var aliasesResponse = await _client.Indices.GetAliasAsync((Indices)$"{version1Index.VersionedName},{version2Index.VersionedName}");
        Assert.Empty(aliasesResponse.Aliases.Values.SelectMany(i => i.Aliases));

        // Indexes exist but no alias so the oldest index version will be used.
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        await version1Index.MaintainAsync();
        aliasesResponse = await _client.Indices.GetAliasAsync((Indices)version1Index.VersionedName);
        Assert.Single(aliasesResponse.Aliases.Single().Value.Aliases);
        aliasesResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.VersionedName);
        Assert.Empty(aliasesResponse.Aliases.Single().Value.Aliases);

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

        await _client.Indices.RefreshAsync(Elastic.Clients.Elasticsearch.Indices.All);
        var aliasesResponse = await _client.Indices.GetAliasAsync((Indices)$"{version1Index.GetVersionedIndex(utcNow.UtcDateTime)},{version2Index.GetVersionedIndex(utcNow.UtcDateTime)}");
        Assert.Empty(aliasesResponse.Aliases.Values.SelectMany(i => i.Aliases));

        // Indexes exist but no alias so the oldest index version will be used.
        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());

        await version1Index.MaintainAsync();
        aliasesResponse = await _client.Indices.GetAliasAsync((Indices)version1Index.GetVersionedIndex(utcNow.UtcDateTime));
        Assert.Equal(version1Index.Aliases.Count + 1, aliasesResponse.Aliases.Single().Value.Aliases.Count);
        aliasesResponse = await _client.Indices.GetAliasAsync((Indices)version2Index.GetVersionedIndex(utcNow.UtcDateTime));
        Assert.Empty(aliasesResponse.Aliases.Single().Value.Aliases);

        Assert.Equal(1, await version1Index.GetCurrentVersionAsync());
        Assert.Equal(1, await version2Index.GetCurrentVersionAsync());
    }

    private async Task DeleteAliasesAsync(string index)
    {
        var aliasesResponse = await _client.Indices.GetAliasAsync((Indices)index);
        var aliases = aliasesResponse.Aliases.Single(a => a.Key == index).Value.Aliases.Select(s => s.Key).ToList();
        foreach (string alias in aliases)
        {
            await _client.Indices.DeleteAliasAsync(new Elastic.Clients.Elasticsearch.IndexManagement.DeleteAliasRequest(index, alias));
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

        var aliasesResponse = await _client.Indices.GetAliasAsync((Indices)index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Single(aliasesResponse.Aliases);
        var aliases = aliasesResponse.Aliases.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeDailyAliases(index, timeProvider.GetUtcNow().UtcDateTime, employee.CreatedUtc), String.Join(", ", aliases));

        timeProvider.Advance(TimeSpan.FromDays(6));
        index.MaxIndexAge = TimeSpan.FromDays(10);
        await index.MaintainAsync();
        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        aliasesResponse = await _client.Indices.GetAliasAsync((Indices)index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Single(aliasesResponse.Aliases);
        aliases = aliasesResponse.Aliases.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeDailyAliases(index, timeProvider.GetUtcNow().UtcDateTime, employee.CreatedUtc), String.Join(", ", aliases));

        timeProvider.Advance(TimeSpan.FromDays(9));
        await index.MaintainAsync();
        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.False(existsResponse.Exists);

        aliasesResponse = await _client.Indices.GetAliasAsync((Indices)index.GetIndex(employee.CreatedUtc));
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

            var aliasesResponse = await _client.Indices.GetAliasAsync((Indices)index.GetIndex(employee.CreatedUtc));
            _logger.LogRequest(aliasesResponse);
            Assert.True(aliasesResponse.IsValidResponse);
            Assert.Single(aliasesResponse.Aliases);

            var aliases = aliasesResponse.Aliases.Values.Single().Aliases.Select(s => s.Key).ToList();
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

            var aliasesResponse = await _client.Indices.GetAliasAsync((Indices)index.GetIndex(employee.CreatedUtc));
            _logger.LogRequest(aliasesResponse);
            Assert.True(aliasesResponse.IsValidResponse);
            Assert.Single(aliasesResponse.Aliases);

            var aliases = aliasesResponse.Aliases.Values.Single().Aliases.Select(s => s.Key).ToList();
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
                .AddOtherSetting("index.mapping.total_fields.limit", 2000)
                .Analysis(a => a.Analyzers(a1 => a1.Custom("custom1", c => c.Filter("uppercase").Tokenizer("whitespace"))))
            ));
        await index1.DeleteAsync();

        await index1.ConfigureAsync();
        var settings = await _client.Indices.GetSettingsAsync((Indices)index1.VersionedName);
        var indexSettings = settings.Settings[index1.VersionedName].Settings;
        // NumberOfReplicas is Union<int, string> - need to extract the actual value
        var replicas = indexSettings.Index?.NumberOfReplicas ?? indexSettings.NumberOfReplicas;
        Assert.NotNull(replicas);
        // Match returns the value from either side of the union
        var replicaCount = replicas.Match(i => i, s => int.Parse(s));
        Assert.Equal(0, replicaCount);
        Assert.NotNull(indexSettings.Index?.Analysis?.Analyzers["custom1"]);

        var index2 = new VersionedEmployeeIndex(_configuration, 1, i => i.Settings(s => s
            .NumberOfReplicas(1)
            .AddOtherSetting("index.mapping.total_fields.limit", 3000)
            .Analysis(a => a.Analyzers(a1 => a1.Custom("custom1", c => c.Filter("uppercase").Tokenizer("whitespace")).Custom("custom2", c => c.Filter("uppercase").Tokenizer("whitespace"))))
        ));

        await index2.ConfigureAsync();
        settings = await _client.Indices.GetSettingsAsync((Indices)index1.VersionedName);
        indexSettings = settings.Settings[index1.VersionedName].Settings;
        replicas = indexSettings.Index?.NumberOfReplicas ?? indexSettings.NumberOfReplicas;
        Assert.NotNull(replicas);
        replicaCount = replicas.Match(i => i, s => int.Parse(s));
        Assert.Equal(1, replicaCount);
        Assert.NotNull(indexSettings.Index?.Analysis?.Analyzers["custom1"]);
    }

    [Fact]
    public async Task CanAddIndexMappings()
    {
        var index1 = new VersionedEmployeeIndex(_configuration, 1, null, m => m.Properties(p => p.Keyword(e => e.EmailAddress)));
        await index1.DeleteAsync();

        await index1.ConfigureAsync();
        var fieldMapping = await _client.Indices.GetFieldMappingAsync<Employee>(new Field("emailAddress"), d => d.Indices(index1.VersionedName));
        Assert.True(fieldMapping.IsValidResponse);
        Assert.True(fieldMapping.FieldMappings.TryGetValue(index1.VersionedName, out var indexMapping));
        Assert.True(indexMapping.Mappings.ContainsKey("emailAddress"));

        var index2 = new VersionedEmployeeIndex(_configuration, 1, null, m => m.Properties(p => p.Keyword(e => e.EmailAddress).IntegerNumber(e => e.Age)));

        await index2.ConfigureAsync();
        fieldMapping = await _client.Indices.GetFieldMappingAsync<Employee>(new Field("age"), d => d.Indices(index2.VersionedName));
        Assert.True(fieldMapping.IsValidResponse);
        Assert.True(fieldMapping.FieldMappings.TryGetValue(index2.VersionedName, out indexMapping));
        Assert.True(indexMapping.Mappings.ContainsKey("age"));
    }

    [Fact]
    public async Task WillWarnWhenAttemptingToChangeFieldMappingType()
    {
        var index1 = new VersionedEmployeeIndex(_configuration, 1, null, m => m.Properties(p => p.Keyword(e => e.EmailAddress)));
        await index1.DeleteAsync();

        await index1.ConfigureAsync();
        var existsResponse = await _client.Indices.ExistsAsync(index1.VersionedName);
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        var index2 = new VersionedEmployeeIndex(_configuration, 1, null, m => m.Properties(p => p.IntegerNumber(e => e.EmailAddress)));

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

        var aliasesResponse = await _client.Indices.GetAliasAsync((Indices)index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Single(aliasesResponse.Aliases);
        var aliases = aliasesResponse.Aliases.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeDailyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

        employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractDays(2)), o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        aliasesResponse = await _client.Indices.GetAliasAsync((Indices)index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Single(aliasesResponse.Aliases);
        aliases = aliasesResponse.Aliases.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeDailyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

        employee = await version1Repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractDays(35)), o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        aliasesResponse = await _client.Indices.GetAliasAsync((Indices)index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Single(aliasesResponse.Aliases);
        aliases = aliasesResponse.Aliases.Values.Single().Aliases.Select(s => s.Key).ToList();
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

        var aliasesResponse = await _client.Indices.GetAliasAsync((Indices)index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Single(aliasesResponse.Aliases);
        var aliases = aliasesResponse.Aliases.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeMonthlyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

        employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractDays(2)), o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        aliasesResponse = await _client.Indices.GetAliasAsync((Indices)index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Single(aliasesResponse.Aliases);
        aliases = aliasesResponse.Aliases.Values.Single().Aliases.Select(s => s.Key).ToList();
        aliases.Sort();
        Assert.Equal(GetExpectedEmployeeMonthlyAliases(index, utcNow, employee.CreatedUtc), String.Join(", ", aliases));

        employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.SubtractDays(35)), o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        existsResponse = await _client.Indices.ExistsAsync(index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(existsResponse);
        Assert.True(existsResponse.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.True(existsResponse.Exists);

        aliasesResponse = await _client.Indices.GetAliasAsync((Indices)index.GetIndex(employee.CreatedUtc));
        _logger.LogRequest(aliasesResponse);
        Assert.True(aliasesResponse.IsValidResponse);
        Assert.Single(aliasesResponse.Aliases);
        aliases = aliasesResponse.Aliases.Values.Single().Aliases.Select(s => s.Key).ToList();
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

    [Fact]
    public async Task Index_MaintainThenIndexing_ShouldCreateIndexWhenNeeded()
    {
        // Arrange
        var utcNow = new DateTimeOffset(2025, 6, 15, 0, 0, 0, TimeSpan.Zero);
        var timeProvider = new FakeTimeProvider(utcNow);
        _configuration.TimeProvider = timeProvider;

        var index = new MonthlyEmployeeIndex(_configuration, 2);
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());

        // Act
        // Simulate calling MaintainAsync before any documents are indexed
        await index.MaintainAsync();

        // Now index a document
        var repository = new EmployeeRepository(index);
        var employee = await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.UtcDateTime));

        // Assert
        Assert.NotNull(employee?.Id);

        // Verify the correct versioned index was created
        string expectedVersionedIndex = index.GetVersionedIndex(utcNow.UtcDateTime);
        var indexExists = await _client.Indices.ExistsAsync(expectedVersionedIndex);
        Assert.True(indexExists.Exists);

        // Verify the alias exists
        string expectedAlias = index.GetIndex(utcNow.UtcDateTime);
        var aliasExists = await _client.Indices.ExistsAliasAsync(Names.Parse(expectedAlias));
        Assert.True(aliasExists.Exists);
    }

    [Fact]
    public async Task Index_ParallelOperations_ShouldNotInterfereWithEachOther()
    {
        // Arrange
        var utcNow = new DateTimeOffset(2025, 6, 15, 0, 0, 0, TimeSpan.Zero);
        var timeProvider = new FakeTimeProvider(utcNow);
        _configuration.TimeProvider = timeProvider;

        var index = new MonthlyEmployeeIndex(_configuration, 2);
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());

        // Act
        // Run multiple operations in parallel
        var task1 = index.ConfigureAsync();
        var task2 = Task.Run(async () =>
        {
            var repository = new EmployeeRepository(index);
            return await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.UtcDateTime));
        });
        var task3 = index.MaintainAsync();

        // Wait for all tasks to complete
        await Task.WhenAll(task1, task3);
        var employee = await task2;

        // Assert
        Assert.NotNull(employee?.Id);

        // Verify the index was created correctly despite the race condition
        string expectedVersionedIndex = "monthly-employees-v2-2025.06";
        string expectedAlias = "monthly-employees-2025.06";

        var indexExistsResponse = await _client.Indices.ExistsAsync(expectedVersionedIndex);
        Assert.True(indexExistsResponse.Exists, $"Versioned index {expectedVersionedIndex} should exist");

        var aliasResponse = await _client.Indices.GetAliasAsync((Indices)expectedAlias);
        Assert.True(aliasResponse.IsValidResponse, $"Alias {expectedAlias} should exist");
    }

    [Fact]
    public async Task EnsureDateIndexAsync_MultipleCallsSimultaneously_ShouldNotThrowException()
    {
        // Arrange
        var utcNow = new DateTimeOffset(2025, 6, 15, 0, 0, 0, TimeSpan.Zero);
        var timeProvider = new FakeTimeProvider(utcNow);
        _configuration.TimeProvider = timeProvider;

        var index = new DailyEmployeeIndex(_configuration, 2);
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());

        int concurrency = 20;

        // Act & Assert
        // We expect no exceptions when running multiple tasks concurrently
        await Parallel.ForEachAsync(
            Enumerable.Range(0, concurrency),
            async (_, _) =>
            {
                await index.EnsureIndexAsync(utcNow.UtcDateTime);
            }
        );
    }

    [Fact]
    public async Task EnsuredDates_AddingManyDates_CouldLeakMemory()
    {
        // Arrange
        var index = new DailyEmployeeIndex(_configuration, 2);
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());

        var repository = new EmployeeRepository(index);
        const int UNIQUE_DATES = 1000;
        var baseDate = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        // Act
        for (int i = 0; i < UNIQUE_DATES; i++)
        {
            var testDate = baseDate.AddDays(i);
            var employee = EmployeeGenerator.Generate(createdUtc: testDate);
            await repository.AddAsync(employee);
        }

        // This test verifies that adding many dates doesn't throw exceptions,
        // but it highlights the fact that _ensuredDates will grow unbounded.
        // A proper fix would implement a cleanup mechanism.
    }

    [Fact(Skip = "Shows an issue where we cannot recover if an index exists with an alias name")]
    public async Task UpdateAliasesAsync_CreateAliasFailure_ShouldHandleGracefully()
    {
        // Arrange
        var index = new DailyEmployeeIndex(_configuration, 2);
        await index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());

        // Create a scenario that causes alias creation to fail
        string indexName = index.GetIndex(DateTime.UtcNow);

        // First create a conflicting index without the alias
        await _client.Indices.CreateAsync(indexName, d => d
            .Mappings(m => m.Properties(p => p.Keyword("id")))
            .Settings(s => s.NumberOfReplicas(0)));

        // Act
        var repository = new EmployeeRepository(index);
        var employee = EmployeeGenerator.Generate(createdUtc: DateTime.UtcNow);

        // This should handle the conflict gracefully in the future
        await repository.AddAsync(employee);
    }

    [Fact]
    public void MaxIndexAge_NegativeValue_ShouldThrowArgumentException()
    {
        // Arrange
        var index = new DailyEmployeeIndex(_configuration, 2);
        var negativeTimeSpan = TimeSpan.FromDays(-1);

        // Act
        var ex = Assert.Throws<ArgumentException>(() => index.MaxIndexAge = negativeTimeSpan);

        // Assert
        Assert.Contains("MaxIndexAge must be positive", ex.Message);
    }

    [Fact]
    public void MaxIndexAge_ZeroValue_ShouldThrowArgumentException()
    {
        // Arrange
        var index = new DailyEmployeeIndex(_configuration, 2);
        var zeroTimeSpan = TimeSpan.Zero;

        // Act
        var ex = Assert.Throws<ArgumentException>(() => index.MaxIndexAge = zeroTimeSpan);

        // Assert
        Assert.Contains("MaxIndexAge must be positive", ex.Message);
    }

    [Fact]
    public void MaxIndexAge_ValidValue_ShouldSetSuccessfully()
    {
        // Arrange
        var index = new DailyEmployeeIndex(_configuration, 2);
        var validTimeSpan = TimeSpan.FromDays(30);

        // Act
        index.MaxIndexAge = validTimeSpan;

        // Assert
        Assert.Equal(validTimeSpan, index.MaxIndexAge);
    }

    [Fact]
    public void GetIndexes_LargeTimeRange_ShouldReturnEmptyForExcessivePeriod()
    {
        // Arrange
        var index = new DailyEmployeeIndex(_configuration, 2);
        index.MaxIndexAge = TimeSpan.FromDays(30);
        var startDate = DateTime.UtcNow.AddDays(-100); // 100 days ago
        var endDate = DateTime.UtcNow;

        // Act
        string[] indexes = index.GetIndexes(startDate, endDate);

        // Assert
        Assert.Empty(indexes);
    }

    [Fact]
    public void GetIndexes_ThreeMonthPeriod_ShouldReturnEmptyForDailyIndex()
    {
        // Arrange
        var index = new DailyEmployeeIndex(_configuration, 2);
        var startDate = DateTime.UtcNow.AddMonths(-3);
        var endDate = DateTime.UtcNow;

        // Act
        string[] indexes = index.GetIndexes(startDate, endDate);

        // Assert
        Assert.Empty(indexes); // Should return empty for periods >= 3 months
    }

    [Fact]
    public void GetIndexes_OneYearPeriod_ShouldReturnEmptyForMonthlyIndex()
    {
        // Arrange
        var index = new MonthlyEmployeeIndex(_configuration, 2);
        var startDate = DateTime.UtcNow.AddYears(-2);
        var endDate = DateTime.UtcNow;

        // Act
        string[] indexes = index.GetIndexes(startDate, endDate);

        // Assert
        Assert.Empty(indexes); // Should return empty for periods > 1 year
    }

    [Fact]
    public async Task PatchAsync_WhenActionPatchAndSingleIndexMissing_CreatesIndex()
    {
        // Arrange
        var index = new EmployeeIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new EmployeeRepository(index);
        string id = ObjectId.GenerateNewId().ToString();
        var patch = new ActionPatch<Employee>(e => e.Name = "Patched");

        // Act
        await Assert.ThrowsAsync<DocumentNotFoundException>(async () => await repository.PatchAsync(id, patch));

        // Assert
        var response = await _client.Indices.ExistsAsync(index.Name);
        Assert.True(response.Exists);
        response = await _client.Indices.ExistsAsync(index.GetIndex(id));
        Assert.True(response.Exists);
    }

    [Fact]
    public async Task PatchAsync_WhenPartialPatchAndSingleIndexMissing_CreatesIndex()
    {
        // Arrange
        var index = new EmployeeIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new EmployeeRepository(index);
        string id = ObjectId.GenerateNewId().ToString();
        var patch = new PartialPatch(new { Name = "Patched" });

        // Act
        await Assert.ThrowsAsync<DocumentNotFoundException>(async () => await repository.PatchAsync(id, patch));

        // Assert
        var response = await _client.Indices.ExistsAsync(index.Name);
        Assert.True(response.Exists);
        response = await _client.Indices.ExistsAsync(index.GetIndex(id));
        Assert.True(response.Exists);
    }

    [Fact]
    public async Task PatchAsync_WhenJsonPatchAndSingleIndexMissing_CreatesIndex()
    {
        // Arrange
        var index = new EmployeeIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new EmployeeRepository(index);
        string id = ObjectId.GenerateNewId().ToString();
        var patchDoc = new PatchDocument();
        patchDoc.Replace("/name", "Patched");
        var patch = new JsonPatch(patchDoc);

        // Act
        await Assert.ThrowsAsync<DocumentNotFoundException>(async () => await repository.PatchAsync(id, patch));

        // Assert
        var response = await _client.Indices.ExistsAsync(index.Name);
        Assert.True(response.Exists);
        response = await _client.Indices.ExistsAsync(index.GetIndex(id));
        Assert.True(response.Exists);
    }

    [Fact]
    public async Task PatchAsync_WhenScriptPatchAndSingleIndexMissing_CreatesIndex()
    {
        // Arrange
        var index = new EmployeeIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new EmployeeRepository(index);
        string id = ObjectId.GenerateNewId().ToString();
        var patch = new ScriptPatch("ctx._source.name = 'Patched';");

        // Act
        await Assert.ThrowsAsync<DocumentNotFoundException>(async () => await repository.PatchAsync(id, patch));

        // Assert
        var response = await _client.Indices.ExistsAsync(index.Name);
        Assert.True(response.Exists);
        response = await _client.Indices.ExistsAsync(index.GetIndex(id));
        Assert.True(response.Exists);
    }

    [Fact]
    public async Task PatchAllAsync_WhenActionPatchAndSingleIndexMissing_CreatesIndex()
    {
        // Arrange
        var index = new EmployeeIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new EmployeeRepository(index);
        var patch = new ActionPatch<Employee>(e => e.Name = "Patched");

        // Act
        await repository.PatchAllAsync(q => q, patch);

        // Assert
        var response = await _client.Indices.ExistsAsync(index.Name);
        Assert.True(response.Exists);
    }

    [Fact]
    public async Task PatchAllAsync_WhenPartialPatchAndSingleIndexMissing_CreatesIndex()
    {
        // Arrange
        var index = new EmployeeIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new EmployeeRepository(index);
        var patch = new PartialPatch(new { Name = "Patched" });

        // Act
        await repository.PatchAllAsync(q => q, patch);

        // Assert
        var response = await _client.Indices.ExistsAsync(index.Name);
        Assert.True(response.Exists);
    }

    [Fact]
    public async Task PatchAllAsync_WhenJsonPatchAndSingleIndexMissing_CreatesIndex()
    {
        // Arrange
        var index = new EmployeeIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new EmployeeRepository(index);
        var patchDoc = new PatchDocument();
        patchDoc.Replace("/name", "Patched");
        var patch = new JsonPatch(patchDoc);

        // Act
        await repository.PatchAllAsync(q => q, patch);

        // Assert
        var response = await _client.Indices.ExistsAsync(index.Name);
        Assert.True(response.Exists);
    }

    [Fact]
    public async Task PatchAllAsync_WhenScriptPatchAndSingleIndexMissing_CreatesIndex()
    {
        // Arrange
        var index = new EmployeeIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new EmployeeRepository(index);
        var patch = new ScriptPatch("ctx._source.name = 'Patched';");

        // Act
        await repository.PatchAllAsync(q => q, patch);

        // Assert
        var response = await _client.Indices.ExistsAsync(index.Name);
        Assert.True(response.Exists);
    }

    [Fact]
    public async Task PatchAsync_WhenActionPatchAndMonthlyIndexMissing_CreatesIndex()
    {
        // Arrange
        var index = new MonthlyLogEventIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new DailyLogEventRepository(index);
        string id = ObjectId.GenerateNewId().ToString();
        var patch = new ActionPatch<LogEvent>(e => e.Value = 99);

        // Act
        await Assert.ThrowsAsync<DocumentNotFoundException>(async () => await repository.PatchAsync(id, patch));

        // Assert
        var aliasResponse = await _client.Indices.ExistsAliasAsync(index.Name);
        Assert.True(aliasResponse.Exists);
        var indexResponse = await _client.Indices.ExistsAsync(index.GetIndex(id));
        Assert.True(indexResponse.Exists);
    }

    [Fact]
    public async Task PatchAsync_WhenPartialPatchAndMonthlyIndexMissing_CreatesIndex()
    {
        // Arrange
        var index = new MonthlyLogEventIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new DailyLogEventRepository(index);
        string id = ObjectId.GenerateNewId().ToString();
        var patch = new PartialPatch(new { Value = 99 });

        // Act
        await Assert.ThrowsAsync<DocumentNotFoundException>(async () => await repository.PatchAsync(id, patch));

        // Assert
        var aliasResponse = await _client.Indices.ExistsAliasAsync(index.Name);
        Assert.True(aliasResponse.Exists);
        var indexResponse = await _client.Indices.ExistsAsync(index.GetIndex(id));
        Assert.True(indexResponse.Exists);
    }

    [Fact]
    public async Task PatchAsync_WhenJsonPatchAndMonthlyIndexMissing_CreatesIndex()
    {
        // Arrange
        var index = new MonthlyLogEventIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new DailyLogEventRepository(index);
        string id = ObjectId.GenerateNewId().ToString();
        var patchDoc = new PatchDocument();
        patchDoc.Replace("/value", 99);
        var patch = new JsonPatch(patchDoc);

        // Act
        await Assert.ThrowsAsync<DocumentNotFoundException>(async () => await repository.PatchAsync(id, patch));

        // Assert
        var aliasResponse = await _client.Indices.ExistsAliasAsync(index.Name);
        Assert.True(aliasResponse.Exists);
        var indexResponse = await _client.Indices.ExistsAsync(index.GetIndex(id));
        Assert.True(indexResponse.Exists);
    }

    [Fact]
    public async Task PatchAsync_WhenScriptPatchAndMonthlyIndexMissing_CreatesIndex()
    {
        // Arrange
        var index = new MonthlyLogEventIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new DailyLogEventRepository(index);
        string id = ObjectId.GenerateNewId().ToString();
        var patch = new ScriptPatch("ctx._source.value = 99;");

        // Act
        await Assert.ThrowsAsync<DocumentNotFoundException>(async () => await repository.PatchAsync(id, patch));

        // Assert
        var aliasResponse = await _client.Indices.ExistsAliasAsync(index.Name);
        Assert.True(aliasResponse.Exists);
        var indexResponse = await _client.Indices.ExistsAsync(index.GetIndex(id));
        Assert.True(indexResponse.Exists);
    }

    [Fact]
    public async Task PatchAllAsync_WhenActionPatchAndMonthlyIndexMissing_DoesNotCreateIndex()
    {
        // Arrange
        var index = new MonthlyLogEventIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new DailyLogEventRepository(index);
        var patch = new ActionPatch<LogEvent>(e => e.Value = 42);

        // Act
        await repository.PatchAllAsync(q => q, patch);

        // Assert
        var response = await _client.Indices.ExistsAliasAsync(index.Name);
        Assert.False(response.Exists);
    }

    [Fact]
    public async Task PatchAllAsync_WhenPartialPatchAndMonthlyIndexMissing_DoesNotCreateIndex()
    {
        // Arrange
        var index = new MonthlyLogEventIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new DailyLogEventRepository(index);
        var patch = new PartialPatch(new { Value = 42 });

        // Act
        await repository.PatchAllAsync(q => q, patch);

        // Assert
        var response = await _client.Indices.ExistsAliasAsync(index.Name);
        Assert.False(response.Exists);
    }

    [Fact]
    public async Task PatchAllAsync_WhenJsonPatchAndMonthlyIndexMissing_DoesNotCreateIndex()
    {
        // Arrange
        var index = new MonthlyLogEventIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new DailyLogEventRepository(index);
        var patchDoc = new PatchDocument();
        patchDoc.Replace("/value", 42);
        var patch = new JsonPatch(patchDoc);

        // Act
        await repository.PatchAllAsync(q => q, patch);

        // Assert
        var response = await _client.Indices.ExistsAliasAsync(index.Name);
        Assert.False(response.Exists);
    }

    [Fact]
    public async Task PatchAllAsync_WhenScriptPatchAndMonthlyIndexMissing_DoesNotCreateIndex()
    {
        // Arrange
        var index = new MonthlyLogEventIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new DailyLogEventRepository(index);
        var patch = new ScriptPatch("ctx._source.value = 42;");

        // Act
        await repository.PatchAllAsync(q => q, patch);

        // Assert
        var response = await _client.Indices.ExistsAliasAsync(index.Name);
        Assert.False(response.Exists);
    }

    [Fact]
    public async Task PatchAllAsync_ByQuery_CreatesAllRelevantDailyIndices()
    {
        // Arrange
        var index = new EmployeeIndex(_configuration);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new EmployeeRepository(index);

        string id1 = ObjectId.GenerateNewId().ToString();
        string id2 = ObjectId.GenerateNewId(DateTime.UtcNow.SubtractDays(1)).ToString();

        // Act
        await repository.PatchAllAsync(q => q.Id(id1, id2), new ActionPatch<Employee>(e => e.Name = "Patched"));

        // Assert
        var response = await _client.Indices.ExistsAsync(index.Name);
        Assert.True(response.Exists);
        response = await _client.Indices.ExistsAsync(index.GetIndex(id1));
        Assert.True(response.Exists);
        response = await _client.Indices.ExistsAsync(index.GetIndex(id2));
        Assert.True(response.Exists);
    }

    [Fact]
    public async Task PatchAllAsync_ByQueryAcrossMultipleDays_DoesNotCreateAllRelevantDailyIndices()
    {
        // Arrange
        var index = new DailyEmployeeIndex(_configuration, 1);
        await index.DeleteAsync();
        await using var _ = new AsyncDisposableAction(() => index.DeleteAsync());
        var repository = new EmployeeRepository(index);

        string id1 = ObjectId.GenerateNewId().ToString();
        string id2 = ObjectId.GenerateNewId(DateTime.UtcNow.SubtractDays(1)).ToString();

        // Act
        await repository.PatchAllAsync(q => q.Id(id1, id2), new ActionPatch<Employee>(e => e.Name = "Patched"));

        // Assert
        var aliasResponse = await _client.Indices.ExistsAliasAsync(index.Name);
        Assert.False(aliasResponse.Exists);
        var indexResponse1 = await _client.Indices.ExistsAsync(index.GetIndex(id1));
        Assert.False(indexResponse1.Exists);
        var indexResponse2 = await _client.Indices.ExistsAsync(index.GetIndex(id2));
        Assert.False(indexResponse2.Exists);
    }
}
