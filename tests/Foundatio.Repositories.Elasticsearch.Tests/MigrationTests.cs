using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Lock;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Migrations;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class MigrationTests : ElasticRepositoryTestBase
{
    private MigrationIndex _migrationIndex = null!;
    private MigrationManager _migrationManager = null!;
    private MigrationStateRepository _migrationStateRepository = null!;
    private ILockProvider _lockProvider = null!;
    private IServiceProvider _serviceProvider = null!;

    public MigrationTests(ITestOutputHelper output) : base(output)
    {
        Log.SetLogLevel<MigrationStateRepository>(LogLevel.Trace);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        _migrationIndex = new MigrationIndex(_configuration);
        await _migrationIndex.DeleteAsync();
        await _migrationIndex.ConfigureAsync();

        _lockProvider = new CacheLockProvider(_cache, _messageBus, Log);
        _migrationStateRepository = new MigrationStateRepository(_migrationIndex);

        var serviceCollection = new ServiceCollection();
        serviceCollection.AddSingleton<VersionedWithoutVersionMigration>();
        serviceCollection.AddSingleton<Version1Migration>();
        serviceCollection.AddSingleton<Version2Migration>();
        serviceCollection.AddSingleton<Version3Migration>();
        serviceCollection.AddSingleton<FailingMigration>();
        serviceCollection.AddSingleton<FailingResumableMigration>();
        serviceCollection.AddSingleton<RepeatableMigration>();
        _serviceProvider = serviceCollection.BuildServiceProvider();
        _migrationManager = new MigrationManager(_serviceProvider, _migrationStateRepository, _lockProvider, Log);
    }

    [Fact]
    public async Task WillIgnoreVersionedMigrationWithoutVersion()
    {
        _migrationManager.AddMigration<VersionedWithoutVersionMigration>();

        var migrationStatus = await _migrationManager.GetMigrationStatus();
        Assert.Empty(migrationStatus.PendingMigrations);
        Assert.False(migrationStatus.NeedsMigration);
    }

    [Fact]
    public async Task WillSetVersionToLatestIfNoMigrationsRun()
    {
        // no existing migrations, should add doc with latest version
        _migrationManager.AddMigration<Version3Migration>();

        var migrationStatus = await _migrationManager.GetMigrationStatus();
        Assert.Empty(migrationStatus.PendingMigrations);
        Assert.False(migrationStatus.NeedsMigration);
        Assert.Equal(3, migrationStatus.CurrentVersion);

        await _client.Indices.RefreshAsync(cancellationToken: TestCancellationToken);

        var migrations = await _migrationStateRepository.GetAllAsync();
        Assert.Single(migrations.Documents);
        var migrationState = migrations.Documents.First();
        Assert.Equal("3", migrationState.Id);
        Assert.Equal(MigrationType.Versioned, migrationState.MigrationType);
        Assert.NotEqual(DateTime.MinValue, migrationState.StartedUtc);
        Assert.NotEqual(DateTime.MinValue, migrationState.CompletedUtc);
        Assert.Null(migrationState.ErrorMessage);
    }

    [Fact]
    public async Task CanRunPendingMigration()
    {
        await AddCompletedVersion1MigrationAsync();

        _migrationManager.AddMigration<Version3Migration>();

        var migrationStatus = await _migrationManager.GetMigrationStatus();
        Assert.Single(migrationStatus.PendingMigrations);
        Assert.True(migrationStatus.NeedsMigration);
        Assert.Equal(1, migrationStatus.CurrentVersion);

        var result = await _migrationManager.RunMigrationsAsync(TestCancellationToken);
        Assert.Equal(MigrationResult.Success, result);

        await _client.Indices.RefreshAsync(cancellationToken: TestCancellationToken);

        var migrations = await _migrationStateRepository.GetAllAsync();
        Assert.Equal(2, migrations.Documents.Count);
        var migrationState = migrations.Documents.First(d => d.Id == "3");
        Assert.Equal("3", migrationState.Id);
        Assert.Equal(MigrationType.Versioned, migrationState.MigrationType);
        Assert.NotEqual(DateTime.MinValue, migrationState.StartedUtc);
        Assert.NotEqual(DateTime.MinValue, migrationState.CompletedUtc);
        Assert.Null(migrationState.ErrorMessage);
    }

    [Fact]
    public async Task ConcurrentInitializationReadsTheWinningMigrationRecord()
    {
        // Arrange
        _migrationManager.AddMigration<Version3Migration>();
        var otherRepository = new MigrationStateRepository(_migrationIndex);
        using var subscription = _migrationStateRepository.DocumentsChanging.AddHandler(async (_, _) =>
        {
            await otherRepository.AddAsync(new MigrationState { Id = "3", Version = 3, StartedUtc = DateTime.UtcNow });
        });

        // Act
        var status = await _migrationManager.GetMigrationStatus();

        // Assert
        var pending = Assert.Single(status.PendingMigrations);
        Assert.NotNull(pending.State);
        Assert.Null(pending.State.CompletedUtc);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CanRunRepeatableMigration(bool hasStoredBaseline)
    {
        // Arrange
        if (hasStoredBaseline)
            await AddCompletedVersion1MigrationAsync();
        else
            _migrationManager.AddMigration<Version1Migration>();

        _migrationManager.AddMigration<RepeatableMigration>();

        // Act
        var migrationStatus = await _migrationManager.GetMigrationStatus();
        var result = await _migrationManager.RunMigrationsAsync(TestCancellationToken);

        // Assert
        Assert.Single(migrationStatus.PendingMigrations);
        Assert.True(migrationStatus.NeedsMigration);
        Assert.Equal(1, migrationStatus.CurrentVersion);
        Assert.Equal(MigrationResult.Success, result);

        await _client.Indices.RefreshAsync(cancellationToken: TestCancellationToken);

        var migrations = await _migrationStateRepository.GetAllAsync();
        Assert.Equal(2, migrations.Documents.Count);
        var migrationState = migrations.Documents.FirstOrDefault(d => d.Id == typeof(RepeatableMigration).FullName);
        Assert.NotNull(migrationState);
        Assert.Equal(MigrationType.Repeatable, migrationState.MigrationType);
        Assert.NotEqual(DateTime.MinValue, migrationState.StartedUtc);
        Assert.NotNull(migrationState.CompletedUtc);
        Assert.Equal(0, migrationState.Version);
        Assert.Null(migrationState.ErrorMessage);

        migrationStatus = await _migrationManager.GetMigrationStatus();
        Assert.Empty(migrationStatus.PendingMigrations);
        Assert.False(migrationStatus.NeedsMigration);
        Assert.Equal(1, migrationStatus.CurrentVersion);

        result = await _migrationManager.RunMigrationsAsync(TestCancellationToken);
        Assert.Equal(MigrationResult.Success, result);

        var repeatableMigration = _serviceProvider.GetRequiredService<RepeatableMigration>();
        Assert.Equal(1, repeatableMigration.Runs);
        repeatableMigration.SetVersion(1);

        migrationStatus = await _migrationManager.GetMigrationStatus();
        Assert.Single(migrationStatus.PendingMigrations);
        Assert.True(migrationStatus.NeedsMigration);
        Assert.Equal(1, migrationStatus.CurrentVersion);

        result = await _migrationManager.RunMigrationsAsync(TestCancellationToken);
        Assert.Equal(MigrationResult.Success, result);

        migrations = await _migrationStateRepository.GetAllAsync();
        Assert.Equal(2, migrations.Documents.Count);
        Assert.Equal(2, repeatableMigration.Runs);
        migrationState = migrations.Documents.FirstOrDefault(d => d.Id == typeof(RepeatableMigration).FullName);
        Assert.NotNull(migrationState);
        Assert.Equal(MigrationType.Repeatable, migrationState.MigrationType);
        Assert.NotEqual(DateTime.MinValue, migrationState.StartedUtc);
        Assert.NotNull(migrationState.CompletedUtc);
        Assert.Equal(1, migrationState.Version);
        Assert.Null(migrationState.ErrorMessage);
    }

    [Fact]
    public async Task CanHandleFailingMigration()
    {
        await AddCompletedVersion1MigrationAsync();

        _migrationManager.AddMigration<FailingMigration>();

        var migrationStatus = await _migrationManager.GetMigrationStatus();
        Assert.Single(migrationStatus.PendingMigrations);
        Assert.True(migrationStatus.NeedsMigration);
        Assert.Equal(1, migrationStatus.CurrentVersion);

        var result = await _migrationManager.RunMigrationsAsync(TestCancellationToken);
        Assert.Equal(MigrationResult.Failed, result);

        var failingMigration = _serviceProvider.GetRequiredService<FailingMigration>();
        Assert.Equal(1, failingMigration.Attempts);

        await _client.Indices.RefreshAsync(cancellationToken: TestCancellationToken);

        var migrations = await _migrationStateRepository.GetAllAsync();
        Assert.Equal(2, migrations.Documents.Count);
        var migrationState = migrations.Documents.FirstOrDefault(d => d.Id == "3");
        Assert.NotNull(migrationState);
        Assert.Equal(MigrationType.Versioned, migrationState.MigrationType);
        Assert.NotEqual(DateTime.MinValue, migrationState.StartedUtc);
        Assert.Null(migrationState.CompletedUtc);
        Assert.Equal("Boom", migrationState.ErrorMessage);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WillRetryFailingMigration(bool unrelatedLockFailure)
    {
        await AddCompletedVersion1MigrationAsync();
        var failingMigration = _serviceProvider.GetRequiredService<FailingResumableMigration>();
        if (unrelatedLockFailure)
            failingMigration.Failure = new LockAcquisitionTimeoutException("another-resource");
        _migrationManager.AddMigration<FailingResumableMigration>();

        var migrationStatus = await _migrationManager.GetMigrationStatus();
        Assert.Single(migrationStatus.PendingMigrations);
        Assert.True(migrationStatus.NeedsMigration);
        Assert.Equal(1, migrationStatus.CurrentVersion);

        var result = await _migrationManager.RunMigrationsAsync(TestCancellationToken);
        Assert.Equal(MigrationResult.Failed, result);

        Assert.Equal(3, failingMigration.Attempts);

        await _client.Indices.RefreshAsync(cancellationToken: TestCancellationToken);

        var migrations = await _migrationStateRepository.GetAllAsync();
        Assert.Equal(2, migrations.Documents.Count);
        var migrationState = migrations.Documents.FirstOrDefault(d => d.Id == "3");
        Assert.NotNull(migrationState);
        Assert.Equal(MigrationType.VersionedAndResumable, migrationState.MigrationType);
        Assert.NotEqual(DateTime.MinValue, migrationState.StartedUtc);
        Assert.Null(migrationState.CompletedUtc);
        Assert.Equal(failingMigration.Failure.Message, migrationState.ErrorMessage);

        // try again, should pass this time
        result = await _migrationManager.RunMigrationsAsync(TestCancellationToken);
        Assert.Equal(MigrationResult.Success, result);

        migrations = await _migrationStateRepository.GetAllAsync();
        Assert.Equal(2, migrations.Documents.Count);
        migrationState = migrations.Documents.FirstOrDefault(d => d.Id == "3");
        Assert.NotNull(migrationState);
        Assert.Equal(MigrationType.VersionedAndResumable, migrationState.MigrationType);
        Assert.NotEqual(DateTime.MinValue, migrationState.StartedUtc);
        Assert.NotNull(migrationState.CompletedUtc);
        Assert.Null(migrationState.ErrorMessage);
    }

    [Theory]
    [InlineData(Takeover.DuringMigration, MigrationType.Versioned, false)]
    [InlineData(Takeover.DuringMigration, MigrationType.Versioned, true)]
    [InlineData(Takeover.DuringMigration, MigrationType.VersionedAndResumable, true)]
    [InlineData(Takeover.DuringStartSave, MigrationType.VersionedAndResumable, false)]
    [InlineData(Takeover.DuringResultSave, MigrationType.Versioned, false)]
    [InlineData(Takeover.DuringResultSave, MigrationType.Versioned, true)]
    public async Task LosingTheLeasePreservesTheNextOwnersState(Takeover takeover, MigrationType type, bool migrationFails)
    {
        // Arrange
        await AddCompletedVersion1MigrationAsync();
        if (takeover is Takeover.DuringStartSave)
            await _migrationStateRepository.AddAsync(new MigrationState { Id = "2", Version = 2, MigrationType = type });

        ILock? successor = null;
        async Task TakeOverAsync()
        {
            await _lockProvider.ReleaseAsync("migration-manager");
            successor = await _lockProvider.AcquireAsync("migration-manager", acquireTimeout: TimeSpan.Zero);
            var repository = new MigrationStateRepository(_migrationIndex);
            var state = await repository.GetByIdAsync("2");
            state!.ErrorMessage = "Recorded by the next owner";
            await repository.SaveAsync(state);
        }

        bool duringSave = takeover is not Takeover.DuringMigration;
        using var subscription = duringSave ? _migrationStateRepository.DocumentsSaving.AddHandler((_, _) => TakeOverAsync()) : null;
        var migration = new CallbackMigration(type, 2, async _ =>
        {
            if (!duringSave)
                await TakeOverAsync();
            if (migrationFails)
                throw new ApplicationException("Original worker failed");
        });
        _migrationManager.Migrations.Add(migration);
        _migrationManager.AddMigration<Version3Migration>();

        // Act
        var result = await _migrationManager.RunMigrationsAsync(TestCancellationToken);

        // Assert
        await using var successorLock = Assert.IsAssignableFrom<ILock>(successor);
        Assert.Equal(MigrationResult.Failed, result);
        Assert.Equal(takeover is Takeover.DuringStartSave ? 0 : 1, migration.Runs);
        await successorLock.RenewAsync();
        var state = await _migrationStateRepository.GetByIdAsync("2");
        Assert.NotNull(state);
        Assert.Null(state.CompletedUtc);
        Assert.Equal("Recorded by the next owner", state.ErrorMessage);
        Assert.Null(await _migrationStateRepository.GetByIdAsync("3"));
        var expectedFailure = duringSave ? typeof(VersionConflictDocumentException) : typeof(LockException);
        Assert.Contains(Log.LogEntries, entry => String.Equals(entry.CategoryName, typeof(MigrationManager).FullName, StringComparison.Ordinal) &&
            entry.Exception is { } ex && ex.GetType() == expectedFailure && entry.Message.EndsWith($": {ex.Message}", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(LeaseLoss.DuringInitialization)]
    [InlineData(LeaseLoss.AfterStart)]
    [InlineData(LeaseLoss.AfterCompletion)]
    public async Task LosingTheLeaseStopsAtTheLastPersistedState(LeaseLoss when)
    {
        // Arrange
        if (when is not LeaseLoss.DuringInitialization)
            await AddCompletedVersion1MigrationAsync();
        var migration = new CallbackMigration(MigrationType.Versioned, 2, _ => Task.CompletedTask);
        _migrationManager.Migrations.Add(migration);
        _migrationManager.AddMigration<Version3Migration>();
        Task LoseLeaseAsync(object sender, EventArgs args) => _lockProvider.ReleaseAsync("migration-manager");
        using var subscription = when switch
        {
            LeaseLoss.DuringInitialization => _migrationStateRepository.AfterQuery.AddHandler(LoseLeaseAsync),
            LeaseLoss.AfterStart => _migrationStateRepository.DocumentsAdded.AddHandler(LoseLeaseAsync),
            LeaseLoss.AfterCompletion => _migrationStateRepository.DocumentsSaved.AddHandler(LoseLeaseAsync),
            _ => throw new ArgumentOutOfRangeException(nameof(when))
        };

        // Act
        var result = await _migrationManager.RunMigrationsAsync(TestCancellationToken);

        // Assert
        Assert.Equal(MigrationResult.Failed, result);
        Assert.Equal(when is LeaseLoss.AfterCompletion ? 1 : 0, migration.Runs);
        Assert.Null(await _migrationStateRepository.GetByIdAsync("3"));
        var state = await _migrationStateRepository.GetByIdAsync("2");
        Assert.Equal(when is not LeaseLoss.DuringInitialization, state is not null);
        Assert.Equal(when is LeaseLoss.AfterCompletion, state?.CompletedUtc is not null);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnUnconfirmedContextRenewalStopsTheRun(bool migrationHandlesException)
    {
        // Arrange
        await AddCompletedVersion1MigrationAsync();
        using var cache = new RejectOneRenewalCache();
        var manager = new MigrationManager(_serviceProvider, _migrationStateRepository, new CacheLockProvider(cache, _messageBus, Log), Log);
        var migration = new CallbackMigration(MigrationType.VersionedAndResumable, 2, async context =>
        {
            cache.RejectNextRenewal = true;
            if (migrationHandlesException)
                await Assert.ThrowsAsync<LockException>(() => context.Lock.RenewAsync());
            else
                await context.Lock.RenewAsync();
        });
        manager.Migrations.Add(migration);
        manager.AddMigration<Version3Migration>();

        // Act
        var result = await manager.RunMigrationsAsync(TestCancellationToken);

        // Assert
        Assert.Equal(1, migration.Runs);
        Assert.Equal(MigrationResult.Failed, result);
        var state = await _migrationStateRepository.GetByIdAsync("2");
        Assert.NotNull(state);
        Assert.Null(state.CompletedUtc);
        Assert.Null(state.ErrorMessage);
        Assert.Null(await _migrationStateRepository.GetByIdAsync("3"));
    }

    [Theory]
    [InlineData(null, true)]
    [InlineData(2, false)]
    public async Task CancelledRepeatableRevisionRunsUnderAFreshLease(int? revision, bool throwOnCancellation)
    {
        // Arrange
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        var migration = new CallbackMigration(MigrationType.Repeatable, revision, context =>
        {
            cancellation.Cancel();
            if (throwOnCancellation)
                context.CancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        });
        _migrationManager.Migrations.Add(migration);
        if (revision.HasValue)
            await AddCompletedVersion1MigrationAsync(migration);

        // Act
        var result = await _migrationManager.RunMigrationsAsync(cancellation.Token);

        // Assert
        Assert.Equal(MigrationResult.Cancelled, result);
        var state = await _migrationStateRepository.GetByIdAsync(migration.GetId()!);
        Assert.NotNull(state);
        Assert.Null(state.CompletedUtc);
        Assert.Null(state.ErrorMessage);
        Assert.Equal(MigrationResult.Success, await _migrationManager.RunMigrationsAsync(TestCancellationToken));
        Assert.Equal(2, migration.Runs);
        Assert.Empty((await _migrationManager.GetMigrationStatus()).PendingMigrations);
    }

    [Fact]
    public async Task OnlyTheMigrationRunnerRejectsAConcurrentRewriteOfTheSameRevision()
    {
        // Arrange
        await AddCompletedVersion1MigrationAsync();
        MigrationState? rewritten = null;
        _migrationManager.Migrations.Add(new CallbackMigration(MigrationType.Versioned, 2, async _ =>
        {
            var first = Assert.IsType<MigrationState>(await _migrationStateRepository.GetByIdAsync("2"));
            var second = Assert.IsType<MigrationState>(await _migrationStateRepository.GetByIdAsync("2"));
            await _migrationStateRepository.SaveAsync(first);
            rewritten = await _migrationStateRepository.SaveAsync(second);
        }));

        // Act
        var result = await _migrationManager.RunMigrationsAsync(TestCancellationToken);

        // Assert
        Assert.Equal(2, rewritten?.Version);
        Assert.Equal(MigrationResult.Failed, result);
        var state = Assert.IsType<MigrationState>(await _migrationStateRepository.GetByIdAsync("2"));
        Assert.Null(state.CompletedUtc);
    }

    [Theory]
    [InlineData(MigrationResult.Failed)]
    [InlineData(MigrationResult.Cancelled, JobCancellation.BeforeStart)]
    [InlineData(MigrationResult.Cancelled, JobCancellation.AfterStart)]
    [InlineData(MigrationResult.Cancelled, JobCancellation.DuringVersionCheck)]
    [InlineData(MigrationResult.UnableToAcquireLock)]
    [InlineData(MigrationResult.Success)]
    [InlineData(MigrationResult.Success, JobCancellation.None, 3)]
    public async Task MigrationJobReindexesOnlyAfterSuccess(MigrationResult outcome, JobCancellation cancelAt = JobCancellation.None, int existingVersion = 1)
    {
        // Arrange
        await AddCompletedVersion1MigrationAsync();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        await using var owner = outcome is MigrationResult.UnableToAcquireLock
            ? await _lockProvider.AcquireAsync("migration-manager", cancellationToken: TestCancellationToken)
            : null;
        if (owner is null)
            _migrationManager.AddMigration<Version2Migration>();
        using var configuration = new ElasticConfiguration();
        var newIndex = new JobTestIndex(_configuration);
        await DeleteWildcardIndicesAsync("migration-job-test-v*");
        await new VersionedIndex(_configuration, newIndex.Name, existingVersion).ConfigureAsync();
        configuration.AddIndex(newIndex);
        var job = new TestMigrationJob(_migrationManager, configuration);
        using var afterStart = cancelAt is JobCancellation.AfterStart
            ? _migrationStateRepository.DocumentsAdded.AddHandler((_, _) => cancellation.CancelAsync())
            : null;
        using var afterSave = _migrationStateRepository.DocumentsSaved.AddHandler((_, _) =>
        {
            if (outcome is MigrationResult.Failed)
                return _lockProvider.ReleaseAsync("migration-manager");
            if (cancelAt is JobCancellation.DuringVersionCheck)
                newIndex.OnVersionCheck = cancellation.Cancel;
            return Task.CompletedTask;
        });
        if (cancelAt is JobCancellation.BeforeStart)
            cancellation.Cancel();

        // Act
        var result = await job.RunAsync(cancellation.Token);

        // Assert
        Assert.Equal(outcome is MigrationResult.Success, result.IsSuccess);
        Assert.Equal(outcome is MigrationResult.Cancelled, result.IsCancelled);
        bool shouldReindex = outcome is MigrationResult.Success && existingVersion < newIndex.Version;
        Assert.Equal(shouldReindex, newIndex.ReindexRequested);
        Assert.Equal(shouldReindex ? newIndex.Version : existingVersion, await newIndex.GetCurrentVersionAsync());
        if (cancelAt is JobCancellation.BeforeStart)
            Assert.False((await _client.Indices.ExistsAsync(newIndex.VersionedName, cancellationToken: TestCancellationToken)).Exists);
        if (cancelAt is JobCancellation.AfterStart)
            Assert.Null(Assert.IsType<MigrationState>(await _migrationStateRepository.GetByIdAsync("2")).CompletedUtc);
        if (owner is not null)
        {
            Assert.Contains(nameof(MigrationResult.UnableToAcquireLock), result.Message);
            Assert.Null(await _migrationStateRepository.GetByIdAsync("2"));
            await owner.RenewAsync();
        }
    }

    public enum JobCancellation
    {
        None,
        BeforeStart,
        AfterStart,
        DuringVersionCheck
    }

    public enum Takeover
    {
        DuringMigration,
        DuringStartSave,
        DuringResultSave
    }

    public enum LeaseLoss
    {
        DuringInitialization,
        AfterStart,
        AfterCompletion
    }

    private Task AddCompletedVersion1MigrationAsync(IMigration? migration = null)
    {
        return _migrationStateRepository.AddAsync(new MigrationState
        {
            Id = migration?.GetId() ?? "1",
            Version = 1,
            MigrationType = migration?.MigrationType ?? MigrationType.Versioned,
            StartedUtc = DateTime.UtcNow,
            CompletedUtc = DateTime.UtcNow
        });
    }

    private sealed class CallbackMigration(MigrationType type, int? version, Func<MigrationContext, Task> run) : IMigration
    {
        public MigrationType MigrationType => type;
        public int? Version => version;
        public bool RequiresOffline => false;
        public int Runs { get; private set; }

        public Task RunAsync(MigrationContext context)
        {
            Runs++;
            return run(context);
        }
    }

    private sealed class RejectOneRenewalCache : InMemoryCacheClient, ICacheClient
    {
        public bool RejectNextRenewal { get; set; }

        Task<bool> ICacheClient.ReplaceIfEqualAsync<T>(string key, T value, T expected, TimeSpan? expiresIn)
        {
            if (!RejectNextRenewal)
                return ReplaceIfEqualAsync(key, value, expected, expiresIn);

            RejectNextRenewal = false;
            return Task.FromResult(false);
        }
    }

    private sealed class TestMigrationJob(MigrationManager manager, IElasticConfiguration configuration)
        : ElasticMigrationJobBase(manager, configuration);

    private sealed class JobTestIndex(IElasticConfiguration configuration) : VersionedIndex(configuration, "migration-job-test", 2)
    {
        public Action? OnVersionCheck { get; set; }
        public bool ReindexRequested { get; private set; }

        public override Task<int> GetCurrentVersionAsync()
        {
            OnVersionCheck?.Invoke();
            return base.GetCurrentVersionAsync();
        }

        public override Task ReindexAsync(Func<int, string?, Task>? progressCallbackAsync = null)
        {
            ReindexRequested = true;
            return base.ReindexAsync(progressCallbackAsync);
        }
    }
}

public class VersionedWithoutVersionMigration : MigrationBase
{
    public override Task RunAsync(MigrationContext context)
    {
        throw new System.NotImplementedException();
    }
}

public class Version1Migration : MigrationBase
{
    public Version1Migration()
    {
        MigrationType = MigrationType.Versioned;
        Version = 1;
    }

    public override Task RunAsync(MigrationContext context)
    {
        return Task.Delay(100);
    }
}

public class Version2Migration : MigrationBase
{
    public Version2Migration()
    {
        MigrationType = MigrationType.Versioned;
        Version = 2;
    }

    public override Task RunAsync(MigrationContext context)
    {
        return Task.Delay(100);
    }
}

public class Version3Migration : MigrationBase
{
    public Version3Migration()
    {
        MigrationType = MigrationType.Versioned;
        Version = 3;
    }

    public override Task RunAsync(MigrationContext context)
    {
        return Task.Delay(100);
    }
}

public class FailingMigration : MigrationBase
{
    public FailingMigration()
    {
        MigrationType = MigrationType.Versioned;
        Version = 3;
    }

    public int Attempts { get; set; }

    public override Task RunAsync(MigrationContext context)
    {
        Attempts++;
        return Task.FromException(new ApplicationException("Boom"));
    }
}

public class FailingResumableMigration : MigrationBase
{
    public FailingResumableMigration()
    {
        MigrationType = MigrationType.VersionedAndResumable;
        Version = 3;
    }

    public int Attempts { get; set; }
    public Exception Failure { get; set; } = new ApplicationException("Boom");

    public override Task RunAsync(MigrationContext context)
    {
        Attempts++;
        if (Attempts <= 3)
            return Task.FromException(Failure);

        return Task.CompletedTask;
    }
}

public class RepeatableMigration : MigrationBase
{
    public RepeatableMigration()
    {
        MigrationType = MigrationType.Repeatable;
    }

    public void SetVersion(int? version)
    {
        Version = version;
    }

    public int Runs { get; set; }

    public override Task RunAsync(MigrationContext context)
    {
        Runs++;
        return Task.CompletedTask;
    }
}
