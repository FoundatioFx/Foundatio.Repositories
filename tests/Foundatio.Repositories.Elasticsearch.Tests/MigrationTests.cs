using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Lock;
using Foundatio.Messaging;
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

        var cacheClient = new InMemoryCacheClient();
        var messageBus = new InMemoryMessageBus();
        _lockProvider = new CacheLockProvider(cacheClient, messageBus, Log);
        _migrationStateRepository = new MigrationStateRepository(_migrationIndex);

        var serviceCollection = new ServiceCollection();
        serviceCollection.AddSingleton<VersionedWithoutVersionMigration>();
        serviceCollection.AddSingleton<Version1Migration>();
        serviceCollection.AddSingleton<Version2Migration>();
        serviceCollection.AddSingleton<Version3Migration>();
        serviceCollection.AddSingleton<FailingMigration>();
        serviceCollection.AddSingleton<FailingResumableMigration>();
        serviceCollection.AddSingleton<RepeatableMigration>();
        serviceCollection.AddSingleton<LockLosingVersionedMigration>();
        serviceCollection.AddSingleton<CountingVersion3Migration>();
        serviceCollection.AddSingleton<LockLosingResumableMigration>();
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
        await _migrationStateRepository.AddAsync(new MigrationState
        {
            Id = "1",
            Version = 1,
            MigrationType = MigrationType.Versioned,
            StartedUtc = DateTime.UtcNow,
            CompletedUtc = DateTime.UtcNow
        });

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
    public async Task CanRunRepeatableMigration()
    {
        await _migrationStateRepository.AddAsync(new MigrationState
        {
            Id = "1",
            Version = 1,
            MigrationType = MigrationType.Versioned,
            StartedUtc = DateTime.UtcNow,
            CompletedUtc = DateTime.UtcNow
        });

        _migrationManager.AddMigration<RepeatableMigration>();

        var migrationStatus = await _migrationManager.GetMigrationStatus();
        Assert.Single(migrationStatus.PendingMigrations);
        Assert.True(migrationStatus.NeedsMigration);
        Assert.Equal(1, migrationStatus.CurrentVersion);

        var result = await _migrationManager.RunMigrationsAsync(TestCancellationToken);
        Assert.Equal(MigrationResult.Success, result);

        await _client.Indices.RefreshAsync(cancellationToken: TestCancellationToken);

        var migrations = await _migrationStateRepository.GetAllAsync();
        Assert.Equal(2, migrations.Documents.Count);
        var migrationState = migrations.Documents.FirstOrDefault(d => d.Id == typeof(RepeatableMigration).FullName);
        Assert.NotNull(migrationState);
        Assert.Equal(MigrationType.Repeatable, migrationState.MigrationType);
        Assert.NotEqual(DateTime.MinValue, migrationState.StartedUtc);
        Assert.NotEqual(DateTime.MinValue, migrationState.CompletedUtc);
        Assert.Equal(0, migrationState.Version);
        Assert.Null(migrationState.ErrorMessage);

        migrationStatus = await _migrationManager.GetMigrationStatus();
        Assert.Empty(migrationStatus.PendingMigrations);
        Assert.False(migrationStatus.NeedsMigration);
        Assert.Equal(1, migrationStatus.CurrentVersion);

        result = await _migrationManager.RunMigrationsAsync(TestCancellationToken);
        Assert.Equal(MigrationResult.Success, result);

        var repeatableMigration = _serviceProvider.GetRequiredService<RepeatableMigration>();
        repeatableMigration.SetVersion(1);

        migrationStatus = await _migrationManager.GetMigrationStatus();
        Assert.Single(migrationStatus.PendingMigrations);
        Assert.True(migrationStatus.NeedsMigration);
        Assert.Equal(1, migrationStatus.CurrentVersion);

        result = await _migrationManager.RunMigrationsAsync(TestCancellationToken);
        Assert.Equal(MigrationResult.Success, result);

        migrations = await _migrationStateRepository.GetAllAsync();
        Assert.Equal(2, migrations.Documents.Count);
        migrationState = migrations.Documents.FirstOrDefault(d => d.Id == typeof(RepeatableMigration).FullName);
        Assert.NotNull(migrationState);
        Assert.Equal(MigrationType.Repeatable, migrationState.MigrationType);
        Assert.NotEqual(DateTime.MinValue, migrationState.StartedUtc);
        Assert.NotEqual(DateTime.MinValue, migrationState.CompletedUtc);
        Assert.Equal(1, migrationState.Version);
        Assert.Null(migrationState.ErrorMessage);
    }

    [Fact]
    public async Task CanHandleFailingMigration()
    {
        await _migrationStateRepository.AddAsync(new MigrationState
        {
            Id = "1",
            Version = 1,
            MigrationType = MigrationType.Versioned,
            StartedUtc = DateTime.UtcNow,
            CompletedUtc = DateTime.UtcNow
        });

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

    [Fact]
    public async Task WillRetryFailingMigration()
    {
        await _migrationStateRepository.AddAsync(new MigrationState
        {
            Id = "1",
            Version = 1,
            MigrationType = MigrationType.Versioned,
            StartedUtc = DateTime.UtcNow,
            CompletedUtc = DateTime.UtcNow
        });

        _migrationManager.AddMigration<FailingResumableMigration>();

        var migrationStatus = await _migrationManager.GetMigrationStatus();
        Assert.Single(migrationStatus.PendingMigrations);
        Assert.True(migrationStatus.NeedsMigration);
        Assert.Equal(1, migrationStatus.CurrentVersion);

        var result = await _migrationManager.RunMigrationsAsync(TestCancellationToken);
        Assert.Equal(MigrationResult.Failed, result);

        var failingMigration = _serviceProvider.GetRequiredService<FailingResumableMigration>();
        Assert.Equal(3, failingMigration.Attempts);

        await _client.Indices.RefreshAsync(cancellationToken: TestCancellationToken);

        var migrations = await _migrationStateRepository.GetAllAsync();
        Assert.Equal(2, migrations.Documents.Count);
        var migrationState = migrations.Documents.FirstOrDefault(d => d.Id == "3");
        Assert.NotNull(migrationState);
        Assert.Equal(MigrationType.VersionedAndResumable, migrationState.MigrationType);
        Assert.NotEqual(DateTime.MinValue, migrationState.StartedUtc);
        Assert.Null(migrationState.CompletedUtc);
        Assert.Equal("Boom", migrationState.ErrorMessage);

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

    [Fact]
    public async Task RunMigrationsAsync_WhenLockLostDuringResumableMigration_FailsWithoutRetrying()
    {
        // Arrange
        await AddCompletedVersion1MigrationAsync();
        var lockProvider = new RenewalCountingLockProvider(_lockProvider);
        var migrationManager = new MigrationManager(_serviceProvider, _migrationStateRepository, lockProvider, Log);
        migrationManager.AddMigration<LockLosingResumableMigration>();
        var migration = _serviceProvider.GetRequiredService<LockLosingResumableMigration>();
        migration.LockProvider = _lockProvider;

        // Act
        var result = await migrationManager.RunMigrationsAsync(TestCancellationToken);

        // Assert
        Assert.Equal(MigrationResult.Failed, result);
        Assert.Equal(1, migration.Attempts);
        Assert.Equal(2, lockProvider.RenewAttempts);

        await _client.Indices.RefreshAsync(cancellationToken: TestCancellationToken);
        var migrationState = await _migrationStateRepository.GetByIdAsync("4");
        Assert.NotNull(migrationState);
        Assert.Null(migrationState.CompletedUtc);
        Assert.NotNull(migrationState.ErrorMessage);
    }

    [Fact]
    public async Task RunMigrationsAsync_WhenLockLostBetweenMigrations_FailsWithoutRunningNextMigration()
    {
        // Arrange
        await AddCompletedVersion1MigrationAsync();
        _migrationManager.AddMigration<LockLosingVersionedMigration>();
        _migrationManager.AddMigration<CountingVersion3Migration>();
        var migration = _serviceProvider.GetRequiredService<LockLosingVersionedMigration>();
        migration.LockProvider = _lockProvider;
        var nextMigration = _serviceProvider.GetRequiredService<CountingVersion3Migration>();

        // Act
        var result = await _migrationManager.RunMigrationsAsync(TestCancellationToken);

        // Assert
        Assert.Equal(MigrationResult.Failed, result);
        Assert.Equal(0, nextMigration.Runs);

        await _client.Indices.RefreshAsync(cancellationToken: TestCancellationToken);
        var migrations = await _migrationStateRepository.GetAllAsync();
        Assert.DoesNotContain(migrations.Documents, d => d.Id == "3");
    }

    private Task AddCompletedVersion1MigrationAsync()
    {
        return _migrationStateRepository.AddAsync(new MigrationState
        {
            Id = "1",
            Version = 1,
            MigrationType = MigrationType.Versioned,
            StartedUtc = DateTime.UtcNow,
            CompletedUtc = DateTime.UtcNow
        });
    }
}

public class LockLosingVersionedMigration : MigrationBase
{
    public LockLosingVersionedMigration()
    {
        MigrationType = MigrationType.Versioned;
        Version = 2;
    }

    public ILockProvider LockProvider { get; set; } = null!;

    public override Task RunAsync(MigrationContext context)
    {
        return LockProvider.ReleaseAsync(context.Lock.Resource);
    }
}

public class CountingVersion3Migration : MigrationBase
{
    public CountingVersion3Migration()
    {
        MigrationType = MigrationType.Versioned;
        Version = 3;
    }

    public int Runs { get; set; }

    public override Task RunAsync(MigrationContext context)
    {
        Runs++;
        return Task.CompletedTask;
    }
}

public class LockLosingResumableMigration : MigrationBase
{
    public LockLosingResumableMigration()
    {
        MigrationType = MigrationType.VersionedAndResumable;
        Version = 4;
    }

    public ILockProvider LockProvider { get; set; } = null!;
    public int Attempts { get; set; }

    public override async Task RunAsync(MigrationContext context)
    {
        Attempts++;
        await LockProvider.ReleaseAsync(context.Lock.Resource);
        throw new ApplicationException("Transient failure");
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

    public override Task RunAsync(MigrationContext context)
    {
        Attempts++;
        if (Attempts <= 3)
            return Task.FromException(new ApplicationException("Boom"));

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

public sealed class RenewalCountingLockProvider(ILockProvider inner) : ILockProvider
{
    private int _renewAttempts;

    public int RenewAttempts => _renewAttempts;

    public async Task<ILock> AcquireAsync(string resource, TimeSpan? timeUntilExpires = null, bool releaseOnDispose = true, CancellationToken cancellationToken = default)
    {
        return new RenewalCountingLock(await inner.AcquireAsync(resource, timeUntilExpires, releaseOnDispose, cancellationToken), this);
    }

    public async Task<ILock?> TryAcquireAsync(string resource, TimeSpan? timeUntilExpires = null, bool releaseOnDispose = true, CancellationToken cancellationToken = default)
    {
        var lockInstance = await inner.TryAcquireAsync(resource, timeUntilExpires, releaseOnDispose, cancellationToken);
        return lockInstance is null ? null : new RenewalCountingLock(lockInstance, this);
    }

    public Task<bool> IsLockedAsync(string resource) => inner.IsLockedAsync(resource);
    public Task ReleaseAsync(string resource, string lockId) => inner.ReleaseAsync(resource, lockId);
    public Task ReleaseAsync(string resource) => inner.ReleaseAsync(resource);

    public Task RenewAsync(string resource, string lockId, TimeSpan? timeUntilExpires = null)
    {
        Interlocked.Increment(ref _renewAttempts);
        return inner.RenewAsync(resource, lockId, timeUntilExpires);
    }

    private sealed class RenewalCountingLock(ILock inner, RenewalCountingLockProvider provider) : ILock
    {
        public string LockId => inner.LockId;
        public string Resource => inner.Resource;
        public DateTime AcquiredTimeUtc => inner.AcquiredTimeUtc;
        public TimeSpan TimeWaitedForLock => inner.TimeWaitedForLock;
        public int RenewalCount => inner.RenewalCount;

        public Task RenewAsync(TimeSpan? timeUntilExpires = null)
        {
            Interlocked.Increment(ref provider._renewAttempts);
            return inner.RenewAsync(timeUntilExpires);
        }

        public Task ReleaseAsync() => inner.ReleaseAsync();
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}
