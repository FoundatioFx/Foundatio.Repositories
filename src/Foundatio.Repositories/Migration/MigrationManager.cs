using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Lock;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Resilience;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Migrations;

public class MigrationManager
{
    private static readonly TimeSpan _leaseDuration = TimeSpan.FromMinutes(30);

    protected readonly IServiceProvider _serviceProvider;
    protected readonly IMigrationStateRepository _migrationStatusRepository;
    protected readonly ILockProvider _lockProvider;
    protected readonly IResiliencePolicyProvider _resiliencePolicyProvider;
    protected readonly IResiliencePolicy _resiliencePolicy;
    protected readonly TimeProvider _timeProvider;
    protected readonly ILoggerFactory _loggerFactory;
    protected readonly ILogger _logger;
    protected readonly List<IMigration> _migrations = new();

    public MigrationManager(IServiceProvider serviceProvider, IMigrationStateRepository migrationStatusRepository, ILockProvider lockProvider, ILoggerFactory loggerFactory)
    {
        _serviceProvider = serviceProvider;
        _timeProvider = serviceProvider.GetService<TimeProvider>() ?? TimeProvider.System;
        _migrationStatusRepository = migrationStatusRepository;
        _lockProvider = lockProvider;
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<MigrationManager>();

        _resiliencePolicyProvider = serviceProvider.GetService<IResiliencePolicyProvider>() ?? new ResiliencePolicyProvider();
        _resiliencePolicy = _resiliencePolicyProvider.GetPolicy<MigrationManager>(fallback => fallback.WithMaxAttempts(3).WithDelay(TimeSpan.Zero), _logger, _timeProvider);
    }

    public void AddMigrationsFromLoadedAssemblies()
    {
        var migrationTypes = GetDerivedTypes<IMigration>(AppDomain.CurrentDomain.GetAssemblies());
        AddMigration(migrationTypes);
    }

    public void AddMigrationsFromAssembly(Assembly assembly)
    {
        var migrationTypes = GetDerivedTypes<IMigration>(new[] { assembly });
        AddMigration(migrationTypes);
    }

    public void AddMigrationsFromAssembly<T>() where T : IMigration
    {
        AddMigrationsFromAssembly(typeof(T).Assembly);
    }

    public void AddMigration<T>() where T : IMigration
    {
        AddMigration(typeof(T));
    }

    public void AddMigration(Type migrationType)
    {
        if (migrationType == null)
            throw new ArgumentNullException(nameof(migrationType));

        object? migrationInstance = _serviceProvider.GetService(migrationType);
        if (migrationInstance == null)
            throw new ArgumentException($"Unable to get instance of type '{migrationType.Name}'. Please ensure it's registered in Dependency Injection.", nameof(migrationType));

        if (migrationInstance is not IMigration migration)
            throw new ArgumentException($"Type '{migrationType.Name}' must implement interface '{nameof(IMigration)}'.", nameof(migrationType));

        var versionedMigrations = _migrations.Where(m => m.MigrationType != MigrationType.Repeatable && m.Version.HasValue);
        if (migration.Version.HasValue && versionedMigrations.Any(m => m.Version is { } v && v == migration.Version))
            throw new ArgumentException($"Duplicate migration version detected for '{migrationType.Name}'", nameof(migrationType));

        _migrations.Add(migration);
    }

    public void AddMigration(IEnumerable<Type> migrationTypes)
    {
        foreach (var migrationType in migrationTypes)
            AddMigration(migrationType);
    }

    public ICollection<IMigration> Migrations => _migrations;

    /// <summary>Runs pending migrations while the migration lock is held. A failed renewal stops the run.</summary>
    public async Task<MigrationResult> RunMigrationsAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
            return MigrationResult.Cancelled;

        if (Migrations.Count is 0)
            AddMigrationsFromLoadedAssemblies();

        var handle = await _lockProvider.TryAcquireAsync("migration-manager", _leaseDuration, TimeSpan.Zero).AnyContext();
        if (handle is null)
            return MigrationResult.UnableToAcquireLock;

        var migrationLock = new MigrationLock(handle);
        try
        {
            var migrationStatus = await GetMigrationStatusAsync(migrationLock, cancellationToken).AnyContext();
            foreach (var migrationInfo in migrationStatus.PendingMigrations)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // A started, non-resumable migration requires manual recovery.
                if (migrationInfo.Migration.MigrationType is MigrationType.Versioned && migrationInfo.State is not null && migrationInfo.State.StartedUtc > DateTime.MinValue)
                {
                    _logger.LogError("Migration {Id} failed to complete and cannot be resumed, please correct the error and then delete the migration record to make it run again", migrationInfo.Migration.GetId());
                    return MigrationResult.Failed;
                }

                var result = await RunMigrationAsync(migrationInfo, migrationLock, cancellationToken).AnyContext();
                if (result is not MigrationResult.Success)
                    return result;
            }

            return cancellationToken.IsCancellationRequested ? MigrationResult.Cancelled : MigrationResult.Success;
        }
        catch (LockException ex) when (migrationLock.RenewalError is not null)
        {
            _logger.LogError(ex, "Migration lock renewal failed, stopping migrations");
            return MigrationResult.Failed;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return MigrationResult.Cancelled;
        }
        catch (Exception ex) when (ex is VersionConflictDocumentException or DuplicateDocumentException)
        {
            _logger.LogError(ex, "Migration state was changed by another writer, stopping migrations");
            return MigrationResult.Failed;
        }
        finally
        {
            try
            {
                await handle.ReleaseAsync().AnyContext();
            }
            catch (Exception ex) when (migrationLock.RenewalError is not null)
            {
                _logger.LogWarning(ex, "Failed to release the lost migration lock");
            }
        }
    }

    private async Task<MigrationResult> RunMigrationAsync(MigrationInfo info, MigrationLock migrationLock, CancellationToken cancellationToken)
    {
        await migrationLock.RenewAsync(_leaseDuration).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        await MarkMigrationStartedAsync(info).AnyContext();

        var logger = _loggerFactory.CreateLogger(info.Migration.GetType());

        async ValueTask RunAttemptAsync(CancellationToken token)
        {
            try
            {
                await migrationLock.RenewAsync(_leaseDuration).AnyContext();
                token.ThrowIfCancellationRequested();
                await info.Migration.RunAsync(new MigrationContext(migrationLock, logger, token)).AnyContext();
                token.ThrowIfCancellationRequested();
            }
            catch (Exception) when (migrationLock.RenewalError is not null)
            {
                // Leave any configured retry policy normally. The failed renewal is checked before saving state.
            }
        }

        try
        {
            if (info.Migration.MigrationType is MigrationType.Versioned)
                await RunAttemptAsync(cancellationToken).AnyContext();
            else
                await _resiliencePolicy.ExecuteAsync(RunAttemptAsync, cancellationToken).AnyContext();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return MigrationResult.Cancelled;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed running migration {Id}", info.Migration.GetId());
            await migrationLock.RenewAsync(_leaseDuration).AnyContext();
            Debug.Assert(info.State is not null);
            info.State.ErrorMessage = ex.Message;
            await _migrationStatusRepository.SaveAsync(info.State).AnyContext();
            return MigrationResult.Failed;
        }

        await migrationLock.RenewAsync(_leaseDuration).AnyContext();
        await MarkMigrationCompleteAsync(info).AnyContext();
        return MigrationResult.Success;
    }

    private sealed class MigrationLock(ILock handle) : ILock
    {
        private LockException? _renewalError;
        public LockException? RenewalError => Volatile.Read(ref _renewalError);

        public async Task RenewAsync(TimeSpan? timeUntilExpires = null)
        {
            if (RenewalError is { } previous)
                ExceptionDispatchInfo.Throw(previous);

            try
            {
                await handle.RenewAsync(timeUntilExpires).AnyContext();
            }
            catch (LockException ex)
            {
                Interlocked.CompareExchange(ref _renewalError, ex, null);
                throw;
            }
        }

        public string LockId => handle.LockId;
        public string Resource => handle.Resource;
        public DateTime AcquiredTimeUtc => handle.AcquiredTimeUtc;
        public TimeSpan TimeWaitedForLock => handle.TimeWaitedForLock;
        public int RenewalCount => handle.RenewalCount;
        public Task ReleaseAsync() => handle.ReleaseAsync();
        public ValueTask DisposeAsync() => handle.DisposeAsync();
    }

    private async Task MarkMigrationStartedAsync(MigrationInfo info)
    {
        _logger.LogInformation("Starting migration {Id}...", info.Migration.GetId());
        if (info.State == null)
        {
            info.State = await _migrationStatusRepository.AddAsync(new MigrationState
            {
                Id = info.Migration.GetId()!,
                MigrationType = info.Migration.MigrationType,
                Version = info.Migration.Version ?? 0,
                StartedUtc = _timeProvider.GetUtcNow().UtcDateTime
            }).AnyContext();
        }
        else
        {
            info.State.StartedUtc = _timeProvider.GetUtcNow().UtcDateTime;
            info.State.CompletedUtc = null;
            info.State.ErrorMessage = null;
            info.State.Version = info.Migration.Version ?? 0;
            info.State = await _migrationStatusRepository.SaveAsync(info.State).AnyContext();
        }
    }

    private async Task MarkMigrationCompleteAsync(MigrationInfo info)
    {
        Debug.Assert(info.State is not null, "State is set by MarkMigrationStartedAsync");
        info.State.CompletedUtc = _timeProvider.GetUtcNow().UtcDateTime;
        info.State.ErrorMessage = null;
        await _migrationStatusRepository.SaveAsync(info.State).AnyContext();
        _logger.LogInformation("Completed migration {Id}", info.State.Id);
    }

    public async Task<MigrationStatus> GetMigrationStatus()
    {
        try
        {
            return await GetMigrationStatusAsync(null, CancellationToken.None).AnyContext();
        }
        catch (DuplicateDocumentException)
        {
            // A concurrent caller created the baseline. Read its persisted state.
            return await GetMigrationStatusAsync(null, CancellationToken.None).AnyContext();
        }
    }

    private async Task<MigrationStatus> GetMigrationStatusAsync(MigrationLock? migrationLock, CancellationToken cancellationToken)
    {
        var migrations = Migrations.OrderBy(m => m.Version).ToList();
        string[] migrationIds = migrations.Select(m => m.GetId()!).ToArray();

        // get by id to ensure latest document versions
        var migrationStatesByIds = await _migrationStatusRepository.GetByIdsAsync(migrationIds).AnyContext();
        var migrationStates = migrationStatesByIds.ToList();

        // get all to add any additional migrations that are not configured
        var otherMigrationStates = await _migrationStatusRepository.GetAllAsync(o => o.PageLimit(1000)).AnyContext();
        migrationStates.AddRange(otherMigrationStates.Documents.Where(m => !migrationStates.Any(s => String.Equals(s.Id, m.Id, StringComparison.Ordinal))));

        var migrationInfos = migrations.Select(m => new MigrationInfo
        {
            Migration = m,
            State = migrationStates.FirstOrDefault(s => s.Id.Equals(m.GetId()))
        }).ToList();

        int max = 0;
        var versioned = migrationInfos.Where(i => i.Migration.MigrationType != MigrationType.Repeatable && i.Migration.Version.HasValue).ToArray();

        // if migrations have never run before, mark highest version as completed
        if (migrationStates.Count == 0)
        {
            if (migrationInfos.Count > 0)
            {
                if (versioned.Length > 0)
                {
                    var now = _timeProvider.GetUtcNow().UtcDateTime;
                    max = versioned.Max(v => v.Migration.Version!.Value);

                    if (migrationLock is not null)
                        await migrationLock.RenewAsync(_leaseDuration).AnyContext();
                    cancellationToken.ThrowIfCancellationRequested();
                    await _migrationStatusRepository.AddAsync(new MigrationState
                    {
                        Id = max.ToString(),
                        Version = max,
                        MigrationType = MigrationType.Versioned,
                        StartedUtc = now,
                        CompletedUtc = now
                    }).AnyContext();

                    return new MigrationStatus(null, max);
                }
            }

            return new MigrationStatus(null, -1);
        }

        var completedVersionedMigrations = migrationStates.Where(i => i.MigrationType != MigrationType.Repeatable && i.CompletedUtc.HasValue).ToList();
        int currentVersion = -1;
        if (completedVersionedMigrations.Count > 0)
            currentVersion = completedVersionedMigrations.Max(m => m.Version);

        var pendingMigrations = new List<MigrationInfo>();
        pendingMigrations.AddRange(versioned.Where(m => m.Migration.Version!.Value > currentVersion).OrderBy(m => m.Migration.Version!.Value));

        // An interrupted repeatable migration remains pending for the same revision.
        pendingMigrations.AddRange(migrationInfos.Where(i => i.Migration.MigrationType is MigrationType.Repeatable &&
            (i.State is null || i.State.Version < i.Migration.Version ||
             (i.State.Version == (i.Migration.Version ?? 0) && (i.State.CompletedUtc is null || i.State.ErrorMessage is not null)))));

        return new MigrationStatus(pendingMigrations, currentVersion);
    }

    private IEnumerable<Type> GetDerivedTypes<TAction>(IList<Assembly>? assemblies = null)
    {
        if (assemblies == null || assemblies.Count == 0)
            assemblies = AppDomain.CurrentDomain.GetAssemblies();

        var types = new List<Type>();
        foreach (var assembly in assemblies)
        {
            try
            {
                types.AddRange(from type in assembly.GetTypes() where type.IsClass && !type.IsNotPublic && !type.IsAbstract && typeof(TAction).IsAssignableFrom(type) select type);
            }
            catch (ReflectionTypeLoadException ex)
            {
                string loaderMessages = String.Join(", ", (ex.LoaderExceptions ?? []).OfType<Exception>().Select(le => le.Message));
                _logger.LogInformation(ex, "Unable to search types from assembly {Assembly} for plugins of type {PluginType}: {LoaderMessages}", assembly.FullName, typeof(TAction).Name, loaderMessages);
            }
        }

        return types;
    }
}

public enum MigrationResult
{
    Success,
    Failed,
    UnableToAcquireLock,
    Cancelled
}

[DebuggerDisplay("Type: {Migration.MigrationType} Version {Migration.Version}")]
public record MigrationInfo
{
    public required IMigration Migration { get; init; }
    public MigrationState? State { get; set; }
}

public class MigrationStatus
{
    public MigrationStatus(IReadOnlyCollection<MigrationInfo>? pendingMigrations, int currentVersion)
    {
        PendingMigrations = pendingMigrations ?? EmptyReadOnly<MigrationInfo>.Collection;
        CurrentVersion = currentVersion;
    }

    public IReadOnlyCollection<MigrationInfo> PendingMigrations { get; }
    public int CurrentVersion { get; }
    public bool NeedsMigration => PendingMigrations.Count > 0;
    public bool RequiresOffline => PendingMigrations.Any(m => m.Migration.RequiresOffline);
}
