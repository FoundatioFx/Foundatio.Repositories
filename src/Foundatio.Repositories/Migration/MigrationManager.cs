using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Lock;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Resilience;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Migrations;

public class MigrationManager
{
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

        object migrationInstance = _serviceProvider.GetService(migrationType);
        if (migrationInstance == null)
            throw new ArgumentException($"Unable to get instance of type '{migrationType.Name}'. Please ensure it's registered in Dependency Injection.", nameof(migrationType));

        if (migrationInstance is not IMigration migration)
            throw new ArgumentException($"Type '{migrationType.Name}' must implement interface '{nameof(IMigration)}'.", nameof(migrationType));

        var versionedMigrations = _migrations.Where(m => m.MigrationType != MigrationType.Repeatable && m.Version.HasValue);
        if (migration.Version.HasValue && versionedMigrations.Any(m => m.Version.Value == migration.Version))
            throw new ArgumentException($"Duplicate migration version detected for '{migrationType.Name}'", nameof(migrationType));

        _migrations.Add(migration);
    }

    public void AddMigration(IEnumerable<Type> migrationTypes)
    {
        foreach (var migrationType in migrationTypes)
            AddMigration(migrationType);
    }

    public ICollection<IMigration> Migrations => _migrations;

    public async Task<MigrationResult> RunMigrationsAsync(CancellationToken cancellationToken = default)
    {
        if (Migrations.Count == 0)
            AddMigrationsFromLoadedAssemblies();

        var migrationsLock = await _lockProvider.AcquireAsync("migration-manager", TimeSpan.FromMinutes(30), TimeSpan.Zero);
        if (migrationsLock == null)
            return MigrationResult.UnableToAcquireLock;

        try
        {
            var migrationStatus = await GetMigrationStatus();
            if (!migrationStatus.NeedsMigration)
                return MigrationResult.Success;

            foreach (var migrationInfo in migrationStatus.PendingMigrations)
            {
                if (cancellationToken.IsCancellationRequested)
                    return MigrationResult.Cancelled;

                // stuck on non-resumable versioned migration, must be manually fixed
                if (migrationInfo.Migration.MigrationType == MigrationType.Versioned && migrationInfo.State != null && migrationInfo.State.StartedUtc > DateTime.MinValue)
                {
                    _logger.LogError("Migration {Id} failed to complete and cannot be resumed, please correct the error and then delete the migration record to make it run again", migrationInfo.Migration.GetId());
                    return MigrationResult.Failed;
                }

                await MarkMigrationStartedAsync(migrationInfo).AnyContext();

                try
                {
                    var context = new MigrationContext(migrationsLock, _loggerFactory.CreateLogger(migrationInfo.Migration.GetType()), cancellationToken);
                    if (migrationInfo.Migration.MigrationType != MigrationType.Versioned)
                        await _resiliencePolicy.ExecuteAsync(async () =>
                        {
                            await migrationsLock.RenewAsync(TimeSpan.FromMinutes(30));
                            if (cancellationToken.IsCancellationRequested)
                                return MigrationResult.Cancelled;

                            await migrationInfo.Migration.RunAsync(context).AnyContext();
                            return MigrationResult.Success;
                        }, cancellationToken).AnyContext();
                    else
                        await migrationInfo.Migration.RunAsync(context).AnyContext();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed running migration {Id}", migrationInfo.Migration.GetId());

                    migrationInfo.State.ErrorMessage = ex.Message;
                    await _migrationStatusRepository.SaveAsync(migrationInfo.State).AnyContext();

                    return MigrationResult.Failed;
                }

                await MarkMigrationCompleteAsync(migrationInfo).AnyContext();

                // renew migration lock
                await migrationsLock.RenewAsync(TimeSpan.FromMinutes(30));
            }
        }
        finally
        {
            await migrationsLock.ReleaseAsync();
        }

        return MigrationResult.Success;
    }

    private Task MarkMigrationStartedAsync(MigrationInfo info)
    {
        _logger.LogInformation("Starting migration {Id}...", info.Migration.GetId());
        if (info.State == null)
        {
            info.State = new MigrationState
            {
                Id = info.Migration.GetId(),
                MigrationType = info.Migration.MigrationType,
                Version = info.Migration.Version ?? 0,
                StartedUtc = _timeProvider.GetUtcNow().UtcDateTime
            };

            return _migrationStatusRepository.AddAsync(info.State);
        }
        else
        {
            info.State.StartedUtc = _timeProvider.GetUtcNow().UtcDateTime;
            info.State.Version = info.Migration.Version ?? 0;
            return _migrationStatusRepository.SaveAsync(info.State);
        }
    }

    private async Task MarkMigrationCompleteAsync(MigrationInfo info)
    {
        info.State.CompletedUtc = _timeProvider.GetUtcNow().UtcDateTime;
        info.State.ErrorMessage = null;
        await _migrationStatusRepository.SaveAsync(info.State).AnyContext();
        _logger.LogInformation("Completed migration {Id}", info.State.Id);
    }

    public async Task<MigrationStatus> GetMigrationStatus()
    {
        var migrations = Migrations.OrderBy(m => m.Version).ToList();
        string[] migrationIds = migrations.Select(m => m.GetId()).ToArray();

        // get by id to ensure latest document versions
        var migrationStatesByIds = await _migrationStatusRepository.GetByIdsAsync(migrationIds).AnyContext();
        var migrationStates = migrationStatesByIds.ToList();

        // get all to add any additional migrations that are not configured
        var otherMigrationStates = await _migrationStatusRepository.GetAllAsync(o => o.PageLimit(1000)).AnyContext();
        migrationStates.AddRange(otherMigrationStates.Documents.Where(m => !migrationStates.Any(s => s.Id == m.Id)));

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
                    max = versioned.Max(v => v.Migration.Version.Value);

                    // marking highest version as completed
                    await _migrationStatusRepository.SaveAsync(new MigrationState
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
        pendingMigrations.AddRange(versioned.Where(m => m.Migration.Version.Value > currentVersion).OrderBy(m => m.Migration.Version.Value));

        // repeatable migrations that haven't run or have a newer version
        pendingMigrations.AddRange(migrationInfos.Where(i => i.Migration.MigrationType == MigrationType.Repeatable && (i.State == null || i.State.Version < i.Migration.Version)));

        return new MigrationStatus(pendingMigrations, currentVersion);
    }

    private static IEnumerable<Type> GetDerivedTypes<TAction>(IList<Assembly> assemblies = null)
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
                string loaderMessages = String.Join(", ", ex.LoaderExceptions.ToList().Select(le => le.Message));
                Trace.TraceInformation("Unable to search types from assembly \"{0}\" for plugins of type \"{1}\": {2}", assembly.FullName, typeof(TAction).Name, loaderMessages);
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
public class MigrationInfo
{
    public IMigration Migration { get; set; }
    public MigrationState State { get; set; }
}

public class MigrationStatus
{
    public MigrationStatus(IReadOnlyCollection<MigrationInfo> pendingMigrations, int currentVersion)
    {
        PendingMigrations = pendingMigrations ?? EmptyReadOnly<MigrationInfo>.Collection;
        CurrentVersion = currentVersion;
    }

    public IReadOnlyCollection<MigrationInfo> PendingMigrations { get; }
    public int CurrentVersion { get; }
    public bool NeedsMigration => PendingMigrations.Count > 0;
    public bool RequiresOffline => PendingMigrations.Any(m => m.Migration.RequiresOffline);
}
