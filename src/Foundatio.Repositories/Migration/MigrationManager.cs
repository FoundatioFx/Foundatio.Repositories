using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Migrations {
    public class MigrationManager {
        private readonly IServiceProvider _container;
        private readonly IMigrationRepository _migrationRepository;
        protected readonly ILogger<MigrationManager> _logger;
        private readonly List<IMigration> _migrations = new List<IMigration>();

        public MigrationManager(IServiceProvider container, IMigrationRepository migrationRepository, ILogger<MigrationManager> logger = null) {
            _container = container;
            _migrationRepository = migrationRepository;
            _logger = logger ?? NullLogger<MigrationManager>.Instance;
        }

        public void AddMigrationsFromLoadedAssemblies() {
            var migrationTypes = GetDerivedTypes<IMigration>(AppDomain.CurrentDomain.GetAssemblies());
            AddMigration(migrationTypes);
        }

        public void AddMigrationsFromAssembly(Assembly assembly) {
            var migrationTypes = GetDerivedTypes<IMigration>(new[] { assembly });
            AddMigration(migrationTypes);
        }

        public void AddMigrationsFromAssembly<T>() where T : IMigration {
            AddMigrationsFromAssembly(typeof(T).Assembly);
        }

        public void AddMigration<T>() where T : IMigration {
            AddMigration(typeof(T));
        }

        public void AddMigration(Type migrationType) {
            if (migrationType == null)
                throw new ArgumentNullException(nameof(migrationType));

            object migrationInstance = _container.GetService(migrationType);
            if (migrationInstance == null)
                throw new ArgumentException($"Unable to get instance of type '{migrationType.Name}'. Please ensure it's registered in Dependency Injection.", nameof(migrationType));

            if (!(migrationInstance is IMigration migration))
                throw new ArgumentException($"Type '{migrationType.Name}' must implement interface '{nameof(IMigration)}'.", nameof(migrationType));

            _migrations.Add(migration);
        }

        public void AddMigration(IEnumerable<Type> migrationTypes) {
            foreach (var migrationType in migrationTypes)
                AddMigration(migrationType);
        }

        public ICollection<IMigration> Migrations => _migrations;

        public bool ShouldRunUnversionedMigrations { get; set; } = false;

        public async Task RunMigrationsAsync() {
            if (Migrations.Count == 0)
                AddMigrationsFromLoadedAssemblies();

            var migrations = await GetPendingMigrationsAsync();
            foreach (var migration in migrations) {
                if (migration.Version.HasValue)
                    await MarkMigrationStartedAsync(migration.Version.Value).AnyContext();

                await migration.RunAsync().AnyContext();

                if (migration.Version.HasValue)
                    await MarkMigrationCompleteAsync(migration.Version.Value).AnyContext();
            }
        }

        private Task MarkMigrationStartedAsync(int version) {
            _logger.LogInformation("Starting migration for version {Version}...", version);
            return _migrationRepository.AddAsync(new Migration { Version = version, StartedUtc = SystemClock.UtcNow });
        }

        private async Task MarkMigrationCompleteAsync(int version) {
            var m = await _migrationRepository.GetByIdAsync("migration-" + version).AnyContext();
            if (m == null)
                m = new Migration { Version = version };

            m.CompletedUtc = SystemClock.UtcNow;
            await _migrationRepository.SaveAsync(m).AnyContext();
            _logger.LogInformation("Completed migration for version {Version}.", version);
        }

        public async Task<ICollection<IMigration>> GetPendingMigrationsAsync() {
            var migrations = Migrations.OrderBy(m => m.Version).ToList();
            var completedMigrations = await _migrationRepository.GetAllAsync(o => o.PageLimit(1000)).AnyContext();

            int max = 0;
            // if migrations have never run before, mark highest version as completed
            if (completedMigrations.Documents.Count == 0) {
                if (migrations.Count > 0)
                    max = migrations.Where(m => m.Version.HasValue).Max(m => m.Version.Value);

                await MarkMigrationCompleteAsync(max);

                return new List<IMigration>();
            }

            int currentVersion = completedMigrations.Documents.Max(m => m.Version);
            return migrations.Where(m => (m.Version.HasValue == false && ShouldRunUnversionedMigrations) || (m.Version.HasValue && m.Version > currentVersion)).ToList();
        }

        private static IEnumerable<Type> GetDerivedTypes<TAction>(IList<Assembly> assemblies = null) {
            if (assemblies == null || assemblies.Count == 0)
                assemblies = AppDomain.CurrentDomain.GetAssemblies();

            var types = new List<Type>();
            foreach (var assembly in assemblies) {
                try {
                    types.AddRange(from type in assembly.GetTypes() where type.IsClass && !type.IsNotPublic && !type.IsAbstract && typeof(TAction).IsAssignableFrom(type) select type);
                } catch (ReflectionTypeLoadException ex) {
                    string loaderMessages = String.Join(", ", ex.LoaderExceptions.ToList().Select(le => le.Message));
                    Trace.TraceInformation("Unable to search types from assembly \"{0}\" for plugins of type \"{1}\": {2}", assembly.FullName, typeof(TAction).Name, loaderMessages);
                }
            }

            return types;
        }
    }
}
