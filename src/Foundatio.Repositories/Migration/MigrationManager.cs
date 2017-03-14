using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Foundatio.Logging;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;

namespace Foundatio.Repositories.Migrations {
    public class MigrationManager {
        private readonly IServiceProvider _container;
        private readonly IMigrationRepository _migrationRepository;
        protected readonly ILogger _logger;
        private readonly IList<Assembly> _assemblies = new List<Assembly>();

        public MigrationManager(IServiceProvider container, IMigrationRepository migrationRepository, ILogger<MigrationManager> logger = null) {
            _container = container;
            _migrationRepository = migrationRepository;
            _logger = logger ?? NullLogger.Instance;
        }

        public void RegisterAssemblyMigrations(Assembly assembly) {
            _assemblies.Add(assembly);
        }

        public void RegisterAssemblyMigrations<T>() {
            _assemblies.Add(typeof(T).Assembly);
        }

        public async Task RunAsync() {
            var migrations = await GetPendingMigrationsAsync().AnyContext();
            foreach (var m in migrations.Where(m => m.Version.HasValue)) {
                await MarkMigrationStartedAsync(m.Version.Value).AnyContext();
                await m.RunAsync().AnyContext();
                await MarkMigrationCompleteAsync(m.Version.Value).AnyContext();
            }
        }

        public Task RunAsync<T>() {
            var migration = _container.GetService(typeof(T)) as IMigration;
            return RunAsync(migration);
        }

        public Task RunAsync(Type migrationType) {
            var migration = _container.GetService(migrationType) as IMigration;
            return RunAsync(migration);
        }

        public async Task RunAsync(IMigration migration) {
            if (migration.Version.HasValue)
                await MarkMigrationStartedAsync(migration.Version.Value).AnyContext();

            await migration.RunAsync().AnyContext();

            if (migration.Version.HasValue)
                await MarkMigrationCompleteAsync(migration.Version.Value).AnyContext();
        }

        private Task MarkMigrationStartedAsync(int version) {
            _logger.Info($"Starting migration for version {version}...");
            return _migrationRepository.AddAsync(new Migration { Version = version, StartedUtc = SystemClock.UtcNow });
        }

        private async Task MarkMigrationCompleteAsync(int version) {
            var m = await _migrationRepository.GetByIdAsync("migration-" + version).AnyContext();
            if (m == null)
                m = new Migration { Version = version };

            m.CompletedUtc = SystemClock.UtcNow;
            await _migrationRepository.SaveAsync(m).AnyContext();
            _logger.Info($"Completed migration for version {version}.");
        }

        private ICollection<IMigration> GetAllMigrations() {
            var migrationTypes = GetDerivedTypes<IMigration>(_assemblies);
            return migrationTypes
                .Select(migrationType => (IMigration)_container.GetService(migrationType))
                .OrderBy(m => m.Version)
                .ToList();
        }

        public async Task<ICollection<IMigration>> GetPendingMigrationsAsync() {
            var allMigrations = GetAllMigrations();
            var completedMigrations = await _migrationRepository.GetAllAsync(o => o.PageLimit(1000)).AnyContext();

            int max = 0;
            // if migrations have never run before, mark highest version as completed
            if (completedMigrations.Documents.Count == 0) {
                if (allMigrations.Count > 0)
                    max = allMigrations.Where(m => m.Version.HasValue).Max(m => m.Version.Value);

                await MarkMigrationCompleteAsync(max);

                return new List<IMigration>();
            }

            int currentVersion = completedMigrations.Documents.Max(m => m.Version);
            return allMigrations.Where(m => m.Version > currentVersion).ToList();
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
