using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;

namespace Foundatio.Repositories.Migrations {
    public class MigrationManager {
        private readonly IServiceProvider _container;
        private readonly IMigrationRepository _migrationRepository;

        public MigrationManager(IServiceProvider container, IMigrationRepository migrationRepository) {
            _container = container;
            _migrationRepository = migrationRepository;
        }

        public async Task RunAsync() {
            var migrations = await GetPendingMigrationsAsync().AnyContext();
            foreach (var m in migrations) {
                await MarkMigrationStartedAsync(m.Version).AnyContext();
                await m.RunAsync().AnyContext();
                await MarkMigrationCompleteAsync(m.Version).AnyContext();
            }
        }

        private async Task MarkMigrationStartedAsync(int version) {
            await _migrationRepository.AddAsync(new Migration { Version = version, StartedUtc = SystemClock.UtcNow }).AnyContext();
        }

        private async Task MarkMigrationCompleteAsync(int version) {
            var m = await _migrationRepository.GetByIdAsync("migration-" + version).AnyContext();
            m.CompletedUtc = SystemClock.UtcNow;
            await _migrationRepository.SaveAsync(m).AnyContext();
        }

        private ICollection<IMigration> GetAllMigrations() {
            var migrationTypes = GetDerivedTypes<IMigration>(new[] { typeof(IMigration).Assembly });
            return migrationTypes
                .Select(migrationType => (IMigration)_container.GetService(migrationType))
                .OrderBy(m => m.Version)
                .ToList();
        }

        private async Task<ICollection<IMigration>> GetPendingMigrationsAsync() {
            var allMigrations = GetAllMigrations();
            var completedMigrations = await _migrationRepository.GetAllAsync(paging: 1000).AnyContext();
            var currentVersion = completedMigrations.Documents.Count > 0 ? completedMigrations.Documents.Max(m => m.Version) : 0;
            return allMigrations.Where(m => m.Version > currentVersion).ToList();
        }

        private static IEnumerable<Type> GetDerivedTypes<TAction>(IEnumerable<Assembly> assemblies = null) {
            if (assemblies == null)
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
