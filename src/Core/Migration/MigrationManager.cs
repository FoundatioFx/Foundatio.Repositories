using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Migrations {
    public class MigrationManager {
        private readonly IMigrationRepository _migrationRepository;

        public MigrationManager(IMigrationRepository migrationRepository) {
            _migrationRepository = migrationRepository;
        }

        public async Task RunAsync(IEnumerable<IMigration> migrations) {
            var pendingMigrations = await GetPendingMigrationsAsync(migrations).AnyContext();
            foreach (var m in pendingMigrations) {
                await MarkMigrationStartedAsync(m.Version).AnyContext();
                await m.RunAsync().AnyContext();
                await MarkMigrationCompleteAsync(m.Version).AnyContext();
            }
        }

        private async Task MarkMigrationStartedAsync(int version) {
            await _migrationRepository.AddAsync(new MigrationResult { Version = version, StartedUtc = DateTime.UtcNow }).AnyContext();
        }

        private async Task MarkMigrationCompleteAsync(int version) {
            var m = await _migrationRepository.GetByIdAsync("migration-" + version).AnyContext();
            m.CompletedUtc = DateTime.UtcNow;
            await _migrationRepository.SaveAsync(m).AnyContext();
        }
        
        private async Task<ICollection<IMigration>> GetPendingMigrationsAsync(IEnumerable<IMigration> migrations) {
            var completedMigrations = await _migrationRepository.GetAllAsync(paging: 1000).AnyContext();
            var currentVersion = completedMigrations.Documents.Count > 0 ? completedMigrations.Documents.Max(m => m.Version) : 0;
            return migrations.OrderBy(m => m.Version).Where(m => m.Version > currentVersion).ToList();
        }
    }
}
