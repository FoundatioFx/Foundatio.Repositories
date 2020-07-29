using System.Threading.Tasks;

namespace Foundatio.Repositories.Migrations {
    public abstract class MigrationBase : IMigration {
        public MigrationType MigrationType { get; protected set; } = MigrationType.Versioned;

        public int? Version { get; protected set; } = null;

        public bool RequiresOffline { get; protected set; } = false;

        public abstract Task RunAsync(MigrationContext context);

        protected int CalculateProgress(long total, long completed, int startProgress = 0, int endProgress = 100) {
            return startProgress + (int)((100 * (double)completed / total) * (((double)endProgress - startProgress) / 100));
        }
    }
}
