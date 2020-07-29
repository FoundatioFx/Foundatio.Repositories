using System.Threading.Tasks;

namespace Foundatio.Repositories.Migrations {
    public interface IMigration {
        /// <summary>
        /// The type of migration.
        /// <see cref="MigrationType.Versioned">Versioned</see> migrations are run once and they are run sequentially.
        /// <see cref="MigrationType.VersionedAndResumable">VersionedAndResumable</see> migrations are the
        /// same as <see cref="MigrationType.Versioned">Versioned</see> migrations except if they fail to complete, they will
        /// automatically be retried.
        /// <see cref="MigrationType.Repeatable">Repeatable</see> migrations will be run after all versioned migrations
        /// are run and can be repeatedly run without causing issues. If the <see cref="Version">Version</see> property on a
        /// repeatable migration is populated, it is treated as a revision for that specific migration and if it has a version
        /// that is higher than from when it was last run, the migration will be run again.
        /// </summary>
        MigrationType MigrationType { get; }

        /// <summary>
        /// <see cref="MigrationType.Versioned">Versioned</see> and <see cref="MigrationType.VersionedAndResumable">VersionedAndResumable</see>
        /// migrations require a version number in order to be run. The version of the migration determines the order in which migrations are run.
        /// If they don't have a version set, they will not be run automatically.
        /// will not be run automatically. For <see cref="MigrationType.Repeatable">Repeatable</see> migrations, if the version is
        /// populated then it is considered a revision of that specific migration and if the version is higher than the last time it was
        /// run then the migration will be run again.
        /// </summary>
        int? Version { get; }

        Task RunAsync();
    }

    public enum MigrationType {
        Versioned,
        VersionedAndResumable,
        Repeatable
    }

    public static class MigrationExtensions {
        public static string GetId(this IMigration migration) {
            return migration.MigrationType != MigrationType.Repeatable ? migration.Version.ToString() : migration.GetType().FullName;
        }
    }
}
