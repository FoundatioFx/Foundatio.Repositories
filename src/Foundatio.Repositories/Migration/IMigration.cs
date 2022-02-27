using System.Threading;
using System.Threading.Tasks;
using Foundatio.Lock;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Migrations;

public interface IMigration {
    /// <summary>
    /// The type of migration.
    /// <see cref="MigrationType.Versioned">Versioned</see> migrations are run once and they are run sequentially.
    /// <see cref="MigrationType.VersionedAndResumable">VersionedAndResumable</see> migrations are the
    /// same as <see cref="MigrationType.Versioned">Versioned</see> migrations except if they fail to complete, they will
    /// automatically be retried.
    /// <see cref="MigrationType.Repeatable">Repeatable</see> migrations will be run after all versioned migrations
    /// are run and can be repeatedly run without causing issues. Version is not required on
    /// <see cref="MigrationType.Repeatable">Repeatable</see> migrations, if the <see cref="Version">Version</see> property
    /// is populated, it is treated as a revision for that specific migration and if it has a revision
    /// that is higher than from when it was last run, then the migration will be run again.
    /// </summary>
    MigrationType MigrationType { get; }

    /// <summary>
    /// <see cref="MigrationType.Versioned">Versioned</see> and <see cref="MigrationType.VersionedAndResumable">VersionedAndResumable</see>
    /// migrations require a version number in order to be run. The version of the migration determines the order in which migrations are run.
    /// If a version is not set, they will be treated as disabled and will not run automatically.
    /// For <see cref="MigrationType.Repeatable">Repeatable</see> migrations, if the version is populated then it is considered a
    /// revision of that specific migration (not an overall version) and if the revision is higher than the last time the migration was
    /// run then the migration will be run again.
    /// </summary>
    int? Version { get; }

    /// <summary>
    /// If set to true, indicates that this migration needs to be run while the application is offline.
    /// </summary>
    bool RequiresOffline { get; }

    Task RunAsync(MigrationContext context);
}

public class MigrationContext {
    public MigrationContext(ILock migrationLock, ILogger logger, CancellationToken cancellationToken) {
        Lock = migrationLock;
        Logger = logger;
        CancellationToken = cancellationToken;
    }

    public ILock Lock { get; }
    public ILogger Logger { get; }
    public CancellationToken CancellationToken { get; }
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
