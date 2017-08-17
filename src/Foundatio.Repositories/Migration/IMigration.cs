using System.Threading.Tasks;

namespace Foundatio.Repositories.Migrations {
    public interface IMigration {
        /// <summary>
        /// The version of the migration. Determines the order in which migrations are run. If no version is set, then the migration will not be run automatically.
        /// </summary>
        int? Version { get; }
        Task RunAsync();
    }
}
