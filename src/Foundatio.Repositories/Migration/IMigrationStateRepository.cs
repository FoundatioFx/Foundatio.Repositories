namespace Foundatio.Repositories.Migrations;

public interface IMigrationStateRepository : IRepository<MigrationState>
{
    /// <summary>
    /// Creates options for the manager's reads by ID and single-record adds and saves during one run.
    /// A store can use them to reject a write when another writer has changed the record.
    /// </summary>
    ICommandOptions<MigrationState>? CreateRunOptions() => null;
}
