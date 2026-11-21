namespace Foundatio.Repositories.Migrations;

public interface IMigrationStateRepository : IRepository<MigrationState>
{
    /// <summary>Creates options for one migration run so the store can reject writes to records changed since the run read or wrote them.</summary>
    ICommandOptions<MigrationState>? CreateRunOptions() => null;
}
