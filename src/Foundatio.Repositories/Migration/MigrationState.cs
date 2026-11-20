using System;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Migrations;

public class MigrationState : IIdentity, IVersioned
{
    public string Id { get; set; } = null!;
    public MigrationType MigrationType { get; set; }
    public int Version { get; set; }
    public DateTime StartedUtc { get; set; }
    public DateTime? CompletedUtc { get; set; }
    public string? ErrorMessage { get; set; }

    // Storage concurrency token. The public Version is the migration revision.
    string IVersioned.Version { get; set; } = String.Empty;
}
