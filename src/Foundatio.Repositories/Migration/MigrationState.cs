using System;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Migrations;

public class MigrationState : IIdentity {
    public string Id { get; set; }
    public MigrationType MigrationType { get; set; }
    public int Version { get; set; }
    public DateTime StartedUtc { get; set; }
    public DateTime? CompletedUtc { get; set; }
    public string ErrorMessage { get; set; }
}
