using System;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Migrations {
    public class MigrationResult : IIdentity {
        public string Id { get { return IndexName + "-" + Version; } set {} }
        public string IndexName { get; set; }
        public int Version { get; set; }
        public DateTime StartedUtc { get; set; }
        public DateTime CompletedUtc { get; set; }
    }
}
