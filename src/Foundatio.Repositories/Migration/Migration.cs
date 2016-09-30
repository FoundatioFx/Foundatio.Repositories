using System;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Migrations {
    public class Migration : IIdentity {
        public string Id { get { return Version.ToString(); } set {} }
        public int Version { get; set; }
        public DateTime StartedUtc { get; set; }
        public DateTime CompletedUtc { get; set; }
    }
}
