using System.Threading.Tasks;

namespace Foundatio.Repositories.Migrations {
    public interface IMigration {
        int Version { get; }
        Task RunAsync();
    }
}
