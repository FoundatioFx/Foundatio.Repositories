using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Migrations;

public abstract class MigrationBase : IMigration {
    protected ILogger _logger;

    public MigrationBase() {}

    public MigrationBase(ILoggerFactory loggerFactory) {
        loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = loggerFactory.CreateLogger(GetType());
    }

    public MigrationType MigrationType { get; protected set; } = MigrationType.Versioned;

    public virtual int? Version { get; protected set; } = null;

    public bool RequiresOffline { get; protected set; } = false;

    public virtual Task RunAsync() {
        return Task.CompletedTask;
    }

    public virtual Task RunAsync(MigrationContext context) {
        return RunAsync();
    }

    protected int CalculateProgress(long total, long completed, int startProgress = 0, int endProgress = 100) {
        return startProgress + (int)((100 * (double)completed / total) * (((double)endProgress - startProgress) / 100));
    }
}
