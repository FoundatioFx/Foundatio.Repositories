using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Migrations {
    public abstract class MigrationBase : IMigration {
        protected ILogger _logger;

        public MigrationBase(ILoggerFactory loggerFactory = null) {
            _logger = loggerFactory?.CreateLogger(GetType()) ?? NullLogger.Instance;
        }

        public virtual int? Version { get; } = null;
        public abstract Task RunAsync();

        protected int CalculateProgress(long total, long completed, int startProgress = 0, int endProgress = 100) {
            return startProgress + (int)((100 * (double)completed / total) * (((double)endProgress - startProgress) / 100));
        }
    }
}
