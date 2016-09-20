using System.Threading.Tasks;
using Foundatio.Logging;

namespace Foundatio.Repositories.Migrations {
    public abstract class MigrationBase : IMigration {
        protected ILogger _logger;

        public MigrationBase(ILoggerFactory loggerFactory = null) {
            _logger = loggerFactory.CreateLogger(GetType());
        }

        public abstract int Version { get; }
        public abstract Task RunAsync();

        protected int CalculateProgress(long total, long completed, int startProgress = 0, int endProgress = 100) {
            return startProgress + (int)((100 * (double)completed / total) * (((double)endProgress - startProgress) / 100));
        }
    }
}
