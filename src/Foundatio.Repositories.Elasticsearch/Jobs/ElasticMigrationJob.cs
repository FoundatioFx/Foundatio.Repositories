using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Jobs;
using Foundatio.Jobs.Commands;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Migrations;
using Microsoft.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Jobs {
    [Job(Description = "Runs any pending system migrations and reindexing tasks.", IsContinuous = false)]
    public abstract class ElasticMigrationJobBase : JobBase {
        protected readonly IElasticConfiguration _configuration;
        protected readonly Lazy<MigrationManager> _migrationManager;

        public ElasticMigrationJobBase(MigrationManager migrationManager, IElasticConfiguration configuration, ILoggerFactory loggerFactory = null)
            : base(loggerFactory) {
            _migrationManager = new Lazy<MigrationManager>(() => {
                Configure(migrationManager);
                return migrationManager;
            });
            _configuration = configuration;
        }

        protected virtual void Configure(MigrationManager manager) {}

        public MigrationManager MigrationManager => _migrationManager.Value;

        protected override async Task<JobResult> RunInternalAsync(JobContext context) {
            await _configuration.ConfigureIndexesAsync(null, false).AnyContext();

            await _migrationManager.Value.RunMigrationsAsync().AnyContext();

            var tasks = _configuration.Indexes.OfType<VersionedIndex>().Select(ReindexIfNecessary);
            await Task.WhenAll(tasks).AnyContext();

            return JobResult.Success;
        }

        private async Task ReindexIfNecessary(VersionedIndex index) {
            if (index.Version != await index.GetCurrentVersionAsync().AnyContext())
                await index.ReindexAsync().AnyContext();
        }

        public static void Configure(JobCommandContext context) {
            var app = context.Application;
            var migrationOption = app.Option("-m|--migration-type", "The type of the migration to run manually.", CommandOptionType.MultipleValue);

            app.OnExecute(() => {
                var provider = context.ServiceProvider.Value;

                var migrationTypeNames = migrationOption.Values;
                if (context.JobType == null)
                    throw new ArgumentNullException(nameof(context), $"{nameof(context)}.{nameof(context.JobType)} cannot be null");

                object jobInstance = provider.GetService(context.JobType);
                if (jobInstance == null)
                    throw new ArgumentException($"Unable to get instance of type '{context.JobType.Name}'. Please ensure it's registered in Dependency Injection.", nameof(context));

                if (!(jobInstance is ElasticMigrationJobBase job))
                    throw new ArgumentException($"Type '{context.JobType.Name}' must implement '{nameof(ElasticMigrationJobBase)}'.", nameof(context));

                if (migrationTypeNames.Any(m => !String.IsNullOrEmpty(m)))
                    job.MigrationManager.Migrations.Clear();

                foreach (string migrationTypeName in migrationTypeNames.Where(m => !String.IsNullOrEmpty(m))) {
                    try {
                        var migrationType = Type.GetType(migrationTypeName);
                        if (migrationType == null) {
                            Console.WriteLine("Migration type is null.");
                            return Task.FromResult(-1);
                        }

                        job.MigrationManager.AddMigration(migrationType);
                    } catch (Exception ex) {
                        Console.WriteLine($"Error getting migration type: {ex.Message}");
                        return Task.FromResult(-1);
                    }
                }

                return new JobRunner(job, context.LoggerFactory, runContinuous: false).RunInConsoleAsync();
            });
        }
    }
}