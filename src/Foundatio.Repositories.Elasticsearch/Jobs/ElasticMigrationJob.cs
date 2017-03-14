using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Jobs;
using Foundatio.Jobs.Commands;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Migrations;
using Microsoft.Extensions.CommandLineUtils;

namespace Foundatio.Repositories.Elasticsearch.Jobs {
    [Job(Description = "Runs any pending system migrations and reindexing tasks.", IsContinuous = false)]
    public class ElasticMigrationJob : JobBase {
        protected readonly IElasticConfiguration _configuration;
        protected readonly Lazy<MigrationManager> _migrationManager;

        public ElasticMigrationJob(MigrationManager migrationManager, IElasticConfiguration configuration, ILoggerFactory loggerFactory = null)
            : base(loggerFactory) {
            _migrationManager = new Lazy<MigrationManager>(() => {
                RegisterMigrations();
                return migrationManager;
            });
            _configuration = configuration;
        }

        public ICollection<IMigration> Migrations { get; } = new List<IMigration>();

        protected virtual void RegisterMigrations() { }

        protected override async Task<JobResult> RunInternalAsync(JobContext context) {
            await _configuration.ConfigureIndexesAsync(null, false).AnyContext();

            if (Migrations.Count == 0) {
                await _migrationManager.Value.RunAsync().AnyContext();
            } else {
                foreach (var migration in Migrations)
                    await _migrationManager.Value.RunAsync(migration).AnyContext();
            }

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
                var job = provider.GetService(typeof(ElasticMigrationJob)) as ElasticMigrationJob;

                var migrations = new List<IMigration>();
                foreach (var migrationTypeName in migrationTypeNames.Where(m => !String.IsNullOrEmpty(m))) {
                    try {
                        var migrationType = Type.GetType(migrationTypeName);
                        var migration = provider.GetService(migrationType) as IMigration;
                        if (migration == null) {
                            Console.WriteLine($"Migration instance is null.");
                            return -1;
                        }
                        migrations.Add(migration);
                    } catch (Exception ex) {
                        Console.WriteLine($"Error getting migration type: {ex.Message}");
                        return -1;
                    }
                }

                job.Migrations.AddRange(migrations);

                return new JobRunner(job, context.LoggerFactory, runContinuous: false).RunInConsole();
            });
        }
    }
}