using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Jobs;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Migrations;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Jobs;

[Job(Description = "Runs any pending system migrations and reindexing tasks.", IsContinuous = false)]
public abstract class ElasticMigrationJobBase : JobBase
{
    protected readonly IElasticConfiguration _configuration;
    protected readonly Lazy<MigrationManager> _migrationManager;

    public ElasticMigrationJobBase(MigrationManager migrationManager, IElasticConfiguration configuration, ILoggerFactory loggerFactory = null)
        : base(loggerFactory)
    {
        _migrationManager = new Lazy<MigrationManager>(() =>
        {
            Configure(migrationManager);
            return migrationManager;
        });
        _configuration = configuration;
    }

    protected virtual void Configure(MigrationManager manager) { }

    public MigrationManager MigrationManager => _migrationManager.Value;

    protected override async Task<JobResult> RunInternalAsync(JobContext context)
    {
        await _configuration.ConfigureIndexesAsync(null, false).AnyContext();

        await _migrationManager.Value.RunMigrationsAsync().AnyContext();

        var tasks = _configuration.Indexes.OfType<IVersionedIndex>().Select(ReindexIfNecessary);
        await Task.WhenAll(tasks).AnyContext();

        return JobResult.Success;
    }

    private async Task ReindexIfNecessary(IVersionedIndex index)
    {
        if (index.Version != await index.GetCurrentVersionAsync().AnyContext())
            await index.ReindexAsync().AnyContext();
    }
}
