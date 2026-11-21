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

    public ElasticMigrationJobBase(MigrationManager migrationManager, IElasticConfiguration configuration, ILoggerFactory? loggerFactory = null)
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
        var cancellationToken = context.CancellationToken;
        if (cancellationToken.IsCancellationRequested)
            return JobResult.Cancelled;

        await _configuration.ConfigureIndexesAsync(null, false).AnyContext();

        var result = await _migrationManager.Value.RunMigrationsAsync(cancellationToken).AnyContext();
        if (result is MigrationResult.Cancelled)
            return JobResult.Cancelled;
        if (result is not MigrationResult.Success)
            return JobResult.FailedWithMessage($"Migrations returned {result}.");

        var tasks = _configuration.Indexes.OfType<IVersionedIndex>().Select(async index =>
        {
            int currentVersion = await index.GetCurrentVersionAsync().AnyContext();
            if (currentVersion < index.Version && !cancellationToken.IsCancellationRequested)
                await index.ReindexAsync().AnyContext();
        });
        await Task.WhenAll(tasks).AnyContext();

        return cancellationToken.IsCancellationRequested ? JobResult.Cancelled : JobResult.Success;
    }
}
