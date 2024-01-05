using Elasticsearch.Net;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.SampleApp.Server.Repositories.Indexes;
using Nest;

namespace Foundatio.SampleApp.Server.Repositories;

public class SampleAppElasticConfiguration : ElasticConfiguration
{
    private string _connectionString;
    private IWebHostEnvironment _env;

    public SampleAppElasticConfiguration(IConfiguration config, IWebHostEnvironment env, ILoggerFactory loggerFactory) : base(loggerFactory: loggerFactory)
    {
        _connectionString = config.GetConnectionString("ElasticsearchConnectionString") ?? "http://localhost:9200";
        _env = env;
        AddIndex(GameReviews = new GameReviewIndex(this));
    }

    protected override IConnectionPool CreateConnectionPool()
    {
        return new SingleNodeConnectionPool(new Uri(_connectionString));
    }

    protected override void ConfigureSettings(ConnectionSettings settings)
    {
        // only do this in test and dev mode to enable better debug logging
        if (_env.IsDevelopment())
            settings.DisableDirectStreaming().PrettyJson();

        base.ConfigureSettings(settings);
    }

    public GameReviewIndex GameReviews { get; }
}
