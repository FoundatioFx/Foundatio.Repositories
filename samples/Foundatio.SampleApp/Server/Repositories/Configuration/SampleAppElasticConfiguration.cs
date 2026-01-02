using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.SampleApp.Server.Repositories.Indexes;

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

    protected override NodePool CreateConnectionPool()
    {
        return new SingleNodePool(new Uri(_connectionString));
    }

    protected override void ConfigureSettings(ElasticsearchClientSettings settings)
    {
        // only do this in test and dev mode to enable better debug logging
        if (_env.IsDevelopment())
            settings.DisableDirectStreaming().PrettyJson();

        base.ConfigureSettings(settings);
    }

    public GameReviewIndex GameReviews { get; }
}
