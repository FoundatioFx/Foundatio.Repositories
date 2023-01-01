using Elasticsearch.Net;
using Foundatio.Extensions.Hosting.Startup;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.SampleApp.Server.Repositories.Indexes;
using Foundatio.SampleApp.Shared;
using Nest;

namespace Foundatio.SampleApp.Server.Repositories;

public class SampleAppElasticConfiguration : ElasticConfiguration {
    private string _connectionString;
    
    public SampleAppElasticConfiguration(IConfiguration config, ILoggerFactory loggerFactory) : base(loggerFactory: loggerFactory) {
        _connectionString = config.GetConnectionString("ElasticsearchConnectionString") ?? "http://localhost:9200";
        AddIndex(GameReviews = new GameReviewIndex(this));
    }

    protected override IConnectionPool CreateConnectionPool()
    {
        return new SingleNodeConnectionPool(new Uri(_connectionString));
    }

    protected override void ConfigureSettings(ConnectionSettings settings) {
        // only do this in test and dev mode to enable better debug logging
        settings.DisableDirectStreaming().PrettyJson();
        
        base.ConfigureSettings(settings);
    }

    public GameReviewIndex GameReviews { get; }
}

public static class ServicesExtensions
{
    public static IServiceCollection AddConfigureIndexesStartupAction(this IServiceCollection services)
    {
        // configure the elasticsearch indexes
        services.AddStartupAction("ConfigureIndexes", async sp =>
        {
            var configuration = sp.GetRequiredService<SampleAppElasticConfiguration>();
            await configuration.ConfigureIndexesAsync(beginReindexingOutdated: false);
        });

        return services;
    }

    public static IServiceCollection AddSampleDataStartupAction(this IServiceCollection services)
    {
        services.AddStartupAction("ConfigureIndexes", async sp =>
        {
            // add some sample data if there is none
            var repository = sp.GetRequiredService<IGameReviewRepository>();
            if (await repository.CountAsync() is { Total: 0 })
            {
                await repository.AddAsync(new GameReview
                {
                    Name = "Super Mario Bros",
                    Description = "Super Mario Bros is a platform video game developed and published by Nintendo.",
                    Category = "Adventure"
                });

                for (int i = 0; i < 100; i++) {
                    await repository.AddAsync(new GameReview
                    {
                        Name = $"Test Game {i}",
                        Description = $"This is a test game {i} review.",
                        Category = i % 2 == 1 ? "Test1" : "Test2",
                        Tags = new[] { "test", "game", $"test{i}" }
                    });
                }
            }
        });

        return services;
    }
}
