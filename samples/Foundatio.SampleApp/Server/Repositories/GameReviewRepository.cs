﻿using Foundatio.Repositories;
using Foundatio.Repositories.Elasticsearch;
using Foundatio.SampleApp.Shared;

namespace Foundatio.SampleApp.Server.Repositories;

public interface IGameReviewRepository : ISearchableRepository<GameReview> { }

public class GameReviewRepository : ElasticRepositoryBase<GameReview>, IGameReviewRepository
{
    public GameReviewRepository(SampleAppElasticConfiguration configuration) : base(configuration.GameReviews) { }
}
