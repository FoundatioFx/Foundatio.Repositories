using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Queries;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Queries;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public class DailyLogEventRepository : ElasticRepositoryBase<LogEvent> {
        public DailyLogEventRepository(MyAppElasticConfiguration elasticConfiguration, ICacheClient cache, ILogger<DailyLogEventRepository> logger) : base(elasticConfiguration.Client, null, cache, null, logger) {
            ElasticType = elasticConfiguration.DailyLogEvents.LogEvent;
        }

        public Task<LogEvent> GetByCompanyAsync(string company) {
            return FindOneAsync(new MyAppQuery().WithCompany(company));
        }

        public Task<FindResults<LogEvent>> GetAllByCompanyAsync(string company) {
            return FindAsync(new MyAppQuery().WithCompany(company));
        }

        public Task<CountResult> GetCountByCompanyAsync(string company) {
            return CountAsync(new MyAppQuery().WithCompany(company).WithCacheKey(company));
        }

        protected override async Task InvalidateCacheAsync(ICollection<ModifiedDocument<LogEvent>> documents) {
            if (!IsCacheEnabled)
                return;

            if (documents != null && documents.Count > 0 && HasIdentity) {
                var keys = documents.Select(d => $"count:{d.Value.CompanyId}").Distinct().ToList();

                if (keys.Count > 0)
                    await Cache.RemoveAllAsync(keys);
            }

            await base.InvalidateCacheAsync(documents);
        }
    }

    public class MonthlyLogEventRepository : DailyLogEventRepository {
        public MonthlyLogEventRepository(MyAppElasticConfiguration elasticConfiguration, ICacheClient cache, ILogger<DailyLogEventRepository> logger) : base(elasticConfiguration, cache, logger) {
            ElasticType = elasticConfiguration.MonthlyLogEvents.LogEvent;
        }
    }
}