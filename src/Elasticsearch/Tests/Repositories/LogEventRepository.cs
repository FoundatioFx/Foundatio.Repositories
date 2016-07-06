using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Queries;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Queries;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public class LogEventRepository : ElasticRepositoryBase<LogEvent> {
        public LogEventRepository(RepositoryConfiguration<LogEvent> configuration) : base(configuration) { }

        public Task<LogEvent> GetByCompanyAsync(string company) {
            return FindOneAsync(new CompanyQuery().WithCompany(company));
        }

        public Task<FindResults<LogEvent>> GetAllByCompanyAsync(string company) {
            return FindAsync(new CompanyQuery().WithCompany(company));
        }
        
        public Task<long> GetCountByCompanyAsync(string company) {
            return CountAsync(new CompanyQuery().WithCompany(company).WithCacheKey(company));
        }

        protected override async Task InvalidateCacheAsync(ICollection<ModifiedDocument<LogEvent>> documents) {
            if (!IsCacheEnabled)
                return;

            if (documents != null && documents.Count > 0 && _hasIdentity) {
                var keys = documents.Select(d => $"count:{d.Value.CompanyId}").Distinct().ToList();

                if (keys.Count > 0)
                    await Cache.RemoveAllAsync(keys);
            }

            await base.InvalidateCacheAsync(documents);
        }
    }
}