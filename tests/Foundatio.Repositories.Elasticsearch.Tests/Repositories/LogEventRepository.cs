using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public interface ILogEventRepository : ISearchableRepository<LogEvent> {
        Task<FindResults<LogEvent>> GetByCompanyAsync(string company);
        Task<FindResults<LogEvent>> GetPartialByCompanyAsync(string company);
        Task<FindResults<LogEvent>> GetAllByCompanyAsync(string company);
        Task<CountResult> GetCountByCompanyAsync(string company);
        Task<FindResults<LogEvent>> GetByDateRange(DateTime utcStart, DateTime utcEnd);
        Task<long> IncrementValueAsync(string[] ids, int value = 1);
        Task<long> IncrementValueAsync(RepositoryQueryDescriptor<LogEvent> query, int value = 1);
    }

    public class DailyLogEventRepository : ElasticRepositoryBase<LogEvent>, ILogEventRepository {
        public DailyLogEventRepository(MyAppElasticConfiguration elasticConfiguration) : base(elasticConfiguration.DailyLogEvents) {
        }

        public DailyLogEventRepository(IIndex index) : base(index) {
        }

        public Task<FindResults<LogEvent>> GetByCompanyAsync(string company) {
            return FindAsync(q => q.Company(company));
        }

        public Task<FindResults<LogEvent>> GetPartialByCompanyAsync(string company) {
            return FindAsync(q => q.Company(company).Include(e => e.Id).Include(l => l.CreatedUtc));
        }

        public Task<FindResults<LogEvent>> GetAllByCompanyAsync(string company) {
            return FindAsync(q => q.Company(company));
        }

        public Task<CountResult> GetCountByCompanyAsync(string company) {
            return CountAsync(q => q.Company(company), o => o.CacheKey(company));
        }
        
        public Task<FindResults<LogEvent>> GetByDateRange(DateTime utcStart, DateTime utcEnd) {
            return FindAsync(q => q
                .DateRange(utcStart, utcEnd, InferField(e => e.CreatedUtc))
                .Index(utcStart, utcEnd)
            );
        }
        
        public async Task<long> IncrementValueAsync(string[] ids, int value = 1) {
            if (ids == null)
                throw new ArgumentNullException(nameof(ids));

            string script = $"ctx._source.value += {value};";
            if (ids.Length == 0)
                return await PatchAllAsync(null, new ScriptPatch(script), o => o.Notifications(false).ImmediateConsistency(true));

            await ((IRepository<LogEvent>)this).PatchAsync(ids, new ScriptPatch(script), o => o.Notifications(false).ImmediateConsistency(true));
            return ids.Length;
        }

        public Task<long> IncrementValueAsync(RepositoryQueryDescriptor<LogEvent> query, int value = 1) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            string script = $"ctx._source.value += {value};";
            return PatchAllAsync(query, new ScriptPatch(script), o => o.ImmediateConsistency(true));
        }

        protected override async Task InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<LogEvent>> documents, ICommandOptions options = null) {
            if (!IsCacheEnabled)
                return;

            if (documents != null && documents.Count > 0 && HasIdentity) {
                var keys = documents.Select(d => $"count:{d.Value.CompanyId}").Distinct().ToList();
                if (keys.Count > 0)
                    await Cache.RemoveAllAsync(keys);
            }

            await base.InvalidateCacheAsync(documents, options);
        }
    }

    public class DailyLogEventWithNoCachingRepository : DailyLogEventRepository {
        public DailyLogEventWithNoCachingRepository(MyAppElasticConfiguration configuration) : base(configuration) {
            DisableCache();
        }
    }

    public class MonthlyLogEventRepository : DailyLogEventRepository {
        public MonthlyLogEventRepository(MyAppElasticConfiguration elasticConfiguration) : base(elasticConfiguration.MonthlyLogEvents) {
        }
    }
}