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
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public class EmployeeRepository : ElasticRepositoryBase<Employee> {
        public EmployeeRepository(MyAppElasticConfiguration elasticConfiguration, ICacheClient cache, ILogger<EmployeeRepository> logger) : base(elasticConfiguration.Client, null, cache, null, logger) {
            ElasticType = elasticConfiguration.Employees.Employee;
        }

        public EmployeeRepository(IElasticClient client, EmployeeType employeeType, ICacheClient cache, ILogger<EmployeeRepository> logger) : base(client, null, cache, null, logger) {
            ElasticType = employeeType;
        }

        public Task<Employee> GetByAgeAsync(int age) {
            return FindOneAsync(new MyAppQuery().WithAge(age));
        }

        public Task<FindResults<Employee>> GetAllByAgeAsync(int age) {
            return FindAsync(new MyAppQuery().WithAge(age));
        }

        public Task<Employee> GetByCompanyAsync(string company) {
            return FindOneAsync(new MyAppQuery().WithCompany(company));
        }

        public Task<FindResults<Employee>> GetAllByCompanyAsync(string company) {
            return FindAsync(new MyAppQuery().WithCompany(company));
        }
        
        public Task<CountResult> GetCountByCompanyAsync(string company) {
            return CountAsync(new MyAppQuery().WithCompany(company).WithCacheKey(company));
        }

        protected override async Task InvalidateCacheAsync(ICollection<ModifiedDocument<Employee>> documents) {
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
}