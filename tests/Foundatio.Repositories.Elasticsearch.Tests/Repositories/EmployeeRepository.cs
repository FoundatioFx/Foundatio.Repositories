using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Queries;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Queries;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public class EmployeeRepository : ElasticRepositoryBase<Employee> {
        public EmployeeRepository(MyAppElasticConfiguration elasticConfiguration) : base(elasticConfiguration.Employees.Employee) {
        }

        public EmployeeRepository(IIndexType<Employee> employeeType) : base(employeeType) {
        }
        
        public Task<FindResults<Employee>> GetAllByAgeAsync(int age) {
            return FindAsync(new MyAppQuery().WithAge(age));
        }

        public Task<FindResults<Employee>> GetAllByCompanyAsync(string company, PagingOptions paging = null, bool useCache = false) {
            return FindAsync(new MyAppQuery().WithCompany(company).WithPaging(paging).WithCacheKey(useCache ? "by-company" : null));
        }

        public Task<CountResult> GetCountByCompanyAsync(string company) {
            return CountAsync(new MyAppQuery().WithCompany(company).WithCacheKey(company));
        }

        public Task<CountResult> GetNumberOfEmployeesWithMissingCompanyName(string company) {
            return CountAsync(new MyAppQuery().WithCompany(company).WithFilter($"_missing_:{EmployeeType.Fields.CompanyName}"));
        }

        public Task<CountResult> GetNumberOfEmployeesWithMissingName(string company) {
            return CountAsync(new MyAppQuery().WithCompany(company).WithFilter($"_missing_:{EmployeeType.Fields.Name}"));
        }

        /// <summary>
        /// Updates company name by company id
        /// </summary>
        /// <param name="company">company id</param>
        /// <param name="name">company name</param>
        /// <param name="limit">OPTIONAL limit that should be applied to bulk updates. This is here only for tests...</param>
        /// <returns></returns>
        public Task<long> UpdateCompanyNameByCompanyAsync(string company, string name, int? limit = null) {
            return PatchAllAsync(new MyAppQuery().WithCompany(company).WithLimit(limit), new { CompanyName = name });
        }

        public Task<long> IncrementYearsEmployeed(string[] ids, int years = 1) {
            string script = $"ctx._source.yearsEmployed += {years};";
            return PatchAllAsync(new MyAppQuery().WithIds(ids), script);
        }

        protected override async Task InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<Employee>> documents) {
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