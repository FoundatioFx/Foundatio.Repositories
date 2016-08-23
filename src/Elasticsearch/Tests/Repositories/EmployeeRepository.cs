using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Queries;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Queries;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public class EmployeeRepository : ElasticRepositoryBase<Employee> {
        public EmployeeRepository(MyAppElasticConfiguration elasticConfiguration, ICacheClient cache, ILogger<EmployeeRepository> logger) : base(elasticConfiguration.Client, null, cache, null, logger) {
            ElasticType = elasticConfiguration.Employees.Employee;
        }

        public EmployeeRepository(IElasticClient client, IIndexType<Employee> employeeType, ICacheClient cache, ILogger<EmployeeRepository> logger) : base(client, null, cache, null, logger) {
            ElasticType = employeeType;
        }
        
        public Task<IFindResults<Employee>> GetAllByAgeAsync(int age) {
            return FindAsync(new MyAppQuery().WithAge(age));
        }

        public Task<IFindResults<Employee>> GetAllByCompanyAsync(string company) {
            return FindAsync(new MyAppQuery().WithCompany(company));
        }
        
        public Task<CountResult> GetCountByCompanyAsync(string company) {
            return CountAsync(new MyAppQuery().WithCompany(company).WithCacheKey(company));
        }

        public Task<long> UpdateCompanyNameByCompanyAsync(string company, string name) {
            return PatchAllAsync(new MyAppQuery().WithCompany(company), new { CompanyName = name });
        }
        
        public Task<IFindResults<Employee>> GetByFilterAsync(string systemFilter, string userFilter, SortingOptions sorting, string field, DateTime utcStart, DateTime utcEnd, PagingOptions paging) {
            if (sorting.Fields.Count == 0)
                sorting.Fields.Add(new FieldSort { Field = EmployeeType.Fields.Age, Order = Foundatio.Repositories.Models.SortOrder.Descending });

            var search = new MyAppQuery()
                .WithIndexes(utcStart, utcEnd)
                .WithSystemFilter(systemFilter)
                .WithFilter(userFilter)
                .WithPaging(paging)
                .WithSort(sorting);

            return FindAsync(search);
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