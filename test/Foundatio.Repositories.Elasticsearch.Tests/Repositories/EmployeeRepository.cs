using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public class EmployeeRepository : ElasticRepositoryBase<Employee> {
        public EmployeeRepository(MyAppElasticConfiguration elasticConfiguration) : base(elasticConfiguration.Employees.Employee) {
        }

        public EmployeeRepository(IIndexType<Employee> employeeType) : base(employeeType) {
            DocumentsChanged.AddHandler((o, args) => {
                DocumentsChangedCount += args.Documents.Count;
                return Task.CompletedTask;
            });

            BeforeQuery.AddHandler((o, args) => {
                QueryCount++;
                return Task.CompletedTask;
            });
        }

        public long DocumentsChangedCount { get; private set; }
        public long QueryCount { get; private set; }

        /// <summary>
        /// This allows us easily test aggregations
        /// </summary>
        public Task<CountResult> GetCountByQueryAsync(RepositoryQueryDescriptor<Employee> query) {
            return CountAsync(query);
        }

        public Task<FindResults<Employee>> GetAllByAgeAsync(int age) {
            return FindAsync(q => q.Age(age));
        }

        /// <summary>
        /// Exposed only for testing purposes.
        /// </summary>
        public Task<FindResults<Employee>> GetByQueryAsync(RepositoryQueryDescriptor<Employee> query) {
            return FindAsync(query);
        }

        public Task<FindResults<Employee>> GetAllByCompanyAsync(string company, CommandOptionsDescriptor<Employee> options = null) {
            var commandOptions = options.Configure();
            if (commandOptions.ShouldUseCache())
                commandOptions.CacheKey(company);

            return FindAsync(q => q.Company(company), o => commandOptions);
        }

        public Task<FindResults<Employee>> GetAllByCompaniesWithFieldEqualsAsync(string[] companies) {
            return FindAsync(q => q.FieldCondition(c => c.CompanyId, ComparisonOperator.Equals, companies));
        }

        public Task<CountResult> GetCountByCompanyAsync(string company) {
            return CountAsync(q => q.Company(company), o => o.CacheKey(company));
        }

        public Task<CountResult> GetNumberOfEmployeesWithMissingCompanyName(string company) {
            return CountAsync(q => q.Company(company).ElasticFilter(!Query<Employee>.Exists(f => f.Field(e => e.CompanyName))));
        }

        public Task<CountResult> GetNumberOfEmployeesWithMissingName(string company) {
            return CountAsync(q => q.Company(company).ElasticFilter(!Query<Employee>.Exists(f => f.Field(e => e.Name))));
        }

        /// <summary>
        /// Updates company name by company id
        /// </summary>
        /// <param name="company">company id</param>
        /// <param name="name">company name</param>
        /// <param name="limit">OPTIONAL limit that should be applied to bulk updates. This is here only for tests...</param>
        /// <returns></returns>
        public Task<long> UpdateCompanyNameByCompanyAsync(string company, string name, int? limit = null) {
            return PatchAllAsync(q => q.Company(company), new PartialPatch(new { CompanyName = name }), o => o.PageLimit(limit).ImmediateConsistency(true));
        }

        public async Task<long> IncrementYearsEmployeedAsync(string[] ids, int years = 1) {
            string script = $"ctx._source.yearsEmployed += {years};";
            if (ids.Length == 0)
                return await PatchAllAsync(null, new ScriptPatch(script), o => o.Notifications(false).ImmediateConsistency(true));

            await this.PatchAsync(ids, new ScriptPatch(script), o => o.ImmediateConsistency(true));
            return ids.Length;
        }

        public async Task<long> IncrementYearsEmployeedAsync(RepositoryQueryDescriptor<Employee> query, int years = 1) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            string script = $"ctx._source.yearsEmployed += {years};";
            return await PatchAllAsync(query, new ScriptPatch(script), o => o.ImmediateConsistency(true));
        }

        public Task<FindResults<Employee>> GetByFilterAsync(string filter) {
            return SearchAsync(null, filter);
        }

        public Task<FindResults<Employee>> GetByCriteriaAsync(string criteria) {
            return SearchAsync(null, null, criteria);
        }

        protected override async Task InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<Employee>> documents, ICommandOptions options = null) {
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
}