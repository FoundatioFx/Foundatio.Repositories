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

namespace Foundatio.Repositories.Elasticsearch.Tests;

public interface IEmployeeWithCustomFieldsRepository : ISearchableRepository<EmployeeWithCustomFields> {
    long DocumentsChangedCount { get; }
    long QueryCount { get; }

    Task<FindHit<EmployeeWithCustomFields>> GetByEmailAddressAsync(string emailAddress);
    Task<FindResults<EmployeeWithCustomFields>> GetAllByAgeAsync(int age);
    Task<FindResults<EmployeeWithCustomFields>> GetAllByCompanyAsync(string company, CommandOptionsDescriptor<EmployeeWithCustomFields> options = null);

    Task<FindResults<EmployeeWithCustomFields>> GetAllByCompaniesWithFieldEqualsAsync(string[] companies);
    Task<CountResult> GetCountByCompanyAsync(string company);
    Task<CountResult> GetNumberOfEmployeesWithMissingCompanyName(string company);
    Task<CountResult> GetNumberOfEmployeesWithMissingName(string company);

    /// <summary>
    /// Updates company name by company id
    /// </summary>
    /// <param name="company">company id</param>
    /// <param name="name">company name</param>
    /// <param name="limit">OPTIONAL limit that should be applied to bulk updates. This is here only for tests...</param>
    /// <returns></returns>
    Task<long> UpdateCompanyNameByCompanyAsync(string company, string name, int? limit = null);

    Task<long> IncrementYearsEmployeedAsync(string[] ids, int years = 1);
    Task<long> IncrementYearsEmployeedAsync(RepositoryQueryDescriptor<EmployeeWithCustomFields> query, int years = 1);
}

public class EmployeeWithCustomFieldsRepository : ElasticRepositoryBase<EmployeeWithCustomFields>, IEmployeeWithCustomFieldsRepository {
    public EmployeeWithCustomFieldsRepository(MyAppElasticConfiguration elasticConfiguration) : this(elasticConfiguration.EmployeeWithCustomFields) {
        AutoCreateCustomFields = true;
    }

    public EmployeeWithCustomFieldsRepository(IIndex employeeIndex) : base(employeeIndex) {
        BeforeQuery.AddHandler((o, args) => {
            QueryCount++;

            return Task.CompletedTask;
        });

        DocumentsChanged.AddHandler((o, args) => {
            DocumentsChangedCount += args.Documents.Count;
            return Task.CompletedTask;
        });
    }

    protected override string GetTenantKey(IRepositoryQuery query) {
        var companies = query.GetCompanies();
        if (companies.Count != 1)
            return null;

        return companies.Single();
    }

    public long DocumentsChangedCount { get; private set; }
    public long QueryCount { get; private set; }

    public Task<FindResults<EmployeeWithCustomFields>> GetAllByAgeAsync(int age) {
        return FindAsync(q => q.Age(age));
    }

    public Task<FindHit<EmployeeWithCustomFields>> GetByEmailAddressAsync(string emailAddress) {
        return FindOneAsync(q => q.EmailAddress(emailAddress), o => o.Cache($"email:{emailAddress.ToLowerInvariant()}"));
    }

    public Task<FindResults<EmployeeWithCustomFields>> GetAllByCompanyAsync(string company, CommandOptionsDescriptor<EmployeeWithCustomFields> options = null) {
        var commandOptions = options.Configure();
        if (commandOptions.ShouldUseCache())
            commandOptions.CacheKey(company);

        return FindAsync(q => q.Company(company), o => commandOptions);
    }

    public Task<FindResults<EmployeeWithCustomFields>> GetAllByCompaniesWithFieldEqualsAsync(string[] companies) {
        return FindAsync(q => q.FieldCondition(c => c.CompanyId, ComparisonOperator.Equals, companies));
    }

    public Task<CountResult> GetCountByCompanyAsync(string companyId) {
        return CountAsync(q => q.Company(companyId), o => o.CacheKey(companyId));
    }

    public Task<CountResult> GetNumberOfEmployeesWithMissingCompanyName(string company) {
        return CountAsync(q => q.Company(company).ElasticFilter(!Query<EmployeeWithCustomFields>.Exists(f => f.Field(e => e.CompanyName))));
    }

    public Task<CountResult> GetNumberOfEmployeesWithMissingName(string company) {
        return CountAsync(q => q.Company(company).ElasticFilter(!Query<EmployeeWithCustomFields>.Exists(f => f.Field(e => e.Name))));
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

        await ((IRepository<EmployeeWithCustomFields>)this).PatchAsync(ids, new ScriptPatch(script), o => o.ImmediateConsistency(true));
        return ids.Length;
    }

    public Task<long> IncrementYearsEmployeedAsync(RepositoryQueryDescriptor<EmployeeWithCustomFields> query, int years = 1) {
        if (query == null)
            throw new ArgumentNullException(nameof(query));

        string script = $"ctx._source.yearsEmployed += {years};";
        return PatchAllAsync(query, new ScriptPatch(script), o => o.ImmediateConsistency(true));
    }

    protected override async Task AddDocumentsToCacheAsync(ICollection<FindHit<EmployeeWithCustomFields>> findHits, ICommandOptions options, bool isDirtyRead) {
        await base.AddDocumentsToCacheAsync(findHits, options, isDirtyRead);

        var cacheEntries = new Dictionary<string, FindHit<EmployeeWithCustomFields>>();
        foreach (var hit in findHits.Where(d => !String.IsNullOrEmpty(d.Document.EmailAddress)))
            cacheEntries.Add($"email:{hit.Document.EmailAddress.ToLowerInvariant()}", hit);

        await AddDocumentsToCacheWithKeyAsync(cacheEntries, options.GetExpiresIn());
    }

    protected override async Task InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<EmployeeWithCustomFields>> documents, ChangeType? changeType = null) {
        await base.InvalidateCacheAsync(documents, changeType);
        await Cache.RemoveAllAsync(documents.Where(d => !String.IsNullOrEmpty(d.Value.EmailAddress)).Select(d => $"email:{d.Value.EmailAddress.ToLowerInvariant()}"));
    }
}

