﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public interface IEmployeeRepository : ISearchableRepository<Employee> {
    long DocumentsChangedCount { get; }
    long QueryCount { get; }

    Task<FindHit<Employee>> GetByEmailAddressAsync(string emailAddress);
    Task<FindResults<Employee>> GetAllByAgeAsync(int age);
    Task<FindResults<Employee>> GetAllByCompanyAsync(string company, CommandOptionsDescriptor<Employee> options = null);

    Task<FindResults<Employee>> GetAllByCompaniesWithFieldEqualsAsync(string[] companies);
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
    Task<long> IncrementYearsEmployeedAsync(RepositoryQueryDescriptor<Employee> query, int years = 1);
}

public class EmployeeRepository : ElasticRepositoryBase<Employee>, IEmployeeRepository {
    public EmployeeRepository(MyAppElasticConfiguration elasticConfiguration) : this(elasticConfiguration.Employees) {}

    public EmployeeRepository(IIndex employeeIndex) : base(employeeIndex) {
        AddDefaultExclude("Idx");

        DocumentsChanging.AddHandler(async (o, args) => {
            var companyGroups = args.Documents.GroupBy(e => e.Value.CompanyId);

            foreach (var company in companyGroups) {
                var companyFieldMapping = await ElasticIndex.Configuration.CustomFieldDefinitionRepository.GetFieldMapping(EntityTypeName, company.Key);

                foreach (var doc in company) {
                    if (doc.Value.CustomFields != null) {
                        if (doc.Value.Idx == null)
                            doc.Value.Idx = new Dictionary<string, object>();

                        // TODO create dynamic templates

                        foreach (var customField in doc.Value.CustomFields) {
                            if (companyFieldMapping.TryGetValue(customField.Key, out var idxName)) {
                                doc.Value.Idx[idxName] = customField.Value;
                            }
                        }
                    }
                }
            }
        });

        DocumentsChanged.AddHandler((o, args) => {
            DocumentsChangedCount += args.Documents.Count;
            return Task.CompletedTask;
        });

        BeforeQuery.AddHandler(async (o, args) => {
            var companies = args.Query.GetCompanies();
            if (companies.Count != 1)
                return;

            var companyId = companies.Single();

            var companyFieldMapping = await ElasticIndex.Configuration.CustomFieldDefinitionRepository.GetFieldMapping(EntityTypeName, companyId);

            args.Options.QueryFieldResolver(companyFieldMapping.ToHierarchicalFieldResolver("idx."));

            QueryCount++;
        });
    }

    public long DocumentsChangedCount { get; private set; }
    public long QueryCount { get; private set; }

    public Task<FindResults<Employee>> GetAllByAgeAsync(int age) {
        return FindAsync(q => q.Age(age));
    }

    public Task<FindHit<Employee>> GetByEmailAddressAsync(string emailAddress) {
        return FindOneAsync(q => q.EmailAddress(emailAddress), o => o.Cache($"email:{emailAddress.ToLowerInvariant()}"));
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

    public Task<CountResult> GetCountByCompanyAsync(string companyId) {
        return CountAsync(q => q.Company(companyId), o => o.CacheKey(companyId));
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

        await ((IRepository<Employee>)this).PatchAsync(ids, new ScriptPatch(script), o => o.ImmediateConsistency(true));
        return ids.Length;
    }

    public Task<long> IncrementYearsEmployeedAsync(RepositoryQueryDescriptor<Employee> query, int years = 1) {
        if (query == null)
            throw new ArgumentNullException(nameof(query));

        string script = $"ctx._source.yearsEmployed += {years};";
        return PatchAllAsync(query, new ScriptPatch(script), o => o.ImmediateConsistency(true));
    }

    protected override async Task AddDocumentsToCacheAsync(ICollection<FindHit<Employee>> findHits, ICommandOptions options, bool isDirtyRead) {
        await base.AddDocumentsToCacheAsync(findHits, options, isDirtyRead);

        var cacheEntries = new Dictionary<string, FindHit<Employee>>();
        foreach (var hit in findHits.Where(d => !String.IsNullOrEmpty(d.Document.EmailAddress)))
            cacheEntries.Add($"email:{hit.Document.EmailAddress.ToLowerInvariant()}", hit);

        await AddDocumentsToCacheWithKeyAsync(cacheEntries, options.GetExpiresIn());
    }

    protected override async Task InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<Employee>> documents, ChangeType? changeType = null) {
        await base.InvalidateCacheAsync(documents, changeType);
        await Cache.RemoveAllAsync(documents.Where(d => !String.IsNullOrEmpty(d.Value.EmailAddress)).Select(d => $"email:{d.Value.EmailAddress.ToLowerInvariant()}"));
    }
}
