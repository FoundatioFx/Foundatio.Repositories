using System;
using Foundatio.Elasticsearch.Repositories.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Builders {
    public class CompanyQueryBuilder : QueryBuilderBase {
        public override void BuildFilter<T>(object query, object options, ref QueryContainer container) {
            var companyQuery = query as ICompanyQuery;
            if (companyQuery?.Companies == null || companyQuery.Companies.Count <= 0)
                return;
            
            container &= Query<T>.Terms(t => t.Field(EmployeeIndex.Fields.Employee.CompanyId).Terms(companyQuery.Companies.ToArray()));
        }
    }
}