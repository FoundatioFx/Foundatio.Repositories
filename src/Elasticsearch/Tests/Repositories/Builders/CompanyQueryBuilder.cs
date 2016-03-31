using System;
using System.Linq;
using Foundatio.Elasticsearch.Repositories.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Builders {
    public class CompanyQueryBuilder : QueryBuilderBase {
        public override void BuildFilter<T>(object query, object options, ref FilterContainer container) {
            var companyQuery = query as ICompanyQuery;
            if (companyQuery?.Companies == null || companyQuery.Companies.Count <= 0)
                return;
            
            if (companyQuery.Companies.Count == 1)
                container &= Filter<T>.Term("company", companyQuery.Companies.First());
            else
                container &= Filter<T>.Terms("company", companyQuery.Companies.Select(a => a.ToString()));
        }
    }
}