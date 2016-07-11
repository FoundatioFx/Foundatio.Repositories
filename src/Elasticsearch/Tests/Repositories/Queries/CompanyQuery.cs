using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Queries {
    public interface ICompanyQuery {
        List<string> Companies { get; set; }
    }

    public static class CompanyQueryExtensions {
        public static T WithCompany<T>(this T query, string companyId) where T : ICompanyQuery {
            query.Companies.Add(companyId);
            return query;
        }
    }

    public class CompanyQueryBuilder : ElasticQueryBuilderBase {
        public override void BuildFilter<T>(object query, object options, ref FilterContainer container) {
            var companyQuery = query as ICompanyQuery;
            if (companyQuery?.Companies == null || companyQuery.Companies.Count <= 0)
                return;

            if (companyQuery.Companies.Count == 1)
                container &= Filter<T>.Term(EmployeeType.Fields.CompanyId, companyQuery.Companies.First());
            else
                container &= Filter<T>.Terms(EmployeeType.Fields.CompanyId, companyQuery.Companies.Select(a => a.ToString()));
        }
    }
}