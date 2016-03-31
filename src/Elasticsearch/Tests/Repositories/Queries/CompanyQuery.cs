using System;
using System.Collections.Generic;
using Foundatio.Elasticsearch.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Tests.Queries {
    public interface ICompanyQuery {
        List<string> Companies { get; set; }
    }

    public static class CompanyQueryExtensions {
        public static T WithCompany<T>(this T query, string companyId) where T : ICompanyQuery {
            query.Companies?.Add(companyId);
            return query;
        }
    }

    public class CompanyQuery : ElasticQuery, ICompanyQuery {
        public CompanyQuery() {
            Companies = new List<string>();
        }

        public List<string> Companies { get; set; }
    }
}