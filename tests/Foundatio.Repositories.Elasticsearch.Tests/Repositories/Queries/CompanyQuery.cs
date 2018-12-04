using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories {
    public static class CompanyQueryExtensions {
        internal const string CompaniesKey = "@Companies";

        public static T Company<T>(this T query, string companyId) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(CompaniesKey, companyId);
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadCompanyQueryExtensions {
        public static ICollection<string> GetCompanies(this IRepositoryQuery query) {
            return query.SafeGetCollection<string>(CompanyQueryExtensions.CompaniesKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries {
    public class CompanyQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var companyIds = ctx.Source.GetCompanies();
            if (companyIds.Count <= 0)
                return Task.CompletedTask;

            if (companyIds.Count == 1)
                ctx.Filter &= Query<Employee>.Term(f => f.CompanyId, companyIds.Single());
            else
                ctx.Filter &= Query<Employee>.Terms(d => d.Field(f => f.CompanyId).Terms(companyIds));

            return Task.CompletedTask;
        }
    }
}