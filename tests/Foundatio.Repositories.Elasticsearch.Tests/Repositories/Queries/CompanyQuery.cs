using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries {
    public interface ICompanyQuery: IRepositoryQuery {
        ICollection<string> Companies { get; set; }
    }

    public static class CompanyQueryExtensions {
        public static T WithCompany<T>(this T query, string companyId) where T : ICompanyQuery {
            query.Companies.Add(companyId);
            return query;
        }
    }

    public class CompanyQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var companyQuery = ctx.GetSourceAs<ICompanyQuery>();
            if (companyQuery?.Companies == null || companyQuery.Companies.Count <= 0)
                return;

            if (companyQuery.Companies.Count == 1)
                ctx.Filter &= Query<Employee>.Term(f => f.CompanyId, companyQuery.Companies.Single());
            else
                ctx.Filter &= Query<Employee>.Terms(d => d.Field(f => f.CompanyId).Terms(companyQuery.Companies));
        }
    }
}