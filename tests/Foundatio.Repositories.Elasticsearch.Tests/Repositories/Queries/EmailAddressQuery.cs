using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories {
    public static class EmailAddressQueryExtensions {
        internal const string EmailAddressKey = "@EmailAddress";

        public static T EmailAddress<T>(this T query, string emailAddress) where T : IRepositoryQuery {
            return query.BuildOption(EmailAddressKey, emailAddress);
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadEmailAddressQueryExtensions {
        public static string GetEmailAddress(this IRepositoryQuery query) {
            return query.SafeGetOption<string>(EmailAddressQueryExtensions.EmailAddressKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries {
    public class EmailAddressQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var emailAddress = ctx.Source.GetEmailAddress();
            if (String.IsNullOrEmpty(emailAddress))
                return Task.CompletedTask;

            ctx.Filter &= Query<Employee>.Term(f => f.EmailAddress, emailAddress);

            return Task.CompletedTask;
        }
    }
}