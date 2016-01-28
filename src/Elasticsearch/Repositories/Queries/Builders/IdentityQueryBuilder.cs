using System;
using System.Linq;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Elasticsearch.Repositories.Queries.Builders {
    public class IdentityQueryBuilder : QueryBuilderBase {
        public override void BuildFilter<T>(object query, object options, ref QueryContainer container) {
            var identityQuery = query as IIdentityQuery;
            if (identityQuery?.Ids == null || identityQuery.Ids.Count <= 0)
                return;

            // TODO: Move Id's into constructor.
            container &= new IdsQuery { Values = identityQuery.Ids.Select(id => new Id(id)) };
        }
    }
}