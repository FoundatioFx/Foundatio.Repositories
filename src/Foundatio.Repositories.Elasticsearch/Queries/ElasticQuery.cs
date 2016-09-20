using System;
using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries {
    public interface IElasticQuery: ISystemFilterQuery, IIdentityQuery, IPagableQuery, ISearchQuery, IAggregationQuery, ISortableQuery, ISelectedFieldsQuery { }

    public class ElasticQuery : Query, IElasticQuery, IElasticFilterQuery, IElasticIndexesQuery {
        public FilterContainer ElasticFilter { get; set; }
        public ICollection<string> Indexes { get; set; } = new List<string>();
        public DateTime? UtcStartIndex { get; set; }
        public DateTime? UtcEndIndex { get; set; }
    }
}
