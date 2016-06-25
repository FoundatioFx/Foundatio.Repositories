using System;
using System.Collections.Generic;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries {
    public class ElasticQuery : Query, IElasticFilterQuery, IElasticIndexesQuery {
        public ElasticQuery() {
            Indexes = new List<String>();
        }

        public FilterContainer ElasticFilter { get; set; }
        public List<string> Indexes { get; set; }
        public DateTime? UtcStartIndex { get; set; }
        public DateTime? UtcEndIndex { get; set; }
    }
}
