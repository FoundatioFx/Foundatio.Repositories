using System;
using System.Collections.Generic;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries {
    public class ElasticQuery : Query, IElasticFilterQuery, IElasticIndexesQuery {
        public ElasticQuery() {
            Indices = new List<String>();
        }

        public FilterContainer ElasticFilter { get; set; }
        public List<string> Indices { get; set; }
        public DateTime? UtcStart { get; set; }
        public DateTime? UtcEnd { get; set; }
    }
}
