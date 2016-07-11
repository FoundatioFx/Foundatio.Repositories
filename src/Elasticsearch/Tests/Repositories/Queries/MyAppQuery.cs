using System;
using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Queries;
using Foundatio.Repositories.Elasticsearch.Tests.Queries;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries {
    public class MyAppQuery : ElasticQuery, IAgeQuery, ICompanyQuery {
        public List<int> Ages { get; set; } = new List<int>();
        public List<string> Companies { get; set; } = new List<string>();
    }
}
