using System;
using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Queries;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries {
    public class MyAppQuery : ElasticQuery, IAgeQuery, ICompanyQuery {
        public ICollection<int> Ages { get; set; } = new List<int>();
        public ICollection<string> Companies { get; set; } = new List<string>();
    }
}
