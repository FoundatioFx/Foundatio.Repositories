﻿using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories {
    public class ChildRepository : ElasticRepositoryBase<Child> {
        public ChildRepository(MyAppElasticConfiguration elasticConfiguration) : base(elasticConfiguration.ParentChild.Child) {
        }

        public Task<FindResults<Child>> QueryAsync(IRepositoryQuery query) {
            return FindAsync(query);
        }
    }
}
