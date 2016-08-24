using System;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models {
    public class Child : IIdentity {
        public string Id { get; set; }
        public string ParentId { get; set; }
    }
}
