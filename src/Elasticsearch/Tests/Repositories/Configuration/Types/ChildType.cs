using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types {
    public class ChildType : ChildIndexType<Child> {
        public ChildType(IIndex index = null): base("parentId", d => d.ParentId, null, index) {}
    }
}
