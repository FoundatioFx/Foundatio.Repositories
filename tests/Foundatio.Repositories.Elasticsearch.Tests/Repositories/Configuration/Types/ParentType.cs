using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types {
    public class ParentType : IndexTypeBase<Parent> {
        public ParentType(IIndex index): base(index) {}
    }
}
