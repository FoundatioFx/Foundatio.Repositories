using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models {
    public class Child : IIdentity {
        public string Id { get; set; }
        public string ParentId { get; set; }
    }

    public static class ChildGenerator {
        public static readonly string DefaultId = ObjectId.GenerateNewId().ToString();

        public static Child Default => new Child {
            Id = DefaultId,
            ParentId = ParentGenerator.DefaultId
        };

        public static Child Generate(string id = null, string parentId = null) {
            return new Child { Id = id, ParentId = parentId };
        }
    }
}
