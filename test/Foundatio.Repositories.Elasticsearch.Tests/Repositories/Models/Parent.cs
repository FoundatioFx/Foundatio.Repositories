using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models {
    public class Parent : IIdentity, ISupportSoftDeletes {
        public string Id { get; set; }
        public bool IsDeleted { get; set; }
        public Nest.JoinField JoinField => Nest.JoinField.Root<Parent>();
    }

    public static class ParentGenerator {
        public static readonly string DefaultId = ObjectId.GenerateNewId().ToString();

        public static Parent Default => new Parent {
            Id = DefaultId
        };

        public static Parent Generate(string id = null) {
            return new Parent { Id = id };
        }
    }
}
