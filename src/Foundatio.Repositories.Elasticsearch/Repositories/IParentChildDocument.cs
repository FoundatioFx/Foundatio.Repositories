using Foundatio.Repositories.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch;

public interface IParentChildDocument : IIdentity {
    string ParentId { get; set; }
    JoinField Discriminator { get; set; }
}
