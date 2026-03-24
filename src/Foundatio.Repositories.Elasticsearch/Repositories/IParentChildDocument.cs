using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch;

public interface IParentChildDocument : IIdentity
{
    string ParentId { get; set; }
    JoinField Discriminator { get; set; }
}
