using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories;

public interface IParentRepository : ISearchableRepository<Parent> { }

public class ParentRepository : ElasticRepositoryBase<Parent>, IParentRepository
{
    public ParentRepository(MyAppElasticConfiguration elasticConfiguration) : base(elasticConfiguration.ParentChild)
    {
        BeforeQuery.AddHandler(OnBeforeQuery);
        DocumentsChanging.AddHandler(OnDocumentsChanging);
    }

    private Task OnDocumentsChanging(object sender, DocumentsChangeEventArgs<Parent> args)
    {
        foreach (var doc in args.Documents.Select(d => d.Value).Cast<IParentChildDocument>())
            doc.Discriminator = JoinField.Root<Parent>();

        return Task.CompletedTask;
    }

    private Task OnBeforeQuery(object sender, BeforeQueryEventArgs<Parent> args)
    {
        args.Query.Discriminator("parent");
        return Task.CompletedTask;
    }
}
