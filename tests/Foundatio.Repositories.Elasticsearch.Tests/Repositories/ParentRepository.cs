 using System.Linq;
 using System.Threading.Tasks;
 using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
 using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
 using Foundatio.Repositories.Models;
 using Nest;

 namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories {
     public class ParentRepository : ElasticRepositoryBase<Parent> {
         public ParentRepository(MyAppElasticConfiguration elasticConfiguration) : base(elasticConfiguration.ParentChild) {
             DocumentsChanging.AddHandler(OnDocumentsChanging);
         }

         protected Task OnDocumentsChanging(object sender, DocumentsChangeEventArgs<Parent> args) {
             foreach (var doc in args.Documents.Select(d => d.Value).Cast<IParentChildDocument>())
                doc.Discriminator = JoinField.Root<Parent>();
                     
             return Task.CompletedTask;
         }

         protected override Task<SearchDescriptor<Parent>> ConfigureSearchDescriptorAsync(SearchDescriptor<Parent> search, IRepositoryQuery query, ICommandOptions options) {
             // this need to be a bool filter
             search.Query(q => q.Bool(b => b.Must(m => m.Term(f => f.Field(c => ((IParentChildDocument)c).Discriminator).Value(RelationName.From<Parent>())))));
             return base.ConfigureSearchDescriptorAsync(search, query, options);
         }

         public Task<FindResults<Parent>> QueryAsync(RepositoryQueryDescriptor<Parent> query) {
             return FindAsync(query);
         }
    }
}
