 using System.Linq;
 using System.Threading.Tasks;
 using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
 using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
 using Foundatio.Repositories.Models;
 using Nest;

 namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories {
     public class ChildRepository : ElasticRepositoryBase<Child> {
         public ChildRepository(MyAppElasticConfiguration elasticConfiguration) : base(elasticConfiguration.ParentChild) {
             DocumentsChanging.AddHandler(OnDocumentsChanging);
             GetParentIdFunc = d => d.ParentId;
         }

         protected Task OnDocumentsChanging(object sender, DocumentsChangeEventArgs<Child> args) {
             foreach (var doc in args.Documents.Select(d => d.Value).Cast<IParentChildDocument>())
                 doc.Discriminator = JoinField.Link<Child>(doc.ParentId);
             
             return Task.CompletedTask;
         }

         protected override Task<SearchDescriptor<Child>> ConfigureSearchDescriptorAsync(SearchDescriptor<Child> search, IRepositoryQuery query, ICommandOptions options) {
             // this need to be a bool filter
             search.Query(q => q.Bool(b => b.Must(m => m.Term(f => f.Field(c => ((IParentChildDocument)c).Discriminator).Value(RelationName.From<Child>())))));
             return base.ConfigureSearchDescriptorAsync(search, query, options);
         }

         public Task<FindResults<Child>> QueryAsync(RepositoryQueryDescriptor<Child> query, CommandOptionsDescriptor<Child> options = null) {
             return FindAsync(query, options);
         }
     }
 }
