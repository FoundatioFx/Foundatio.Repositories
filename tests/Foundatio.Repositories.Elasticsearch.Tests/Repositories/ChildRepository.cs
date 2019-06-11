 using System.Linq;
 using System.Threading.Tasks;
 using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
 using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
 using Foundatio.Repositories.Models;
 using Foundatio.Repositories.Options;
 using Nest;

 namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories {
     public class ChildRepository : ElasticRepositoryBase<Child> {
         public ChildRepository(MyAppElasticConfiguration elasticConfiguration) : base(elasticConfiguration.ParentChild) {
             BeforeQuery.AddHandler(OnBeforeQuery);
             DocumentsChanging.AddHandler(OnDocumentsChanging);
             GetParentIdFunc = d => d.ParentId;
         }

         protected Task OnDocumentsChanging(object sender, DocumentsChangeEventArgs<Child> args) {
             foreach (var doc in args.Documents.Select(d => d.Value).Cast<IParentChildDocument>())
                 doc.Discriminator = JoinField.Link<Child>(doc.ParentId);
             
             return Task.CompletedTask;
         }

         protected Task OnBeforeQuery(object sender, BeforeQueryEventArgs<Child> args) {
             args.Query.Discriminator("child");
             return Task.CompletedTask;
         }

         protected override ICommandOptions ConfigureOptions(ICommandOptions options) {
             return base.ConfigureOptions(options).ParentDocumentType(typeof(Parent));
         }

         public Task<FindResults<Child>> QueryAsync(RepositoryQueryDescriptor<Child> query, CommandOptionsDescriptor<Child> options = null) {
             return FindAsync(query, options);
         }
     }
 }
