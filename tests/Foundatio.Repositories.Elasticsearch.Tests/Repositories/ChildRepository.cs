 using System.Linq;
 using System.Threading.Tasks;
 using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
 using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
 using Foundatio.Repositories.Models;
 using Foundatio.Repositories.Options;
 using Nest;

 namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories {
     public interface IChildRepository : ISearchableRepository<Child> {}
     
     public class ChildRepository : ElasticRepositoryBase<Child>, IChildRepository {
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

         protected override ICommandOptions<Child> ConfigureOptions(ICommandOptions<Child> options) {
             return base.ConfigureOptions(options).ParentDocumentType(typeof(Parent));
         }
     }
 }
