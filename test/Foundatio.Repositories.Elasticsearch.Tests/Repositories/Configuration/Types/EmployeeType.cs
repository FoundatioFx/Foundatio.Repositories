// using System;
// using System.Collections.Generic;
// using System.Linq;
// using System.Threading.Tasks;
// using Foundatio.Logging;
// using Foundatio.Parsers.ElasticQueries;
// using Foundatio.Parsers.ElasticQueries.Extensions;
// using Foundatio.Repositories.Elasticsearch.Configuration;
// using Foundatio.Repositories.Elasticsearch.Extensions;
// using Foundatio.Repositories.Elasticsearch.Queries.Builders;
// using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
// using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries;
// using Microsoft.Extensions.Logging;
// using Nest;
// #pragma warning disable 618

// namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types {
//     public class EmployeeType : IndexTypeBase<Employee> {
//         public EmployeeType(IIndex index) : base(index) { }
//     }

//     public class EmployeeTypeWithYearsEmployed : EmployeeType {
//         public EmployeeTypeWithYearsEmployed(IIndex index) : base(index: index) { }

//         public override TypeMappingDescriptor<Employee> BuildMapping(TypeMappingDescriptor<Employee> map) {
//             return base.BuildMapping(map
//                 .Dynamic(false)
//                 .Properties(p => p
//                     .SetupDefaults()
//                     .Keyword(f => f.Name(e => e.Id))
//                     .Keyword(f => f.Name(e => e.CompanyId))
//                     .Keyword(f => f.Name(e => e.CompanyName))
//                     .Text(f => f.Name(e => e.Name).AddKeywordField())
//                     .Scalar(f => f.Age, f => f.Name(e => e.Age))
//                     .Scalar(f => f.YearsEmployed, f => f.Name(e => e.YearsEmployed))
//                     .Date(f => f.Name(e => e.LastReview))
//                     .Scalar(f => f.NextReview, f => f.Name(e => e.NextReview).Alias("next"))
//                 ));
//         }
//     }

//     public class EmployeeTypeWithWithPipeline : EmployeeType, IHavePipelinedIndexType {
//         public EmployeeTypeWithWithPipeline(IIndex index) : base(index: index) { }

//         public string Pipeline { get; } = "increment-age-pipeline";

//         public override async Task ConfigureAsync() {
//             var response = await Configuration.Client.PutPipelineAsync(Pipeline, d => d
//                 .Processors(p => p
//                     .Lowercase<Employee>(l => l.Field(f => f.Name))
//                     .Trim<Employee>(t => t.Field(f => f.Name))
//                 ).OnFailure(of => of.Set<Employee>(s => s.Field(f => f.Name).Value(String.Empty))));

//             var logger = Configuration.LoggerFactory.CreateLogger(typeof(EmployeeTypeWithWithPipeline));
//             logger.LogTrace(response.GetRequest());
//             if (response.IsValid)
//                 return;

//             string message = $"Error creating the pipeline {Pipeline}: {response.GetErrorMessage()}";
//             logger.LogError(response.OriginalException, message);
//             throw new ApplicationException(message, response.OriginalException);
//         }
//     }

//     public class DailyEmployeeType : DailyIndexType<Employee> {
//         public DailyEmployeeType(IIndex index) : base(index: index) { }

//         public override TypeMappingDescriptor<Employee> BuildMapping(TypeMappingDescriptor<Employee> map) {
//             return base.BuildMapping(map
//                 .Dynamic(false)
//                 .Properties(p => p
//                     .SetupDefaults()
//                     .Keyword(f => f.Name(e => e.Id))
//                     .Keyword(f => f.Name(e => e.CompanyId))
//                     .Keyword(f => f.Name(e => e.CompanyName))
//                     .Text(f => f.Name(e => e.Name).AddKeywordField())
//                     .Scalar(f => f.Age, f => f.Name(e => e.Age))
//                     .Date(f => f.Name(e => e.LastReview))
//                     .Scalar(f => f.NextReview, f => f.Name(e => e.NextReview).Alias("next"))
//                 ));
//         }

//         protected override void ConfigureQueryBuilder(ElasticQueryBuilder builder) {
//             builder.Register<AgeQueryBuilder>();
//             builder.Register<CompanyQueryBuilder>();
//         }
//     }

//     public class MonthlyEmployeeType : MonthlyIndexType<Employee> {
//         public MonthlyEmployeeType(IIndex index) : base(index: index) { }

//         public override TypeMappingDescriptor<Employee> BuildMapping(TypeMappingDescriptor<Employee> map) {
//             return base.BuildMapping(map
//                 .Dynamic(false)
//                 .Properties(p => p
//                     .SetupDefaults()
//                     .Keyword(f => f.Name(e => e.Id))
//                     .Keyword(f => f.Name(e => e.CompanyId))
//                     .Keyword(f => f.Name(e => e.CompanyName))
//                     .Text(f => f.Name(e => e.Name).AddKeywordField())
//                     .Scalar(f => f.Age, f => f.Name(e => e.Age))
//                     .Date(f => f.Name(e => e.LastReview))
//                     .Scalar(f => f.NextReview, f => f.Name(e => e.NextReview).Alias("next"))
//                 ));
//         }

//         protected override void ConfigureQueryBuilder(ElasticQueryBuilder builder) {
//             builder.Register<AgeQueryBuilder>();
//             builder.Register<CompanyQueryBuilder>();
//         }
//     }
// }