using System;
using System.Threading.Tasks;
using Foundatio.Logging;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries;
using Nest;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types {
    public class EmployeeType : IndexTypeBase<Employee> {
        public EmployeeType(IIndex index) : base(index) { }

        public override TypeMappingDescriptor<Employee> BuildMapping(TypeMappingDescriptor<Employee> map) {
            return base.BuildMapping(map
                .Dynamic(false)
                .Properties(p => p
                    .SetupDefaults()
                    .Keyword(f => f.Name(e => e.Id))
                    .Keyword(f => f.Name(e => e.CompanyId))
                    .Keyword(f => f.Name(e => e.CompanyName))
                    .Text(f => f.Name(e => e.Name))
                    .Scalar(f => f.Age, f => f.Name(e => e.Age))
                    .Scalar(f => f.NextReview, f => f.Name(e => e.NextReview))
                    .GeoPoint(f => f.Name(e => e.Location))
                ));
        }

        protected override void ConfigureQueryBuilder(ElasticQueryBuilder builder) {
            var aliasMap = new AliasMap {
                { "aliasedage", "age" }
            };

            builder.Register<AgeQueryBuilder>();
            builder.Register<CompanyQueryBuilder>();
            builder.UseQueryParser(this, c => c
                .UseIncludes(i => ResolveIncludeAsync(i))
                .UseAliases(aliasMap)
            );
        }

        private async Task<string> ResolveIncludeAsync(string name) {
            await Task.Delay(100);
            return "aliasedage:10";
        }
    }

    public class EmployeeTypeWithYearsEmployed : EmployeeType {
        public EmployeeTypeWithYearsEmployed(IIndex index) : base(index: index) { }

        public override TypeMappingDescriptor<Employee> BuildMapping(TypeMappingDescriptor<Employee> map) {
            return base.BuildMapping(map
                .Dynamic(false)
                .Properties(p => p
                    .SetupDefaults()
                    .Keyword(f => f.Name(e => e.Id))
                    .Keyword(f => f.Name(e => e.CompanyId))
                    .Keyword(f => f.Name(e => e.CompanyName))
                    .Text(f => f.Name(e => e.Name))
                    .Scalar(f => f.Age, f => f.Name(e => e.Age))
                    .Scalar(f => f.YearsEmployed, f => f.Name(e => e.YearsEmployed))
                    .Date(f => f.Name(e => e.LastReview))
                    .Scalar(f => f.NextReview, f => f.Name(e => e.NextReview))
                ));
        }
    }

    public class EmployeeTypeWithWithPipeline : EmployeeType, IHavePipelinedIndexType {
        public EmployeeTypeWithWithPipeline(IIndex index) : base(index: index) { }

        public string Pipeline { get; } = "increment-age-pipeline";

        public override async Task ConfigureAsync() {
            var response = await Configuration.Client.PutPipelineAsync(Pipeline, d => d
                .Processors(p => p
                    .Lowercase<Employee>(l => l.Field(f => f.Name))
                    .Trim<Employee>(t => t.Field(f => f.Name))
                ).OnFailure(of => of.Set<Employee>(s => s.Field(f => f.Name).Value(String.Empty))));

            var logger = Configuration.LoggerFactory.CreateLogger(typeof(EmployeeTypeWithWithPipeline));
            logger.Trace(() => response.GetRequest());
            if (response.IsValid)
                return;

            string message = $"Error creating the pipeline {Pipeline}: {response.GetErrorMessage()}";
            logger.Error().Exception(response.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
            throw new ApplicationException(message, response.OriginalException);
        }
    }

    public class DailyEmployeeType : DailyIndexType<Employee> {
        public DailyEmployeeType(IIndex index) : base(index: index) { }

        public override TypeMappingDescriptor<Employee> BuildMapping(TypeMappingDescriptor<Employee> map) {
            return base.BuildMapping(map
                .Dynamic(false)
                .Properties(p => p
                    .SetupDefaults()
                    .Keyword(f => f.Name(e => e.Id))
                    .Keyword(f => f.Name(e => e.CompanyId))
                    .Keyword(f => f.Name(e => e.CompanyName))
                    .Text(f => f.Name(e => e.Name))
                    .Scalar(f => f.Age, f => f.Name(e => e.Age))
                    .Date(f => f.Name(e => e.LastReview))
                    .Scalar(f => f.NextReview, f => f.Name(e => e.NextReview))
                ));
        }

        protected override void ConfigureQueryBuilder(ElasticQueryBuilder builder) {
            builder.Register<AgeQueryBuilder>();
            builder.Register<CompanyQueryBuilder>();
            builder.UseQueryParser(this);
        }
    }

    public class MonthlyEmployeeType : MonthlyIndexType<Employee> {
        public MonthlyEmployeeType(IIndex index) : base(index: index) { }

        public override TypeMappingDescriptor<Employee> BuildMapping(TypeMappingDescriptor<Employee> map) {
            return base.BuildMapping(map
                .Dynamic(false)
                .Properties(p => p
                    .SetupDefaults()
                    .Keyword(f => f.Name(e => e.Id))
                    .Keyword(f => f.Name(e => e.CompanyId))
                    .Keyword(f => f.Name(e => e.CompanyName))
                    .Text(f => f.Name(e => e.Name))
                    .Scalar(f => f.Age, f => f.Name(e => e.Age))
                    .Date(f => f.Name(e => e.LastReview))
                    .Scalar(f => f.NextReview, f => f.Name(e => e.NextReview))
                ));
        }

        protected override void ConfigureQueryBuilder(ElasticQueryBuilder builder) {
            builder.Register<AgeQueryBuilder>();
            builder.Register<CompanyQueryBuilder>();
            builder.UseQueryParser(this);
        }
    }
}