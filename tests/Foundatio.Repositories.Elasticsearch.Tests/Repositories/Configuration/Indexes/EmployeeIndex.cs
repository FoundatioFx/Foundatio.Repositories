using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Nest;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Parsers;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes {
    public sealed class EmployeeIndex : Index<Employee> {
        public EmployeeIndex(IElasticConfiguration configuration): base(configuration, "employees") {}

        public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            return base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1).Analysis(a => a.AddSortNormalizer())));
        }

        public override TypeMappingDescriptor<Employee> ConfigureIndexMapping(TypeMappingDescriptor<Employee> map) {
            return map
                .Dynamic(false)
                .Properties(p => p
                    .SetupDefaults()
                    .Text(f => f.Name("_all"))
                    .Keyword(f => f.Name(e => e.Id))
                    .Keyword(f => f.Name(e => e.EmailAddress))
                    .Keyword(f => f.Name(e => e.CompanyId))
                    .Keyword(f => f.Name(e => e.CompanyName))
                    .Text(f => f.Name(e => e.Name).AddKeywordAndSortFields().CopyTo(c => c.Field("_all")))
                    .Scalar(f => f.Age, f => f.Name(e => e.Age))
                    .FieldAlias(a => a.Name("aliasedage").Path(f => f.Age))
                    .Scalar(f => f.NextReview, f => f.Name(e => e.NextReview))
                    .FieldAlias(a => a.Name("next").Path(f => f.NextReview))
                    .GeoPoint(f => f.Name(e => e.Location))
                    .FieldAlias(a => a.Name("phone").Path(f => f.PhoneNumbers.First().Number))
                    .Object<PhoneInfo>(f => f
                        .Name(u => u.PhoneNumbers.First()).Properties(mp => mp
                            .Text(fu => fu.Name(m => m.Number).CopyTo(c => c.Field("_all")))))
                    .FieldAlias(a => a.Name("twitter").Path("data.@user_meta.twitter_id"))
                    .FieldAlias(a => a.Name("followers").Path("data.@user_meta.twitter_followers"))
                    .Object<Dictionary<string, object>>(f => f.Name(e => e.Data).Properties(p1 => p1
                        .Object<object>(f2 => f2.Name("@user_meta").Properties(p2 => p2
                            .Keyword(f3 => f3.Name("twitter_id").Boost(1.1).CopyTo(c => c.Field("_all")))
                            .Number(f3 => f3.Name("twitter_followers"))
                        ))))
                    .Nested<PeerReview>(f => f.Name(e => e.PeerReviews).Properties(p1 => p1
                        .Keyword(f2 => f2.Name(p2 => p2.ReviewerEmployeeId))
                        .Scalar(p3 => p3.Rating, f2 => f2.Name(p3 => p3.Rating))))
                    );
        }

        protected override void ConfigureQueryBuilder(ElasticQueryBuilder builder) {
            builder.Register<AgeQueryBuilder>();
            builder.Register<CompanyQueryBuilder>();
            builder.Register<EmailAddressQueryBuilder>();
        }

        protected override void ConfigureQueryParser(ElasticQueryParserConfiguration config) {
            base.ConfigureQueryParser(config);
            config.UseIncludes(ResolveIncludeAsync).UseOptInRuntimeFieldResolver(ResolveRuntimeFieldAsync);
        }

        private async Task<string> ResolveIncludeAsync(string name) {
            await Task.Delay(100);
            return "aliasedage:10";
        }

        private async Task<ElasticRuntimeField> ResolveRuntimeFieldAsync(string name) {
            await Task.Delay(100);

            if (name.Equals("unmappedEmailAddress", StringComparison.OrdinalIgnoreCase))
                return new ElasticRuntimeField { Name = "unmappedEmailAddress" };
            
            return null;
        }
    }

    public sealed class EmployeeIndexWithYearsEmployed : Index<Employee> {
        public EmployeeIndexWithYearsEmployed(IElasticConfiguration configuration) : base(configuration, "employees") {}

        public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            return base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
        }

        public override TypeMappingDescriptor<Employee> ConfigureIndexMapping(TypeMappingDescriptor<Employee> map) {
            return base.ConfigureIndexMapping(map
                .Dynamic(false)
                .Properties(p => p
                    .SetupDefaults()
                    .Keyword(f => f.Name(e => e.Id))
                    .Keyword(f => f.Name(e => e.CompanyId))
                    .Keyword(f => f.Name(e => e.CompanyName))
                    .Text(f => f.Name(e => e.Name).AddKeywordField())
                    .Scalar(f => f.Age, f => f.Name(e => e.Age))
                    .Scalar(f => f.YearsEmployed, f => f.Name(e => e.YearsEmployed))
                    .Date(f => f.Name(e => e.LastReview))
                    .Scalar(f => f.NextReview, f => f.Name(e => e.NextReview))
                    .FieldAlias(a => a.Name("next").Path(f2 => f2.NextReview))
                ));
        }
    }

    public sealed class VersionedEmployeeIndex : VersionedIndex<Employee> {
        private readonly Func<CreateIndexDescriptor, CreateIndexDescriptor> _createIndex;
        private readonly Func<TypeMappingDescriptor<Employee>, TypeMappingDescriptor<Employee>> _createMappings;

        public VersionedEmployeeIndex(IElasticConfiguration configuration, int version,
            Func<CreateIndexDescriptor, CreateIndexDescriptor> createIndex = null,
            Func<TypeMappingDescriptor<Employee>, TypeMappingDescriptor<Employee>> createMappings = null) : base(configuration, "employees", version) {
            _createIndex = createIndex;
            _createMappings = createMappings;
            AddReindexScript(20, "ctx._source.companyName = 'scripted';");
            AddReindexScript(21, "ctx._source.companyName = 'typed script';");
            AddReindexScript(22, "ctx._source.FAIL = 'should not work");
        }

        public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            if (_createIndex != null)
                return _createIndex(idx);
            
            return base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
        }

        public override TypeMappingDescriptor<Employee> ConfigureIndexMapping(TypeMappingDescriptor<Employee> map) {
            if (_createMappings != null)
                return _createMappings(map);
            
            return base.ConfigureIndexMapping(map);
        }
    }

    public sealed class DailyEmployeeIndex : DailyIndex<Employee> {
        public DailyEmployeeIndex(IElasticConfiguration configuration, int version) : base(configuration, "daily-employees", version) {
            AddAlias($"{Name}-today", TimeSpan.FromDays(1));
            AddAlias($"{Name}-last7days", TimeSpan.FromDays(7));
            AddAlias($"{Name}-last30days", TimeSpan.FromDays(30));
        }

        public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            return base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
        }

        public override TypeMappingDescriptor<Employee> ConfigureIndexMapping(TypeMappingDescriptor<Employee> map) {
            return base.ConfigureIndexMapping(map
                .Dynamic(false)
                .Properties(p => p
                    .SetupDefaults()
                    .Keyword(f => f.Name(e => e.Id))
                    .Keyword(f => f.Name(e => e.CompanyId))
                    .Keyword(f => f.Name(e => e.CompanyName))
                    .Text(f => f.Name(e => e.Name).AddKeywordField())
                    .Scalar(f => f.Age, f => f.Name(e => e.Age))
                    .Date(f => f.Name(e => e.LastReview))
                    .Scalar(f => f.NextReview, f => f.Name(e => e.NextReview))
                    .FieldAlias(a => a.Name("next").Path(f2 => f2.NextReview))
                ));
        }

        protected override void ConfigureQueryBuilder(ElasticQueryBuilder builder) {
            builder.Register<AgeQueryBuilder>();
            builder.Register<CompanyQueryBuilder>();
        }
    }

    public sealed class DailyEmployeeIndexWithWrongEmployeeType : DailyIndex<Employee> {
        public DailyEmployeeIndexWithWrongEmployeeType(IElasticConfiguration configuration, int version) : base(configuration, "daily-employees", version) {}
    }

    public sealed class MonthlyEmployeeIndex : MonthlyIndex<Employee> {
        public MonthlyEmployeeIndex(IElasticConfiguration configuration, int version) : base(configuration, "monthly-employees", version) {
            AddAlias($"{Name}-today", TimeSpan.FromDays(1));
            AddAlias($"{Name}-last7days", TimeSpan.FromDays(7));
            AddAlias($"{Name}-last30days", TimeSpan.FromDays(30));
            AddAlias($"{Name}-last60days", TimeSpan.FromDays(60));
        }

        public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            return base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
        }

        public override TypeMappingDescriptor<Employee> ConfigureIndexMapping(TypeMappingDescriptor<Employee> map) {
            return base.ConfigureIndexMapping(map
                .Dynamic(false)
                .Properties(p => p
                    .SetupDefaults()
                    .Keyword(f => f.Name(e => e.Id))
                    .Keyword(f => f.Name(e => e.CompanyId))
                    .Keyword(f => f.Name(e => e.CompanyName))
                    .Text(f => f.Name(e => e.Name).AddKeywordField())
                    .Scalar(f => f.Age, f => f.Name(e => e.Age))
                    .Date(f => f.Name(e => e.LastReview))
                    .Scalar(f => f.NextReview, f => f.Name(e => e.NextReview))
                    .FieldAlias(a => a.Name("next").Path(f => f.NextReview))
                ));
        }

        protected override void ConfigureQueryBuilder(ElasticQueryBuilder builder) {
            builder.Register<AgeQueryBuilder>();
            builder.Register<CompanyQueryBuilder>();
        }
    }
}