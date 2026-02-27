using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries;
using Foundatio.Serializer;
using Microsoft.Extensions.Logging.Abstractions;
namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;

public sealed class EmployeeIndex : Index<Employee>
{
    public EmployeeIndex(IElasticConfiguration configuration) : base(configuration, "employees")
    {
        AddStandardCustomFieldTypes();

        // overrides the normal integer field type with one that supports expressions to calculate values
        AddCustomFieldType(new CalculatedIntegerFieldType(new ScriptService(new SystemTextJsonSerializer(), NullLogger<ScriptService>.Instance)));
    }

    public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
    {
        base.ConfigureIndex(idx.Settings(s => s
            .AddOtherSetting("index.mapping.ignore_malformed", "true")
            .NumberOfReplicas(0)
            .NumberOfShards(1)
            .Analysis(a => a.AddSortNormalizer())));
    }

    public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
    {
        map
            .Dynamic(DynamicMapping.False)
            .Properties(p => p
                .SetupDefaults()
                .Text("_all")
                .Keyword(e => e.EmailAddress)
                .Keyword(e => e.CompanyId)
                .Keyword(e => e.EmploymentType)
                .Keyword(e => e.CompanyName)
                .Text(e => e.Name, t => t.AddKeywordAndSortFields().CopyTo("_all"))
                .IntegerNumber(e => e.Age)
                .FieldAlias("aliasedage", a => a.Path(e => e.Age))
                .DoubleNumber(e => e.DecimalAge)
                .Date(e => e.NextReview)
                .FieldAlias("next", a => a.Path(e => e.NextReview))
                .GeoPoint(e => e.Location)
                .FieldAlias("phone", a => a.Path("phoneNumbers.number"))
                .Object(e => e.PhoneNumbers, mp => mp
                    .Properties(p => p.Text("number", t => t.CopyTo("_all"))))
                .FieldAlias("twitter", a => a.Path("data.@user_meta.twitter_id"))
                .FieldAlias("followers", a => a.Path("data.@user_meta.twitter_followers"))
                .Object(e => e.Data, p1 => p1
                    .Properties(p => p.Object("@user_meta", p2 => p2
                        .Properties(p3 => p3
                            .Keyword("twitter_id", f3 => f3.CopyTo("_all"))
                            .LongNumber("twitter_followers")))))
                .Nested(e => e.PeerReviews, p1 => p1
                    .Properties(p => p
                        .Keyword("reviewerEmployeeId")
                        .IntegerNumber("rating")))
                );
    }

    protected override void ConfigureQueryBuilder(ElasticQueryBuilder builder)
    {
        builder.Register<AgeQueryBuilder>();
        builder.Register<CompanyQueryBuilder>();
        builder.Register<EmailAddressQueryBuilder>();
    }

    protected override void ConfigureQueryParser(ElasticQueryParserConfiguration config)
    {
        base.ConfigureQueryParser(config);
        config.UseIncludes(ResolveIncludeAsync).UseOptInRuntimeFieldResolver(ResolveRuntimeFieldAsync);
        config.SetDefaultFields([
            nameof(Employee.Id).ToLowerInvariant(),
            nameof(Employee.Name).ToLowerInvariant(),
            "peerReviews.reviewerEmployeeId"
        ]);
    }

    private async Task<string> ResolveIncludeAsync(string name)
    {
        await Task.Delay(100);
        return "aliasedage:10";
    }

    private async Task<ElasticRuntimeField> ResolveRuntimeFieldAsync(string name)
    {
        await Task.Delay(100);

        if (name.Equals("unmappedEmailAddress", StringComparison.OrdinalIgnoreCase))
            return new ElasticRuntimeField { Name = "unmappedEmailAddress" };

        return null;
    }
}

public sealed class EmployeeIndexWithYearsEmployed : Index<Employee>
{
    public EmployeeIndexWithYearsEmployed(IElasticConfiguration configuration) : base(configuration, "employees") { }

    public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
    {
        base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
    }

    public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
    {
        map
            .Dynamic(DynamicMapping.False)
            .Properties(p => p
                .SetupDefaults()
                .Keyword(e => e.CompanyId)
                .Keyword(e => e.CompanyName)
                .Text(e => e.Name, t => t.AddKeywordField())
                .IntegerNumber(e => e.Age)
                .IntegerNumber(e => e.YearsEmployed)
                .Date(e => e.LastReview)
                .Date(e => e.NextReview)
                .FieldAlias("next", a => a.Path(e => e.NextReview))
            );
    }
}

public sealed class VersionedEmployeeIndex : VersionedIndex<Employee>
{
    private readonly Action<CreateIndexRequestDescriptor> _createIndex;
    private readonly Action<TypeMappingDescriptor<Employee>> _createMappings;

    public VersionedEmployeeIndex(IElasticConfiguration configuration, int version,
        Action<CreateIndexRequestDescriptor> createIndex = null,
        Action<TypeMappingDescriptor<Employee>> createMappings = null) : base(configuration, "employees", version)
    {
        _createIndex = createIndex;
        _createMappings = createMappings;
        AddReindexScript(20, "ctx._source.companyName = 'scripted';");
        AddReindexScript(21, "ctx._source.companyName = 'typed script';");
        AddReindexScript(22, "ctx._source.FAIL = 'should not work");
    }

    public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
    {
        if (_createIndex != null)
        {
            _createIndex(idx);
            return;
        }

        base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
    }

    public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
    {
        if (_createMappings != null)
        {
            _createMappings(map);
            return;
        }

        base.ConfigureIndexMapping(map);
    }
}

public sealed class DailyEmployeeIndex : DailyIndex<Employee>
{
    public DailyEmployeeIndex(IElasticConfiguration configuration, int version) : base(configuration, "daily-employees", version)
    {
        AddAlias($"{Name}-today", TimeSpan.FromDays(1));
        AddAlias($"{Name}-last7days", TimeSpan.FromDays(7));
        AddAlias($"{Name}-last30days", TimeSpan.FromDays(30));
    }

    public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
    {
        base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
    }

    public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
    {
        map
            .Dynamic(DynamicMapping.False)
            .Properties(p => p
                .SetupDefaults()
                .Keyword(e => e.CompanyId)
                .Keyword(e => e.CompanyName)
                .Text(e => e.Name, t => t.AddKeywordField())
                .IntegerNumber(e => e.Age)
                .Date(e => e.LastReview)
                .Date(e => e.NextReview)
                .FieldAlias("next", a => a.Path(e => e.NextReview))
            );
    }

    protected override void ConfigureQueryBuilder(ElasticQueryBuilder builder)
    {
        builder.Register<AgeQueryBuilder>();
        builder.Register<CompanyQueryBuilder>();
    }
}

public sealed class DailyEmployeeIndexWithWrongEmployeeType : DailyIndex<Employee>
{
    public DailyEmployeeIndexWithWrongEmployeeType(IElasticConfiguration configuration, int version) : base(configuration, "daily-employees", version) { }
}

public sealed class MonthlyEmployeeIndex : MonthlyIndex<Employee>
{
    public MonthlyEmployeeIndex(IElasticConfiguration configuration, int version) : base(configuration, "monthly-employees", version)
    {
        AddAlias($"{Name}-today", TimeSpan.FromDays(1));
        AddAlias($"{Name}-last7days", TimeSpan.FromDays(7));
        AddAlias($"{Name}-last30days", TimeSpan.FromDays(30));
        AddAlias($"{Name}-last60days", TimeSpan.FromDays(60));
    }

    public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
    {
        base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
    }

    public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
    {
        map
            .Dynamic(DynamicMapping.False)
            .Properties(p => p
                .SetupDefaults()
                .Keyword(e => e.CompanyId)
                .Keyword(e => e.CompanyName)
                .Text(e => e.Name, t => t.AddKeywordField())
                .IntegerNumber(e => e.Age)
                .Date(e => e.LastReview)
                .Date(e => e.NextReview)
                .FieldAlias("next", a => a.Path(e => e.NextReview))
            );
    }

    protected override void ConfigureQueryBuilder(ElasticQueryBuilder builder)
    {
        builder.Register<AgeQueryBuilder>();
        builder.Register<CompanyQueryBuilder>();
    }
}
