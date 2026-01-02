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

public sealed class EmployeeWithCustomFieldsIndex : VersionedIndex<EmployeeWithCustomFields>
{
    public EmployeeWithCustomFieldsIndex(IElasticConfiguration configuration) : base(configuration, "employees-customfields")
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

    public override void ConfigureIndexMapping(TypeMappingDescriptor<EmployeeWithCustomFields> map)
    {
        map
            .Dynamic(DynamicMapping.False)
            .Properties(p => p
                .SetupDefaults()
                .Text("_all")
                .Keyword(e => e.EmailAddress)
                .Keyword(e => e.CompanyId)
                .Keyword(e => e.CompanyName)
                .Text(e => e.Name, t => t.AddKeywordAndSortFields().CopyTo("_all"))
                .IntegerNumber(e => e.Age)
                .FieldAlias("aliasedage", a => a.Path(e => e.Age))
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
