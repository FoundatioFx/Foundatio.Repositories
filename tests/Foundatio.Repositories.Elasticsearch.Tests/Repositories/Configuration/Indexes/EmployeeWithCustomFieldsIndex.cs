using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries;
using Foundatio.Serializer;
using Microsoft.Extensions.Logging.Abstractions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;

public sealed class EmployeeWithCustomFieldsIndex : VersionedIndex<EmployeeWithCustomFields>
{
    public EmployeeWithCustomFieldsIndex(IElasticConfiguration configuration) : base(configuration, "employees-customfields")
    {
        AddStandardCustomFieldTypes();

        // overrides the normal integer field type with one that supports expressions to calculate values
        AddCustomFieldType(new CalculatedIntegerFieldType(new ScriptService(new SystemTextJsonSerializer(), NullLogger<ScriptService>.Instance)));
    }

    public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx)
    {
        return base.ConfigureIndex(idx.Settings(s => s
            .Setting("index.mapping.ignore_malformed", "true")
            .NumberOfReplicas(0)
            .NumberOfShards(1)
            .Analysis(a => a.AddSortNormalizer())));
    }

    public override TypeMappingDescriptor<EmployeeWithCustomFields> ConfigureIndexMapping(TypeMappingDescriptor<EmployeeWithCustomFields> map)
    {
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
                        .Keyword(f3 => f3.Name("twitter_id").CopyTo(c => c.Field("_all")))
                        .Number(f3 => f3.Name("twitter_followers"))
                    ))))
                .Nested<PeerReview>(f => f.Name(e => e.PeerReviews).Properties(p1 => p1
                    .Keyword(f2 => f2.Name(p2 => p2.ReviewerEmployeeId))
                    .Scalar(p3 => p3.Rating, f2 => f2.Name(p3 => p3.Rating))))
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
