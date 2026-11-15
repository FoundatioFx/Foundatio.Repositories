using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Transport;
using Elastic.Transport.Extensions;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Utility;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class NativeFieldMappingTests
{
    [Fact]
    public void InferFields_WithConcreteJsonPropertyNames_UsesTheStoredNames()
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);

        Assert.Equal("document_id", repository.IdField);
        Assert.Equal("updated_at", repository.UpdatedField);
        Assert.Equal(new[] { "document_id", "created_at", "updated_at" }, repository.RequiredFieldNames);
        Assert.Equal("is_deleted", configuration.Client.Infer.Field(DocumentFields<TestDocument>.IsDeleted!));
    }

    [Fact]
    public void InferFields_WithDefaultAndInheritedMembers_PreservesNativeInference()
    {
        using var configuration = new TestConfiguration();
        var infer = configuration.Client.Infer;

        Assert.Equal("id", infer.Field(DocumentFields<PlainDocument>.Id!));
        Assert.Equal("createdUtc", infer.Field(DocumentFields<PlainDocument>.CreatedUtc!));
        Assert.Equal("updatedUtc", infer.Field(DocumentFields<PlainDocument>.UpdatedUtc!));
        Assert.Equal("isDeleted", infer.Field(DocumentFields<PlainDocument>.IsDeleted!));
        Assert.Equal("document_id", infer.Field(DocumentFields<InheritedDocument>.Id!));
        Assert.Equal("updated_at", infer.Field(DocumentFields<InheritedDocument>.UpdatedUtc!));
        Assert.Equal("overridden_updated_at", infer.Field(DocumentFields<OverriddenDocument>.UpdatedUtc!));
        Assert.Null(DocumentFields<CreatedOnlyDocument>.UpdatedUtc);
    }

    [Fact]
    public void InferFields_WithExplicitInterfaces_DoesNotSelectUnrelatedPublicProperties()
    {
        using var configuration = new TestConfiguration();
        var infer = configuration.Client.Infer;

        Assert.Equal("id", infer.Field(DocumentFields<ExplicitDocument>.Id!));
        Assert.Equal("createdUtc", infer.Field(DocumentFields<ExplicitDocument>.CreatedUtc!));
        Assert.Equal("updatedUtc", infer.Field(DocumentFields<ExplicitDocument>.UpdatedUtc!));
        Assert.Equal("isDeleted", infer.Field(DocumentFields<ExplicitDocument>.IsDeleted!));
        Assert.Equal("id", infer.Field(DocumentFields<IIdentity>.Id!));
    }

    [Fact]
    public void InferFields_WithConfiguredNativeNameInferrer_UsesConfiguredNames()
    {
        using var configuration = new TestConfiguration(configureSettings: settings => settings.DefaultFieldNameInferrer(name => "native_" + name));
        var infer = configuration.Client.Infer;

        Assert.Equal("native_Id", infer.Field(DocumentFields<PlainDocument>.Id!));
        Assert.Equal("native_CreatedUtc", infer.Field(DocumentFields<PlainDocument>.CreatedUtc!));
        Assert.Equal("native_UpdatedUtc", infer.Field(DocumentFields<PlainDocument>.UpdatedUtc!));
        Assert.Equal("native_IsDeleted", infer.Field(DocumentFields<PlainDocument>.IsDeleted!));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task QuerySort_WithRenamedIdentity_UsesStoredFieldWithoutAnotherIdSort(bool searchAfter, bool existingIdSort)
    {
        using var configuration = new TestConfiguration();
        using var index = new TestStandardIndex<TestDocument>(configuration);
        var options = new CommandOptions<TestDocument>().ElasticIndex(index).SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        var context = new QueryBuilderContext<TestDocument>(new RepositoryQuery<TestDocument>(), options);
        if (existingIdSort)
            context.Data[SortQueryBuilder.SortFieldsKey] = new List<SortOptions> { new FieldSort { Field = Infer.Field<TestDocument>(document => document.Id) } };
        IElasticQueryBuilder builder = searchAfter ? new SearchAfterQueryBuilder() : new DefaultSortQueryBuilder();

        await builder.BuildAsync(context);

        Assert.Equal(new[] { "document_id" }, GetSortFieldNames(configuration, context, searchAfter));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task QuerySort_WithoutIndex_PreservesLiteralIdentityFallback(bool searchAfter)
    {
        using var configuration = new TestConfiguration();
        var options = new CommandOptions<TestDocument>().SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        var context = new QueryBuilderContext<TestDocument>(new RepositoryQuery<TestDocument>(), options);
        IElasticQueryBuilder builder = searchAfter ? new SearchAfterQueryBuilder() : new DefaultSortQueryBuilder();

        await builder.BuildAsync(context);

        Assert.Equal(new[] { "Id" }, GetSortFieldNames(configuration, context, searchAfter));
    }

    private static string[] GetSortFieldNames(TestConfiguration configuration, QueryBuilderContext<TestDocument> context, bool searchAfter)
    {
        if (!searchAfter)
            context.Search.Sort(Assert.IsType<List<SortOptions>>(context.Data[SortQueryBuilder.SortFieldsKey]));

        SearchRequest request = context.Search;
        using var json = JsonDocument.Parse(configuration.Client.ElasticsearchClientSettings.RequestResponseSerializer.SerializeToString(request));
        var sort = json.RootElement.GetProperty("sort");
        var sorts = sort.ValueKind is JsonValueKind.Array ? sort.EnumerateArray().ToArray() : [sort];
        return sorts.SelectMany(value => value.EnumerateObject()).Select(property => property.Name).ToArray();
    }

    [Fact]
    public void SetupDefaults_WithRenamedInterfaceMembers_UsesTheStoredFieldsAndAliases()
    {
        using var configuration = new TestConfiguration();
        TypeMapping mapping = new TypeMappingDescriptor<TestDocument>().Properties(properties => properties.SetupDefaults());

        using var json = JsonDocument.Parse(configuration.Client.ElasticsearchClientSettings.RequestResponseSerializer.SerializeToString(mapping));

        var properties = json.RootElement.GetProperty("properties");
        Assert.Equal("keyword", properties.GetProperty("document_id").GetProperty("type").GetString());
        Assert.Equal("date", properties.GetProperty("created_at").GetProperty("type").GetString());
        Assert.Equal("date", properties.GetProperty("updated_at").GetProperty("type").GetString());
        Assert.Equal("boolean", properties.GetProperty("is_deleted").GetProperty("type").GetString());
        Assert.Equal("created_at", properties.GetProperty("created").GetProperty("path").GetString());
        Assert.Equal("updated_at", properties.GetProperty("updated").GetProperty("path").GetString());
        Assert.Equal("is_deleted", properties.GetProperty("deleted").GetProperty("path").GetString());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SetupDefaults_WhenStoredFieldsMatchDefaultAliases_PreservesConcreteMappings(bool configuredInferrer)
    {
        using var configuration = new TestConfiguration(configureSettings: settings =>
        {
            if (configuredInferrer)
                settings.DefaultFieldNameInferrer(name => name switch
                {
                    nameof(IHaveCreatedDate.CreatedUtc) => "created",
                    nameof(IHaveDates.UpdatedUtc) => "updated",
                    nameof(ISupportSoftDeletes.IsDeleted) => "deleted",
                    _ => JsonNamingPolicy.CamelCase.ConvertName(name)
                });
        });
        TypeMapping mapping = configuredInferrer
            ? new TypeMappingDescriptor<PlainDocument>().Properties(properties => properties.SetupDefaults())
            : new TypeMappingDescriptor<AliasNamedDocument>().Properties(properties => properties.SetupDefaults());

        using var json = JsonDocument.Parse(configuration.Client.ElasticsearchClientSettings.RequestResponseSerializer.SerializeToString(mapping));

        var properties = json.RootElement.GetProperty("properties");
        Assert.Equal("date", properties.GetProperty("created").GetProperty("type").GetString());
        Assert.Equal("date", properties.GetProperty("updated").GetProperty("type").GetString());
        Assert.Equal("boolean", properties.GetProperty("deleted").GetProperty("type").GetString());
        Assert.Equal(4, properties.EnumerateObject().Count());
    }

    [Theory]
    [InlineData("standard")]
    [InlineData("versioned")]
    [InlineData("daily")]
    public void GetTimeStampField_WithRenamedDates_UsesTheStoredName(string indexKind)
    {
        using var configuration = new TestConfiguration();

        Assert.Equal("updated_at", GetTimestampField<TestDocument>(configuration, indexKind));
        Assert.Equal("created_at", GetTimestampField<CreatedOnlyDocument>(configuration, indexKind));
    }

    private static string? GetTimestampField<TDocument>(TestConfiguration configuration, string kind) where TDocument : class
    {
        return kind switch
        {
            "versioned" => new TestVersionedIndex<TDocument>(configuration).TimestampField,
            "daily" => new TestDailyIndex<TDocument>(configuration).TimestampField,
            _ => new TestStandardIndex<TDocument>(configuration).TimestampField
        };
    }

    [Fact]
    public void JsonPatch_WithRenamedUpdatedUtc_UpdatesTheNativeModelMember()
    {
        var utcNow = new DateTimeOffset(2026, 11, 15, 12, 0, 0, TimeSpan.Zero);
        using var configuration = new TestConfiguration(new FakeTimeProvider(utcNow));
        using var repository = new TestRepository(configuration);
        var source = new TestDocument
        {
            Id = "one",
            Name = "before",
            CreatedUtc = utcNow.AddDays(-2).UtcDateTime,
            UpdatedUtc = utcNow.AddDays(-1).UtcDateTime
        };
        var applier = new ClientPatchApplier<TestDocument>(configuration.Client.ElasticsearchClientSettings.SourceSerializer,
            repository.ApplyJsonDates, _ => { });
        var patch = new JsonPatch(new PatchDocument(new ReplaceOperation { Path = "/name", Value = "patched" }));

        var patched = applier.Apply(new FindHit<TestDocument>(source.Id, source, 0), patch);

        Assert.NotNull(patched);
        Assert.Equal("patched", patched.Name);
        Assert.Equal(utcNow.UtcDateTime, patched.UpdatedUtc);
        Assert.Equal(source.CreatedUtc, patched.CreatedUtc);
    }

    [Fact]
    public void PartialPatch_WithExplicitRenamedUpdatedUtc_DoesNotAddAnotherTimestamp()
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        var suppliedDate = new DateTime(2026, 11, 14, 12, 0, 0, DateTimeKind.Utc);
        var patch = new PartialPatch(new { name = "patched", updated_at = suppliedDate });

        var prepared = repository.PreparePartialPatch(patch);

        var json = JsonNode.Parse(configuration.Client.ElasticsearchClientSettings.SourceSerializer.SerializeToString(prepared.Document));
        Assert.NotNull(json);
        Assert.Equal(suppliedDate, json["updated_at"]!.GetValue<DateTime>());
        Assert.Null(json["updatedUtc"]);
    }

    [Fact]
    public void ScriptPatch_WithRenamedUpdatedUtc_UsesTheStoredFieldAndParameter()
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);

        var prepared = repository.PrepareScriptPatch(new ScriptPatch("ctx._source.name = 'patched';"));

        Assert.Contains("ctx._source.updated_at = params.updated_at;", prepared.Script, StringComparison.Ordinal);
        Assert.NotNull(prepared.Params);
        Assert.True(prepared.Params.ContainsKey("updated_at"));
        Assert.False(prepared.Params.ContainsKey("updatedUtc"));
    }

    [Theory]
    [InlineData("updated-at", "updated-at")]
    [InlineData("updated'at", "updated\\'at")]
    [InlineData("updated\\at", "updated\\\\at")]
    public void ScriptPatch_WithPunctuationInNativeUpdatedUtc_UsesEscapedLiteralAccess(string fieldName, string escapedName)
    {
        using var configuration = new TestConfiguration(configureSettings: settings => settings
            .DefaultFieldNameInferrer(name => name == nameof(PlainDocument.UpdatedUtc) ? fieldName : JsonNamingPolicy.CamelCase.ConvertName(name)));
        using var repository = new PlainTestRepository(configuration);

        var prepared = repository.PrepareScriptPatch(new ScriptPatch("ctx._source.name = 'patched';"));

        Assert.Equal(fieldName, repository.UpdatedField);
        Assert.Contains($"ctx._source['{escapedName}'] = params['{escapedName}'];", prepared.Script, StringComparison.Ordinal);
        Assert.NotNull(prepared.Params);
        Assert.True(prepared.Params.ContainsKey(fieldName));
        Assert.Single(prepared.Params);
    }

    [Theory]
    [InlineData("date-meta", "updated-at", "date-meta", "updated-at")]
    [InlineData("date'meta", "updated'at", "date\\'meta", "updated\\'at")]
    [InlineData("date\\meta", "updated\\at", "date\\\\meta", "updated\\\\at")]
    public void ScriptPatch_WithPunctuationInNestedTimestampPath_UsesEscapedLiteralAccess(
        string parentName, string fieldName, string escapedParent, string escapedField)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration, $"{parentName}.{fieldName}");

        var prepared = repository.PrepareScriptPatch(new ScriptPatch("ctx._source.name = 'patched';"));

        Assert.Contains($"if (ctx._source['{escapedParent}'] == null) {{ ctx._source['{escapedParent}'] = [:]; }}", prepared.Script, StringComparison.Ordinal);
        Assert.Contains($"ctx._source['{escapedParent}']['{escapedField}'] = params['{escapedField}'];", prepared.Script, StringComparison.Ordinal);
        Assert.NotNull(prepared.Params);
        Assert.True(prepared.Params.ContainsKey(fieldName));
        Assert.Single(prepared.Params);
    }

    private sealed class TestConfiguration(TimeProvider? timeProvider = null, Action<ElasticsearchClientSettings>? configureSettings = null) : ElasticConfiguration(timeProvider: timeProvider)
    {
        protected override IRequestInvoker CreateRequestInvoker() => new InMemoryRequestInvoker();

        protected override void ConfigureSettings(ElasticsearchClientSettings settings)
        {
            base.ConfigureSettings(settings);
            configureSettings?.Invoke(settings);
        }
    }

    private sealed class TestStandardIndex<TDocument>(TestConfiguration configuration) : Index<TDocument>(configuration) where TDocument : class
    {
        public string? TimestampField => GetTimeStampField();

        protected override ElasticMappingResolver CreateMappingResolver() =>
            ElasticMappingResolver.Create<TDocument>(ConfigureIndexMapping, Configuration.Client.Infer, () => null);
    }

    private sealed class TestVersionedIndex<TDocument>(TestConfiguration configuration) : VersionedIndex<TDocument>(configuration) where TDocument : class
    {
        public string? TimestampField => GetTimeStampField();
    }

    private sealed class TestDailyIndex<TDocument>(TestConfiguration configuration) : DailyIndex<TDocument>(configuration) where TDocument : class
    {
        public string? TimestampField => GetTimeStampField();
    }

    private sealed class TestRepository(TestConfiguration configuration, string? updatedFieldPath = null)
        : ElasticRepositoryBase<TestDocument>(new Index<TestDocument>(configuration, "native-field-mapping"))
    {
        public string IdField => _idField!.Value;
        public string UpdatedField => GetUpdatedUtcFieldPath();
        public string[] RequiredFieldNames => RequiredFields.Select(mappedField => Infer.Field(mappedField.Value)).ToArray();

        public void ApplyJsonDates(JsonNode target) => ApplyDateTracking(target);
        public PartialPatch PreparePartialPatch(PartialPatch patch) => ApplyDateTracking(patch);
        public ScriptPatch PrepareScriptPatch(ScriptPatch patch) => ApplyDateTracking(patch);

        protected override string GetUpdatedUtcFieldPath() => updatedFieldPath ?? base.GetUpdatedUtcFieldPath();
    }

    private sealed class PlainTestRepository(TestConfiguration configuration)
        : ElasticRepositoryBase<PlainDocument>(new Index<PlainDocument>(configuration, "native-field-mapping"))
    {
        public string UpdatedField => GetUpdatedUtcFieldPath();
        public ScriptPatch PrepareScriptPatch(ScriptPatch patch) => ApplyDateTracking(patch);
    }

    public class TestDocument : IIdentity, IHaveDates, ISupportSoftDeletes
    {
        [JsonPropertyName("document_id")]
        public string Id { get; set; } = null!;
        public string? Name { get; set; }
        [JsonPropertyName("created_at")]
        public DateTime CreatedUtc { get; set; }
        [JsonPropertyName("updated_at")]
        public virtual DateTime UpdatedUtc { get; set; }
        [JsonPropertyName("is_deleted")]
        public bool IsDeleted { get; set; }
    }

    private sealed class InheritedDocument : TestDocument { }

    private sealed class OverriddenDocument : TestDocument
    {
        [JsonPropertyName("overridden_updated_at")]
        public override DateTime UpdatedUtc { get; set; }
    }

    private sealed class PlainDocument : IIdentity, IHaveDates, ISupportSoftDeletes
    {
        public string Id { get; set; } = null!;
        public DateTime CreatedUtc { get; set; }
        public DateTime UpdatedUtc { get; set; }
        public bool IsDeleted { get; set; }
    }

    private sealed class AliasNamedDocument : IIdentity, IHaveDates, ISupportSoftDeletes
    {
        public string Id { get; set; } = null!;
        [JsonPropertyName("created")]
        public DateTime CreatedUtc { get; set; }
        [JsonPropertyName("updated")]
        public DateTime UpdatedUtc { get; set; }
        [JsonPropertyName("deleted")]
        public bool IsDeleted { get; set; }
    }

    private sealed class CreatedOnlyDocument : IHaveCreatedDate
    {
        [JsonPropertyName("created_at")]
        public DateTime CreatedUtc { get; set; }
    }

    private sealed class ExplicitDocument : IIdentity, IHaveDates, ISupportSoftDeletes
    {
        [JsonPropertyName("business_id")]
        public string? Id { get; set; }
        [JsonPropertyName("business_created")]
        public DateTime CreatedUtc { get; set; }
        [JsonPropertyName("business_updated")]
        public DateTime UpdatedUtc { get; set; }
        [JsonPropertyName("business_deleted")]
        public bool IsDeleted { get; set; }

        string IIdentity.Id { get; set; } = null!;
        DateTime IHaveCreatedDate.CreatedUtc { get; set; }
        DateTime IHaveDates.UpdatedUtc { get; set; }
        bool ISupportSoftDeletes.IsDeleted { get; set; }
    }
}
