using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Serialization;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Migrations;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Serialization;
using Foundatio.Repositories.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ClientPatchApplierTests
{
    [Theory]
    [InlineData(false, 0)]
    [InlineData(true, 0)]
    [InlineData(false, 140000)]
    [InlineData(true, 140000)]
    public void Apply_WithUtf8PreambleAndPooledBuffers_PreservesSourceAndPatch(bool partial, int padding)
    {
        var settings = new ElasticsearchClientSettings(new InMemoryRequestInvoker());
        var serializer = new PreambleSerializer(settings);
        var applier = new ClientPatchApplier<PartialDocument>(serializer, _ => { }, _ => { });
        string originalLabel = new string('é', padding) + " 🌍";
        var document = new PartialDocument { Label = originalLabel, Retained = originalLabel, Details = new PartialDetails { Keep = 7 }, Values = [1, 2] };
        var hit = new FindHit<PartialDocument>("one", document, 0);
        IPatchOperation patch = partial
            ? new PartialPatch(new { label = "更新 🌍" })
            : new JsonPatch(new PatchDocument(new ReplaceOperation { Path = "/label", Value = "更新 🌍" }));

        var patched = applier.Apply(hit, patch);

        Assert.NotNull(patched);
        Assert.Equal("更新 🌍", patched.Label);
        Assert.Equal(originalLabel, patched.Retained);
        Assert.Equal(7, patched.Details.Keep);
        Assert.Equal([1, 2], patched.Values);
        Assert.Equal(originalLabel, document.Label);
        Assert.Equal(partial ? 2 : 1, serializer.SerializationCount);
    }

    [Fact]
    public void Apply_WithCustomizedJsonValue_PreservesTheCapturedConverter()
    {
        using var configuration = new ElasticConfiguration();
        var options = new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };
        options.Converters.Add(new StringInt32Converter());
        var typeInfo = (JsonTypeInfo<int>)options.GetTypeInfo(typeof(int));
        var applier = new ClientPatchApplier<PartialDocument>(configuration.Client.ElasticsearchClientSettings.SourceSerializer,
            node => node["label"] = JsonValue.Create(7, typeInfo), _ => { });
        var document = new PartialDocument();
        var hit = new FindHit<PartialDocument>("one", document, 0);
        var patch = new JsonPatch(new PatchDocument(new ReplaceOperation { Path = "/details/change", Value = 2 }));

        var patched = applier.Apply(hit, patch);

        Assert.NotNull(patched);
        Assert.Equal("7", patched.Label);
        Assert.Equal(2, patched.Details.Change);
        Assert.Null(document.Label);
        Assert.Equal(0, document.Details.Change);
    }

    [Theory]
    [InlineData("version")]
    [InlineData("/version")]
    public void Apply_JsonPatchTargetsBusinessVersion_UpdatesTheProperty(string path)
    {
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new InMemoryRequestInvoker()));
        var applier = new ClientPatchApplier<MigrationState>(client.ElasticsearchClientSettings.SourceSerializer, _ => { }, _ => { });
        var document = new MigrationState { Id = "migration", Version = 1 };
        var hit = new FindHit<MigrationState>(document.Id, document, 0, "1:4");
        var patch = new JsonPatch(new PatchDocument(new ReplaceOperation { Path = path, Value = 2 }));

        var patched = applier.Apply(hit, patch);

        Assert.NotNull(patched);
        Assert.Equal(2, patched.Version);
        Assert.Equal(1, document.Version);
    }

    [Theory]
    [InlineData("version")]
    [InlineData("/version")]
    public void Apply_JsonPatchTargetsPublicConcurrencyVersion_PreservesHitMetadata(string path)
    {
        using var configuration = new ElasticConfiguration();
        var applier = new ClientPatchApplier<VersionedDocument>(configuration.Client.ElasticsearchClientSettings.SourceSerializer, _ => { }, _ => { });
        var document = new VersionedDocument { Version = "9:9" };
        var hit = new FindHit<VersionedDocument>("one", document, 0, "1:4");
        var patch = new JsonPatch(new PatchDocument(new ReplaceOperation { Path = path, Value = "2:5" }));

        var patched = applier.Apply(hit, patch);

        Assert.NotNull(patched);
        Assert.Equal("1:4", patched.Version);
        Assert.Equal("9:9", document.Version);
    }

    [Fact]
    public void Apply_WithExplicitConcurrencyVersion_PatchesPublicBusinessVersionAndPreservesMetadata()
    {
        using var configuration = new ElasticConfiguration();
        var applier = new ClientPatchApplier<ExplicitVersionDocument>(configuration.Client.ElasticsearchClientSettings.SourceSerializer, _ => { }, _ => { });
        var document = new ExplicitVersionDocument { Version = "business-1" };
        ((IVersioned)document).Version = "9:9";
        var hit = new FindHit<ExplicitVersionDocument>("one", document, 0, "1:4");
        var patch = new JsonPatch(new PatchDocument(new ReplaceOperation { Path = "/version", Value = "business-2" }));

        var patched = applier.Apply(hit, patch);

        Assert.NotNull(patched);
        Assert.Equal("business-2", patched.Version);
        Assert.Equal("1:4", ((IVersioned)patched).Version);
        Assert.Equal("business-1", document.Version);
        Assert.Equal("9:9", ((IVersioned)document).Version);
    }

    [Fact]
    public void Apply_WithRenamedConcurrencyVersion_PreservesHitMetadata()
    {
        using var configuration = new ElasticConfiguration();
        var applier = new ClientPatchApplier<RenamedVersionDocument>(configuration.Client.ElasticsearchClientSettings.SourceSerializer, _ => { }, _ => { });
        var document = new RenamedVersionDocument { Version = "9:9" };
        var hit = new FindHit<RenamedVersionDocument>("one", document, 0, "1:4");
        var patch = new JsonPatch(new PatchDocument(new ReplaceOperation { Path = "/token", Value = "2:5" }));

        var patched = applier.Apply(hit, patch);

        Assert.NotNull(patched);
        Assert.Equal("1:4", patched.Version);
        Assert.Equal("9:9", document.Version);
    }

    [Fact]
    public void Apply_PartialPatchWithNestedObjects_MergesObjectsAndReplacesArraysAndNulls()
    {
        using var configuration = new ElasticConfiguration();
        var applier = new ClientPatchApplier<PartialDocument>(configuration.Client.ElasticsearchClientSettings.SourceSerializer, _ => { }, _ => { });
        var document = new PartialDocument
        {
            Details = new PartialDetails { Keep = 7, Change = 1, Nested = new PartialDetails { Keep = 8, Change = 1 } },
            Values = [1, 2],
            Label = "clear"
        };
        var hit = new FindHit<PartialDocument>("partial", document, 0);
        var patch = new PartialPatch(new { details = new { change = 2, nested = new { change = 3 } }, values = new[] { 9 }, label = (string?)null });

        var patched = applier.Apply(hit, patch);

        Assert.NotNull(patched);
        Assert.Equal(7, patched.Details.Keep);
        Assert.Equal(2, patched.Details.Change);
        Assert.NotNull(patched.Details.Nested);
        Assert.Equal(8, patched.Details.Nested.Keep);
        Assert.Equal(3, patched.Details.Nested.Change);
        Assert.Equal([9], patched.Values);
        Assert.Null(patched.Label);
        Assert.Equal(1, document.Details.Change);
        Assert.Equal([1, 2], document.Values);
        Assert.Equal("clear", document.Label);
    }

    private sealed class VersionedDocument : IVersioned
    {
        public string? Version { get; set; }
    }

    private sealed class ExplicitVersionDocument : IVersioned
    {
        public string? Version { get; set; }
        string? IVersioned.Version { get; set; }
    }

    private sealed class RenamedVersionDocument : IVersioned
    {
        [JsonPropertyName("token")]
        public string? Version { get; set; }
    }

    private sealed class PartialDocument
    {
        public PartialDetails Details { get; set; } = new();
        public int[] Values { get; set; } = [];
        public string? Label { get; set; }
        public string? Retained { get; set; }
    }

    private sealed class PartialDetails
    {
        public int Keep { get; set; }
        public int Change { get; set; }
        public PartialDetails? Nested { get; set; }
    }

    private sealed class PreambleSerializer(IElasticsearchClientSettings settings)
        : DefaultSourceSerializer(settings, options => options.ConfigureFoundatioRepositoryDefaults())
    {
        public int SerializationCount { get; private set; }

        protected override bool SupportsFastPath(Type type) => false;

        public override void Serialize<T>(T data, Stream stream, SerializationFormatting formatting = SerializationFormatting.None)
        {
            SerializationCount++;
            stream.Write(Encoding.UTF8.Preamble);
            base.Serialize(data, stream, formatting);
        }
    }

    private sealed class StringInt32Converter : JsonConverter<int>
    {
        public override int Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return Int32.Parse(reader.GetString()!, CultureInfo.InvariantCulture);
        }

        public override void Write(Utf8JsonWriter writer, int value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value.ToString(CultureInfo.InvariantCulture));
        }
    }
}
