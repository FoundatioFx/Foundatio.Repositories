using System;
using System.Text.Json.Serialization;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Migrations;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ClientPatchApplierTests
{
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
    }

    private sealed class PartialDetails
    {
        public int Keep { get; set; }
        public int Change { get; set; }
        public PartialDetails? Nested { get; set; }
    }
}
