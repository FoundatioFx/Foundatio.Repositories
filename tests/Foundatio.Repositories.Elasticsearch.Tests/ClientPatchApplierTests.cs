using System;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
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

        ClientPatchApplier<MigrationState>.ValidateOperation(patch);
        var patched = applier.Apply(hit, patch);

        Assert.NotNull(patched);
        Assert.Equal(2, patched.Version);
        Assert.Equal(1, document.Version);
    }

    [Theory]
    [InlineData("version")]
    [InlineData("/version")]
    [InlineData("/Version")]
    public void ValidateOperation_JsonPatchTargetsConcurrencyVersion_Throws(string path)
    {
        var patch = new JsonPatch(new PatchDocument(new ReplaceOperation { Path = path, Value = "2:5" }));

        Assert.Throws<ArgumentException>(() => ClientPatchApplier<VersionedDocument>.ValidateOperation(patch));
    }

    [Fact]
    public void ValidateOperation_JsonPatchTargetsNestedVersion_AllowsTheProperty()
    {
        var patch = new JsonPatch(new PatchDocument(new ReplaceOperation { Path = "/details/version", Value = "business-2" }));

        ClientPatchApplier<VersionedDocument>.ValidateOperation(patch);
    }

    private sealed class VersionedDocument : IVersioned
    {
        public string? Version { get; set; }
    }
}
