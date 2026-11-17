using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Get;
using Elastic.Clients.Elasticsearch.Core.MGet;
using Elastic.Clients.Elasticsearch.Core.Search;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ReadHitVersionTests
{
    [Theory]
    [InlineData("Get", 3, 42, "3:42")]
    [InlineData("Get", 1, 0, "1:0")]
    [InlineData("Get", 0, -2, null)]
    [InlineData("Search", 3, 42, "3:42")]
    [InlineData("Search", 1, 0, "1:0")]
    [InlineData("Search", 0, -2, null)]
    [InlineData("MultiGet", 3, 42, "3:42")]
    [InlineData("MultiGet", 1, 0, "1:0")]
    [InlineData("MultiGet", 0, -2, null)]
    public void ReadHit_UsesResponseMetadataAndPreservesBusinessVersion(string operation, long primaryTerm, long sequenceNumber, string? expected)
    {
        var document = new TestDocument { Version = "business-2" };
        ((IVersioned)document).Version = "9:9";

        FindHit<TestDocument>? hit = operation switch
        {
            "Get" => new GetResponse<TestDocument> { Id = "one", Index = "test", Found = true, Source = document, PrimaryTerm = primaryTerm, SeqNo = sequenceNumber }.ToFindHit(),
            "Search" => new Hit<TestDocument> { Id = "one", Index = "test", Source = document, PrimaryTerm = primaryTerm, SeqNo = sequenceNumber }.ToFindHit(),
            _ => Assert.Single(new MultiGetResponse<TestDocument>
            {
                Docs = [new MultiGetResponseItem<TestDocument>(new GetResult<TestDocument> { Id = "one", Index = "test", Found = true, Source = document, PrimaryTerm = primaryTerm, SeqNo = sequenceNumber })]
            }.ToMultiGetItems()).Hit
        };

        Assert.NotNull(hit);
        Assert.Same(document, hit.Document);
        Assert.Equal(expected, hit.Version);
        Assert.Equal(expected, ((IVersioned)document).Version);
        Assert.Equal("business-2", document.Version);
    }

    private sealed class TestDocument : IVersioned
    {
        public string? Version { get; set; }
        string? IVersioned.Version { get; set; }
    }
}
