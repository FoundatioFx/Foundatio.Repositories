using System;
using System.Text.Json.Serialization;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

public class Child : IParentChildDocument, IHaveDates, ISupportSoftDeletes
{
    public string Id { get; set; }
    public string ParentId { get; set; }

    [JsonPropertyName("discriminator")]
    public JoinField Discriminator { get; set; }

    public string ChildProperty { get; set; }
    public DateTime CreatedUtc { get; set; }
    public DateTime UpdatedUtc { get; set; }
    public bool IsDeleted { get; set; }
}

public static class ChildGenerator
{
    public static readonly string DefaultId = ObjectId.GenerateNewId().ToString();

    public static Child Default => new() { Id = DefaultId, ParentId = ParentGenerator.DefaultId };

    public static Child Generate(string id = null, string parentId = null)
    {
        return new Child { Id = id, ParentId = parentId };
    }
}
